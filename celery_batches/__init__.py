from itertools import count, filterfalse, tee
from queue import Empty, Queue
from time import monotonic
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    NoReturn,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

from celery_batches.trace import apply_batches_task

from celery import VERSION as CELERY_VERSION
from celery import signals
from celery.app import Celery
from celery.app.task import Task
from celery.concurrency.base import BasePool
from celery.utils.imports import symbol_by_name
from celery.utils.log import get_logger
from celery.utils.nodenames import gethostname
from celery.utils.time import timezone
from celery.worker.consumer import Consumer
from celery.worker.request import Request, create_request_cls
from celery.worker.strategy import hybrid_to_proto2, proto1_to_proto2
from kombu.asynchronous.timer import Timer, to_timestamp
from kombu.message import Message
from kombu.utils.uuid import uuid
from vine import promise

__all__ = ["Batches"]

logger = get_logger(__name__)


T = TypeVar("T")


def consume_queue(queue: "Queue[T]") -> Iterable[T]:
    """Iterator yielding all immediately available items in a
    :class:`Queue.Queue`.

    The iterator stops as soon as the queue raises :exc:`Queue.Empty`.

    *Examples*

        >>> q = Queue()
        >>> map(q.put, range(4))
        >>> list(consume_queue(q))
        [0, 1, 2, 3]
        >>> list(consume_queue(q))
        []

    """
    get = queue.get_nowait
    while 1:
        try:
            yield get()
        except Empty:
            break


def partition(
    predicate: Callable[[T], bool], iterable: Iterable[T]
) -> Tuple[Iterable[T], Iterable[T]]:
    "Use a predicate to partition entries into false entries and true entries"
    t1, t2 = tee(iterable)
    return filterfalse(predicate, t1), filter(predicate, t2)


class SimpleRequest:
    """
    A request to execute a task.

    A list of :class:`~celery_batches.SimpleRequest` instances is provided to the
    batch task during execution.

    This must be pickleable (if using the prefork pool), but generally should
    have the same properties as :class:`~celery.worker.request.Request`.
    """

    #: task id
    id = None

    #: task name
    name = None

    #: positional arguments
    args: Tuple[Any, ...] = ()

    #: keyword arguments
    kwargs: Dict[Any, Any] = {}

    #: message delivery information.
    delivery_info = None

    #: worker node name
    hostname = None

    #: if the results of this request should be ignored
    ignore_result = None

    #: used by rpc backend when failures reported by parent process
    reply_to = None

    #: used similarly to reply_to
    correlation_id = None

    #: includes all of the original request headers
    request_dict: Optional[Dict[str, Any]] = {}

    #: TODO
    chord = None

    def __init__(
        self,
        id: str,
        name: str,
        args: Tuple[Any, ...],
        kwargs: Dict[Any, Any],
        delivery_info: dict,
        hostname: str,
        ignore_result: bool,
        reply_to: Optional[str],
        correlation_id: Optional[str],
        request_dict: Optional[Dict[str, Any]],
    ):
        self.id = id
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.delivery_info = delivery_info
        self.hostname = hostname
        self.ignore_result = ignore_result
        self.reply_to = reply_to
        self.correlation_id = correlation_id
        self.request_dict = request_dict

    @classmethod
    def from_request(cls, request: Request) -> "SimpleRequest":
        # Support both protocol v1 and v2.
        args, kwargs, embed = request._payload
        # Celery 5.1.0 added an ignore_result option.
        ignore_result = getattr(request, "ignore_result", False)
        return cls(
            request.id,
            request.name,
            args,
            kwargs,
            request.delivery_info,
            request.hostname,
            ignore_result,
            request.reply_to,
            request.correlation_id,
            request.request_dict,
        )


class Batches(Task):
    abstract = True

    # Disable typing since the signature of batch tasks take only a single item
    # (the list of SimpleRequest objects), but when calling it it should be
    # possible to provide more arguments.
    #
    # This unfortunately pushes more onto the user to ensure that each call to
    # a batch task is using the expected signature.
    typing = False

    #: Maximum number of message in buffer.
    flush_every = 10

    #: Timeout in seconds before buffer is flushed anyway.
    flush_interval = 30

    def __init__(self) -> None:
        self._buffer: Queue[Request] = Queue()
        self._pending: Queue[Request] = Queue()
        self._count = count(1)
        self._tref: Optional[Timer] = None
        self._pool: BasePool = None

    def run(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("must implement run(requests)")

    def Strategy(self, task: "Batches", app: Celery, consumer: Consumer) -> Callable:
        # See celery.worker.strategy.default for inspiration.
        #
        # This adds to a buffer at the end, instead of executing the task as
        # the default strategy does.
        #
        # See Batches._do_flush for ETA handling.
        self._pool = consumer.pool

        hostname = consumer.hostname
        connection_errors = consumer.connection_errors

        eventer = consumer.event_dispatcher
        events = eventer and eventer.enabled
        send_event = eventer and eventer.send
        task_sends_events = events and task.send_events

        Request = symbol_by_name(task.Request)
        # Celery 5.1 added the app argument to create_request_cls.
        if CELERY_VERSION < (5, 1):
            Req = create_request_cls(Request, task, consumer.pool, hostname, eventer)
        else:
            Req = create_request_cls(
                Request, task, consumer.pool, hostname, eventer, app=app
            )

        timer = consumer.timer
        put_buffer = self._buffer.put
        flush_buffer = self._do_flush

        def task_message_handler(
            message: Message,
            body: Optional[Dict[str, Any]],
            ack: promise,
            reject: promise,
            callbacks: Set,
            **kw: Any,
        ) -> None:
            if body is None and "args" not in message.payload:
                body, headers, decoded, utc = (
                    message.body,
                    message.headers,
                    False,
                    app.uses_utc_timezone(),
                )
            else:
                if "args" in message.payload:
                    body, headers, decoded, utc = hybrid_to_proto2(
                        message, message.payload
                    )
                else:
                    body, headers, decoded, utc = proto1_to_proto2(message, body)

            request = Req(
                message,
                on_ack=ack,
                on_reject=reject,
                app=app,
                hostname=hostname,
                eventer=eventer,
                task=task,
                body=body,
                headers=headers,
                decoded=decoded,
                utc=utc,
                connection_errors=connection_errors,
            )
            put_buffer(request)

            signals.task_received.send(sender=consumer, request=request)
            if task_sends_events:
                send_event(
                    "task-received",
                    uuid=request.id,
                    name=request.name,
                    args=request.argsrepr,
                    kwargs=request.kwargsrepr,
                    root_id=request.root_id,
                    parent_id=request.parent_id,
                    retries=request.request_dict.get("retries", 0),
                    eta=request.eta and request.eta.isoformat(),
                    expires=request.expires and request.expires.isoformat(),
                )

            if self._tref is None:  # first request starts flush timer.
                self._tref = timer.call_repeatedly(self.flush_interval, flush_buffer)

            if not next(self._count) % self.flush_every:
                flush_buffer()

        return task_message_handler

    def apply(
        self,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[dict] = None,
        *_args: Any,
        **options: Any,
    ) -> Any:
        """
        Execute the task synchronously as a batch of size 1.

        Arguments:
            args: positional arguments passed on to the task.
        Returns:
            celery.result.EagerResult: pre-evaluated result.
        """
        request = SimpleRequest(
            id=options.get("task_id", uuid()),
            name="batch request",
            args=args or (),
            kwargs=kwargs or {},
            delivery_info={
                "is_eager": True,
                "exchange": options.get("exchange"),
                "routing_key": options.get("routing_key"),
                "priority": options.get("priority"),
            },
            hostname=gethostname(),
            ignore_result=options.get("ignore_result", False),
            reply_to=None,
            correlation_id=None,
            request_dict={},
        )

        return super().apply(([request],), {}, *_args, **options)

    def _do_flush(self) -> None:
        logger.debug("Batches: Wake-up to flush buffers...")

        ready_requests = []
        app = self.app
        to_system_tz = timezone.to_system
        now = monotonic()

        all_requests = list(consume_queue(self._buffer)) + list(
            consume_queue(self._pending)
        )
        for req in all_requests:
            # Similar to logic in celery.worker.strategy.default.
            if req.eta:
                try:
                    if req.utc:
                        eta = to_timestamp(to_system_tz(req.eta))
                    else:
                        eta = to_timestamp(req.eta, app.timezone)
                except (OverflowError, ValueError) as exc:
                    logger.error(
                        "Couldn't convert ETA %r to timestamp: %r. Task: %r",
                        req.eta,
                        exc,
                        req.info(safe=True),
                        exc_info=True,
                    )
                    req.reject(requeue=False)
                    continue

                if eta <= now:
                    # ETA has elapsed, request is ready.
                    ready_requests.append(req)
                else:
                    # ETA has not elapsed, add to pending queue.
                    self._pending.put(req)
            else:
                # Request does not have an ETA, ready immediately
                ready_requests.append(req)

        if len(ready_requests) > 0:
            logger.debug("Batches: Ready buffer complete: %s", len(ready_requests))
            self.flush(ready_requests)

        if not ready_requests and self._pending.qsize() == 0:
            logger.debug("Batches: Canceling timer: Nothing in buffers.")
            if self._tref:
                self._tref.cancel()  # cancel timer.
            self._tref = None

    def flush(self, requests: Collection[Request]) -> Any:
        acks_early, acks_late = partition(lambda r: bool(r.task.acks_late), requests)

        # Ensure the requests can be serialized using pickle for the prefork pool.
        serializable_requests = ([SimpleRequest.from_request(r) for r in requests],)

        def on_accepted(pid: int, time_accepted: float) -> None:
            for req in acks_early:
                req.acknowledge()
            for request in requests:
                request.send_event("task-started")

        def on_return(result: Optional[Any]) -> None:
            for req in acks_late:
                req.acknowledge()
            for request in requests:
                runtime = 0
                if isinstance(result, int):
                    runtime = result
                request.send_event("task-succeeded", result=None, runtime=runtime)

        return self._pool.apply_async(
            apply_batches_task,
            (self, serializable_requests, 0, None),
            accept_callback=on_accepted,
            callback=on_return,
        )
