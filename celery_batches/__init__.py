from itertools import count
from queue import Empty, Queue

from celery_batches.trace import apply_batches_task

from celery.app.task import Task
from celery.utils import noop
from celery.utils.imports import symbol_by_name
from celery.utils.log import get_logger
from celery.utils.nodenames import gethostname
from celery.worker.request import create_request_cls
from celery.worker.strategy import proto1_to_proto2
from kombu.utils.uuid import uuid

__all__ = ["Batches"]

logger = get_logger(__name__)


def consume_queue(queue):
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
    args = ()

    #: keyword arguments
    kwargs = {}

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

    #: TODO
    chord = None

    def __init__(
        self,
        id,
        name,
        args,
        kwargs,
        delivery_info,
        hostname,
        ignore_result,
        reply_to,
        correlation_id,
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

    @classmethod
    def from_request(cls, request):
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

    def __init__(self):
        self._buffer = Queue()
        self._count = count(1)
        self._tref = None
        self._pool = None

    def run(self, requests):
        raise NotImplementedError("must implement run(requests)")

    def Strategy(self, task, app, consumer):
        # See celery.worker.strategy.default for inspiration.
        #
        # This adds to a buffer at the end, instead of executing the task as
        # the default strategy does.
        self._pool = consumer.pool

        hostname = consumer.hostname
        connection_errors = consumer.connection_errors

        eventer = consumer.event_dispatcher

        Request = symbol_by_name(task.Request)
        Req = create_request_cls(
            Request, task, consumer.pool, hostname, eventer, app=app
        )

        timer = consumer.timer
        put_buffer = self._buffer.put
        flush_buffer = self._do_flush

        def task_message_handler(message, body, ack, reject, callbacks, **kw):
            if body is None:
                body, headers, decoded, utc = (
                    message.body,
                    message.headers,
                    False,
                    True,
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

            if self._tref is None:  # first request starts flush timer.
                self._tref = timer.call_repeatedly(self.flush_interval, flush_buffer)

            if not next(self._count) % self.flush_every:
                flush_buffer()

        return task_message_handler

    def apply(self, args=None, kwargs=None, *_args, **options):
        """
        Execute the task synchronously as a batch of size 1.

        Arguments:
            args (Tuple): positional arguments passed on to the task.
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
        )

        return super().apply(([request],), {}, *_args, **options)

    def _do_flush(self):
        logger.debug("Batches: Wake-up to flush buffer...")
        requests = None
        if self._buffer.qsize():
            requests = list(consume_queue(self._buffer))
            if requests:
                logger.debug("Batches: Buffer complete: %s", len(requests))
                self.flush(requests)
        if not requests:
            logger.debug("Batches: Canceling timer: Nothing in buffer.")
            if self._tref:
                self._tref.cancel()  # cancel timer.
            self._tref = None

    def flush(self, requests):
        acks_late = [], []
        [acks_late[r.task.acks_late].append(r) for r in requests]
        assert requests and (acks_late[True] or acks_late[False])

        # Ensure the requests can be serialized using pickle for the prefork pool.
        serializable_requests = ([SimpleRequest.from_request(r) for r in requests],)

        def on_accepted(pid, time_accepted):
            [req.acknowledge() for req in acks_late[False]]

        def on_return(result):
            [req.acknowledge() for req in acks_late[True]]

        return self._pool.apply_async(
            apply_batches_task,
            (self, serializable_requests, 0, None),
            accept_callback=on_accepted,
            callback=acks_late[True] and on_return or noop,
        )
