"""Trace task execution.

This module defines how the task execution is traced:
errors are recorded, handlers are applied and so on.

Mimics some of the functionality found in celery.app.trace.trace_task.
"""

from typing import TYPE_CHECKING, Any, List, Tuple, Union

from celery import signals, states
from celery._state import _task_stack
from celery.app.task import Context
from celery.utils.log import get_logger
from kombu.utils.uuid import uuid

if TYPE_CHECKING:
    from celery_batches import Batches, SimpleRequest

logger = get_logger(__name__)

send_prerun = signals.task_prerun.send
send_postrun = signals.task_postrun.send
send_success = signals.task_success.send
send_failure = signals.task_failure.send
send_revoked = signals.task_revoked.send
SUCCESS = states.SUCCESS
FAILURE = states.FAILURE
REVOKED = states.REVOKED


def apply_batches_task(
    task: "Batches", args: Tuple[List["SimpleRequest"]], loglevel: int, logfile: None
) -> Any:
    request_stack = task.request_stack
    push_request = request_stack.push
    pop_request = request_stack.pop
    push_task = _task_stack.push
    pop_task = _task_stack.pop

    prerun_receivers = signals.task_prerun.receivers
    postrun_receivers = signals.task_postrun.receivers
    success_receivers = signals.task_success.receivers
    failure_receivers = signals.task_failure.receivers
    revoked_receivers = signals.task_revoked.receivers

    logger.debug(f"Debug: prerun_receivers: {prerun_receivers}")
    logger.debug(f"Debug: postrun_receivers: {postrun_receivers}")
    logger.debug(f"Debug: success_receivers: {success_receivers}")
    logger.debug(f"Debug: failure_receivers: {failure_receivers}")
    logger.debug(f"Debug: revoked_receivers: {revoked_receivers}")

    # Corresponds to multiple requests, so generate a new UUID.
    task_id = uuid()

    push_task(task)
    task_request = Context(loglevel=loglevel, logfile=logfile)
    push_request(task_request)

    result: Union[Any, Exception]
    state: str

    try:
        # -*- PRE -*-
        if prerun_receivers:
            logger.debug("Debug: Sending prerun signal")
            send_prerun(sender=task, task_id=task_id, task=task, args=args, kwargs={})

        # -*- TRACE -*-
        try:
            result = task(*args)
            state = (
                REVOKED
                if (hasattr(task.request, "state") and task.request.state == REVOKED)
                else SUCCESS
            )
        except Exception as exc:
            result = exc
            state = FAILURE
            logger.error("Error: %r", exc, exc_info=True)
            if failure_receivers:
                logger.debug("Debug: Sending failure signal")
                send_failure(
                    sender=task,
                    task_id=task_id,
                    exception=exc,
                    args=args,
                    kwargs={},
                    einfo=None,
                )

        # Handle signals based on the state
        if state == REVOKED and revoked_receivers:
            logger.debug("Debug: Sending revoked signal")
            send_revoked(
                sender=task,
                request=task_request,
                terminated=True,
                signum=None,
                expired=False,
            )
        elif state == SUCCESS and success_receivers:
            logger.debug("Debug: Sending success signal")
            send_success(sender=task, result=result)

    finally:
        try:
            if postrun_receivers:
                logger.debug("Debug: Sending postrun signal")
                send_postrun(
                    sender=task,
                    task_id=task_id,
                    task=task,
                    args=args,
                    kwargs={},
                    retval=result,
                    state=state,
                )
        finally:
            pop_task()
            pop_request()

    return result
