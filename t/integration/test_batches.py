from datetime import datetime, timedelta
from time import sleep
from typing import Any, Callable, List, Optional, Union

from celery_batches import Batches, SimpleRequest

from celery import Celery, signals, states
from celery.app.task import Task
from celery.contrib.testing.tasks import ping
from celery.contrib.testing.worker import TestWorkController
from celery.result import allow_join_result
from celery.worker.request import Request

import pytest

from .tasks import add, cumadd


class SignalCounter:
    def __init__(
        self, expected_calls: int, callback: Optional[Callable[..., None]] = None
    ):
        self.calls = 0
        self.expected_calls = expected_calls
        self.callback = callback

    def __call__(self, sender: Union[Task, str], **kwargs: Any) -> None:
        if isinstance(sender, Task):
            sender_name = sender.name
        else:
            sender_name = sender

        # Ignore pings, those are used to ensure the worker processes tasks.
        if sender_name == "celery.ping":
            return

        self.calls += 1

        # Call the "real" signal, if necessary.
        if self.callback:
            self.callback(sender, **kwargs)

    def assert_calls(self) -> None:
        assert self.calls == self.expected_calls


def _wait_for_ping(ping_task_timeout: float = 10.0) -> None:
    """
    Wait for the celery worker to respond to a ping.

    This should ensure that any other running tasks are done.
    """
    with allow_join_result():
        assert ping.delay().get(timeout=ping_task_timeout) == "pong"


@pytest.mark.usefixtures("depends_on_current_app")
def test_always_eager(celery_app: Celery) -> None:
    """The batch task runs immediately, in the same thread."""
    celery_app.conf.task_always_eager = True
    result = add.delay(1)

    # An EagerResult that resolve to 1 should be returned.
    assert result.get() == 1


def test_apply() -> None:
    """The batch task runs immediately, in the same thread."""
    result = add.apply(args=(1,))

    # An EagerResult that resolve to 1 should be returned.
    assert result.get() == 1


def test_flush_interval(celery_app: Celery, celery_worker: TestWorkController) -> None:
    """The batch task runs after the flush interval has elapsed."""

    if not celery_app.conf.broker_url.startswith("memory"):
        raise pytest.skip("Flaky on live brokers")

    result = add.delay(1)

    # The flush interval is 0.1 second, this is longer.
    sleep(0.2)

    # Let the worker work.
    _wait_for_ping()

    assert result.get() == 1


def test_flush_calls(celery_worker: TestWorkController) -> None:
    """The batch task runs after two calls."""
    result_1 = add.delay(1)
    result_2 = add.delay(3)

    # Let the worker work.
    _wait_for_ping()

    assert result_1.get() == 4
    assert result_2.get() == 4


def test_multi_arg(celery_worker: TestWorkController) -> None:
    """The batch task runs after two calls."""
    result_1 = add.delay(1, 2)
    result_2 = add.delay(3, 4)

    # Let the worker work.
    _wait_for_ping()

    assert result_1.get() == 10
    assert result_2.get() == 10


def test_kwarg(celery_worker: TestWorkController) -> None:
    """The batch task runs after two calls."""
    result_1 = add.delay(a=1, b=2)
    result_2 = add.delay(a=3, b=4)

    # Let the worker work.
    _wait_for_ping()

    assert result_1.get() == 10
    assert result_2.get() == 10


def test_result(celery_worker: TestWorkController) -> None:
    """Each task call can return a result."""
    result_1 = cumadd.delay(1)
    result_2 = cumadd.delay(2)

    # Let the worker work.
    _wait_for_ping()

    assert result_1.get(timeout=3) == 1
    assert result_2.get(timeout=3) == 3


def test_signals(celery_app: Celery, celery_worker: TestWorkController) -> None:
    """Ensure that Celery signals run for the batch task."""
    # Configure a SignalCounter for each task signal.
    checks = (
        # Each task request gets published separately.
        (signals.before_task_publish, 2),
        (signals.after_task_publish, 2),
        # The task only runs a single time.
        (signals.task_prerun, 1),
        (signals.task_postrun, 1),
        # Other task signals are not implemented.
        (signals.task_retry, 0),
        (signals.task_success, 1),
        (signals.task_received, 3),
        (signals.task_failure, 0),
        (signals.task_revoked, 0),
        (signals.task_unknown, 0),
        (signals.task_rejected, 0),
    )
    signal_counters = []
    for sig, expected_count in checks:
        counter = SignalCounter(expected_count)
        sig.connect(counter)
        signal_counters.append(counter)

    # The batch runs after 2 task calls.
    result_1 = add.delay(1)
    result_2 = add.delay(3)

    # Let the worker work.
    _wait_for_ping()

    # Should still have the correct result.
    assert result_1.get() == 4
    assert result_2.get() == 4

    for counter in signal_counters:
        counter.assert_calls()


def test_current_task(celery_app: Celery, celery_worker: TestWorkController) -> None:
    """Ensure the current_task is properly set when running the task."""

    def signal(sender: Union[Task, str], **kwargs: Any) -> None:
        assert celery_app.current_task.name == "t.integration.tasks.add"

    counter = SignalCounter(1, signal)
    signals.task_prerun.connect(counter)

    # The batch runs after 2 task calls.
    result_1 = add.delay(1)
    result_2 = add.delay(3)

    # Let the worker work.
    _wait_for_ping()

    # Should still have the correct result.
    assert result_1.get() == 4
    assert result_2.get() == 4

    counter.assert_calls()


def test_acks_early(celery_app: Celery, celery_worker: TestWorkController) -> None:
    """Ensure that acking early works properly."""
    # Setup a new task and track which Requests are acked.
    acked = []

    class AckRequest(Request):
        def acknowledge(self) -> None:
            acked.append(self.id)

    @celery_app.task(
        base=Batches, flush_every=2, flush_interval=0.1, Request=AckRequest
    )
    def acks(requests: List[SimpleRequest]) -> None:
        # The tasks are acked before running.
        assert acked == [result_1.id, result_2.id]

    # The task is acking before completion.
    assert acks.acks_late is False

    # Register the task with the worker.
    celery_worker.consumer.update_strategies()

    # Call the tasks, they should ack before flush.
    result_1 = acks.delay()
    result_2 = acks.delay()

    assert acked == []

    # Let the worker work.
    _wait_for_ping()

    # The results are stilled acked after running.
    assert acked == [result_1.id, result_2.id]


def test_acks_late(celery_app: Celery, celery_worker: TestWorkController) -> None:
    """Ensure that acking late works properly."""
    # Setup a new task and track which Requests are acked.
    acked = []

    class AckRequest(Request):
        def acknowledge(self) -> None:
            acked.append(self.id)

    @celery_app.task(
        base=Batches,
        acks_late=True,
        flush_every=2,
        flush_interval=0.1,
        Request=AckRequest,
    )
    def acks(requests: List[SimpleRequest]) -> None:
        # When the tasks are running, nothing is acked.
        assert acked == []

    # The task is acking after completion.
    assert acks.acks_late is True

    # Register the task with the worker.
    celery_worker.consumer.update_strategies()

    # Call the tasks, they should ack before flush.
    result_1 = acks.delay()
    result_2 = acks.delay()

    assert acked == []

    # Let the worker work.
    _wait_for_ping()

    # After the tasks are done, both results are acked.
    assert acked == [result_1.id, result_2.id]


def test_countdown(celery_app: Celery, celery_worker: TestWorkController) -> None:
    """Ensure that countdowns work properly.

    The batch task handles only the first request initially (as the second request
    is not ready). A subsequent call handles the second request.
    """

    if not celery_app.conf.broker_url.startswith("memory"):
        raise pytest.skip("Flaky on live brokers")

    result_1 = add.apply_async(args=(1,))
    # The countdown is longer than the flush interval + first sleep, but shorter
    # than the flush interval + first sleep + second sleep.
    result_2 = add.apply_async(args=(2,), countdown=3)

    # The flush interval is 0.1 seconds and the retry interval is 0.5 seconds,
    # this is longer.
    sleep(1)

    # Let the worker work.
    _wait_for_ping()

    assert result_1.get() == 1
    assert result_2.state == states.PENDING

    sleep(3)

    assert result_2.get() == 2


def test_eta(celery_app: Celery, celery_worker: TestWorkController) -> None:
    """Ensure that ETAs work properly."""

    if not celery_app.conf.broker_url.startswith("memory"):
        raise pytest.skip("Flaky on live brokers")

    result_1 = add.apply_async(args=(1,))
    # The countdown is longer than the flush interval + first sleep, but shorter
    # than the flush interval + first sleep + second sleep.
    result_2 = add.apply_async(args=(2,), eta=datetime.utcnow() + timedelta(seconds=3))

    # The flush interval is 0.1 seconds and the retry interval is 0.5 seconds,
    # this is longer.
    sleep(1)

    # Let the worker work.
    _wait_for_ping()

    assert result_1.get() == 1
    assert result_2.state == states.PENDING

    sleep(3)

    assert result_2.get() == 2
