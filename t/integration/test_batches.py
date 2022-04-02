from time import sleep

from celery import signals
from celery.app.task import Task
from celery.contrib.testing.tasks import ping
from celery.result import allow_join_result

import pytest

from .tasks import add, cumadd


class SignalCounter:
    def __init__(self, expected_calls, callback=None):
        self.calls = 0
        self.expected_calls = expected_calls
        self.callback = callback

    def __call__(self, sender, **kwargs):
        if isinstance(sender, Task):
            sender_name = sender.name
        else:
            sender_name = sender

        # Ignore pings, those are used to ensure the worker processes tasks.
        if sender_name == 'celery.ping':
            return

        self.calls += 1

        # Call the "real" signal, if necessary.
        if self.callback:
            self.callback(sender, **kwargs)

    def assert_calls(self):
        assert self.calls == self.expected_calls


def _wait_for_ping(ping_task_timeout=10.0):
    """
    Wait for the celery worker to respond to a ping.

    This should ensure that any other running tasks are done.
    """
    with allow_join_result():
        assert ping.delay().get(timeout=ping_task_timeout) == 'pong'


@pytest.mark.usefixtures('depends_on_current_app')
def test_always_eager(celery_app):
    """The batch task runs immediately, in the same thread."""
    celery_app.conf.task_always_eager = True
    result = add.delay(1)

    # An EagerResult that resolve to 1 should be returned.
    assert result.get() == 1


def test_apply():
    """The batch task runs immediately, in the same thread."""
    result = add.apply(args=(1, ))

    # An EagerResult that resolve to 1 should be returned.
    assert result.get() == 1


def test_flush_interval(celery_app, celery_worker):
    """The batch task runs after the flush interval has elapsed."""

    if not celery_app.conf.broker_url.startswith('memory'):
        raise pytest.skip('Flaky on live brokers')

    result = add.delay(1)

    # The flush interval is 0.1 second, this is longer.
    sleep(0.2)

    # Let the worker work.
    _wait_for_ping()

    assert result.get() == 1


def test_flush_calls(celery_worker):
    """The batch task runs after two calls."""
    result_1 = add.delay(1)
    result_2 = add.delay(3)

    # Let the worker work.
    _wait_for_ping()

    assert result_1.get() == 4
    assert result_2.get() == 4


def test_multi_arg(celery_worker):
    """The batch task runs after two calls."""
    result_1 = add.delay(1, 2)
    result_2 = add.delay(3, 4)

    # Let the worker work.
    _wait_for_ping()

    assert result_1.get() == 10
    assert result_2.get() == 10


def test_kwarg(celery_worker):
    """The batch task runs after two calls."""
    result_1 = add.delay(a=1, b=2)
    result_2 = add.delay(a=3, b=4)

    # Let the worker work.
    _wait_for_ping()

    assert result_1.get() == 10
    assert result_2.get() == 10


def test_result(celery_worker):
    """Each task call can return a result."""
    result_1 = cumadd.delay(1)
    result_2 = cumadd.delay(2)

    # Let the worker work.
    _wait_for_ping()

    assert result_1.get(timeout=3) == 1
    assert result_2.get(timeout=3) == 3


def test_signals(celery_app, celery_worker):
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


def test_current_task(celery_app, celery_worker):
    """Ensure the current_task is properly set when running the task."""
    def signal(sender, **kwargs):
        assert celery_app.current_task.name == 't.integration.tasks.add'

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

# TODO
# * Test acking
