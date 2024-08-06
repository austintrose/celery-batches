from typing import Any, Generator, List, Optional
from unittest.mock import DEFAULT, patch

from celery_batches import Batches, SimpleRequest
from celery_batches.trace import apply_batches_task

from celery import signals
from celery.utils.collections import AttributeDict

import pytest


class TestBatchTask(Batches):
    _request_stack: List[Any]
    _request: AttributeDict

    @property
    def request_stack(self) -> Any:
        class RequestStack:
            def __init__(self, stack: List[Any]):
                self.stack = stack

            def push(self, item: Any) -> None:
                self.stack.append(item)

            def pop(self) -> Optional[Any]:
                if self.stack:
                    return self.stack.pop()
                return None

        return RequestStack(self._request_stack)

    @property
    def request(self) -> AttributeDict:
        return self._request

    @request.setter
    def request(self, value: AttributeDict) -> None:
        self._request = value

    def run(self, *args: Any, **kwargs: Any) -> List[str]:
        requests = args[0] if args else kwargs.get("requests", [])
        result = [request.id for request in requests if hasattr(request, "id")]
        return result  # Changed from raise NoReturn to return


@pytest.fixture
def batch_task() -> TestBatchTask:
    tb = TestBatchTask()
    tb._request_stack = []
    tb._request = AttributeDict({"state": None})

    return tb


@pytest.fixture
def simple_request() -> SimpleRequest:
    return SimpleRequest(
        id="test_id",
        name="test_task",
        args=(),
        kwargs={},
        delivery_info={},
        hostname="test_host",
        ignore_result=False,
        reply_to=None,
        correlation_id=None,
        request_dict={},
    )


@pytest.fixture(autouse=True)
def setup_signal_receivers() -> Generator[None, None, None]:
    def dummy_receiver(*args: Any, **kwargs: Any) -> None:
        pass

    signals.task_prerun.connect(dummy_receiver)
    signals.task_postrun.connect(dummy_receiver)
    signals.task_success.connect(dummy_receiver)
    signals.task_failure.connect(dummy_receiver)
    signals.task_revoked.connect(dummy_receiver)

    yield

    signals.task_prerun.disconnect(dummy_receiver)
    signals.task_postrun.disconnect(dummy_receiver)
    signals.task_success.disconnect(dummy_receiver)
    signals.task_failure.disconnect(dummy_receiver)
    signals.task_revoked.disconnect(dummy_receiver)


def test_task_prerun_signal(
    batch_task: TestBatchTask, simple_request: SimpleRequest
) -> None:
    with patch("celery_batches.trace.send_prerun") as mock_send:
        apply_batches_task(batch_task, ([simple_request],), 0, None)
        mock_send.assert_called_once()


def test_task_postrun_signal(
    batch_task: TestBatchTask, simple_request: SimpleRequest
) -> None:
    with patch("celery_batches.trace.send_postrun") as mock_send:
        apply_batches_task(batch_task, ([simple_request],), 0, None)
        mock_send.assert_called_once()


def test_task_success_signal(
    batch_task: TestBatchTask, simple_request: SimpleRequest
) -> None:
    with patch("celery_batches.trace.send_success") as mock_send:
        apply_batches_task(batch_task, ([simple_request],), 0, None)
        mock_send.assert_called_once()


def test_task_failure_signal(
    batch_task: TestBatchTask, simple_request: SimpleRequest
) -> None:
    def failing_run(*args: Any, **kwargs: Any) -> None:
        raise ValueError("Test exception")

    batch_task.run = failing_run  # type: ignore

    with patch("celery_batches.trace.send_failure") as mock_send:
        apply_batches_task(batch_task, ([simple_request],), 0, None)
        mock_send.assert_called_once()


def test_task_revoked_signal(
    batch_task: TestBatchTask, simple_request: SimpleRequest
) -> None:
    def revoking_run(*args: Any, **kwargs: Any) -> List:
        batch_task.request.state = "REVOKED"
        return []  # Changed from raise NoReturn to return

    batch_task.run = revoking_run  # type: ignore

    with patch("celery_batches.trace.send_revoked") as mock_send:
        apply_batches_task(batch_task, ([simple_request],), 0, None)
        mock_send.assert_called_once()


def test_all_signals_sent(
    batch_task: TestBatchTask, simple_request: SimpleRequest
) -> None:
    with patch.multiple(
        "celery_batches.trace",
        send_prerun=DEFAULT,
        send_postrun=DEFAULT,
        send_success=DEFAULT,
    ) as mocks:
        apply_batches_task(batch_task, ([simple_request],), 0, None)
        for mock in mocks.values():
            mock.assert_called_once()


def test_failure_signals_sent(
    batch_task: TestBatchTask, simple_request: SimpleRequest
) -> None:
    def failing_run(*args: Any, **kwargs: Any) -> None:
        raise ValueError("Test exception")

    batch_task.run = failing_run  # type: ignore

    with patch.multiple(
        "celery_batches.trace",
        send_prerun=DEFAULT,
        send_postrun=DEFAULT,
        send_failure=DEFAULT,
    ) as mocks:
        apply_batches_task(batch_task, ([simple_request],), 0, None)

        for mock in mocks.values():
            mock.assert_called_once()
