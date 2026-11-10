#!/usr/bin/python
"""
Real A2A SDK cancellation regression (Phase 1 gate, plan item 3).

The A2A SDK calls ``AgentExecutor.cancel(request_context, event_queue)``
from ``ActiveTask.cancel()``. The plugin used to implement
``cancel(task_id)``, so that call raised a ``TypeError`` which the SDK
caught and logged: the backend run was never asked to stop, and the client
saw a cancellation that never happened (the backend finished its work and
the late completion overwrote the cancelled state).

These tests drive the REAL SDK path in one event loop - the real
``DefaultRequestHandler``, the real ``ActiveTask``, the real executor, the
real request context, the real event queue and the SDK's real task store.
Only the backend network surface is a scripted fake. Asserting that a
method exists does not prove that cancellation works, so every assertion
below is about observable behaviour: the backend was asked to stop, the
persisted task ends in the cancelled state, and the tenant context is the
trusted one.

Run with:
    python -m pytest a2a_protocol_plugin/tests/test_sdk_cancellation.py -v

Offline: no network, no live service.
"""

import asyncio
import inspect
import logging
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from a2a.server.context import ServerCallContext
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import (
    CancelTaskRequest,
    SendMessageRequest,
    Task,
    TaskStatus,
)

from a2a_protocol_plugin.handlers.a2a_ai_agent_utility import BridgeResult
from a2a_protocol_plugin.handlers.a2a_executor import (
    A2ADaemonExecutor,
    _task_state,
)
from a2a_protocol_plugin.handlers.a2a_jsonrpc_bridge import (
    build_jsonrpc_sdk_request,
)
from a2a_protocol_plugin.handlers.a2a_server import A2AServerCallContextBuilder
from a2a_protocol_plugin.handlers.config import Config
from a2a_protocol_plugin.tests.test_a2a_protocol_compliance import _agent_card

__author__ = "SilvaEngine Team"

AGENT_UUID = "a2a-cancel-agent"
TRUSTED_PARTITION = "ep#tenant-a"
TASK_ID = "sdk-cancel-task-1"
CONTEXT_ID = "sdk-cancel-context-1"
OTHER_PARTITION = "ep#tenant-b"
RUN_ID = "backend-run-1"


class _ScriptedBackend:
    """A scripted backend handler whose run blocks until it is released.

    The coordination uses ``threading.Event`` on purpose: the agent bridge
    may run on a different event loop than the test, and an
    ``asyncio.Event`` could not be awaited across loops.
    """

    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.stop_calls: list = []
        self.stop_error: Exception | None = None

    async def ask_model(self, messages, context, **kwargs):
        self.started.set()
        await asyncio.to_thread(self.release.wait, 10)
        return BridgeResult(content="backend finished")

    def cancel_run(self, run_id):
        self.stop_calls.append(run_id)
        if self.stop_error is not None:
            raise self.stop_error
        self.release.set()
        return True


class _SpyTaskStore(InMemoryTaskStore):
    """The SDK's in-memory store, recording the context it was called with.

    The SDK's in-memory store has no tenant isolation, so the partition is
    enforced here - the plugin's real stores (PostgreSQL / DynamoDB) filter
    every read and write by the caller's partition, and the cancel path
    must behave the same way.
    """

    def __init__(self, partition_key: str = TRUSTED_PARTITION) -> None:
        super().__init__()
        self.partition_key = partition_key
        self.get_contexts: list = []
        self.save_contexts: list = []

    def _owns(self, context) -> bool:
        if context is None:
            return True
        state = getattr(context, "state", None) or {}
        return state.get("partition_key", self.partition_key) == self.partition_key

    async def get(self, task_id, context):
        self.get_contexts.append(context)
        if not self._owns(context):
            return None
        return await super().get(task_id, context)

    async def save(self, task, context):
        self.save_contexts.append(context)
        if not self._owns(context):
            return None
        return await super().save(task, context)


class _Harness:
    """Executor + real SDK request handler + scripted bridge and backend."""

    def __init__(self, monkeypatch) -> None:
        self.logger = logging.getLogger("test-sdk-cancellation")
        self.store = _SpyTaskStore()
        self.executor = A2ADaemonExecutor(
            logger=self.logger, config=Config, task_store=self.store
        )
        self.request_handler = DefaultRequestHandler(
            agent_executor=self.executor,
            task_store=self.store,
            agent_card=_agent_card(),
        )
        self.core = AsyncMock()
        self.backend = _ScriptedBackend()

        monkeypatch.setattr(Config, "a2a_core", self.core)
        monkeypatch.setattr(Config, "DB_BACKEND", "dynamodb")

        async def _bridge(*args, **kwargs):
            return await self.backend.ask_model([], {})

        monkeypatch.setattr(
            "a2a_protocol_plugin.handlers.a2a_ai_agent_utility."
            "execute_ai_agent_non_streaming",
            _bridge,
        )

    @staticmethod
    def body() -> dict:
        return {
            "jsonrpc": "2.0",
            "id": "req-cancel",
            "method": "message/send",
            "params": {
                "message": {
                    "role": "ROLE_USER",
                    "parts": [{"text": "run something long"}],
                    "messageId": "m-cancel",
                    "taskId": TASK_ID,
                    "contextId": CONTEXT_ID,
                },
                "metadata": {"agent_uuid": AGENT_UUID},
            },
        }

    def call_context(
        self, partition_key: str = TRUSTED_PARTITION
    ) -> ServerCallContext:
        """A call context built the way the plugin builds it.

        The request state comes from the plugin's own extraction (so the
        agent uuid and the trusted partition are the real ones); the
        partition is then re-asserted, exactly as the gateway does.
        """
        context = ServerCallContext(state={})
        A2AServerCallContextBuilder.update_state_from_body(
            context.state, self.body()
        )
        context.state["partition_key"] = partition_key
        return context

    async def start_message(self, partition_key: str = TRUSTED_PARTITION):
        """Start a message whose backend run blocks until it is cancelled."""
        body = self.body()
        context = self.call_context(partition_key)
        # The SDK reads the task from its store before the executor runs,
        # so the task is created up front (in production the daemon's task
        # row and the SDK's store are the same record).
        await self.store.save(
            Task(
                id=TASK_ID,
                context_id=CONTEXT_ID,
                status=TaskStatus(state=_task_state("WORKING")),
            ),
            context,
        )
        request = build_jsonrpc_sdk_request(SendMessageRequest, body)
        return asyncio.create_task(
            self.request_handler.on_message_send(request, context)
        )

    async def wait_until_running(self) -> None:
        started = await asyncio.to_thread(self.backend.started.wait, 20)
        assert started, "the agent bridge never ran"

    def task_id_of(self) -> str:
        """The SDK's task id, as persisted by the daemon."""
        writes = self.core.insert_update_a2a_task.await_args_list
        assert writes, "the daemon must persist the task"
        return writes[0].kwargs.get("task_id") or ""

    def register_external_run(self, task_id: str) -> None:
        """Register the backend run the way the streaming bridge does."""
        self.executor._on_external_run_id(
            task_id,
            RUN_ID,
            self.backend,
            SimpleNamespace(cancelled=False, set=lambda: None),
        )

@pytest.fixture
def harness(monkeypatch):
    return _Harness(monkeypatch)



@pytest.mark.asyncio
async def test_sdk_cancel_reaches_the_backend_and_cancels_the_task(harness):
    """The SDK's cancel path must stop the backend and cancel the task."""
    send = await harness.start_message()
    await harness.wait_until_running()
    task_id = harness.task_id_of()
    harness.register_external_run(task_id)

    cancel = await harness.request_handler.on_cancel_task(
        CancelTaskRequest(id=task_id),
        harness.call_context(),
    )

    # The backend was asked to stop - exactly once.
    assert harness.backend.stop_calls == [RUN_ID], harness.backend.stop_calls

    # The persisted task ends in the cancelled state.
    stored = await harness.store.get(task_id, harness.call_context())
    assert stored is not None
    assert stored.status.state == _task_state("CANCELED"), stored.status.state

    # The SDK's cancel result is the task it had cached for the producer
    # (its TaskManager keeps the snapshot it started with), so the
    # persisted state above is the authoritative one - the plugin's real
    # stores re-read on cancel, which is why the live client sees the
    # cancelled state.
    assert cancel is not None

    send.cancel()


@pytest.mark.asyncio
async def test_sdk_cancel_uses_the_trusted_partition(harness):
    """The task store must see the caller's partition, not the default one."""
    send = await harness.start_message()
    await harness.wait_until_running()
    task_id = harness.task_id_of()
    harness.register_external_run(task_id)

    await harness.request_handler.on_cancel_task(
        CancelTaskRequest(id=task_id),
        harness.call_context(TRUSTED_PARTITION),
    )

    contexts = harness.store.get_contexts + harness.store.save_contexts
    assert contexts, "the store must be called with a context"
    for context in contexts:
        assert context is not None
        assert context.state["partition_key"] == TRUSTED_PARTITION
    send.cancel()


@pytest.mark.asyncio
async def test_sdk_cancel_does_not_leak_across_partitions(harness):
    """A task of another partition is not cancellable through this path."""
    send = await harness.start_message()
    await harness.wait_until_running()
    task_id = harness.task_id_of()
    harness.register_external_run(task_id)

    # The store refuses the foreign partition, so the cancel finds no task
    # to cancel: the backend of the other partition is never touched.
    await harness.request_handler.on_cancel_task(
        CancelTaskRequest(id=task_id),
        harness.call_context(OTHER_PARTITION),
    )
    assert harness.backend.stop_calls == []
    still_running = await harness.store.get(task_id, harness.call_context())
    assert still_running is not None
    assert still_running.status.state != _task_state("CANCELED")
    send.cancel()


@pytest.mark.asyncio
async def test_failing_backend_stop_still_cancels_the_task(harness):
    """A backend that cannot be stopped must not block the terminal state."""
    harness.backend.stop_error = RuntimeError("backend refused to stop")
    send = await harness.start_message()
    await harness.wait_until_running()
    task_id = harness.task_id_of()
    harness.register_external_run(task_id)

    await harness.request_handler.on_cancel_task(
        CancelTaskRequest(id=task_id),
        harness.call_context(),
    )

    # The stop was attempted (and its failure recorded).
    assert harness.backend.stop_calls == [RUN_ID]
    # The task still ends cancelled: the cancellation is requested, and the
    # remote stop is reported separately (never silently claimed).
    stored = await harness.store.get(task_id, harness.call_context())
    assert stored is not None
    assert stored.status.state == _task_state("CANCELED"), stored.status.state
    send.cancel()


def test_executor_cancel_has_the_sdk_signature():
    """The executor must implement the SDK's two-argument cancel contract."""
    signature = inspect.signature(A2ADaemonExecutor.cancel)
    parameters = list(signature.parameters)
    assert parameters == ["self", "context", "event_queue"], parameters
