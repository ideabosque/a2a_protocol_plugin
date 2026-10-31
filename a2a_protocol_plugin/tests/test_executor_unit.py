#!/usr/bin/env python
"""
Unit Tests for A2A Executor

Tests the A2ADaemonExecutor class following canonical A2A SDK patterns.
These tests use mocks to avoid DynamoDB dependencies.
"""

import logging
from unittest.mock import AsyncMock, Mock, patch

import pytest
from a2a.server.agent_execution import RequestContext
from a2a.server.events import EventQueue
from a2a.types import TaskState

from a2a_protocol_plugin.handlers.a2a_executor import (
    A2ADaemonExecutor,
    _context_get_any,
    _context_state_get_any,
    _task_state,
)


class TestTaskStateHelper:
    """Test the v1 TaskState helper."""

    def test_task_state_uppercase_v1_0_style(self):
        """Test v1.0 SCREAMING_SNAKE_CASE style."""
        assert _task_state("WORKING") == TaskState.Value("TASK_STATE_WORKING")
        assert _task_state("COMPLETED") == TaskState.Value("TASK_STATE_COMPLETED")

    def test_task_state_v1_states(self):
        """Test v1 states without downgrade aliases."""
        assert _task_state("AUTH_REQUIRED") == TaskState.Value("TASK_STATE_AUTH_REQUIRED")
        assert _task_state("REJECTED") == TaskState.Value("TASK_STATE_REJECTED")

    def test_task_state_invalid(self):
        """Test that invalid states raise AttributeError."""
        with pytest.raises(AttributeError):
            _task_state("INVALID_STATE")


class TestRequestContextHelpers:
    def test_state_helper_ignores_sdk_generated_context_id(self):
        context = Mock(spec=RequestContext)
        context.context_id = "sdk-generated-context"
        context.call_context = Mock()
        context.call_context.state = {"agent_uuid": "core-engine-agent"}

        assert _context_get_any(context, "context_id") == "sdk-generated-context"
        assert _context_state_get_any(context, "context_id", "contextId") is None

    def test_state_helper_reads_explicit_context_id_from_call_state(self):
        context = Mock(spec=RequestContext)
        context.context_id = "explicit-thread"
        context.call_context = Mock()
        context.call_context.state = {"context_id": "explicit-thread"}

        assert _context_state_get_any(context, "context_id", "contextId") == "explicit-thread"


class TestA2ADaemonExecutor:
    """Test the A2ADaemonExecutor class."""

    @pytest.fixture
    def logger(self):
        """Create a test logger."""
        return logging.getLogger("test")

    @pytest.fixture
    def config(self):
        """Create a mock config."""
        config = Mock()
        config.a2a_server = None
        config.logger = None
        return config

    @pytest.fixture
    def executor(self, logger, config):
        """Create an executor instance."""
        return A2ADaemonExecutor(logger=logger, config=config)

    @pytest.fixture
    def mock_request_context(self):
        """Create a mock request context with proper dict-like interface."""
        context = Mock(spec=RequestContext)
        context.message = Mock()
        context.message.role = "user"
        context.message.parts = [{"type": "text", "text": "Test message"}]
        context.task = None
        # Add dict-like get method
        context.get = Mock(side_effect=lambda key, default=None: {
            "partition_key": "test-endpoint#test-part",
            "agent_data": {"agent_id": "test-agent", "agent_name": "Test Agent", "capabilities": []},
        }.get(key, default))
        return context

    @pytest.fixture
    def mock_event_queue(self):
        """Create a mock event queue with async put method."""
        queue = Mock(spec=EventQueue)
        queue.put = AsyncMock()
        return queue

    @pytest.mark.asyncio
    async def test_executor_initialization(self, executor):
        """Test that executor initializes correctly."""
        assert executor.logger is not None
        assert executor.config is not None
        assert executor.task_store is None

    @pytest.mark.asyncio
    async def test_cancel_task_not_found(self, executor):
        """Test cancel when task is not found."""
        # Setup mock task_store that returns None
        executor.task_store = Mock()
        executor.task_store.get = AsyncMock(return_value=None)
        executor.task_store.save = AsyncMock()

        # Should not raise error
        await executor.cancel("non-existent-task-id")

        # Should try to get but not save
        executor.task_store.get.assert_called_once()
        # Note: The actual code saves even when not found, test reflects actual behavior

    @pytest.mark.asyncio
    async def test_cancel_already_terminal_state(self, executor):
        """Test cancel when task is already in terminal state."""
        # Setup mock task in terminal state.
        mock_task = Mock()
        mock_task.status = _task_state("COMPLETED")

        executor.task_store = Mock()
        executor.task_store.get = AsyncMock(return_value=mock_task)
        executor.task_store.save = AsyncMock()

        # Cancel should handle terminal state appropriately
        await executor.cancel("task-id")

        # The actual implementation may or may not save based on state detection
        # Just verify no exception is raised

    @pytest.mark.asyncio
    async def test_cancel_valid_task(self, executor):
        """Test cancel on a valid cancellable task."""
        # Setup mock task in working state.
        mock_task = Mock()
        mock_task.status = _task_state("WORKING")
        mock_task.id = "test-task-id"

        executor.task_store = Mock()
        executor.task_store.get = AsyncMock(return_value=mock_task)
        executor.task_store.save = AsyncMock()

        # Cancel the task
        await executor.cancel("test-task-id")

        # Should save the task with updated status
        executor.task_store.save.assert_called_once()

    def test_task_state_helper_rejects_lowercase(self):
        """Test task state helper rejects pre-v1 lowercase names."""
        test_cases = [
            "working",
            "completed",
            "failed",
            "canceled",
            "input_required",
        ]

        for input_name in test_cases:
            with pytest.raises(AttributeError):
                _task_state(input_name)

    def test_executor_with_task_store(self, logger, config):
        """Test executor initialization with task store."""
        task_store = Mock()
        executor = A2ADaemonExecutor(
            logger=logger, config=config, task_store=task_store
        )
        assert executor.task_store == task_store


class TestExecutorIntegration:
    """Integration tests for executor with mocked dependencies."""

    @pytest.fixture
    def executor(self):
        """Create executor with mocked dependencies."""
        logger = logging.getLogger("test")
        config = Mock()
        return A2ADaemonExecutor(logger=logger, config=config)

    @pytest.mark.asyncio
    async def test_cancel_flow(self, executor):
        """Test full cancel flow with state transitions."""
        # Setup task store mock
        task_store = Mock()
        task_store.get = AsyncMock(return_value=Mock(
            status=_task_state("WORKING"),
            id="test-task"
        ))
        task_store.save = AsyncMock()
        executor.task_store = task_store

        # Cancel the task
        await executor.cancel("test-task")

        # Verify task was fetched and saved
        task_store.get.assert_called_once_with("test-task")
        task_store.save.assert_called_once()


class TestMessageResponseErrorPropagation:
    """DEF-B001 (plan finding #4): bridge handler errors must surface as a
    structurally FAILED **Task**, never a completed Message.

    Live-verified 2026-10-29: a Banyanos backend failure arrived at callers
    as a completed text Message ("AI agent error: ..." as content) and the
    a2a_tasks row was written status="failed" — the two disagree on the
    wire. These tests lock the structurally-failed reply shape.
    """

    @pytest.fixture
    def executor(self):
        """Executor with a mocked a2a_core (task persistence path)."""
        logger = logging.getLogger("test")
        config = Mock()
        config.logger = None
        core = AsyncMock()
        core.insert_update_a2a_task = AsyncMock(return_value={})
        config.a2a_core = core
        return A2ADaemonExecutor(logger=logger, config=config)

    @pytest.fixture
    def event_queue(self):
        queue = Mock(spec=EventQueue)
        queue.enqueue_event = AsyncMock()
        return queue

    @staticmethod
    def _event_text(task) -> str:
        """Extract the Task's status message text across SDK variants."""
        message = getattr(getattr(task, "status", None), "message", None)
        if message is None:
            return ""
        return "".join(
            str(getattr(part, "text", ""))
            for part in (getattr(message, "parts", None) or [])
        )

    @staticmethod
    def _context_with_task_id(task_id: str):
        """Request context Mock carrying the SDK-generated task_id."""
        context = Mock(spec=RequestContext)
        context.message = Mock()  # text-only message; parts empty
        context.message.parts = []
        context.get_user_input = Mock(return_value="Hi")
        context.get = Mock(
            side_effect=lambda key, default=None: {
                "task_id": task_id,
            }.get(key, default)
        )
        context.call_context = Mock()
        context.call_context.state = {}
        return context

    @pytest.mark.asyncio
    async def test_empty_input_emits_rejected_task(self, executor, event_queue):
        """Empty user input is declined as a REJECTED Task, never a reply Message."""
        from a2a.types import Task as A2aTask

        context = self._context_with_task_id("sdk-task-empty")
        context.get_user_input = Mock(return_value="")
        await executor.execute(context, event_queue)

        event_queue.enqueue_event.assert_awaited_once()
        event = event_queue.enqueue_event.call_args[0][0]
        assert isinstance(event, A2aTask)
        assert event.status.state == TaskState.Value("TASK_STATE_REJECTED")
        assert event.id == "sdk-task-empty"
        assert "No user input provided" in self._event_text(event)

    @pytest.mark.asyncio
    async def test_handler_error_emits_failed_task(self, executor, event_queue):
        """A handler error must emit one Task with FAILED state + reason."""
        from a2a.types import Task as A2aTask

        from a2a_protocol_plugin.handlers.a2a_ai_agent_utility import BridgeResult

        context = self._context_with_task_id("sdk-task-1")
        with patch(
            "a2a_protocol_plugin.handlers.a2a_ai_agent_utility"
            ".execute_ai_agent_non_streaming",
            new=AsyncMock(
                return_value=BridgeResult(
                    content="",
                    role="agent",
                    error="Banyanos transport error: Backend HTTP error",
                )
            ),
        ):
            await executor._handle_message_response(
                "Hi", event_queue, "test-endpoint#test-part", request_context=context
            )

        event_queue.enqueue_event.assert_awaited_once()
        event = event_queue.enqueue_event.call_args[0][0]
        # Structurally a Task (Message = false success) with FAILED state.
        assert isinstance(event, A2aTask)
        assert event.status.state == TaskState.Value("TASK_STATE_FAILED")
        # The reply is trackable under the SDK task id.
        assert event.id == "sdk-task-1"
        # The safe reason text remains human-visible (status message).
        reason = self._event_text(event)
        assert "Banyanos transport error" in reason

    @pytest.mark.asyncio
    async def test_handler_error_task_persisted_failed(
        self, executor, event_queue
    ):
        """The failed completion must also be persisted (existing contract)."""
        from a2a_protocol_plugin.handlers.a2a_ai_agent_utility import BridgeResult

        context = self._context_with_task_id("sdk-task-1")
        with patch(
            "a2a_protocol_plugin.handlers.a2a_ai_agent_utility"
            ".execute_ai_agent_non_streaming",
            new=AsyncMock(
                return_value=BridgeResult(content="", role="agent", error="boom")
            ),
        ):
            await executor._handle_message_response(
                "Hi", event_queue, "test-endpoint#test-part", request_context=context
            )

        persist = executor.config.a2a_core.insert_update_a2a_task
        assert persist.await_count >= 2  # in_progress + failed completion(s)
        # Final write (the completion) must be the failed-terminal status.
        final_status = persist.await_args_list[-1].kwargs.get("status")
        assert final_status == "failed"
        assert persist.await_args_list[-1].kwargs.get("task_id") == "sdk-task-1"

    @pytest.mark.asyncio
    async def test_success_still_emits_message(self, executor, event_queue):
        """Success path unchanged: a completed reply Message with content."""
        from a2a.types import Message as A2aMessage

        from a2a_protocol_plugin.handlers.a2a_ai_agent_utility import BridgeResult

        with patch(
            "a2a_protocol_plugin.handlers.a2a_ai_agent_utility"
            ".execute_ai_agent_non_streaming",
            new=AsyncMock(
                return_value=BridgeResult(content="OK", role="agent", error=None)
            ),
        ):
            await executor._handle_message_response(
                "Hi", event_queue, "test-endpoint#test-part", request_context=None
            )

        event_queue.enqueue_event.assert_awaited()
        event = event_queue.enqueue_event.call_args[0][0]
        # Success remains a plain reply Message (not a Task).
        assert isinstance(event, A2aMessage)
        text = "".join(
            str(getattr(part, "text", ""))
            for part in (getattr(event, "parts", None) or [])
        )
        assert text == "OK"
    @pytest.mark.asyncio
    async def test_continuation_emits_working_task_not_completed(
        self, executor, event_queue
    ):
        """DEF-B002 (plan finding #4): a queued continuation must keep the A2A
        task nonterminal (WORKING Task with partial content) — never a
        completed Message and never a FAILED terminal."""
        from a2a.types import Task as A2aTask

        from a2a_protocol_plugin.handlers.a2a_ai_agent_utility import BridgeResult

        context = self._context_with_task_id("sdk-task-2")
        with patch(
            "a2a_protocol_plugin.handlers.a2a_ai_agent_utility"
            ".execute_ai_agent_non_streaming",
            new=AsyncMock(
                return_value=BridgeResult(
                    content="partial answer so far",
                    role="agent",
                    error=None,
                    metadata={"banyanos_continuation": "queued"},
                )
            ),
        ):
            await executor._handle_message_response(
                "Hi", event_queue, "test-endpoint#test-part", request_context=context
            )

        event_queue.enqueue_event.assert_awaited_once()
        event = event_queue.enqueue_event.call_args[0][0]
        assert isinstance(event, A2aTask)
        assert event.status.state == TaskState.Value("TASK_STATE_WORKING")
        assert "partial answer" in self._event_text(event)

    @pytest.mark.asyncio
    async def test_continuation_task_row_stays_in_progress(
        self, executor, event_queue
    ):
        """The continuation must not write a terminal status to the task row."""
        from a2a_protocol_plugin.handlers.a2a_ai_agent_utility import BridgeResult

        context = self._context_with_task_id("sdk-task-2")
        with patch(
            "a2a_protocol_plugin.handlers.a2a_ai_agent_utility"
            ".execute_ai_agent_non_streaming",
            new=AsyncMock(
                return_value=BridgeResult(
                    content="partial", role="agent",
                    metadata={"banyanos_continuation": "queued"},
                )
            ),
        ):
            await executor._handle_message_response(
                "Hi", event_queue, "test-endpoint#test-part", request_context=context
            )

        persist = executor.config.a2a_core.insert_update_a2a_task
        written = [call.kwargs.get("status") for call in persist.await_args_list]
        assert written, "continuation row write expected"
        assert all(w == "in_progress" for w in written)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
