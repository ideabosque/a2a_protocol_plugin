#!/usr/bin/python
"""
Message / response contract regression tests (Phase 1, plan item 5).

These tests pin the *existing* response behavior of the plugin so later
long-running-task work has a regression baseline:

- the direct-Message reply profile (``message/send`` answered with a Message
  carrying the assistant text),
- the blocking profile (a task-based response waits for a terminal/interrupted
  state; the SDK's ``DefaultRequestHandler`` keeps consuming the executor's
  event stream until then),
- the LEGACY collected-event profile (``message/stream`` returns the collected
  JSON event list inside the ``streaming_complete`` envelope),
- the partition/ownership context that must be applied to every one of them.

Everything runs offline: the A2A SDK request handler is real, the task store
is the SDK's in-memory store, ``a2a_core`` persistence is mocked and the
agent bridge is replaced by a scripted fake — no network, no live service.

Run with: python -m pytest a2a_protocol_plugin/tests/test_message_response_contract.py -v
"""

import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore

from a2a_protocol_plugin.handlers.a2a_ai_agent_utility import BridgeResult
from a2a_protocol_plugin.handlers.a2a_executor import A2ADaemonExecutor
from a2a_protocol_plugin.handlers.config import Config
from a2a_protocol_plugin.main import A2ADaemonEngine
from a2a_protocol_plugin.tests.test_a2a_protocol_compliance import _agent_card

__author__ = "SilvaEngine Team"

AGENT_UUID = "a2a-contract-agent"
TRUSTED_PARTITION = "ep#tenant-a"
SPOOFED_PARTITION = "ep#tenant-b"

# The A2A SDK range this contract was verified against (see the development
# plan, Phase 1 item 2). Bumping the SDK requires re-running this suite.
TESTED_SDK_RANGE = ("1.0.0", "2.0.0")


class _Harness:
    """Engine + real SDK request handler + mocked persistence and bridge."""

    def __init__(self, monkeypatch) -> None:
        self.logger = logging.getLogger("test-message-response-contract")
        store = InMemoryTaskStore()
        self.executor = A2ADaemonExecutor(
            logger=self.logger, config=Config, task_store=store
        )
        self.request_handler = DefaultRequestHandler(
            agent_executor=self.executor, task_store=store, agent_card=_agent_card()
        )
        self.core = AsyncMock()
        self.bridge = AsyncMock(
            return_value=BridgeResult(content="assistant answer")
        )
        monkeypatch.setattr(
            Config,
            "a2a_server",
            SimpleNamespace(
                request_handler=self.request_handler, agent_card=_agent_card()
            ),
        )
        monkeypatch.setattr(Config, "a2a_core", self.core)
        monkeypatch.setattr(Config, "DB_BACKEND", "dynamodb")

        self.engine = object.__new__(A2ADaemonEngine)
        self.engine.logger = self.logger
        self.engine.setting = {}

    def send(
        self,
        text: str = "Hi",
        method: str = "message/send",
        partition_key: str = TRUSTED_PARTITION,
        metadata: dict | None = None,
        message_id: str = "m1",
    ) -> dict:
        """Drive the gateway JSON-RPC entry point and return the envelope."""
        message = {
            "role": "ROLE_USER",
            "parts": [{"text": text}],
            "messageId": message_id,
        }
        params = {
            "message": message,
            "metadata": metadata
            if metadata is not None
            else {"agent_uuid": AGENT_UUID},
        }
        with patch(
            "a2a_protocol_plugin.handlers.a2a_ai_agent_utility."
            "execute_ai_agent_non_streaming",
            self.bridge,
        ):
            raw = self.engine.a2a(
                jsonrpc="2.0",
                id=f"req-{message_id}",
                method=method,
                partition_key=partition_key,
                params=params,
            )
        return json.loads(raw)

    def task_writes(self) -> list:
        """Every a2a_core task write issued during the run."""
        return [
            call
            for call in self.core.insert_update_a2a_task.await_args_list
        ]


@pytest.fixture
def harness(monkeypatch):
    return _Harness(monkeypatch)


def _reply_text(payload: dict) -> str:
    return "".join(part.get("text", "") for part in payload.get("parts", []))


class TestDirectMessageReplyProfile:
    """``message/send`` answered with a Message carrying the assistant text."""

    def test_message_send_returns_message_with_assistant_text(self, harness):
        response = harness.send("Hi")

        assert "error" not in response, response
        result = response["result"]
        assert result["role"] == "ROLE_AGENT"
        assert _reply_text(result) == "assistant answer"
        # A Message reply, not a Task: no status object is present.
        assert "status" not in result
        assert result["taskId"]
        assert result["contextId"]

    def test_direct_reply_persists_a_completed_message_response_task(self, harness):
        harness.send("Hi")

        writes = harness.task_writes()
        assert writes, "the daemon must persist the message_response task"
        completion = writes[-1]
        assert completion.kwargs["status"] == "completed"
        assert completion.kwargs["output_data"] == {"content": "assistant answer"}
        assert completion.kwargs["task_type"] == "message_response"
        assert completion.kwargs["assigned_agent_id"] == AGENT_UUID


class TestBlockingProfile:
    """A task-based response waits for a terminal/interrupted state."""

    def test_blocking_call_returns_task_when_backend_queues_continuation(
        self, harness
    ):
        # The backend accepted the work but continues asynchronously: the
        # executor emits a WORKING task, not a completed reply.
        harness.bridge.return_value = BridgeResult(
            content="partial",
            metadata={
                "banyanos_continuation": "queued",
                "banyanos_backend_task_id": "backend-1",
            },
        )

        response = harness.send("Hi")

        assert "error" not in response, response
        result = response["result"]
        # Task-based response: the SDK keeps consuming until the executor's
        # event stream ends, so the caller never receives a Message reply.
        assert "parts" not in result
        assert result["status"]["state"] == "TASK_STATE_WORKING"
        assert (
            _reply_text(result["status"]["message"]) == "partial"
        )
        # The bridge ran to completion before the response was produced.
        harness.bridge.assert_awaited_once()

    def test_blocking_call_does_not_fabricate_a_completed_reply(self, harness):
        harness.bridge.return_value = BridgeResult(
            content="partial",
            metadata={
                "banyanos_continuation": "queued",
                "banyanos_backend_task_id": "backend-1",
            },
        )

        harness.send("Hi")

        statuses = [call.kwargs["status"] for call in harness.task_writes()]
        assert statuses, "the daemon must persist the task row"
        assert set(statuses) == {"in_progress"}, statuses


class TestLegacyCollectedEventProfile:
    """LEGACY profile — the collected JSON event list must not change silently.

    ``message/stream`` answers with a JSON-RPC result envelope that carries
    the collected SDK events as plain JSON objects. Existing clients depend on
    this shape (and on its media type), so any change has to be an explicit,
    separately reviewed compatibility change — never a silent one.
    """

    def test_stream_returns_the_legacy_collected_event_envelope(self, harness):
        response = harness.send("Hi", method="message/stream")

        assert "error" not in response, response
        result = response["result"]
        assert result["status"] == "streaming_complete"
        assert result["events_emitted"] == len(result["events"]) == 1
        event = result["events"][0]
        assert event["role"] == "ROLE_AGENT"
        assert _reply_text(event) == "assistant answer"
        assert event["taskId"]
        assert event["contextId"]

    def test_legacy_envelope_keys_are_stable(self, harness):
        response = harness.send("Hi", method="message/stream")

        assert set(response["result"]) == {"status", "events_emitted", "events"}
        assert set(response["result"]["events"][0]) >= {
            "taskId",
            "role",
            "parts",
        }


class TestPartitionAndOwnershipContext:
    """The trusted partition/ownership context is applied to every profile."""

    def test_client_metadata_cannot_override_the_trusted_partition(self, harness):
        harness.send(
            "Hi",
            metadata={
                "agent_uuid": AGENT_UUID,
                "partition_key": SPOOFED_PARTITION,
            },
        )

        assert harness.bridge.await_args.kwargs["partition_key"] == TRUSTED_PARTITION
        for call in harness.task_writes():
            assert call.kwargs["partition_key"] == TRUSTED_PARTITION

    def test_reply_context_id_matches_the_persisted_task_context(self, harness):
        response = harness.send("Hi")

        context_id = response["result"]["contextId"]
        writes = harness.task_writes()
        assert writes
        assert writes[-1].kwargs["context_id"] == context_id

    def test_stream_profile_uses_the_trusted_partition(self, harness):
        harness.send(
            "Hi",
            method="message/stream",
            metadata={
                "agent_uuid": AGENT_UUID,
                "partition_key": SPOOFED_PARTITION,
            },
        )

        assert harness.bridge.await_args.kwargs["partition_key"] == TRUSTED_PARTITION


class TestTestedSdkRange:
    """Record which SDK the contract was verified against."""

    def test_installed_sdk_version_is_inside_the_tested_range(self):
        from importlib.metadata import version

        installed = version("a2a-sdk")
        low, high = TESTED_SDK_RANGE
        assert low <= installed < high, (
            f"a2a-sdk {installed} is outside the tested range "
            f"[{low}, {high}); re-verify this contract before widening it"
        )
