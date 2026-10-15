#!/usr/bin/python
"""
Goose bridge — executor-level tests

Drives a real A2A SDK ``DefaultRequestHandler`` + ``A2ADaemonExecutor`` through
``A2ADaemonEngine.a2a()`` (the gateway JSON-RPC entry point), with the goose
server mocked at the HTTP transport (``MockAcpServer`` from
test_goose_handler.py) and ``a2a_core`` persistence mocked.

Covers what the handler-level suite cannot: the executor wiring between a
JSON-RPC ``message/send`` and ``GooseAgentHandler`` — the reply, conversation
resume across turns, and tenant isolation of the session mapping.

``tasks/get`` / ``tasks/cancel`` are not covered here: they depend on the
repository-backed task store, which the docker live suite exercises
(docker-a2a-gateway tests/goose/test_gateway_live.py).

Run with: python -m pytest a2a_protocol_plugin/tests/test_goose_executor.py -v
"""

import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore

from a2a_protocol_plugin.handlers.a2a_executor import A2ADaemonExecutor
from a2a_protocol_plugin.handlers.config import Config
from a2a_protocol_plugin.main import A2ADaemonEngine
from a2a_protocol_plugin.tests.test_a2a_protocol_compliance import _agent_card
from a2a_protocol_plugin.tests.test_goose_handler import MockAcpServer, _handler

__author__ = "bibow"

AGENT_UUID = "a2a-goose-agent"


class _Harness:
    """Engine + mock goose server + shared in-memory a2a_settings rows."""

    def __init__(self, monkeypatch) -> None:
        self.logger = logging.getLogger("test-goose-executor")
        self.server = MockAcpServer()
        self.rows: dict = {}
        self.session_news = 0

        store = InMemoryTaskStore()
        executor = A2ADaemonExecutor(logger=self.logger, config=Config, task_store=store)
        request_handler = DefaultRequestHandler(
            agent_executor=executor, task_store=store, agent_card=_agent_card()
        )
        core = AsyncMock()
        core.get_a2a_agent = AsyncMock(return_value={
            "agent_id": AGENT_UUID,
            "agent_name": "Goose Agent",
            "metadata": json.dumps({"agent_type": "goose"}),
        })
        monkeypatch.setattr(Config, "a2a_server", SimpleNamespace(
            request_handler=request_handler, agent_card=_agent_card()))
        monkeypatch.setattr(Config, "a2a_core", core)
        monkeypatch.setattr(Config, "DB_BACKEND", "dynamodb")

        self.engine = object.__new__(A2ADaemonEngine)
        self.engine.logger = self.logger
        self.engine.setting = {}
        self.loaded_configs: list[dict] = []

    def _transport(self) -> httpx.MockTransport:
        original = self.server.handler

        def spy(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                try:
                    body = json.loads(request.content.decode() or "{}")
                except ValueError:
                    body = {}
                if body.get("method") == "session/new":
                    self.session_news += 1
            return original(request)

        return httpx.MockTransport(spy)

    def load_agent_handler(self, agent_config, context_setting=None, logger=None):
        # The real resolver picked the goose handler from agent_type; build it
        # over the mock transport and the shared settings rows instead.
        self.loaded_configs.append(agent_config)
        return _handler(self.logger, self._transport(), rows=self.rows)

    def send(self, text: str, partition_key: str = "ep#p1", message_id: str = "m1",
             thread_uuid: str | None = None, context_id: str | None = None) -> dict:
        message = {"role": "ROLE_USER", "parts": [{"text": text}],
                   "messageId": message_id}
        if context_id:
            message["contextId"] = context_id
        metadata = {"agent_uuid": AGENT_UUID}
        if thread_uuid:
            metadata["thread_uuid"] = thread_uuid
        with patch(
            "a2a_protocol_plugin.handlers.a2a_ai_agent_utility.load_agent_handler",
            self.load_agent_handler,
        ):
            raw = self.engine.a2a(
                jsonrpc="2.0", id=f"req-{message_id}", method="message/send",
                partition_key=partition_key,
                params={"message": message, "metadata": metadata},
            )
        return json.loads(raw)


@pytest.fixture
def harness(monkeypatch):
    return _Harness(monkeypatch)


def _reply_text(response: dict) -> str:
    result = response["result"]
    return "".join(part.get("text", "") for part in result.get("parts", []))


class TestGooseThroughExecutor:
    def test_message_send_returns_goose_reply(self, harness):
        response = harness.send("Hi")

        assert "error" not in response, response
        assert _reply_text(response) == "mock reply"
        assert response["result"]["role"] == "ROLE_AGENT"
        assert response["result"].get("taskId")
        assert harness.loaded_configs[0]["class_name"] == "GooseAgentHandler"
        assert harness.session_news == 1
        assert harness.server.deleted.is_set(), "ACP connection must be closed"

    def test_thread_uuid_resumes_goose_session(self, harness):
        harness.send("Hi", message_id="m1", thread_uuid="thread-1")
        stored = harness.rows[("ep#p1", "goose_session#thread-1")]["setting"]

        response = harness.send("Again", message_id="m2", thread_uuid="thread-1")

        assert "error" not in response, response
        assert harness.session_news == 1, "second turn must not open a new session"
        assert harness.server.load_calls == [stored["session_id"]]

    def test_thread_uuid_from_another_tenant_starts_new_session(self, harness):
        harness.send("Hi", partition_key="ep#tenant-a", message_id="m1",
                     thread_uuid="shared-thread")

        harness.send("Hi", partition_key="ep#tenant-b", message_id="m2",
                     thread_uuid="shared-thread")

        assert harness.server.load_calls == [], "must not load tenant-a's session"
        assert harness.session_news == 2

    @pytest.mark.xfail(
        strict=True,
        reason="Known gap: message.contextId is not forwarded to the handler as "
        "thread_uuid, so echoing the returned contextId starts a new goose "
        "session. Clients must send metadata.thread_uuid today.",
    )
    def test_returned_context_id_resumes_goose_session(self, harness):
        first = harness.send("Hi", message_id="m1")
        context_id = first["result"]["contextId"]

        harness.send("Again", message_id="m2", context_id=context_id)

        assert harness.session_news == 1
        assert len(harness.server.load_calls) == 1
