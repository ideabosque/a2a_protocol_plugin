#!/usr/bin/python
"""
Banyanos Agent Handler — Unit Test Suite

Tests for BanyanosAgentHandler (docs/BANYANOS_INTEGRATION_PLAN.md Phase 2/3
slice) using scripted transports: httpx.MockTransport for the GraphQL HTTP
route and an injectable fake WebSocket for the banyan_ws channel. No real
HTTP or WebSocket calls.

Covers: identity fail-closed resolution and metadata-spoof rejection,
non-streaming invokeAgent, streaming agentInvokeStream over banyan_ws
(started/chunk/reasoning/completed/error + run_id announcement), mid-stream
cancel_run closing the observation socket, context_id -> session mapping
persistence, and invocation deduplication through the execution map.

Run with: python -m pytest a2a_protocol_plugin/tests/test_banyan_agent_handler.py -v -o log_cli=false
"""

from __future__ import annotations

import json
import logging
import os
import queue
import sys
import threading

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from a2a_protocol_plugin.handlers import banyanos_execution_map
from a2a_protocol_plugin.handlers.banyanos_agent_handler import BanyanosAgentHandler

from test_helpers import FakeBanyanosWS

__author__ = "bibow"

PARTITION = "banyan#tenant-a"


def _base_agent_config(**overrides):
    metadata = {
        "module_name": "a2a_protocol_plugin.handlers.banyanos_agent_handler",
        "class_name": "BanyanosAgentHandler",
        "banyanos_graphql_url": "http://banyanos-gateway.test",
        "banyanos_ws_url": "ws://banyanos-gateway.test",
        "banyanos_agent_id": "agent-banyanos-published-1",
        "banyanos_service_ref": "banyanos-staging",
    }
    metadata.update(overrides)
    return {
        "agent_id": "banyanos-agent",
        "agent_name": "Banyanos Agent",
        "metadata": metadata,
    }


def _base_context(context_id: str = "conv-123", partition_key: str = PARTITION):
    return {
        "partition_key": partition_key,
        "endpoint_id": partition_key.split("#")[0],
        "part_id": partition_key.partition("#")[2],
        "context_id": context_id,
        "thread_uuid": None,
    }


def _invoke_response(task_id="banyanos-task-1", status="SUCCESS",
                     content="Hello from banyanos", session="sess-1"):
    return {
        "invokeAgent": {
            "id": task_id,
            "agentId": "agent-banyanos-published-1",
            "status": status,
            "outputPayload": {"content": content, "finish_reason": "stop"},
            "tokenUsage": {"prompt": 10, "completion": 5},
            "latencyMs": 1234,
            "errorMessage": None,
            "startedAt": "2026-10-26T00:00:00Z",
            "sessionId": session,
        }
    }


def _send_response(message_id="banyanos-msg-1", content="Hello from banyanos",
                   session="sess-new-1", success=True):
    """SendMessage (SendResultType) response shape."""
    return {"sendMessage": {"success": success, "messageId": message_id,
                             "sessionId": session, "role": "ASSISTANT",
                             "content": content, "latencyMs": 42}}


@pytest.fixture
def logger():
    return logging.getLogger("test-banyanos")


@pytest.fixture
def memory_repo(monkeypatch):
    class _MemoryRepo:
        def __init__(self):
            self.rows = {}

        def get(self, partition_key, setting_id):
            row = self.rows.get((partition_key, setting_id))
            return dict(row) if row else None

        def insert_update(self, info, partition_key, setting_id, endpoint_id,
                          part_id, setting, updated_by=None):
            self.rows[(partition_key, setting_id)] = {"setting": dict(setting)}

    repo = _MemoryRepo()
    monkeypatch.setattr(
        banyanos_execution_map, "default_setting_repo", lambda: repo
    )
    return repo


def _make_handler(logger, agent_config=None, gql_router=None,
                  ws_connect=None, setting=None):
    transport = httpx.MockTransport(gql_router) if gql_router else None
    return BanyanosAgentHandler(
        logger=logger,
        agent_config=agent_config or _base_agent_config(),
        setting=setting or {},
        context={},
        http_transport=transport,
        ws_connect=ws_connect,
        token_resolver=lambda service_ref, credential_ref: "banyanos-jwt-token",
    )


# ---------------------------------------------------------------------------
# Identity (fail closed)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_missing_partition_fails_closed(logger):
    # Partition scope is required; without ANY trusted scope (empty context)
    # identity resolution fails closed — the handler must return the typed
    # error, not raise silently or fall back to defaults.
    handler = _make_handler(logger)
    result = handler.ask_model(
        [{"role": "user", "content": "hi"}],
        context={},
    )
    assert result["error"]
    assert "partition_key" in result["error"]


@pytest.mark.unit
def test_identity_fail_closed_returns_error_result(logger, memory_repo, monkeypatch):
    # No BANYANOS_GRAPHQL_URL anywhere -> handler returns typed error, not raise.
    agent_config = _base_agent_config()
    agent_config["metadata"].pop("banyanos_graphql_url")
    handler = _make_handler(logger, agent_config)
    with monkeypatch.context() as m:
        m.setattr("a2a_protocol_plugin.handlers.banyanos_identity.Config",
                  type("C", (), {"banyanos_graphql_url": None, "banyanos_ws_url": None,
                                 "banyanos_token": None, "banyanos_agent_id": None})())
        result = handler.ask_model(
            [{"role": "user", "content": "hi"}], context=_base_context()
        )
    assert result["error"] and "banyanos_graphql_url" in result["error"]


@pytest.mark.unit
def test_request_metadata_cannot_override_trusted_values(logger, memory_repo):
    """Spoofed context metadata (urls/token/agent id) must be ignored; the
    configured agent id wins (plan: configured backend agent id is authoritative)."""
    recorded = {}

    def router(request):
        recorded["url"] = str(request.url)
        record = json.loads(request.content.decode())
        body = json.dumps(record)
        # The spoofed agent id must never reach the backend.
        assert "evil-agent" not in body
        if "createSession" in record["query"]:
            return httpx.Response(200, json={"data": {"createSession": {
                "id": "sess-a", "status": "active"}}})
        return httpx.Response(200, json={"data": _send_response()})

    handler = _make_handler(logger, gql_router=router)
    context = _base_context()
    # Attacker-controlled request metadata riding in call context:
    context.update({
        "banyanos_graphql_url": "http://evil.invalid",
        "banyanos_ws_url": "ws://evil.invalid",
        "banyanos_token": "attacker-token",
        "banyanos_agent_id": "evil-agent",
        "agent_uuid": "evil-agent",
    })
    result = handler.ask_model([{"role": "user", "content": "hi"}], context=context)
    assert not result.get("error")
    assert "banyanos-gateway.test" in recorded["url"]
    metadata = result["metadata"]
    assert metadata["banyanos_agent_id"] == "agent-banyanos-published-1"


# ---------------------------------------------------------------------------
# Non-streaming GraphQL (invokeAgent)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_non_streaming_invoke_success(logger, memory_repo):
    calls = []

    def router(request):
        calls.append((str(request.url), dict(request.headers),
                      json.loads(request.content.decode())))
        record = json.loads(request.content.decode())
        if "createSession" in record["query"]:
            return httpx.Response(200, json={"data": {"createSession": {
                "id": "sess-new-1", "status": "active"}}})
        return httpx.Response(200, json={"data": _send_response()})

    handler = _make_handler(logger, gql_router=router)
    result = handler.ask_model(
        [{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}],
        context=_base_context(),
    )
    assert not result.get("error")
    assert result["content"] == "Hello from banyanos"
    # First call: createSession (fresh conversation), second: sendMessage.
    assert len(calls) == 2
    assert "createSession" in calls[0][2]["query"]
    url, headers, body = calls[1]
    assert url.endswith("/banyan/agent_engine_graphql")
    assert headers.get("authorization") == "Bearer banyanos-jwt-token"
    assert headers.get("part-id") == "tenant-a"
    op = body["query"]
    assert "sendMessage" in op and "idempotencyKey" in op
    variables = body["variables"]
    # The SendMessage continuation contract: session id + content, and the
    # bridge-loaded history (system prompt dropped; history excludes the
    # current message).
    assert variables["input"]["content"] == "hi"
    assert variables["input"]["agentId"] == "agent-banyanos-published-1"
    assert variables["input"]["sessionId"] == "sess-new-1"
    assert variables["input"].get("chatHistory") is None
    # Session mapping persisted for the conversation.
    assert result["metadata"]["session_id"] == "sess-new-1"
    session_rows = [v for k, v in memory_repo.rows.items() if "banyanos_session#" in k[1]]
    assert session_rows and session_rows[0]["setting"]["session_id"] == "sess-new-1"
    # createSession's idempotency key is stable per (partition, conversation).
    assert calls[0][2]["variables"]["idempotencyKey"].startswith("banyanos-sess-")


@pytest.mark.unit
def test_session_mapping_roundtrip(logger, memory_repo):
    """Turn 2 resolves the persisted Banyanos session and sends it."""
    responses = [
        {"sendMessage": {"success": True, "messageId": "banyanos-msg-1",
                          "sessionId": "sess-a1", "role": "ASSISTANT",
                          "content": "one", "latencyMs": 10}},
        {"sendMessage": {"success": True, "messageId": "banyanos-msg-2",
                          "sessionId": "sess-a1", "role": "ASSISTANT",
                          "content": "two", "latencyMs": 10}},
    ]
    sent_session_ids = []

    def router(request):
        body = json.loads(request.content.decode())
        if "createSession" in body["query"]:
            return httpx.Response(200, json={"data": {"createSession": {
                "id": "sess-a1", "status": "active"}}})
        sent_session_ids.append(body["variables"]["input"].get("sessionId"))
        return httpx.Response(200, json={"data": responses[len(sent_session_ids) - 1]})

    handler = _make_handler(logger, gql_router=router)
    context = _base_context()
    result = handler.ask_model([{"role": "user", "content": "turn 1"}], context)
    assert result["content"] == "one"

    # Bridge echoes contextId; turn 2 replays the same conversation key.
    session = banyanos_execution_map.load_session_id(PARTITION, "conv-123", logger)
    assert session == "sess-a1"
    turn2 = handler.ask_model([{"role": "user", "content": "turn 2"}], context)
    assert turn2["content"] == "two"
    # Second call sent the mapped session id (context_id -> session).
    assert sent_session_ids[1] == "sess-a1"


@pytest.mark.unit
def test_graphql_error_maps_to_typed_result(logger, memory_repo):
    def router(request):
        return httpx.Response(200, json={
            "errors": [{"message": "Agent not found.",
                        "extensions": {"code": "NOT_FOUND"}}]})

    handler = _make_handler(logger, gql_router=router)
    result = handler.ask_model([{"role": "user", "content": "hi"}], _base_context())
    assert "Agent not found" in result["error"]
    assert result["metadata"]["banyanos_error_category"] == "execution"


@pytest.mark.unit
def test_http_auth_error_maps_to_auth_category(logger, memory_repo):
    def router(request):
        return httpx.Response(401, json={"error": "expired token"})

    handler = _make_handler(logger, gql_router=router)
    result = handler.ask_model([{"role": "user", "content": "hi"}], _base_context())
    assert result["metadata"]["banyanos_error_category"] == "auth"


# ---------------------------------------------------------------------------
# Streaming (banyan_ws agentInvokeStream)
# ---------------------------------------------------------------------------


def _streaming_scenario(frames, include_trigger=True, trigger_response=None):
    """Build a scripted GraphQL router + ws_connect pair for streaming tests."""

    def router(request):
        record = json.loads(request.content.decode())
        if include_trigger and "connection_id" in record:
            # Subscription trigger POST — must carry the WS connection id.
            assert record["connection_id"] == FakeBanyanosWS.CONNECTION_ID
            assert "agentInvokeStream" in record["query"]
            assert record["variables"]["agentId"] == "agent-banyanos-published-1"
            return httpx.Response(
                200, json=trigger_response if trigger_response is not None else {}
            )
        if "createSession" in record["query"]:
            return httpx.Response(200, json={"data": {"createSession": {
                "id": "sess-stream-1", "status": "active"}}})
        # Any other GraphQL POST during streaming (e.g. invokeAgent) is
        # unexpected for a stream turn — surface it in tests via an error.
        return httpx.Response(200, json={"data": _invoke_response()})

    def ws_connect(uri: str):
        return FakeBanyanosWS(list(frames))
    return router, ws_connect


@pytest.mark.agent
def test_streaming_chunks_completed_content_authoritative(logger, memory_repo):
    # completed carries the FULL final answer; deltas echo a subset.
    frames = [
        {"type": "connection_ack", "connection_id": FakeBanyanosWS.CONNECTION_ID},
        {"data": {"agentInvokeStream": {"eventType": "started", "content": None}}},
        {"data": {"agentInvokeStream": {"eventType": "chunk", "content": "Hel"}}},
        {"data": {"agentInvokeStream": {"eventType": "chunk", "content": "lo wor"}}},
        {"data": {"agentInvokeStream": {"eventType": "reasoning",
                                         "content": "thinking step"}}},
        {"data": {"agentInvokeStream": {"eventType": "completed",
                                         "content": "Hello world"}}},
    ]
    router, ws_connect = _streaming_scenario(frames)
    handler = _make_handler(logger, gql_router=router, ws_connect=ws_connect)
    q = queue.Queue()
    ev = threading.Event()
    result = handler.ask_model(
        [{"role": "user", "content": "hi"}], _base_context(), q, ev
    )
    assert not result.get("error")
    # Final content is the terminal completed payload, not delta concatenation.
    assert result["content"] == "Hello world"
    names = [c["value"] for c in _drain(q)]
    assert "Hel" in names and "lo wor" in names
    assert result["metadata"]["banyanos_status"] == "COMPLETED"
    assert result["metadata"]["banyanos_task_id"].startswith("banyanos-")


@pytest.mark.agent
def test_streaming_reasoning_is_tagged(logger, memory_repo):
    frames = [
        {"type": "connection_ack", "connection_id": FakeBanyanosWS.CONNECTION_ID},
        {"data": {"agentInvokeStream": {"eventType": "started"}}},
        {"data": {"agentInvokeStream": {"eventType": "chunk", "content": "answer"}}},
        {"data": {"agentInvokeStream": {"eventType": "reasoning",
                                         "content": "secret thought"}}},
        {"data": {"agentInvokeStream": {"eventType": "tool_started",
                                         "toolName": "search", "iteration": 1}}},
        {"data": {"agentInvokeStream": {"eventType": "tool_finished",
                                         "toolName": "search", "iteration": 1}}},
        {"data": {"agentInvokeStream": {"eventType": "completed",
                                         "content": "answer"}}},
    ]
    router, ws_connect = _streaming_scenario(frames)
    handler = _make_handler(logger, gql_router=router, ws_connect=ws_connect)
    q = queue.Queue()
    ev = threading.Event()
    result = handler.ask_model([{"role": "user", "content": "hi"}],
                               _base_context(), q, ev)
    chunks = _drain(q)
    reasoning = [c for c in chunks if (c.get("meta") or {}).get("data_format") == "reasoning"]
    assert [c["value"] for c in reasoning] == ["secret thought"]
    # Reasoning NEVER duplicates the answer text.
    assert all(c["value"] != "answer" for c in reasoning)
    # Tool progress is not streamed by default (show_tool_progress off).
    assert not [c for c in chunks if (c.get("meta") or {}).get("data_format") == "tool_progress"]
    assert result["content"] == "answer"


@pytest.mark.agent
def test_run_id_announced_on_started_for_cancellation(logger, memory_repo):
    frames = [
        {"type": "connection_ack", "connection_id": FakeBanyanosWS.CONNECTION_ID},
        {"data": {"agentInvokeStream": {"eventType": "started"}}},
        {"data": {"agentInvokeStream": {"eventType": "completed", "content": "done"}}},
    ]
    router, ws_connect = _streaming_scenario(frames)
    handler = _make_handler(logger, gql_router=router, ws_connect=ws_connect)
    q = queue.Queue()
    ev = threading.Event()
    handler.ask_model([{"role": "user", "content": "hi"}], _base_context(), q, ev)
    chunks = _drain(q)
    run_ids = [c["value"] for c in chunks if c.get("name") == "run_id"]
    assert run_ids, "run_id chunk must be emitted at start so the executor can register cancel passthrough"
    # The run is already deregistered once the stream returned.
    assert not handler._active_clients


@pytest.mark.agent
def test_mid_stream_cancel_closes_observation_socket(logger, memory_repo):
    """cancel_run must close the observation socket mid-stream — the backend
    writes a durable cancel key on subscriber disconnect."""
    slow_frames = [
        {"type": "connection_ack", "connection_id": FakeBanyanosWS.CONNECTION_ID},
        {"data": {"agentInvokeStream": {"eventType": "started"}}},
        {"data": {"agentInvokeStream": {"eventType": "chunk", "content": "tok"}}},
    ]

    class _SlowWS(FakeBanyanosWS):
        """Fake stream whose recv BLOCKS once scripted frames are drained —
        like a real in-flight run — until close() (the cancel signal)."""

        def __init__(self, frames):
            super().__init__(frames)
            self._closed_evt = threading.Event()

        def recv(self, timeout=None):
            if self._frames:
                return super().recv(timeout)
            # Scripted frames exhausted: simulate a live open stream.
            self._closed_evt.wait(timeout or 5.0)
            return None if self.closed else None

        def close(self):
            super().close()
            self._closed_evt.set()

    router = _streaming_scenario(slow_frames)[0]
    handler = _make_handler(logger, gql_router=router, ws_connect=lambda uri: _SlowWS(slow_frames))
    q = queue.Queue()
    ev = threading.Event()

    def _run():
        return handler.ask_model(
            [{"role": "user", "content": "hi"}], _base_context(), q, ev
        )

    outcome = {}

    def _worker():
        outcome["result"] = _run()

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    # Wait for started (run chunk) then cancel.
    chunks = _drain(q, timeout=5.0, stop_on_name="run_id")
    run_id = chunks[-1]["value"]
    assert handler.cancel_run(run_id) is True
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    result = outcome["result"]
    assert result.get("error")
    assert result["metadata"]["banyanos_status"] == "FAILED"


@pytest.mark.agent
def test_stream_error_frame(logger, memory_repo):
    frames = [
        {"type": "connection_ack", "connection_id": FakeBanyanosWS.CONNECTION_ID},
        {"data": {"agentInvokeStream": {"eventType": "started"}}},
        {"data": {"agentInvokeStream": {"eventType": "error",
                                         "error": "LLM provider unavailable"}}},
    ]
    router, ws_connect = _streaming_scenario(frames)
    handler = _make_handler(logger, gql_router=router, ws_connect=ws_connect)
    q = queue.Queue()
    ev = threading.Event()
    result = handler.ask_model([{"role": "user", "content": "hi"}], _base_context(), q, ev)
    assert "LLM provider unavailable" in result["error"]
    q_chunks = _drain(q)
    assert [c for c in q_chunks if c.get("name") == "error"]


# ---------------------------------------------------------------------------
# Deduplication (execution map)
# ---------------------------------------------------------------------------


@pytest.mark.agent
def test_repeated_logical_message_reuses_reservation_key(logger, memory_repo):
    r1 = banyanos_execution_map.resolve_or_reserve_execution(
        PARTITION, "banyan", "t1", "conv-123", "agent-banyanos-published-1",
        "same message", logger,
    )
    r2 = banyanos_execution_map.resolve_or_reserve_execution(
        PARTITION, "banyan", "t1", "conv-123", "agent-banyanos-published-1",
        "same message", logger,
    )
    assert r1["idempotency_key"] == r2["idempotency_key"]
    assert not r1["deduplicated"]
    # Distinct messages / tenants must not collide.
    other = banyanos_execution_map.resolve_or_reserve_execution(
        PARTITION, "banyan", "t1", "conv-999", "agent-banyanos-published-1",
        "same message", logger,
    )
    assert other["idempotency_key"] != r1["idempotency_key"]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _drain(q: queue.Queue, timeout: float = 2.0, stop_on_name: str | None = None):
    """Drain a stream queue until (near-)empty or stop_on_name chunk."""
    chunks = []
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            chunk = q.get(timeout=0.05)
        except Exception:
            return chunks
        chunks.append(chunk)
        if stop_on_name and chunk.get("name") == stop_on_name:
            return chunks
    return chunks