#!/usr/bin/python
"""
A2A Proxy Handler — Unit Test Suite

Tests for A2AProxyHandler using httpx.MockTransport to mock A2A-compliant
backend responses. No real HTTP calls.

The proxy speaks the **standard A2A JSON-RPC transport** — the same dialect
this project's own server accepts (message/send, message/stream, tasks/cancel;
role "user"; camelCase fields; hyphenated task states; status-update /
artifact-update SSE events). These tests assert that wire format.

Run with: python -m pytest a2a_protocol_plugin/tests/test_a2a_proxy_handler.py -v
"""

import json
import logging
import os
import queue
import sys
import threading

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from a2a_protocol_plugin.handlers.a2a_proxy_handler import A2AProxyHandler

__author__ = "bibow"


@pytest.fixture
def logger():
    return logging.getLogger("test-a2a-proxy")


@pytest.fixture
def base_agent_config():
    return {
        "agent_id": "a2a-proxy-agent",
        "agent_name": "A2A Proxy Agent",
        "metadata": {
            "agent_type": "a2a_proxy",
            "a2a_proxy_url": "http://localhost:9900",
            "a2a_proxy_token": "test-token",
            "a2a_proxy_timeout": 30.0,
        },
    }


def _make_handler(logger, base_agent_config, transport=None, setting=None):
    return A2AProxyHandler(
        logger=logger,
        agent_config=base_agent_config,
        setting=setting or {},
        context={},
        http_transport=transport,
    )


def _sse(*frames):
    """Serialize JSON-RPC frames as an SSE body."""
    body = "\n".join("data: " + json.dumps(f) for f in frames) + "\n"
    return httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        content=body.encode("utf-8"),
    )


def _drain(q):
    chunks = []
    while not q.empty():
        chunks.append(q.get())
    return chunks


class TestNonStreaming:
    def test_non_streaming_basic(self, logger, base_agent_config):
        """Mock A2A message/send response with a Message result."""

        def mock_a2a(request: httpx.Request):
            body = json.loads(request.content.decode())
            assert body["jsonrpc"] == "2.0"
            assert body["method"] == "message/send"
            msg = body["params"]["message"]
            assert msg["role"] == "user"
            assert msg["parts"][0] == {"kind": "text", "text": "Hello"}

            return httpx.Response(200, json={
                "jsonrpc": "2.0",
                "result": {
                    "kind": "message",
                    "messageId": "resp-001",
                    "role": "agent",
                    "parts": [{"kind": "text", "text": "Hello from A2A backend!"}],
                    "contextId": "ctx-123",
                },
                "id": body["id"],
            })

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "Hello"}],
            context={"endpoint_id": "gpt", "part_id": "nestaging"},
        )
        assert result["content"] == "Hello from A2A backend!"
        assert result["role"] == "agent"
        assert result["message_id"] == "resp-001"
        assert result["metadata"]["thread_uuid"] == "ctx-123"

    def test_non_streaming_task_result(self, logger, base_agent_config):
        """Mock A2A message/send response with a Task result (completed)."""

        def mock_a2a(request: httpx.Request):
            return httpx.Response(200, json={
                "jsonrpc": "2.0",
                "result": {
                    "kind": "task",
                    "id": "task-001",
                    "contextId": "ctx-456",
                    "status": {"state": "completed"},
                    "artifacts": [{"parts": [{"kind": "text", "text": "Task result text"}]}],
                },
                "id": "test",
            })

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "Run a task"}],
            context={"endpoint_id": "gpt", "part_id": "nestaging"},
        )
        assert result["content"] == "Task result text"
        assert result["metadata"]["task_id"] == "task-001"
        assert result["metadata"]["thread_uuid"] == "ctx-456"

    def test_non_streaming_error(self, logger, base_agent_config):
        """Mock JSON-RPC error response."""

        def mock_a2a(request: httpx.Request):
            return httpx.Response(200, json={
                "jsonrpc": "2.0",
                "error": {"code": -32603, "message": "Backend error"},
                "id": "test",
            })

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "Hi"}],
            context={},
        )
        assert "error" in result
        assert "Backend error" in result["error"]

    def test_non_streaming_http_error(self, logger, base_agent_config):
        """Mock HTTP 500."""

        def mock_a2a(request: httpx.Request):
            return httpx.Response(500, text="server error")

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "Hi"}],
            context={},
        )
        assert "error" in result

    def test_non_streaming_no_url(self, logger):
        """No a2a_proxy_url configured → error."""
        cfg = {"agent_id": "proxy-agent", "metadata": {"agent_type": "a2a_proxy"}}
        handler = _make_handler(logger, cfg)
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "Hi"}],
            context={},
        )
        assert "error" in result
        assert "No a2a_proxy_url" in result["error"]

    def test_non_streaming_context_id_passthrough(self, logger, base_agent_config):
        """Verify contextId is passed through to the backend."""

        def mock_a2a(request: httpx.Request):
            body = json.loads(request.content.decode())
            msg = body["params"]["message"]
            assert msg["contextId"] == "existing-ctx"

            return httpx.Response(200, json={
                "jsonrpc": "2.0",
                "result": {
                    "kind": "message",
                    "messageId": "m1",
                    "role": "agent",
                    "parts": [{"kind": "text", "text": "OK"}],
                    "contextId": "existing-ctx",
                },
                "id": body["id"],
            })

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "Follow up"}],
            context={"thread_uuid": "existing-ctx", "endpoint_id": "gpt", "part_id": "nestaging"},
        )
        assert result["content"] == "OK"
        assert result["metadata"]["thread_uuid"] == "existing-ctx"

    def test_non_streaming_omits_empty_context_id(self, logger, base_agent_config):
        """First turn (no thread_uuid) must not send an empty contextId."""

        def mock_a2a(request: httpx.Request):
            body = json.loads(request.content.decode())
            assert "contextId" not in body["params"]["message"]
            return httpx.Response(200, json={
                "jsonrpc": "2.0",
                "result": {
                    "kind": "message",
                    "messageId": "m1",
                    "role": "agent",
                    "parts": [{"kind": "text", "text": "hi"}],
                    "contextId": "new-ctx",
                },
                "id": body["id"],
            })

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "start"}],
            context={},
        )
        assert result["metadata"]["thread_uuid"] == "new-ctx"

    def test_non_streaming_forwards_file_and_data_parts(self, logger, base_agent_config):
        """Inbound file/data parts are forwarded as spec FilePart/DataPart."""

        def mock_a2a(request: httpx.Request):
            body = json.loads(request.content.decode())
            parts = body["params"]["message"]["parts"]
            kinds = [p["kind"] for p in parts]
            assert kinds == ["text", "file", "file", "data"]
            assert parts[1]["file"] == {
                "uri": "http://f/x.pdf", "name": "x.pdf", "mimeType": "application/pdf",
            }
            assert parts[2]["file"] == {"bytes": "QUJD", "name": "y.txt", "mimeType": "text/plain"}
            assert parts[3]["data"] == {"k": "v"}
            return httpx.Response(200, json={
                "jsonrpc": "2.0",
                "result": {"kind": "message", "messageId": "m", "role": "agent",
                           "parts": [{"kind": "text", "text": "ok"}]},
                "id": body["id"],
            })

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "look"}],
            context={
                "input_files": [
                    {"url": "http://f/x.pdf", "filename": "x.pdf", "media_type": "application/pdf"},
                    {"bytes": "QUJD", "filename": "y.txt", "media_type": "text/plain"},
                ],
                "input_data_parts": [{"k": "v"}],
            },
        )
        assert result["content"] == "ok"


class TestStreaming:
    def test_streaming_basic(self, logger, base_agent_config):
        """Mock A2A SSE stream with Message frames + a final status-update."""

        def mock_a2a(request: httpx.Request):
            body = json.loads(request.content.decode())
            assert body["method"] == "message/stream"
            return _sse(
                {"jsonrpc": "2.0", "id": body["id"], "result": {
                    "kind": "message", "messageId": "m1", "role": "agent",
                    "parts": [{"kind": "text", "text": "Hello "}], "contextId": "ctx-stream",
                }},
                {"jsonrpc": "2.0", "id": body["id"], "result": {
                    "kind": "message", "messageId": "m2", "role": "agent",
                    "parts": [{"kind": "text", "text": "World!"}], "contextId": "ctx-stream",
                }},
                {"jsonrpc": "2.0", "id": body["id"], "result": {
                    "kind": "status-update", "taskId": "task-stream", "contextId": "ctx-stream",
                    "status": {"state": "completed"}, "final": True,
                }},
            )

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        q = queue.Queue()
        ev = threading.Event()
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "Stream a response"}],
            context={"endpoint_id": "gpt", "part_id": "nestaging"},
            stream_queue=q,
            stream_event=ev,
        )

        assert ev.is_set()
        assert result["content"] == "Hello World!"
        assert result["metadata"]["thread_uuid"] == "ctx-stream"
        assert result["metadata"]["run_id"] == "task-stream"

        chunks = _drain(q)
        names = [c["name"] for c in chunks]
        assert names.count("token") == 2
        # The backend task id is surfaced once for cancel/approval passthrough.
        run_ids = [c["value"] for c in chunks if c["name"] == "run_id"]
        assert run_ids == ["task-stream"]

    def test_streaming_artifact_update(self, logger, base_agent_config):
        """artifact-update frames are the primary streamed-delta channel."""

        def mock_a2a(request: httpx.Request):
            body = json.loads(request.content.decode())
            return _sse(
                {"jsonrpc": "2.0", "id": body["id"], "result": {
                    "kind": "artifact-update", "taskId": "t1", "contextId": "ctx-a",
                    "artifact": {"parts": [{"kind": "text", "text": "chunk-1 "}]},
                }},
                {"jsonrpc": "2.0", "id": body["id"], "result": {
                    "kind": "artifact-update", "taskId": "t1", "contextId": "ctx-a",
                    "artifact": {"parts": [{"kind": "text", "text": "chunk-2"}]}, "lastChunk": True,
                }},
                {"jsonrpc": "2.0", "id": body["id"], "result": {
                    "kind": "status-update", "taskId": "t1", "contextId": "ctx-a",
                    "status": {"state": "completed"}, "final": True,
                }},
            )

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        q = queue.Queue()
        ev = threading.Event()
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "x"}],
            context={},
            stream_queue=q,
            stream_event=ev,
        )
        assert result["content"] == "chunk-1 chunk-2"
        assert result["metadata"]["run_id"] == "t1"
        tokens = [c["value"] for c in _drain(q) if c["name"] == "token"]
        assert tokens == ["chunk-1 ", "chunk-2"]

    def test_streaming_input_required(self, logger, base_agent_config):
        """Mock A2A SSE stream with an input-required status-update."""

        def mock_a2a(request: httpx.Request):
            return _sse(
                {"jsonrpc": "2.0", "id": "test", "result": {
                    "kind": "message", "messageId": "m1", "role": "agent",
                    "parts": [{"kind": "text", "text": "Do you approve?"}], "contextId": "ctx-approval",
                }},
                {"jsonrpc": "2.0", "id": "test", "result": {
                    "kind": "status-update", "taskId": "task-approval", "contextId": "ctx-approval",
                    "status": {"state": "input-required"},
                }},
            )

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        q = queue.Queue()
        ev = threading.Event()
        handler.ask_model(
            input_messages=[{"role": "user", "content": "Do something risky"}],
            context={"endpoint_id": "gpt", "part_id": "nestaging"},
            stream_queue=q,
            stream_event=ev,
        )
        approval_chunks = [c for c in _drain(q) if c["name"] == "approval"]
        assert len(approval_chunks) == 1

    def test_streaming_auth_required(self, logger, base_agent_config):
        """auth-required status-update maps to an auth_required chunk (C7)."""

        def mock_a2a(request: httpx.Request):
            return _sse(
                {"jsonrpc": "2.0", "id": "test", "result": {
                    "kind": "status-update", "taskId": "t", "contextId": "c",
                    "status": {"state": "auth-required"},
                }},
            )

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        q = queue.Queue()
        ev = threading.Event()
        handler.ask_model(
            input_messages=[{"role": "user", "content": "x"}],
            context={},
            stream_queue=q,
            stream_event=ev,
        )
        assert any(c["name"] == "auth_required" for c in _drain(q))

    def test_streaming_error(self, logger, base_agent_config):
        """Mock A2A SSE stream with a JSON-RPC error frame."""

        def mock_a2a(request: httpx.Request):
            return _sse(
                {"jsonrpc": "2.0", "id": "test", "error": {"message": "Backend streaming error"}},
            )

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        q = queue.Queue()
        ev = threading.Event()
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "x"}],
            context={},
            stream_queue=q,
            stream_event=ev,
        )
        assert "error" in result
        assert "Backend streaming error" in result["error"]
        assert any(c["name"] == "error" for c in _drain(q))

    def test_streaming_failed_state(self, logger, base_agent_config):
        """Mock A2A SSE with a failed status-update."""

        def mock_a2a(request: httpx.Request):
            return _sse(
                {"jsonrpc": "2.0", "id": "test", "result": {
                    "kind": "status-update", "taskId": "task-fail", "contextId": "c",
                    "status": {"state": "failed"}, "final": True,
                }},
            )

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        q = queue.Queue()
        ev = threading.Event()
        result = handler.ask_model(
            input_messages=[{"role": "user", "content": "x"}],
            context={},
            stream_queue=q,
            stream_event=ev,
        )
        assert "error" in result


class TestCancel:
    def test_cancel_run(self, logger, base_agent_config):
        def mock_a2a(request: httpx.Request):
            body = json.loads(request.content.decode())
            assert body["method"] == "tasks/cancel"
            assert body["params"]["id"] == "task-123"
            return httpx.Response(200, json={
                "jsonrpc": "2.0",
                "result": {"kind": "task", "id": "task-123", "status": {"state": "canceled"}},
                "id": body["id"],
            })

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        assert handler.cancel_run("task-123") is True

    def test_cancel_run_failure(self, logger, base_agent_config):
        def mock_a2a(request: httpx.Request):
            return httpx.Response(500, text="fail")

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        assert handler.cancel_run("run-xyz") is False

    def test_cancel_run_no_url(self, logger):
        cfg = {"agent_id": "proxy-agent", "metadata": {"agent_type": "a2a_proxy"}}
        handler = _make_handler(logger, cfg)
        assert handler.cancel_run("run-abc") is False


class TestApproval:
    def test_resolve_approval(self, logger, base_agent_config):
        def mock_a2a(request: httpx.Request):
            body = json.loads(request.content.decode())
            assert body["method"] == "message/send"
            msg = body["params"]["message"]
            assert msg["role"] == "user"
            assert msg["contextId"] == "ctx-approval"
            assert "APPROVE" in msg["parts"][0]["text"]
            return httpx.Response(200, json={
                "jsonrpc": "2.0",
                "result": {
                    "kind": "message", "messageId": "resp", "role": "agent",
                    "parts": [{"kind": "text", "text": "Approved and continuing"}],
                    "contextId": "ctx-approval",
                },
                "id": body["id"],
            })

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        assert handler.resolve_approval("ctx-approval", True, "looks good") is True

    def test_resolve_approval_reject(self, logger, base_agent_config):
        def mock_a2a(request: httpx.Request):
            body = json.loads(request.content.decode())
            msg = body["params"]["message"]
            assert "REJECT" in msg["parts"][0]["text"]
            return httpx.Response(200, json={
                "jsonrpc": "2.0",
                "result": {"kind": "message", "parts": [{"kind": "text", "text": "Rejected"}]},
                "id": body["id"],
            })

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        assert handler.resolve_approval("ctx-approval", False, "too risky") is True

    def test_resolve_approval_failure(self, logger, base_agent_config):
        def mock_a2a(request: httpx.Request):
            return httpx.Response(500, text="fail")

        transport = httpx.MockTransport(mock_a2a)
        handler = _make_handler(logger, base_agent_config, transport=transport)
        assert handler.resolve_approval("ctx-xyz", True) is False


class TestConfigResolution:
    def test_config_from_metadata(self, logger):
        cfg = {
            "agent_id": "proxy-agent",
            "metadata": {
                "a2a_proxy_url": "http://backend:9900",
                "a2a_proxy_token": "meta-token",
                "a2a_proxy_timeout": 60.0,
            },
        }
        h = A2AProxyHandler(logger=logger, agent_config=cfg, setting={}, context={})
        assert h.proxy_url == "http://backend:9900"
        assert h.proxy_token == "meta-token"
        assert h.timeout == 60.0

    def test_config_defaults_when_metadata_empty(self, logger):
        """Empty metadata → empty url/token, default timeout."""
        cfg = {"agent_id": "proxy-agent", "metadata": {}}
        h = A2AProxyHandler(logger=logger, agent_config=cfg, setting={}, context={})
        assert h.proxy_url == ""
        assert h.proxy_token == ""
        assert h.timeout == 120.0

    def test_setting_dict_is_not_used(self, logger):
        """Proxy config is metadata-only — setting dict must be ignored."""
        cfg = {"agent_id": "proxy-agent", "metadata": {}}
        h = A2AProxyHandler(
            logger=logger,
            agent_config=cfg,
            setting={
                "A2A_PROXY_URL": "http://setting-host:9900",
                "A2A_PROXY_TOKEN": "setting-token",
                "A2A_PROXY_TIMEOUT": 90.0,
            },
            context={},
        )
        assert h.proxy_url == ""
        assert h.proxy_token == ""
        assert h.timeout == 120.0

    def test_metadata_overrides_everything(self, logger):
        """Metadata always wins for proxy connection details."""
        cfg = {
            "agent_id": "proxy-agent",
            "metadata": {"a2a_proxy_url": "http://meta-host:9900"},
        }
        h = A2AProxyHandler(
            logger=logger,
            agent_config=cfg,
            setting={"A2A_PROXY_URL": "http://setting-host:9900"},
            context={},
        )
        assert h.proxy_url == "http://meta-host:9900"


class TestHeaders:
    def test_headers_with_token(self, logger, base_agent_config):
        handler = _make_handler(logger, base_agent_config)
        headers = handler._headers()
        assert headers["Authorization"] == "Bearer test-token"
        assert headers["Content-Type"] == "application/json"

    def test_headers_without_token(self, logger):
        cfg = {"agent_id": "proxy-agent", "metadata": {"a2a_proxy_token": ""}}
        h = A2AProxyHandler(logger=logger, agent_config=cfg, setting={}, context={})
        headers = h._headers()
        assert "Authorization" not in headers
