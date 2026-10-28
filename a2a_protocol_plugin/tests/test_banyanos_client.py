#!/usr/bin/python
"""
Banyanos Engine Client — Unit Test Suite

Scripted transports only: httpx.MockTransport for the agent_engine_graphql
route, FakeBanyanosWS for the banyan_ws channel. Verifies the wire contracts
documented in docs/BANYANOS_INTEGRATION_PLAN.md §"Backend operation contracts"
and docker-banyanos-gateway module_routes (banyan_ws handshake, subscription
trigger with top-level connection_id, frame decoding, error categories).

Run with: python -m pytest a2a_protocol_plugin/tests/test_banyan_engine_client.py -v -o log_cli=false
"""

from __future__ import annotations

import json
import logging
import os
import sys

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from a2a_protocol_plugin.handlers.banyanos_client import (
    BanyanosClientError,
    BanyanosClient,
    BanyanosStreamEvent,
)
from test_helpers import FakeBanyanosWS

__author__ = "bibow"


@pytest.fixture
def logger():
    return logging.getLogger("test-banyanos-client")


def _client(router=None, ws_connect=None, **overrides):
    kwargs = dict(
        logger=logging.getLogger("test-banyanos-client"),
        graphql_url="http://banyanos-gateway.test",
        ws_url="ws://banyanos-gateway.test",
        endpoint_id="banyan",
        token="jwt-token",
        part_id="t1",
        stream_timeout=10.0,
    )
    kwargs.update(overrides)
    if router is not None:
        kwargs["http_transport"] = httpx.MockTransport(router)
    if ws_connect is not None:
        kwargs["ws_connect"] = ws_connect
    return BanyanosClient(**kwargs)


def _ok(data):
    return {"data": data}


def _router_for_invoke(body=None):
    def router(request):
        assert str(request.url).endswith("/banyan/agent_engine_graphql")
        assert request.headers["authorization"] == "Bearer jwt-token"
        assert request.headers["part-id"] == "t1"
        record = json.loads(request.content.decode())
        if record["query"].lstrip().startswith("mutation InvokeAgent") or \
                "invokeAgent(" in record["query"]:
            assert record["variables"]["idempotencyKey"]
            return httpx.Response(200, json=_ok(body or _invoke_response()))
        raise AssertionError("unexpected query: " + record["query"][:60])

    return router


def _invoke_response():
    return {
        "invokeAgent": {
            "id": "task-1",
            "agentId": "agent-1",
            "status": "SUCCESS",
            "outputPayload": {"content": "Hello", "finish_reason": "stop"},
            "tokenUsage": {"prompt": 1},
            "latencyMs": 42,
            "errorMessage": None,
            "startedAt": "2026-10-26T00:00:00Z",
        }
    }


# ---------------------------------------------------------------------------
# HTTP GraphQL transport
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_invoke_agent_success(logger):
    client = _client(_router_for_invoke())
    invocation = client.invoke_agent("agent-1", "hi", idempotency_key="banyanos-abc")
    assert invocation["id"] == "task-1"
    assert invocation["status"] == "SUCCESS"
    assert invocation["idempotency_key"] == "banyanos-abc"
    # outputPayload.content is the answer container
    assert invocation["outputPayload"]["content"] == "Hello"


@pytest.mark.unit
def test_invoke_agent_passes_session_and_options(logger):
    captured = {}

    def router(request):
        captured.update(json.loads(request.content.decode()))
        return httpx.Response(200, json=_ok(_invoke_response()))

    _client(router).invoke_agent(
        "agent-1", "hi", session_id="sess-9", temperature=0.2, max_tokens=64,
        idempotency_key="banyanos-abc",
    )
    variables = captured["variables"]
    assert variables["input"]["sessionId"] == "sess-9"
    assert variables["input"]["temperature"] == 0.2
    assert variables["input"]["maxTokens"] == 64
    assert variables["input"]["stream"] is False


@pytest.mark.unit
def test_http_401_is_auth_category(logger):
    def router(request):
        return httpx.Response(401, json={"error": "token expired"})

    with pytest.raises(BanyanosClientError) as exc_info:
        _client(router).invoke_agent("agent-1", "hi")
    assert exc_info.value.category == "auth"
    # Credentials/backend internals are not echoed into the message.
    assert "token expired" not in str(exc_info.value)


@pytest.mark.unit
def test_graphql_errors_are_executions_unauthenticated_is_auth(logger):
    def router(request):
        if "invokeAgent" in json.loads(request.content.decode())["query"]:
            return httpx.Response(200, json={
                "errors": [{"message": "denied",
                            "extensions": {"code": "UNAUTHENTICATED"}}]})
        return httpx.Response(200, json=_ok(_invoke_response()))

    with pytest.raises(BanyanosClientError) as exc_info:
        _client(router).invoke_agent("agent-1", "hi")
    assert exc_info.value.category == "auth"


@pytest.mark.unit
def test_graphql_200_with_errors_is_execution_failure(logger):
    def router(request):
        return httpx.Response(200, json={
            "errors": [{"message": "boom", "extensions": {"code": "INTERNAL"}}]})

    with pytest.raises(BanyanosClientError) as exc_info:
        _client(router).invoke_agent("agent-1", "hi")
    assert exc_info.value.category == "execution"


@pytest.mark.unit
def test_missing_data_field_is_protocol_error(logger):
    def router(request):
        return httpx.Response(200, json={"data": {}})

    with pytest.raises(BanyanosClientError) as exc_info:
        _client(router).invoke_agent("agent-1", "hi")
    assert exc_info.value.category == "protocol"


def _task_response():
    return {"taskStatus": {"id": "task-1", "agentId": "agent-1", "status": "RUNNING",
                            "sessionId": "s1", "outputPayload": None,
                            "errorMessage": None, "createdAt": "2026-10-26T00:00:00Z"}}


@pytest.mark.unit
def test_task_status_roundtrip(logger):
    def router(request):
        record = json.loads(request.content.decode())
        assert "taskStatus" in record["query"]
        assert record["variables"] == {"taskId": "task-1"}
        return httpx.Response(200, json=_ok(_task_response()))

    task = _client(router).task_status("task-1")
    assert task["status"] == "RUNNING"


@pytest.mark.unit
def test_cancel_task_roundtrip(logger):
    def router(request):
        record = json.loads(request.content.decode())
        assert "cancelTask" in record["query"]
        assert record["variables"]["taskId"] == "task-1"
        return httpx.Response(200, json=_ok(
            {"cancelTask": {"id": "task-1", "status": "CANCELLED",
                             "errorMessage": None}}))

    task = _client(router).cancel_task("task-1", idempotency_key="k1")
    assert task["status"] == "CANCELLED"


# ---------------------------------------------------------------------------
# banyan_ws streaming transport
# ---------------------------------------------------------------------------


_TRIGGER_ACK = [{"type": "connection_ack", "connection_id": FakeBanyanosWS.CONNECTION_ID}]


def _trigger_router(trigger_response=None, capture=None):
    def router(request):
        record = json.loads(request.content.decode())
        if capture is not None:
            capture.append(record)
        assert record.get("connection_id") == FakeBanyanosWS.CONNECTION_ID, (
            "subscription trigger must carry the established WS connection id "
            "as a top-level body field"
        )
        assert "agentInvokeStream" in record["query"]
        return httpx.Response(200, json=trigger_response if trigger_response is not None else {})

    return router


def _stream_frames():
    return [
        *_TRIGGER_ACK,
        {"data": {"agentInvokeStream": {"eventType": "started", "content": None}}},
        {"data": {"agentInvokeStream": {"eventType": "chunk", "content": "He"}}},
        {"data": {"agentInvokeStream": {"eventType": "chunk", "content": "llo"}}},
        {"data": {"agentInvokeStream": {"eventType": "completed",
                                         "content": "Hello"}}},
    ]


@pytest.mark.unit
def test_stream_happy_path(logger):
    captured = []
    def ws_connect(uri: str):

        return FakeBanyanosWS(_stream_frames())
    client = _client(router=_trigger_router(capture=captured),
                     ws_connect=ws_connect)
    events = []
    terminal = client.invoke_agent_stream(
        "agent-1", "hi", on_event=events.append,
    )
    assert terminal.event_type == "completed"
    assert terminal.content == "Hello"
    types = [e.event_type for e in events]
    assert types[0] == "started"
    assert types[-1] == "completed"
    assert "He" in [e.content for e in events if e.event_type == "chunk"]


@pytest.mark.unit
def test_ws_uri_carries_token_and_part_id(logger):
    captured = {}

    class _CaptureWS(FakeBanyanosWS):
        def __init__(self, uri, frames):
            captured["uri"] = uri
            super().__init__(frames)

    client = _client(router=_trigger_router(), ws_connect=None)
    client._ws_connect = lambda uri: _CaptureWS(uri, _stream_frames())
    terminal = client.invoke_agent_stream("agent-1", "hi")
    assert terminal.event_type == "completed"
    uri = captured["uri"]
    assert "/banyan/banyan_ws?" in uri
    assert "token=jwt-token" in uri
    assert "part_id=t1" in uri


@pytest.mark.unit
def test_stream_handshake_failure_raises_protocol_error(logger):
    def ws_connect(uri: str):

        return FakeBanyanosWS([{"type": "welcome"}])
    with pytest.raises(BanyanosClientError) as exc_info:
        _client(ws_connect=ws_connect).invoke_agent_stream("agent-1", "hi")
    assert exc_info.value.category == "protocol"


@pytest.mark.unit
def test_stream_ack_without_connection_id(logger):
    def ws_connect(uri: str):

        return FakeBanyanosWS([{"type": "connection_ack"}])
    with pytest.raises(BanyanosClientError) as exc_info:
        _client(ws_connect=ws_connect).invoke_agent_stream("agent-1", "hi")
    assert "connection_id" in str(exc_info.value)


@pytest.mark.unit
def test_stream_socket_close_before_terminal_is_error(logger):
    def ws_connect(uri: str):

        return FakeBanyanosWS(_TRIGGER_ACK)
    terminal = _client(router=_trigger_router(), ws_connect=ws_connect)         .invoke_agent_stream("agent-1", "hi")
    assert terminal.event_type == "error"
    assert "closed" in terminal.error


@pytest.mark.unit
def test_stream_gateway_error_frame_is_terminal(logger):
    frames = [*_TRIGGER_ACK,
              {"type": "error", "detail": "Partition mismatch"}]
    terminal = _client(router=_trigger_router(),
                       ws_connect=lambda uri: FakeBanyanosWS(frames)) \
        .invoke_agent_stream("agent-1", "hi")
    assert terminal.event_type == "error"
    assert "Partition mismatch" in terminal.error


@pytest.mark.unit
def test_stream_rejected_subscription_closes_socket(logger):
    frames = [*_TRIGGER_ACK]
    ws = FakeBanyanosWS(frames)

    def erroring_trigger(request):
        record = json.loads(request.content.decode())
        if "agentInvokeStream" in record["query"]:
            return httpx.Response(200, json={
                "errors": [{"message": "agent blocked",
                            "extensions": {"code": "FORBIDDEN"}}]})
        return httpx.Response(200, json=_ok(_invoke_response()))

    client = _client(router=erroring_trigger, ws_connect=lambda uri: ws)
    with pytest.raises(BanyanosClientError) as exc_info:
        client.invoke_agent_stream("agent-1", "hi")
    assert "agent blocked" in str(exc_info.value)
    # The socket is closed so late frames cannot join the next invocation.
    assert ws.closed


@pytest.mark.unit
def test_frame_decoder_skips_unrelated_frames(logger):
    assert BanyanosClient._decode_frame({"note": "keepalive"}) is None
    assert BanyanosClient._decode_frame("nonsense") is None
    event = BanyanosClient._decode_frame(
        {"data": {"agentInvokeStream": {"eventType": "chunk", "content": "x"}}}
    )
    assert event.event_type == "chunk"
    assert BanyanosStreamEvent.from_payload({}).event_type == ""


@pytest.mark.unit
def test_idempotency_keys_are_unique_or_stable(logger):
    key1 = BanyanosClient.new_idempotency_key()
    key2 = BanyanosClient.new_idempotency_key()
    assert key1 != key2
    assert key1.startswith("banyanos-")