#!/usr/bin/python
"""
Goose Agent Handler — Unit Test Suite

Tests for GooseAgentHandler + GooseAcpClient using httpx.MockTransport
(no real HTTP calls). All wire behaviour is transcribed from the live
Phase G1 spike against goose 1.53.0 (tests/goose_acp_transcript.json).

Covers: initialize/auth, session lifecycle, the context_id -> sessionId
mapping (resume + never trusting client-supplied ids), the session/load
replay guard, reasoning tagging, tool calls, stopReason mapping, cancel,
every permission mode (auto_allow / deny / ask + timeout), the active-run
retry, and the 401/404 error paths.

Run with: python -m pytest a2a_protocol_plugin/tests/test_goose_handler.py -v
"""

import json
import logging
import os
import queue as _queue
import sys
import threading
import time
import uuid

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from a2a_protocol_plugin.handlers.goose_acp_client import GooseAcpClient, GooseAcpError
from a2a_protocol_plugin.handlers.goose_handler import GooseAgentHandler

__author__ = "bibow"

CONN_ID = "mockconn-0000"
CAPS_RESULT = {
    "protocolVersion": 1,
    "agentCapabilities": {
        "loadSession": True,
        "promptCapabilities": {"image": True, "audio": False, "embeddedContext": True},
        "sessionCapabilities": {"list": {}, "delete": {}, "close": {}},
    },
    "agentInfo": {"name": "goose", "version": "1.53.0"},
    "authMethods": [],
}


def _sse(data) -> bytes:
    return f"data: {data}\r\n\r\n".encode()


def _rpc(msg_id=None, method=None, result=None, error=None, params=None):
    body: dict = {"jsonrpc": "2.0"}
    if msg_id is not None:
        body["id"] = msg_id
    if method is not None:
        body["method"] = method
        if params is not None:
            body["params"] = params
    if result is not None:
        body["result"] = result
    if error is not None:
        body["error"] = error
    return json.dumps(body)


class MockAcpServer:
    """A scripted ``goose serve`` behind httpx.MockTransport.

    Streams are served lazily from per-stream queues so that events can be
    ordered relative to POSTs (this is what makes the replay-guard tests
    meaningful). ``None`` closes a stream.
    """

    def __init__(self, secret_key: str | None = None, auth_reject: bool = False):
        self.secret_key = secret_key
        self.auth_reject = auth_reject
        self.posts: list[dict] = []
        self.permission_answers: list[dict] = []
        self.cancel_calls: list[str] = []
        self.load_calls: list[str] = []
        self._conn_counter = 0
        # conn-scoped queues (the session/new result ride these) keyed by the
        # server-assigned connection id; session-scoped queues keyed by
        # (conn_id, session_id). One stream per connection, like goose.
        self.conn_queues: dict[str, _queue.Queue] = {}
        self.session_queues: dict[tuple[str, str], _queue.Queue] = {}
        # session_id -> scripted frames for the next prompt run
        self.next_run: dict[str, list[dict]] = {}
        self.pending_prompt: dict[str, int] = {}  # session_id -> prompt msg id
        self.deleted = threading.Event()

    # ------------------------------------------------------------------
    # scripting
    # ------------------------------------------------------------------

    def script_run(self, session_id: str, frames: list[dict]) -> None:
        self.next_run[session_id] = list(frames)

    def text_run(self, session_id: str, text: str = "mock reply", stop="end_turn") -> None:
        """script an agent_message_chunk run"""
        self.script_run(session_id, [
            {"frame": "update", "update": {"sessionUpdate": "agent_message_chunk",
                                           "content": {"type": "text", "text": text},
                                           "messageId": "mock-1"}},
            {"frame": "result", "result": {"stopReason": stop,
                                           "usage": {"inputTokens": 5, "outputTokens": 2,
                                                     "totalTokens": 7}}},
        ])

    def _feed(self, conn_id: str, session_id: str, prompt_msg_id, run: list[dict]) -> None:
        stream = self.session_queues.setdefault((conn_id, session_id), _queue.Queue())
        time.sleep(0.02)
        try:
            for fr in run:
                if "load" in fr:
                    continue  # load-scoped frames ride session/load, not prompt
                if fr["frame"] == "update":
                    stream.put(_sse(_rpc(method="session/update",
                                         params={"sessionId": session_id,
                                                 "update": fr["update"]})))
                elif fr["frame"] == "server_request":
                    stream.put(_sse(_rpc(msg_id=fr["request_id"], method=fr["method"],
                                         params=fr["params"])))
                elif fr["frame"] == "result":
                    rid = fr.get("request_id")
                    if rid in (None, "pending"):
                        rid = prompt_msg_id
                    stream.put(_sse(_rpc(msg_id=rid, result=fr["result"],
                                         error=fr.get("error"))))
        finally:
            self.pending_prompt.pop(session_id, None)

    # ------------------------------------------------------------------
    # httpx.MockTransport handler
    # ------------------------------------------------------------------

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.posts.append({"method": request.method, "path": request.url.path,
                           "headers": {k.lower(): v for k, v in request.headers.items()}})
        if self.auth_reject:
            if request.headers.get("X-Secret-Key") != (self.secret_key or ""):
                return httpx.Response(401, text="unauthorized")
        if request.method == "DELETE":
            self.deleted.set()
            return httpx.Response(202)
        if request.method != "POST":
            conn = request.headers.get("acp-connection-id") or ""
            session = request.headers.get("acp-session-id")
            if session is None:
                stream = self.conn_queues.setdefault(conn, _queue.Queue())
                return httpx.Response(
                    200, headers={"content-type": "text/event-stream"},
                    content=self._gen(stream),
                )
            key = (conn, session)
            stream = self.session_queues.setdefault(key, _queue.Queue())
            return httpx.Response(
                200, headers={"content-type": "text/event-stream"},
                content=self._gen(stream),
            )

        try:
            payload = json.loads(request.content.decode() or "{}")
        except ValueError:
            return httpx.Response(400, text="bad json")

        method = payload.get("method")
        msg_id = payload.get("id")
        params = payload.get("params") or {}

        # client -> server JSON-RPC response (e.g. permission answer)
        if method is None and msg_id is not None and ("result" in payload or "error" in payload):
            self.permission_answers.append(payload)
            return httpx.Response(202)

        if method == "initialize":
            if not request.headers.get("acp-connection-id"):
                return httpx.Response(400, text="Acp-Connection-Id header required")
            self._conn_counter += 1
            conn_id = f"mockconn-{self._conn_counter:04d}"
            return httpx.Response(
                200, headers={"Acp-Connection-Id": conn_id},
                # the real goose serve returns initialize inline as JSON
                json={"jsonrpc": "2.0", "id": msg_id, "result": CAPS_RESULT},
            )
        # every session POST is scoped to the sending connection
        conn_id = request.headers.get("acp-connection-id") or ""
        if method == "session/new":
            session_id = f"20260101_{uuid.uuid4().hex[:4]}"
            self.conn_queues.setdefault(conn_id, _queue.Queue())
            self.conn_queues[conn_id].put(_sse(_rpc(msg_id, result={
                "sessionId": session_id,
                "modes": {"currentModeId": "auto", "availableModes": [
                    {"id": "auto", "name": "auto", "description": "auto"},
                    {"id": "approve", "name": "approve", "description": "ask"}]},
            })))
            return httpx.Response(202)
        if method == "session/load":
            session_id = params.get("sessionId")
            self.load_calls.append(session_id)
            stream = self.session_queues.setdefault((conn_id, session_id), _queue.Queue())
            script = self.next_run.get(session_id)
            if script and any(fr.get("load") == "fail" for fr in script):
                stream.put(_sse(_rpc(msg_id, error={"code": -32602,
                                                    "message": "session not found"})))
                return httpx.Response(202)
            for fr in script or []:
                if fr.get("load") == "replay":
                    stream.put(_sse(_rpc(method="session/update",
                                         params={"sessionId": session_id,
                                                 "update": fr["update"]})))
            stream.put(_sse(_rpc(msg_id, result={
                "modes": {"currentModeId": "auto",
                          "availableModes": [{"id": "auto", "name": "auto"},
                                             {"id": "approve", "name": "approve"}]}})))
            return httpx.Response(202)
        if method == "session/set_mode":
            session_id = params.get("sessionId")
            stream = self.session_queues.setdefault((conn_id, session_id), _queue.Queue())
            stream.put(_sse(_rpc(method="session/update",
                                 params={"sessionId": session_id,
                                         "update": {"sessionUpdate": "current_mode_update",
                                                    "currentModeId": params.get("modeId")}})))
            stream.put(_sse(_rpc(msg_id, result={})))
            return httpx.Response(202)
        if method == "session/prompt":
            session_id = params.get("sessionId")
            run = self.next_run.get(session_id)
            if run is None:
                run = self.text_run_script(session_id)
            self.pending_prompt[session_id] = msg_id
            threading.Thread(target=self._feed,
                             args=(conn_id, session_id, msg_id, run),
                             daemon=True).start()
            return httpx.Response(202)
        if method == "session/cancel":
            session_id = params.get("sessionId")
            self.cancel_calls.append(session_id)
            prompt_id = self.pending_prompt.pop(session_id, None)
            if prompt_id is not None:
                stream = self.session_queues.setdefault((conn_id, session_id), _queue.Queue())
                stream.put(_sse(_rpc(prompt_id, result={
                    "stopReason": "cancelled",
                    "usage": {"inputTokens": 1, "outputTokens": 0, "totalTokens": 1}})))
            return httpx.Response(202)
        return httpx.Response(200, content=_sse(_rpc(msg_id, result={})))

    def text_run_script(self, session_id: str) -> list[dict]:
        run = [
            {"frame": "update", "update": {"sessionUpdate": "agent_message_chunk",
                                           "content": {"type": "text", "text": "mock reply"},
                                           "messageId": "mock-1"}},
            {"frame": "result", "result": {"stopReason": "end_turn",
                                           "usage": {"inputTokens": 5, "outputTokens": 2,
                                                     "totalTokens": 7}}},
        ]
        self.next_run[session_id] = run
        return run

    @staticmethod
    def _gen(stream: _queue.Queue, timeout: float = 6.0):
        while True:
            try:
                item = stream.get(timeout=timeout)
            except _queue.Empty:
                return
            if item is None:
                return
            yield item


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def logger():
    return logging.getLogger("test-goose")


@pytest.fixture
def server():
    return MockAcpServer()


@pytest.fixture
def transport(server):
    return httpx.MockTransport(server.handler)


def _resume_ctx(h, session_id: str, partition: str = "ep#x") -> dict:
    """Store a mapping for ``session_id`` and return a resume context (this
    is exactly what the executor does between turns with a live repo)."""
    endpoint_id, _, part_id = partition.partition("#")
    h._store_session_mapping(partition, endpoint_id, part_id, "ctx-" + session_id,
                             session_id)
    return {"partition_key": partition, "thread_uuid": "ctx-" + session_id}


SETTING = {"GOOSE_API_URL": "http://goose.test", "GOOSE_SECRET_KEY": "k1",
           "GOOSE_CWD": "/workspace",
           # fail fast if a scripted result never arrives
           "GOOSE_STREAM_TIMEOUT": "25"}

AGENT_CONFIG = {
    "agent_id": "goose-agent",
    "metadata": {"module_name": "a2a_protocol_plugin.handlers.goose_handler"},
}


def _handler(logger, transport, setting=None, metadata=None, rows=None, server=None):
    """Build a GooseAgentHandler over the mock transport with an in-memory
    partition-scoped a2a_settings repo shared via ``rows``."""
    cfg = dict(AGENT_CONFIG)
    cfg["metadata"] = {**(AGENT_CONFIG["metadata"]), **(metadata or {})}
    h = GooseAgentHandler(logger=logger, agent_config=cfg,
                          setting=setting or dict(SETTING),
                          context={}, http_transport=transport)
    my_rows = rows if rows is not None else {}

    class _Repo:
        def get(self, partition_key, setting_id):
            return my_rows.get((partition_key, setting_id))

        def insert_update(self, info, partition_key, setting_id, endpoint_id,
                          part_id, setting, updated_by=None):
            my_rows[(partition_key, setting_id)] = {"setting": setting}

    h._setting_repo = lambda: _Repo()
    h._rows = my_rows
    return h


def _spy(server, capture: dict) -> httpx.MockTransport:
    """Wrap the mock handler, capturing session/new & session/load calls."""
    original = server.handler

    def spy(request: httpx.Request):
        if request.method == "POST":
            try:
                body = json.loads(request.content.decode() or "{}")
            except ValueError:
                body = {}
            m = body.get("method")
            if m == "session/new":
                capture.setdefault("news", 0)
                capture["news"] += 1
            elif m == "session/load":
                capture.setdefault("loads", []).append((body.get("params") or {}).get("sessionId"))
            elif m == "session/prompt":
                capture.setdefault("prompts", []).append(body["params"]["prompt"])
        return original(request)

    return httpx.MockTransport(spy)


# ---------------------------------------------------------------------------
# Transport-level tests
# ---------------------------------------------------------------------------

class TestGooseAcpClientTransport:
    def test_initialize_sets_canonical_conn_id(self, logger, transport, server):
        client = GooseAcpClient(logger, "http://goose.test", secret_key="k1",
                                http_transport=transport)
        caps = client.initialize()
        assert caps["agentCapabilities"]["loadSession"] is True
        assert client.connection_id and client.connection_id.startswith("mockconn-")

    def test_auth_rejected_401(self, logger):
        server = MockAcpServer(secret_key="k1", auth_reject=True)
        client = GooseAcpClient(logger, "http://goose.test",
                                secret_key="wrong",
                                http_transport=httpx.MockTransport(server.handler))
        with pytest.raises(GooseAcpError) as exc:
            client.initialize()
        assert exc.value.is_auth_error()

    def test_client_capabilities_never_advertise_fs(self, logger, server):
        posts: list[dict] = []
        original = server.handler

        def spy(request):
            if request.method == "POST":
                try:
                    body = json.loads(request.content.decode() or "{}")
                except ValueError:
                    body = {}
                if body.get("method") == "initialize":
                    posts.append(body)
            return original(request)

        client = GooseAcpClient(logger, "http://goose.test", secret_key="k1",
                                http_transport=httpx.MockTransport(spy))
        client.initialize()
        caps = posts[0]["params"]["clientCapabilities"]
        # fs/write_text_file server requests hang a run until answered; a
        # headless bridge must never advertise fs capabilities.
        assert caps.get("fs", {}).get("readTextFile") is False
        assert caps.get("fs", {}).get("writeTextFile") is False


# ---------------------------------------------------------------------------
# Handler — first turn / non-streaming
# ---------------------------------------------------------------------------

class TestNonStreaming:
    def test_first_turn_end_turn(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler))
        result = h.ask_model([{"role": "user", "content": "Hi"}], {})
        assert result["content"] == "mock reply"
        assert result["role"] == "agent"
        assert result.get("error") is None
        meta = result["metadata"]
        assert meta["goose"]["stopReason"] == "end_turn"
        assert meta["thread_uuid"] == meta["goose"]["sessionId"]
        assert meta["run_id"].endswith(f":{meta['thread_uuid']}")
        assert meta["run_id"].startswith("mockconn-")
        assert meta["goose"]["usage"]["totalTokens"] == 7

    def test_only_newest_user_turn_is_sent(self, logger, server):
        capture: dict = {}
        h = _handler(logger, _spy(server, capture))
        h.ask_model([
            {"role": "user", "content": "old turn 1"},
            {"role": "agent", "content": "assistant turn"},
            {"role": "user", "content": "newest turn"},
        ], {})
        assert len(capture["prompts"]) == 1
        blocks = capture["prompts"][0]
        texts = " ".join(b.get("text", "") for b in blocks)
        assert "newest turn" in texts
        assert "old turn 1" not in texts
        assert "assistant turn" not in texts

    def test_connection_closed_after_run(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler))
        h.ask_model([{"role": "user", "content": "Hi"}], {})
        assert server.deleted.is_set(), "DELETE /acp must run in a finally"


# ---------------------------------------------------------------------------
# context_id -> sessionId mapping
# ---------------------------------------------------------------------------

class TestContextMapping:
    def test_resume_uses_mapped_session_via_load(self, logger, server):
        rows: dict = {}
        capture: dict = {}
        h = _handler(logger, httpx.MockTransport(server.handler), rows=rows)
        first = h.ask_model([{"role": "user", "content": "Hi"}],
                            {"partition_key": "ep#t1", "thread_uuid": None})
        session_id = first["metadata"]["thread_uuid"]
        ctx_id = "ctx-1"
        h._store_session_mapping("ep#t1", "ep", "t1", ctx_id, session_id)

        h2 = _handler(logger, _spy(server, capture), rows=rows)
        second = h2.ask_model([{"role": "user", "content": "follow-up"}],
                              {"partition_key": "ep#t1", "thread_uuid": ctx_id})
        assert second["metadata"]["thread_uuid"] == session_id
        assert capture.get("loads") == [session_id], "resume must use session/load"
        assert not capture.get("news"), "resume must not create a new session"

    def test_client_supplied_session_id_never_loaded(self, logger, server):
        """Cross-tenant guard: a context_id with no mapping must never be
        forwarded to session/load (goose sessions are not tenant-aware)."""
        capture: dict = {}
        h = _handler(logger, _spy(server, capture))
        attacker_ctx = "20260101_9"  # another tenant's goose session id
        result = h.ask_model([{"role": "user", "content": "Hi"}],
                             {"partition_key": "ep#t2", "thread_uuid": attacker_ctx})
        assert result["content"] == "mock reply"  # fresh session, no error
        assert capture.get("loads", []) == [], "unmapped ids never reach session/load"
        assert result["metadata"]["thread_uuid"] != attacker_ctx

    def test_load_failure_falls_back_to_new_session(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler))
        ctx_id = "ctx-dead"
        h._store_session_mapping("ep#t3", "ep", "t3", ctx_id, "20260101_99")
        server.next_run["20260101_99"] = [{"load": "fail"}]
        result = h.ask_model([{"role": "user", "content": "Hi"}],
                             {"partition_key": "ep#t3", "thread_uuid": ctx_id})
        assert result["content"] == "mock reply"
        assert result["metadata"]["thread_uuid"] != "20260101_99"


# ---------------------------------------------------------------------------
# session/load replay guard + streaming mappings
# ---------------------------------------------------------------------------

class TestReplayGuard:
    def test_session_load_replay_is_dropped(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler))
        first = h.ask_model([{"role": "user", "content": "Hi"}],
                            {"partition_key": "ep#r", "thread_uuid": None})
        session_id = first["metadata"]["thread_uuid"]
        ctx_id = "ctx-r"
        h._store_session_mapping("ep#r", "ep", "r", ctx_id, session_id)
        server.next_run[session_id] = [
            {"load": "replay", "update": {"sessionUpdate": "user_message_chunk",
                                          "content": {"type": "text", "text": "Hi"},
                                          "messageId": "old-user"}},
            {"load": "replay", "update": {"sessionUpdate": "agent_message_chunk",
                                          "content": {"type": "text", "text": "mock reply"},
                                          "messageId": "old-agent"}},
            {"frame": "update", "update": {"sessionUpdate": "agent_message_chunk",
                                           "content": {"type": "text", "text": "fresh answer"},
                                           "messageId": "new"}},
            {"frame": "result", "result": {"stopReason": "end_turn"}},
        ]
        second = h.ask_model([{"role": "user", "content": "Hi"}],
                             {"partition_key": "ep#r", "thread_uuid": ctx_id})
        assert second["content"] == "fresh answer", (
            "history replay must not surface as answer tokens"
        )


class TestStreamingMapping:
    def _resumed_stream(
        self, logger, server, rows, script, prompt="again",
    ):
        base = _handler(logger, httpx.MockTransport(server.handler), rows=rows)
        first = base.ask_model([{"role": "user", "content": "Hi"}],
                               {"partition_key": "ep#s", "thread_uuid": None})
        sid = first["metadata"]["thread_uuid"]
        base._store_session_mapping("ep#s", "ep", "s", "ctx-s", sid)
        server.next_run[sid] = script

        h2 = _handler(logger, httpx.MockTransport(server.handler), rows=rows)
        q = _queue.Queue()
        out = h2.ask_model([{"role": "user", "content": prompt}],
                           {"partition_key": "ep#s", "thread_uuid": "ctx-s"},
                           stream_queue=q, stream_event=threading.Event())
        chunks = []
        while not q.empty():
            chunks.append(q.get())
        return out, chunks, sid

    def test_reasoning_tagged_and_kept_out_of_answer(self, logger, server):
        rows: dict = {}
        out, chunks, _sid = self._resumed_stream(logger, server, rows, [
            {"frame": "update", "update": {"sessionUpdate": "agent_thought_chunk",
                                           "content": {"type": "text", "text": "thinking hard"},
                                           "messageId": "t1"}},
            {"frame": "update", "update": {"sessionUpdate": "agent_message_chunk",
                                           "content": {"type": "text", "text": "final"},
                                           "messageId": "t2"}},
            {"frame": "result", "result": {"stopReason": "end_turn"}},
        ])
        assert out["content"] == "final"
        tagged = [c for c in chunks
                  if c["name"] == "token" and (c.get("meta") or {}).get("data_format") == "reasoning"]
        assert tagged and "thinking hard" in tagged[0]["value"]
        plain = "".join(c["value"] for c in chunks
                        if c["name"] == "token" and not (c.get("meta") or {}).get("data_format"))
        assert "thinking hard" not in plain
        assert "final" in plain

    def test_run_id_chunk_sent_before_updates(self, logger, server):
        rows: dict = {}
        _out, chunks, _sid = self._resumed_stream(logger, server, rows, [
            {"frame": "update", "update": {"sessionUpdate": "agent_message_chunk",
                                           "content": {"type": "text", "text": "x"},
                                           "messageId": "t"}},
            {"frame": "result", "result": {"stopReason": "end_turn"}},
        ])
        assert chunks and chunks[0]["name"] == "run_id", (
            "run_id must precede update chunks so cancel/approval can register"
        )
        assert chunks[0]["value"].endswith(f":{_sid}")

    def test_tool_call_and_result_chunks(self, logger, server):
        rows: dict = {}
        _out, chunks, _sid = self._resumed_stream(logger, server, rows, [
            {"frame": "update", "update": {"sessionUpdate": "tool_call",
                                           "toolCallId": "call_1",
                                           "title": "write / x.txt",
                                           "kind": "edit", "status": "pending",
                                           "rawInput": {"path": "/x.txt"}}},
            {"frame": "update", "update": {"sessionUpdate": "tool_call_update",
                                           "toolCallId": "call_1", "status": "completed",
                                           "content": [{"type": "content",
                                                        "content": {"type": "text",
                                                                    "text": "wrote 1 lines"}}]}},
            {"frame": "update", "update": {"sessionUpdate": "agent_message_chunk",
                                           "content": {"type": "text", "text": "done"},
                                           "messageId": "t"}},
            {"frame": "result", "result": {"stopReason": "end_turn"}},
        ])
        tool_calls = [c for c in chunks if c["name"] == "tool_call"]
        tool_results = [c for c in chunks if c["name"] == "tool_result"]
        assert tool_calls and json.loads(tool_calls[0]["value"])["toolCallId"] == "call_1"
        assert tool_results and json.loads(tool_results[0]["value"])["output"] == "wrote 1 lines"


# ---------------------------------------------------------------------------
# stop reasons
# ---------------------------------------------------------------------------

class TestStopReasons:
    @pytest.mark.parametrize("stop_reason", [
        "max_tokens", "refusal", "max_turn_requests", "cancelled",
    ])
    def test_partial_stop_reasons_report_error(self, logger, server, stop_reason):
        h = _handler(logger, httpx.MockTransport(server.handler))
        q = _queue.Queue()
        res = h.ask_model([{"role": "user", "content": "Hi"}], {},
                          stream_queue=q, stream_event=threading.Event())
        sid = res["metadata"]["thread_uuid"]
        server.next_run[sid] = [
            {"frame": "update", "update": {"sessionUpdate": "agent_message_chunk",
                                           "content": {"type": "text", "text": "partial"},
                                           "messageId": "t"}},
            {"frame": "result", "result": {"stopReason": stop_reason}},
        ]
        h2 = _handler(logger, httpx.MockTransport(server.handler), rows=h._rows)
        out = h2.ask_model([{"role": "user", "content": "x"}], _resume_ctx(h2, sid),
                           stream_queue=_queue.Queue(),
                           stream_event=threading.Event())
        assert out["content"] == "partial", "partial content must be preserved"
        assert out.get("error"), f"stopReason={stop_reason} must expose an error"

    def test_end_turn_no_error(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler))
        out = h.ask_model([{"role": "user", "content": "Hi"}], {})
        assert out.get("error") is None


# ---------------------------------------------------------------------------
# cancel
# ---------------------------------------------------------------------------

class TestCancel:
    def test_cancel_resolves_prompt_as_cancelled(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler))
        q = _queue.Queue()
        holder = {}

        def run_ask():
            holder["res"] = h.ask_model([{"role": "user", "content": "Hi"}], {},
                                        stream_queue=q,
                                        stream_event=threading.Event())

        t = threading.Thread(target=run_ask, daemon=True)
        t.start()
        run_id = None
        for _ in range(100):
            chunk = q.get(timeout=10)
            if chunk["name"] == "run_id":
                run_id = chunk["value"]
                break
        session_id = run_id.split(":", 1)[1]
        # script: on cancel, goose resolves the pending prompt as cancelled
        assert h.cancel_run(run_id) is True
        assert server.cancel_calls == [session_id]
        t.join(timeout=15)
        assert not t.is_alive()

    def test_cancel_run_bad_id(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler))
        assert h.cancel_run("") is False


# ---------------------------------------------------------------------------
# permission modes
# ---------------------------------------------------------------------------

PERMISSION_RUN = [
    {"frame": "update", "update": {"sessionUpdate": "tool_call",
                                   "toolCallId": "call_p", "title": "shell rm -rf",
                                   "kind": "other", "status": "pending"}},
    {"frame": "server_request", "request_id": "perm-1",
     "method": "session/request_permission",
     "params": {"toolCall": {"toolCallId": "call_p", "kind": "other",
                             "status": "pending", "title": "shell rm -rf"},
                "options": [
                    {"optionId": "allow_always", "kind": "allow_always"},
                    {"optionId": "allow_once", "kind": "allow_once"},
                    {"optionId": "reject_once", "kind": "reject_once"},
                ]}},
    {"frame": "update", "update": {"sessionUpdate": "agent_message_chunk",
                                   "content": {"type": "text", "text": "ok"},
                                   "messageId": "t"}},
    {"frame": "result", "result": {"stopReason": "end_turn"}},
]


def _session_with_permission_run(logger, server, rows=None):
    h = _handler(logger, httpx.MockTransport(server.handler), rows=rows)
    res = h.ask_model([{"role": "user", "content": "Hi"}], {})
    sid = res["metadata"]["thread_uuid"]
    server.next_run[sid] = list(PERMISSION_RUN)
    return h, sid, _resume_ctx(h, sid)


class TestPermissions:
    def test_auto_allow_answers_allow_once(self, logger, server):
        h, sid, ctx = _session_with_permission_run(logger, server)
        h.goose_permission_mode = "auto_allow"
        out = h.ask_model([{"role": "user", "content": "x"}], ctx)
        assert out["content"] == "ok"
        answers = server.permission_answers
        assert answers, "auto_allow must POST a permission answer"
        assert answers[0]["result"]["outcome"]["optionId"] == "allow_once"

    def test_deny_answers_reject_once(self, logger, server):
        h, sid, ctx = _session_with_permission_run(logger, server)
        h.goose_permission_mode = "deny"
        out = h.ask_model([{"role": "user", "content": "x"}], ctx)
        assert out["content"] == "ok"
        answers = server.permission_answers
        assert answers and answers[0]["result"]["outcome"]["optionId"] == "reject_once"

    @pytest.mark.parametrize("policy, expected_mode", [
        ("auto_allow", "auto"),
        # deny must make goose ASK (approve mode) so the bridge can reject;
        # in auto mode goose never sends request_permission and tools run.
        ("deny", "approve"),
        ("ask", "approve"),
    ])
    def test_session_mode_sent_for_each_policy(self, logger, server, policy,
                                               expected_mode):
        modes: list[str] = []
        original = server.handler

        def spy(request):
            if request.method == "POST":
                try:
                    body = json.loads(request.content.decode() or "{}")
                except ValueError:
                    body = {}
                if body.get("method") == "session/set_mode":
                    modes.append(body["params"]["modeId"])
            return original(request)

        h = _handler(logger, httpx.MockTransport(spy),
                     setting={**SETTING, "GOOSE_PERMISSION_MODE": policy})
        assert h.goose_permission_mode == policy
        h.ask_model([{"role": "user", "content": "Hi"}], {})
        assert modes == [expected_mode]

    def test_ask_emits_approval_chunk_then_allow(self, logger, server):
        h, sid, ctx = _session_with_permission_run(logger, server)
        h.goose_permission_mode = "ask"
        h.approval_timeout = 10.0
        q = _queue.Queue()
        h._stream_queue = q

        def resolve_later():
            deadline = time.time() + 20
            run_id = None
            while time.time() < deadline:
                try:
                    chunk = q.get(timeout=0.5)
                except _queue.Empty:
                    continue
                if chunk["name"] == "approval":
                    run_id = json.loads(chunk["value"])["run_id"]
                    break
            assert run_id, "approval chunk missing"
            assert h.resolve_approval(run_id, approved=True) is True

        threading.Thread(target=resolve_later, daemon=True).start()
        out = h.ask_model([{"role": "user", "content": "x"}], ctx,
                          stream_queue=q, stream_event=threading.Event())
        assert out["content"] == "ok"
        # the worker answers goose asynchronously after resolve_approval()
        deadline = time.time() + 15
        while time.time() < deadline and not server.permission_answers:
            time.sleep(0.1)
        assert server.permission_answers, "approved permission must be POSTed"
        assert server.permission_answers[0]["result"]["outcome"]["optionId"] == "allow_once"

    def test_ask_timeout_rejects(self, logger, server):
        h, sid, ctx = _session_with_permission_run(logger, server)
        h.goose_permission_mode = "ask"
        h.approval_timeout = 1.0
        q = _queue.Queue()
        h._stream_queue = q
        out = h.ask_model([{"role": "user", "content": "x"}], ctx,
                          stream_queue=q, stream_event=threading.Event())
        assert out["content"] == "ok"
        # the worker rejects goose asynchronously after the approval timeout
        deadline = time.time() + 15
        while time.time() < deadline and not server.permission_answers:
            time.sleep(0.1)
        assert server.permission_answers, "timed-out permission must be rejected"
        assert server.permission_answers[0]["result"]["outcome"]["optionId"] == "reject_once"


# ---------------------------------------------------------------------------
# errors + prompt blocks
# ---------------------------------------------------------------------------

class TestErrorAndBlocks:
    def test_goose_404_on_session_ops(self, logger):
        def _handler404(request: httpx.Request):
            if request.method == "POST":
                try:
                    body = json.loads(request.content.decode() or "{}")
                except ValueError:
                    body = {}
                if body.get("method") != "initialize":
                    return httpx.Response(404, text="not found")
                return httpx.Response(
                    200, headers={"Acp-Connection-Id": "c1"},
                    json={"jsonrpc": "2.0", "id": body.get("id"),
                          "result": CAPS_RESULT},
                )
            return httpx.Response(200, headers={"content-type": "text/event-stream"},
                                  content=b"")

        h = _handler(logger, httpx.MockTransport(_handler404))
        out = h.ask_model([{"role": "user", "content": "Hi"}], {})
        assert out.get("error")
        assert out.get("metadata", {}).get("goose_error")

    def test_resolve_approval_no_pending(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler))
        assert h.resolve_approval("no-such", True) is False

    def test_prompt_blocks_text_only(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler))
        blocks = h._to_prompt_blocks([{"role": "user", "content": "hello"}])
        assert blocks == [{"type": "text", "text": "hello"}]

    def test_prompt_blocks_multimodal(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler))
        blocks = h._to_prompt_blocks([{
            "role": "user",
            "content": [
                {"text": "look"},
                {"file": {"bytes": "aGk=", "mimeType": "image/png"}},
                {"file": {"uri": "https://x/y.png", "name": "y.png"}},
                {"data": {"k": "v"}},
            ],
        }])
        types = [b["type"] for b in blocks]
        assert types == ["text", "image", "resource_link", "text"]
        assert blocks[1]["data"] == "aGk="
        assert json.loads(blocks[3]["text"]) == {"k": "v"}

    def test_invalid_permission_mode_falls_back(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler),
                     setting={**SETTING, "GOOSE_PERMISSION_MODE": "bogus"})
        assert h.goose_permission_mode == "auto_allow"

    def test_env_fallback_when_setting_empty(self, logger, server):
        """Older gateway builds have no GOOSE_* in settings.yaml; the handler
        falls back to os.environ (metadata -> setting -> env priority)."""
        import os as _os
        from unittest import mock

        env = {
            "GOOSE_API_URL": "http://goose-from-env:3284",
            "GOOSE_SECRET_KEY": "env-secret",
            "GOOSE_CWD": "/workspace-from-env",
            "GOOSE_PERMISSION_MODE": "deny",
        }
        with mock.patch.dict(_os.environ, env, clear=False):
            # setting WITHOUT any goose keys (simulates gateway env contract gap)
            h = _handler(logger, httpx.MockTransport(server.handler),
                         setting={"GOOSE_API_URL": None, "GOOSE_SECRET_KEY": None})
        assert h.goose_api_url == "http://goose-from-env:3284"
        assert h.goose_secret_key == "env-secret"
        assert h.goose_cwd == "/workspace-from-env"
        assert h.goose_permission_mode == "deny"

    def test_metadata_cannot_override_env_secret(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler),
                     metadata={"goose_secret_key": "from-metadata"})
        # Setting wins over metadata so tenant-writable metadata cannot pick
        # up the goose server secret.
        assert h.goose_secret_key == "k1"


# ---------------------------------------------------------------------------
# active-run retry
# ---------------------------------------------------------------------------

class TestActiveRunRetry:
    def test_active_run_error_retries_after_cancel(self, logger, server):
        h = _handler(logger, httpx.MockTransport(server.handler))
        res = h.ask_model([{"role": "user", "content": "Hi"}], {})
        sid = res["metadata"]["thread_uuid"]
        server.text_run(sid, "recovered")

        calls = {"n": 0}
        original_prompt = GooseAcpClient.prompt

        client = GooseAcpClient(
            logger, h.goose_api_url, secret_key=h.goose_secret_key,
            request_permission_callback=h._on_permission_request,
            http_transport=h.http_transport,
        )
        h._client = client
        client.initialize()
        client.start_streams()

        def flaky_prompt(session_id, blocks, timeout=None, on_update=None):
            calls["n"] += 1
            if calls["n"] == 1:
                raise GooseAcpError(
                    "ACP session/prompt failed: Invalid params", rpc_code=-32602,
                )
            return original_prompt(client, session_id, blocks,
                                   timeout=timeout, on_update=on_update)

        # instance-level patch avoids descriptor binding subtleties
        client.prompt = flaky_prompt

        result, _updates = h._prompt_with_retry(
            client, sid, [{"type": "text", "text": "hi"}], None,
        )
        assert calls["n"] == 2
        assert (result.get("result") or {}).get("stopReason") == "end_turn"