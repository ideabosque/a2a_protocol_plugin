#!/usr/bin/python
"""
omp Agent Handler — Unit Test Suite

Tests for OmpAgentHandler (the omp / oh-my-pi sibling of the pi bridge) on
top of httpx.MockTransport — no real HTTP calls, no omp installation.

The event frames follow the omp RPC protocol as captured from a REAL omp
18.18.2 run — tests/omp_rpc_transcript.json (captured by the
docker-a2a-gateway omp spike; the raw capture lives in
docker-a2a-gateway/docs/test_results/omp_review/spike_transcript.json):
ready frame, ``negotiate_protocol`` v2, ``text_delta`` / ``thinking_delta``
deltas, ``tool_execution_*`` frames, the native approval
``extension_ui_request`` (``method: "select"`` with omp's
Approve / Approve <tool> for this session / Deny options), the
``prompt_result`` completion frames and the local-slash-command answer
(``data.agentInvoked == false``).

Covers: the conversation-key → sessionId mapping (``omp_session#``), tenant
isolation, answer vs tagged reasoning, tool events, abort (CANCELED) and
provider failure (FAILED), the busy-session policy, the approval policy per
permission mode (auto_allow / deny / ask + timeout), the secret precedence
and the transport failures (401 / 404 / 409 / dropped stream).

Run with: python -m pytest a2a_protocol_plugin/tests/test_omp_handler.py -v
"""

import json
import logging
import queue as _queue
import threading
import time

import httpx
import pytest

from a2a_protocol_plugin.handlers.omp_handler import OmpAgentHandler
from a2a_protocol_plugin.handlers.pi_client import PiSidecarClient, PiSidecarError

__author__ = "bibow"


# ---------------------------------------------------------------------------
# Frame helpers (omp RPC shapes — see tests/omp_rpc_transcript.json)
# ---------------------------------------------------------------------------
def _text_delta(text: str, content_index: int = 0) -> dict:
    return {
        "type": "message_update",
        "assistantMessageEvent": {
            "type": "text_delta", "contentIndex": content_index, "delta": text,
        },
    }


def _thinking_delta(text: str, content_index: int = 0) -> dict:
    return {
        "type": "message_update",
        "assistantMessageEvent": {
            "type": "thinking_delta", "contentIndex": content_index,
            "delta": text,
        },
    }


def _thinking_end(text: str, content_index: int = 0) -> dict:
    """Authoritative full-block copy — must never be re-emitted."""
    return {
        "type": "message_update",
        "assistantMessageEvent": {
            "type": "thinking_end", "contentIndex": content_index,
            "content": text,
        },
    }


def _tool_start(tool_call_id: str, tool_name: str, args: dict | None = None) -> dict:
    return {
        "type": "tool_execution_start",
        "toolCallId": tool_call_id, "toolName": tool_name, "args": args or {},
    }


def _tool_end(tool_call_id: str, tool_name: str, output: str,
              is_error: bool = False) -> dict:
    return {
        "type": "tool_execution_end",
        "toolCallId": tool_call_id, "toolName": tool_name,
        "result": {"content": [{"type": "text", "text": output}], "details": {}},
        "isError": is_error,
    }


def _approval(dialog_id: str, **extra) -> dict:
    """omp's native tool approval — a `select` dialog (no sessionId, the
    details ride the title)."""
    event = {
        "type": "extension_ui_request",
        "id": dialog_id,
        "method": "select",
        "title": "Allow tool: bash\nCommand: echo spike-tool",
        "options": ["Approve", "Approve bash for this session", "Deny"],
    }
    event.update(extra)
    return event


def _settled() -> dict:
    return {"type": "session_settled"}


def _prompt_result(status: str = "completed", **extra) -> dict:
    frame = {
        "type": "prompt_result", "id": "req_1", "agentInvoked": True,
        "status": status, "sessionSettled": True,
    }
    frame.update(extra)
    return frame


RUN_END_DEFAULT = {
    "type": "run_end",
    "aborted": False,
    "stopReason": "end_turn",
    "usage": {"input": 11, "output": 7, "totalTokens": 18},
}


# ---------------------------------------------------------------------------
# Mock sidecar
# ---------------------------------------------------------------------------
class MockOmpSidecar:
    """A scripted omp sidecar behind httpx.MockTransport (HTTP + SSE).

    ``script_run(session_id, steps)`` queues the frames of the next prompt
    run. Steps are::

      {"event": <omp event dict>}            — forwarded SSE frame
      {"event": {...}, "wait_ui": seconds}  — emit, then poll until a
            ui-response for this event id is recorded (max wait_ui)
      {"sleep": seconds}
      {"run_end": {...}}                    — custom final frame

    A ``run_end`` (default ``RUN_END_DEFAULT``) is appended automatically.

    Behaviour flags:
      busy_once   — the next prompt for any session answers 409 once
      not_found   — every request answers 404 (lost session)
      auth_reject — every request answers 401
    """

    def __init__(self, secret_key: str = "k1"):
        self.secret_key = secret_key
        self.auth_reject = False
        self.busy_once = False
        self.not_found = False
        self.next_runs: dict[str, list[dict]] = {}
        # Steps used for a session that was not scripted explicitly (the
        # sidecar mints the session id, so a first turn cannot be scripted
        # by id).
        self.default_steps: list[dict] = []
        # observability
        self.creates: list[dict] = []
        self.prompts: list[tuple[str, dict]] = []
        self.aborts: list[str] = []
        self.ui_answers: list[dict] = []
        self.deletes: list[str] = []
        self.gets: list[str] = []
        self.sessions: dict[str, dict] = {}
        self.resume_index: dict[str, dict] = {}
        self._seq = 0

    def new_session_id(self) -> str:
        self._seq += 1
        return f"omp-sid-{self._seq:04d}"

    def script_run(self, session_id: str, steps: list[dict]) -> None:
        self.next_runs[session_id] = list(steps)

    def _wait_ui(self, dialog_id: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for answer in self.ui_answers:
                if (answer.get("payload") or {}).get("id") == dialog_id:
                    return
            time.sleep(0.02)

    def _sse_chunks(self, sid: str, steps: list[dict]):
        for step in steps:
            if "sleep" in step:
                time.sleep(float(step["sleep"]))
                continue
            event = step.get("event")
            if event is None and "run_end" in step:
                yield f"data: {json.dumps(step['run_end'])}\n\n".encode()
                return
            yield f"data: {json.dumps(event)}\n\n".encode()
            if step.get("wait_ui"):
                self._wait_ui((event or {}).get("id", ""),
                              float(step["wait_ui"]))
            if "run_end" in step:
                yield f"data: {json.dumps(step['run_end'])}\n\n".encode()
                return
        yield f"data: {json.dumps(dict(RUN_END_DEFAULT, sessionId=sid))}\n\n".encode()

    # ------------------------------------------------------------------
    # HTTP handler
    # ------------------------------------------------------------------
    def handler(self, request: httpx.Request) -> httpx.Response:
        if self.auth_reject:
            return httpx.Response(401, json={"error": "invalid secret"})

        key = request.headers.get("X-Secret-Key", "")
        if self.secret_key and key != self.secret_key:
            return httpx.Response(401, json={"error": "invalid secret"})

        path = request.url.path
        method = request.method

        if method == "GET" and path == "/health":
            return httpx.Response(200, json={"status": "ok", "omp": "18.18.2"})

        if not path.startswith("/sessions"):
            return httpx.Response(404, json={"error": f"no route {path}"})

        parts = [p for p in path.split("/") if p][1:]  # drop "sessions"
        sid = parts[0] if parts else ""

        if method == "POST" and not parts:
            body = json.loads(request.content.decode() or "{}")
            self.creates.append(body)
            new_sid = self.new_session_id()
            self.sessions[new_sid] = {
                "sessionFile": f"/data/sessions/{new_sid}.jsonl",
                "state": {
                    "sessionId": new_sid, "isStreaming": False,
                    "isSettled": True, "model": {"provider": "mock",
                                                 "id": "mock-model"},
                },
            }
            return httpx.Response(200, json={
                "sessionId": new_sid,
                "sessionFile": self.sessions[new_sid]["sessionFile"],
            })

        if method == "GET" and len(parts) == 1:
            self.gets.append(sid)
            if self.not_found:
                return httpx.Response(404, json={"error": f"unknown session {sid}"})
            if sid not in self.sessions and sid in self.resume_index:
                self.sessions[sid] = dict(self.resume_index[sid])
            if sid not in self.sessions:
                return httpx.Response(404, json={"error": f"unknown session {sid}"})
            return httpx.Response(200, json=self.sessions[sid])

        if method == "DELETE" and len(parts) == 1:
            self.deletes.append(sid)
            if sid in self.sessions:
                self.resume_index[sid] = dict(self.sessions[sid])
            self.sessions.pop(sid, None)
            return httpx.Response(204)

        if method == "POST" and len(parts) == 2 and parts[1] == "abort":
            self.aborts.append(sid)
            if self.not_found:
                return httpx.Response(404, json={"error": "unknown session"})
            return httpx.Response(200, json={"aborted": True})

        if method == "POST" and len(parts) == 2 and parts[1] == "ui-response":
            payload = json.loads(request.content.decode() or "{}")
            self.ui_answers.append({"session": sid, "payload": payload})
            return httpx.Response(200, json={"ok": True})

        if method == "POST" and len(parts) == 2 and parts[1] == "prompt":
            body = json.loads(request.content.decode() or "{}")
            self.prompts.append((sid, body))
            if self.not_found:
                return httpx.Response(404, json={"error": f"unknown session {sid}"})
            if self.busy_once:
                self.busy_once = False
                return httpx.Response(409, json={"error": "session already streaming"})
            steps = self.next_runs.get(sid, self.default_steps)
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=self._sse_chunks(sid, steps),
            )

        return httpx.Response(405, json={"error": "method not allowed"})


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------
@pytest.fixture
def logger():
    return logging.getLogger("test-omp")


@pytest.fixture
def server():
    return MockOmpSidecar()


@pytest.fixture
def transport(server):
    return httpx.MockTransport(server.handler)


SETTING = {
    "OMP_API_URL": "http://omp.test",
    "OMP_SECRET_KEY": "k1",
    "OMP_CWD": "/workspace",
    # fail fast if a scripted run never arrives
    "OMP_STREAM_TIMEOUT": "25",
}

AGENT_CONFIG = {
    "agent_id": "omp-agent",
    "metadata": {"module_name": "a2a_protocol_plugin.handlers.omp_handler"},
}


def _handler(logger, transport, setting=None, metadata=None, rows=None,
             server=None):
    """Build an OmpAgentHandler over the mock transport with an in-memory
    partition-scoped a2a_settings repo shared via ``rows``."""
    cfg = dict(AGENT_CONFIG)
    cfg["metadata"] = {**(AGENT_CONFIG["metadata"]), **(metadata or {})}
    h = OmpAgentHandler(logger=logger, agent_config=cfg,
                        setting=setting if setting is not None else dict(SETTING),
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


def _mapped(h, sid: str, partition: str = "ep#x", context_id: str = "ctx-1",
            ) -> dict:
    """Store a mapping for ``sid`` and return the matching context."""
    endpoint_id, _, part_id = partition.partition("#")
    h._store_session_mapping(partition, endpoint_id, part_id, context_id, sid)
    return {"partition_key": partition, "thread_uuid": context_id}


def _drain(q: _queue.Queue, timeout: float = 6.0) -> list[dict]:
    items = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            items.append(q.get(timeout=0.2))
        except _queue.Empty:
            break
    return items


def _names(chunks: list[dict]) -> list[str]:
    return [c.get("name") for c in chunks]


# ---------------------------------------------------------------------------
# Transport-level tests
# ---------------------------------------------------------------------------
class TestTransport:
    def test_health_and_session_create(self, logger, server, transport):
        client = PiSidecarClient(logger, "http://omp.test", secret_key="k1",
                                 http_transport=transport, label="omp")
        health = client.health()
        assert health["status"] == "ok"
        assert health["omp"] == "18.18.2"
        session = client.create_session(cwd="/workspace", model="compat/m1")
        assert session["sessionId"].startswith("omp-sid-")
        assert server.creates == [{"cwd": "/workspace", "model": "compat/m1"}]
        client.close()

    def test_wrong_secret_401(self, logger, server, transport):
        client = PiSidecarClient(logger, "http://omp.test", secret_key="nope",
                                 http_transport=transport, label="omp")
        try:
            with pytest.raises(PiSidecarError) as error:
                client.health()
            assert error.value.status_code == 401
        finally:
            client.close()

    def test_unknown_session_404(self, logger, server, transport):
        client = PiSidecarClient(logger, "http://omp.test", secret_key="k1",
                                 http_transport=transport, label="omp")
        try:
            with pytest.raises(PiSidecarError) as error:
                client.get_session("omp-sid-unknown")
            assert error.value.status_code == 404
        finally:
            client.close()

    def test_transport_error_message_names_the_dialect(self, logger):
        client = PiSidecarClient(
            logger, "http://omp.test", secret_key="k1",
            http_transport=httpx.MockTransport(
                lambda request: httpx.Response(401, json={"error": "nope"})
            ),
            label="omp",
        )
        try:
            with pytest.raises(PiSidecarError) as error:
                client.health()
            assert "omp sidecar" in str(error.value)
            assert "OMP_SIDECAR_SECRET_KEY" in str(error.value)
        finally:
            client.close()


# ---------------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------------
class TestConfig:
    def test_metadata_prefix_and_defaults(self, logger, server, transport):
        h = _handler(logger, transport, server=server)
        assert h.api_url == "http://omp.test"
        assert h.cwd == "/workspace"
        assert h.permission_mode == "auto_allow"
        assert h.busy_behavior == "followUp"
        assert h.timeout == 25.0
        assert h.approval_timeout == 120.0

    def test_default_api_url_without_setting(self, logger, server, transport):
        h = _handler(logger, transport, setting={}, server=server)
        assert h.api_url == "http://localhost:3310"

    def test_metadata_cannot_override_the_secret(self, logger, server,
                                                  transport):
        h = _handler(logger, transport,
                     metadata={"omp_secret_key": "attacker"}, server=server)
        assert h.secret_key == "k1"

    def test_env_fallback(self, logger, server, transport, monkeypatch):
        monkeypatch.setenv("OMP_API_URL", "http://omp.env")
        monkeypatch.setenv("OMP_CWD", "/env/workspace")
        h = _handler(logger, transport, setting={}, server=server)
        assert h.api_url == "http://omp.env"
        assert h.cwd == "/env/workspace"

    def test_unknown_thinking_level_is_dropped(self, logger, server, transport):
        h = _handler(logger, transport, metadata={"omp_thinking": "loud"},
                     server=server)
        assert h.thinking == ""

    def test_known_thinking_level_is_kept(self, logger, server, transport):
        h = _handler(logger, transport, metadata={"omp_thinking": "high"},
                     server=server)
        assert h.thinking == "high"


# ---------------------------------------------------------------------------
# Conversation-key mapping
# ---------------------------------------------------------------------------
class TestContextMapping:
    def test_first_turn_stores_omp_session_mapping(self, logger, server,
                                                    transport):
        server.default_steps = [{"event": _text_delta("hello")}]
        h = _handler(logger, transport, server=server)
        ctx = {"partition_key": "ep#x", "thread_uuid": "ctx-1"}
        result = h.ask_model([{"role": "user", "content": "go"}], ctx)
        assert result["content"] == "hello"
        stored = h._rows[("ep#x", "omp_session#ctx-1")]["setting"]
        assert stored["session_id"] == server.prompts[0][0]
        assert stored["context_id"] == "ctx-1"

    def test_resume_reuses_the_mapped_session(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [{"event": _text_delta("second turn")}])
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        result = h.ask_model([{"role": "user", "content": "go"}], ctx)
        assert result["content"] == "second turn"
        # the mapped session was probed, not re-created
        assert server.creates == []
        assert sid in server.gets

    def test_client_supplied_session_is_never_forwarded(self, logger, server,
                                                         transport):
        h = _handler(logger, transport, server=server)
        ctx = {
            "partition_key": "ep#x", "thread_uuid": "ctx-1",
            "session_id": "/etc/passwd", "sessionFile": "/rogue.jsonl",
        }
        h.ask_model([{"role": "user", "content": "go"}], ctx)
        # only the sidecar-minted session id is used
        assert all("rogue" not in str(body) for _sid, body in server.prompts)
        assert server.creates and server.creates[0].get("cwd") == "/workspace"

    def test_cross_tenant_key_is_isolated(self, logger, server, transport):
        rows = {}
        server.default_steps = [{"event": _text_delta("tenant a")}]
        h_a = _handler(logger, transport, rows=rows, server=server)
        h_a.ask_model([{"role": "user", "content": "go"}],
                      {"partition_key": "ep#p1", "thread_uuid": "thread-1"})

        server.default_steps = [{"event": _text_delta("tenant b")}]
        h_b = _handler(logger, transport, rows=rows, server=server)
        result = h_b.ask_model([{"role": "user", "content": "go"}],
                               {"partition_key": "ep#p2", "thread_uuid": "thread-1"})
        # the same conversation key in another partition starts a NEW session
        assert result["content"] == "tenant b"
        assert ("ep#p1", "omp_session#thread-1") in rows
        assert ("ep#p2", "omp_session#thread-1") in rows
        assert rows[("ep#p1", "omp_session#thread-1")]["setting"]["session_id"] \
            != rows[("ep#p2", "omp_session#thread-1")]["setting"]["session_id"]


# ---------------------------------------------------------------------------
# Streaming mapping
# ---------------------------------------------------------------------------
class TestStreamingMapping:
    def test_run_id_chunk_comes_first(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [{"event": _text_delta("hi")}])
        h = _handler(logger, transport, server=server)
        q = _queue.Queue()
        h.ask_model([{"role": "user", "content": "go"}], _mapped(h, sid),
                    stream_queue=q, stream_event=threading.Event())
        chunks = _drain(q)
        assert chunks[0]["name"] == "run_id"
        assert chunks[0]["value"].startswith(f"{sid}:")

    def test_answer_and_reasoning_are_separated(self, logger, server,
                                                 transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _thinking_delta("plan the work")},
            {"event": _text_delta("answer part 1, ")},
            {"event": _text_delta("part 2")},
            {"event": _thinking_end("plan the work")},
        ])
        h = _handler(logger, transport, server=server)
        q = _queue.Queue()
        result = h.ask_model([{"role": "user", "content": "go"}],
                             _mapped(h, sid), stream_queue=q,
                             stream_event=threading.Event())
        chunks = _drain(q)
        tokens = [c for c in chunks if c["name"] == "token" and "meta" not in c]
        reasoning = [c for c in chunks
                     if c.get("meta", {}).get("data_format") == "reasoning"]
        assert "".join(c["value"] for c in tokens) == "answer part 1, part 2"
        assert result["content"] == "answer part 1, part 2"
        assert [c["value"] for c in reasoning] == ["plan the work"]

    def test_tool_call_and_result_chunks(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _tool_start("call_1", "bash", {"command": "ls"})},
            {"event": _tool_end("call_1", "bash", "file.txt")},
            {"event": _text_delta("done")},
        ])
        h = _handler(logger, transport, server=server)
        q = _queue.Queue()
        h.ask_model([{"role": "user", "content": "go"}], _mapped(h, sid),
                    stream_queue=q, stream_event=threading.Event())
        chunks = _drain(q)
        calls = [c for c in chunks if c["name"] == "tool_call"]
        results = [c for c in chunks if c["name"] == "tool_result"]
        assert len(calls) == 1 and len(results) == 1
        assert json.loads(calls[0]["value"])["toolName"] == "bash"
        assert json.loads(results[0]["value"])["output"] == "file.txt"

    def test_unknown_omp_frames_never_become_tokens(self, logger, server,
                                                     transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": {"type": "queue_update", "steering": [],
                       "followUp": ["later"], "liveSteered": 0}},
            {"event": {"type": "command_output", "text": "Usage\nCost: $0"}},
            {"event": {"type": "tool_execution_update", "toolCallId": "c1",
                       "toolName": "bash", "partialResult": {"content": []}}},
            {"event": _settled()},
            {"event": _text_delta("only this")},
        ])
        h = _handler(logger, transport, server=server)
        q = _queue.Queue()
        result = h.ask_model([{"role": "user", "content": "go"}],
                             _mapped(h, sid), stream_queue=q,
                             stream_event=threading.Event())
        chunks = _drain(q)
        tokens = [c for c in chunks if c["name"] == "token"]
        assert "".join(c["value"] for c in tokens) == "only this"
        assert result["content"] == "only this"

    def test_stream_drop_yields_error_result(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.next_runs[sid] = [{"run_end": None}]  # stream drops w/o run_end
        h = _handler(logger, transport, server=server)
        result = h.ask_model([{"role": "user", "content": "go"}],
                             _mapped(h, sid))
        assert "without run_end" in result["error"]


# ---------------------------------------------------------------------------
# Run outcomes
# ---------------------------------------------------------------------------
class TestRunOutcomes:
    def test_aborted_run_is_reported_as_aborted(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _text_delta("partial")},
            {"run_end": {"type": "run_end", "aborted": True,
                         "stopReason": "aborted"}},
        ])
        h = _handler(logger, transport, server=server)
        result = h.ask_model([{"role": "user", "content": "go"}],
                             _mapped(h, sid))
        assert result["metadata"]["omp"]["aborted"] is True
        assert result["error"] == "Run aborted"

    def test_provider_failure_is_reported_as_error(self, logger, server,
                                                    transport):
        """omp's prompt_result.status == "error" reaches the plugin as the
        sidecar's run_end.error."""
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"run_end": {
                "type": "run_end", "aborted": False, "stopReason": "error",
                "error": "400 spike mock provider failure",
            }},
        ])
        h = _handler(logger, transport, server=server)
        result = h.ask_model([{"role": "user", "content": "go"}],
                             _mapped(h, sid))
        assert result["error"] == "400 spike mock provider failure"
        assert result["metadata"]["omp"]["stopReason"] == "error"

    def test_cancel_aborts_the_sidecar_session(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [{"event": _text_delta("running")}])
        h = _handler(logger, transport, server=server)
        assert h.cancel_run(f"{sid}:1") is True
        assert server.aborts == [sid]


# ---------------------------------------------------------------------------
# Busy policy
# ---------------------------------------------------------------------------
class TestBusyPolicy:
    def _busy_test(self, logger, behavior):
        server = MockOmpSidecar()
        transport = httpx.MockTransport(server.handler)
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [{"event": _text_delta("queued")},
                                {"event": _settled()}])
        server.busy_once = True
        h = _handler(logger, transport,
                     metadata={"omp_busy_behavior": behavior}, server=server)
        q = _queue.Queue()
        result = h.ask_model([{"role": "user", "content": "go"}],
                             _mapped(h, sid), stream_queue=q,
                             stream_event=threading.Event())
        chunks = _drain(q)
        return result, chunks, server

    def test_busy_follow_up_resubmits_with_streaming_behavior(self, logger):
        result, _chunks, server = self._busy_test(logger, "followUp")
        assert result["content"] == "queued"
        assert len(server.prompts) == 2
        assert server.prompts[0][1].get("streamingBehavior") is None
        assert server.prompts[1][1]["streamingBehavior"] == "followUp"

    def test_busy_steer_resubmits_with_steer(self, logger):
        _result, _chunks, server = self._busy_test(logger, "steer")
        assert len(server.prompts) == 2
        assert server.prompts[1][1]["streamingBehavior"] == "steer"

    def test_busy_reject_surfaces_error(self, logger):
        result, chunks, server = self._busy_test(logger, "reject")
        errors = [c for c in chunks if c["name"] == "error"]
        assert len(errors) == 1
        assert "409" in result["error"]


# ---------------------------------------------------------------------------
# Approval policy (omp's native tool approval)
# ---------------------------------------------------------------------------
class TestApprovalPolicy:
    def test_auto_allow_answers_with_the_approve_option(self, logger, server,
                                                         transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _approval("d1"), "wait_ui": 5.0},
            {"event": _text_delta("approved and running")},
        ])
        h = _handler(logger, transport,
                     metadata={"omp_permission_mode": "auto_allow"},
                     server=server)
        result = h.ask_model([{"role": "user", "content": "go"}],
                             _mapped(h, sid), stream_queue=_queue.Queue(),
                             stream_event=threading.Event())
        assert result["content"] == "approved and running"
        answers = {a["payload"]["id"]: a["payload"] for a in server.ui_answers}
        assert answers["d1"]["value"] == "Approve"

    def test_deny_answers_with_the_deny_option(self, logger, server,
                                                transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _approval("d1"), "wait_ui": 5.0},
            {"event": _text_delta("denied")},
        ])
        h = _handler(logger, transport, metadata={"omp_permission_mode": "deny"},
                     server=server)
        h.ask_model([{"role": "user", "content": "go"}], _mapped(h, sid),
                    stream_queue=_queue.Queue(), stream_event=threading.Event())
        answers = {a["payload"]["id"]: a["payload"] for a in server.ui_answers}
        assert answers["d1"]["value"] == "Deny"

    def test_configured_label_lists_are_honoured(self, logger, server,
                                                 transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _approval("d1", options=["Oui", "Non"]), "wait_ui": 5.0},
            {"event": _text_delta("ok")},
        ])
        h = _handler(logger, transport,
                     metadata={"omp_permission_mode": "auto_allow",
                               "omp_approval_allow_labels": "oui"},
                     server=server)
        h.ask_model([{"role": "user", "content": "go"}], _mapped(h, sid),
                    stream_queue=_queue.Queue(), stream_event=threading.Event())
        answers = {a["payload"]["id"]: a["payload"] for a in server.ui_answers}
        assert answers["d1"]["value"] == "Oui"

    def test_unclassifiable_options_are_cancelled(self, logger, server,
                                                  transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _approval("d1", options=["Maybe", "Whatever"]),
             "wait_ui": 5.0},
            {"event": _text_delta("ok")},
        ])
        h = _handler(logger, transport,
                     metadata={"omp_permission_mode": "auto_allow"},
                     server=server)
        h.ask_model([{"role": "user", "content": "go"}], _mapped(h, sid),
                    stream_queue=_queue.Queue(), stream_event=threading.Event())
        answers = {a["payload"]["id"]: a["payload"] for a in server.ui_answers}
        assert answers["d1"]["cancelled"] is True

    def test_ask_emits_approval_then_answers_with_approve(self, logger, server,
                                                           transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _approval("d1"), "wait_ui": 8.0},
            {"event": _text_delta("resumed after approval")},
        ])
        h = _handler(logger, transport,
                     metadata={"omp_permission_mode": "ask",
                               "omp_approval_timeout": "5"},
                     server=server)
        q = _queue.Queue()
        worker = threading.Thread(
            target=lambda: h.ask_model([{"role": "user", "content": "go"}],
                                       _mapped(h, sid), stream_queue=q,
                                       stream_event=threading.Event()),
            daemon=True,
        )
        worker.start()
        chunks = []
        approvals = []
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not approvals:
            try:
                c = q.get(timeout=0.3)
                chunks.append(c)
                if c["name"] == "approval":
                    approvals.append(c)
            except _queue.Empty:
                pass
        assert len(approvals) == 1
        approval = json.loads(approvals[0]["value"])
        assert approval["session_id"] == sid
        assert approval["method"] == "select"
        assert approval["options"] == ["Approve", "Approve bash for this session",
                                       "Deny"]
        # omp carries the approval details in the title (no `message` field)
        assert "echo spike-tool" in approval["message"]
        run_id = next(c["value"] for c in chunks if c["name"] == "run_id")
        assert h.resolve_approval(run_id, True) is True
        worker.join(timeout=12)
        answers = {a["payload"]["id"]: a["payload"] for a in server.ui_answers}
        assert answers["d1"]["value"] == "Approve"

    def test_ask_timeout_fails_closed_with_the_deny_option(self, logger,
                                                            server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _approval("d1"), "wait_ui": 6.0},
            {"event": _text_delta("continued")},
        ])
        h = _handler(logger, transport,
                     metadata={"omp_permission_mode": "ask",
                               "omp_approval_timeout": "0.3"},
                     server=server)
        h.ask_model([{"role": "user", "content": "go"}], _mapped(h, sid),
                    stream_queue=_queue.Queue(), stream_event=threading.Event())
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline and not server.ui_answers:
            time.sleep(0.05)
        answers = {a["payload"]["id"]: a["payload"] for a in server.ui_answers}
        assert answers["d1"]["value"] == "Deny"

    def test_fire_and_forget_ui_is_ignored(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": {"type": "extension_ui_request", "id": "w1",
                       "method": "setWidget", "widgetKey": "autoresearch"}},
            {"event": _text_delta("ok")},
        ])
        h = _handler(logger, transport, server=server)
        h.ask_model([{"role": "user", "content": "go"}], _mapped(h, sid),
                    stream_queue=_queue.Queue(), stream_event=threading.Event())
        time.sleep(0.3)
        assert server.ui_answers == []


# ---------------------------------------------------------------------------
# Bridge-level contract (reasoning switch)
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_a2a_core():
    from unittest.mock import AsyncMock

    core = AsyncMock()
    core.get_a2a_agent = AsyncMock(return_value={
        "agent_id": "test-agent-001",
        "agent_name": "Test Agent",
        "metadata": '{"module_name": "mock", "class_name": "MockHandler"}',
    })
    core.insert_update_a2a_thread = AsyncMock(return_value={"thread_id": "t-1"})
    core.insert_update_a2a_run = AsyncMock(return_value={"run_id": "r-1"})
    core.insert_update_a2a_message = AsyncMock(return_value={"message_id": "m-1"})
    return core


@pytest.mark.asyncio
class TestBridgeStreamingContract:
    """The omp handler feeds the shared bridge; verify reasoning stays out of
    the answer, is returned as metadata.reasoning, and drops with
    show_reasoning=False (same assertions as
    test_phase10.TestStreamingShowReasoning)."""

    def _script(self, server, h):
        h_sid = server.new_session_id()
        server.sessions[h_sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(h_sid, [
            {"event": _thinking_delta("Think first.")},
            {"event": _text_delta("Answer.")},
        ])
        h._store_session_mapping("ep#part", "ep", "part", "t-1", h_sid)

    async def _run(self, mock_a2a_core, mock_logger, show_reasoning, server,
                   transport):
        from unittest.mock import MagicMock, patch

        from a2a_protocol_plugin.handlers.a2a_ai_agent_utility import (
            execute_ai_agent_streaming,
        )
        rows = {}
        scripted = _handler(mock_logger, transport, rows=rows, server=server)
        self._script(server, scripted)  # mapping stored in the shared rows
        with patch(
            "a2a_protocol_plugin.handlers.a2a_ai_agent_utility.Config.a2a_core",
            mock_a2a_core,
        ), patch(
            "a2a_protocol_plugin.handlers.a2a_ai_agent_utility.load_agent_handler",
            return_value=_handler(mock_logger, transport, rows=rows, server=server),
        ):
            return await execute_ai_agent_streaming(
                partition_key="ep#part",
                agent_uuid="test-agent-001",
                user_query="hi",
                event_queue=MagicMock(),
                streaming_manager=MagicMock(),
                thread_uuid="t-1",
                stream_timeout=10.0,
                logger=mock_logger,
                show_reasoning=show_reasoning,
            )

    async def test_reasoning_returned_when_on(self, mock_a2a_core, mock_logger,
                                              server, transport):
        result = await self._run(mock_a2a_core, mock_logger, True, server,
                                 transport)
        assert result.content == "Answer."
        assert result.metadata["reasoning"] == "Think first."

    async def test_reasoning_dropped_when_off(self, mock_a2a_core, mock_logger,
                                              server, transport):
        result = await self._run(mock_a2a_core, mock_logger, False, server,
                                 transport)
        assert result.content == "Answer."
        assert "reasoning" not in result.metadata
