#!/usr/bin/python
"""
pi Agent Handler — Unit Test Suite

Tests for PiAgentHandler + PiSidecarClient using httpx.MockTransport
(no real HTTP calls and no pi installation). Event frames follow the pi RPC
event stream as captured from a REAL pi sidecar run —
tests/pi_rpc_transcript.json (captured 2026-10-25 with the
docker-a2a-gateway tests/pi/test_spike_protocol.py P1 spike against pi
v1.4.0 / compat/glm-5.2: text/thinking deltas, bash tool call, 409 +
followUp ack, abort, index-based session resume). Re-run the spike when the
pinned pi version changes (docker-a2a-gateway `pi/` sibling).

Covers: transport (health / auth / session lifecycle), streaming mapping
(``run_id`` first, ``text_delta`` → token, ``thinking_delta`` → tagged
reasoning, authoritative ``*_end`` copies never re-emitted, tool events),
the extension-UI permission modes (auto_allow / deny / ask + timeout),
busy-session policy (followUp / steer / reject), the conversation-key →
sessionId mapping (resume + never trusting client-supplied session paths),
abort/cancel, stream-drop, and a bridge-level proof of the shared streaming
contract (reasoning kept out of the answer; ``show_reasoning`` switch).

Run with: python -m pytest a2a_protocol_plugin/tests/test_pi_handler.py -v
"""

import json
import logging
import queue as _queue
import threading
import time

import httpx
import pytest

from a2a_protocol_plugin.handlers.pi_client import PiSidecarClient, PiSidecarError
from a2a_protocol_plugin.handlers.pi_handler import PiAgentHandler

__author__ = "bibow"


# ---------------------------------------------------------------------------
# Frame helpers (pi event shapes per docs/json.md)
# ---------------------------------------------------------------------------
def _text_delta(text: str, content_index: int = 0) -> dict:
    return {
        "type": "message_update",
        "usage": {},
        "assistantMessageEvent": {
            "type": "text_delta", "contentIndex": content_index, "delta": text,
        },
    }


def _thinking_delta(text: str, content_index: int = 0) -> dict:
    return {
        "type": "message_update",
        "usage": {},
        "assistantMessageEvent": {
            "type": "thinking_delta", "contentIndex": content_index, "delta": text,
        },
    }


def _thinking_end(text: str, content_index: int = 0) -> dict:
    """Authoritative full-block copy — must never be re-emitted."""
    return {
        "type": "message_update",
        "usage": {},
        "assistantMessageEvent": {
            "type": "thinking_end", "contentIndex": content_index, "content": text,
        },
    }


def _message_end(text: str) -> dict:
    """Authoritative final message — must never be re-emitted as tokens."""
    return {
        "type": "message_end",
        "message": {"role": "assistant", "content": [{"type": "text", "text": text}]},
    }


def _tool_start(tool_call_id: str, tool_name: str, args: dict | None = None) -> dict:
    return {
        "type": "tool_execution_start",
        "toolCallId": tool_call_id, "toolName": tool_name, "args": args or {},
    }


def _tool_end(tool_call_id: str, tool_name: str, output: str, is_error=False) -> dict:
    return {
        "type": "tool_execution_end",
        "toolCallId": tool_call_id, "toolName": tool_name,
        "result": {"content": [{"type": "text", "text": output}]},
        "isError": is_error, "durationMs": 12,
    }


def _error_event(reason: str = "overloaded_provider") -> dict:
    return {
        "type": "message_update",
        "usage": {},
        "assistantMessageEvent": {"type": "error", "reason": reason, "error": reason},
    }


def _settled(aborted: bool = False) -> dict:
    return {"type": "agent_settled", "aborted": aborted}


def _dialog(method: str, dialog_id: str, **extra) -> dict:
    # Real pi wire shape (P1 spike): NO sessionId — the handler maps the
    # dialog to its current session itself.
    event = {
        "type": "extension_ui_request", "id": dialog_id, "method": method,
        "title": f"{method} dialog",
    }
    event.update(extra)
    return event


RUN_END_DEFAULT = {
    "type": "run_end",
    "aborted": False,
    "stopReason": "end_turn",
    "usage": {"input": 11, "output": 7, "totalTokens": 18},
}


# ---------------------------------------------------------------------------
# Mock sidecar
# ---------------------------------------------------------------------------
class MockPiSidecar:
    """A scripted pi sidecar behind httpx.MockTransport (HTTP + SSE).

    ``script_run(session_id, steps)`` queues the frames for the next prompt
    run of a session. Steps are:

      {"event": <pi event dict>}            — forwarded SSE frame
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
        # observability
        self.creates: list[dict] = []
        self.prompts: list[tuple[str, dict]] = []
        self.aborts: list[str] = []
        self.ui_answers: list[dict] = []
        self.deletes: list[str] = []
        self.gets: list[str] = []
        self.sessions: dict[str, dict] = {}
        # Real sidecar contract: DELETE stops the process; GET transparently
        # re-opens the session from the persisted index (spike §08).
        self.resume_index: dict[str, dict] = {}
        self._seq = 0

    # ------------------------------------------------------------------
    # scripting
    # ------------------------------------------------------------------
    def new_session_id(self) -> str:
        self._seq += 1
        return f"pi-sid-{self._seq:04d}"

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
                # custom final frame replaces the default run_end
                yield f"data: {json.dumps(step['run_end'])}\n\n".encode()
                return
            if isinstance(event, dict) and event.get("type") == "extension_ui_request":
                event = dict(event)
                event["sessionId"] = sid
            yield f"data: {json.dumps(event)}\n\n".encode()
            if isinstance(step, dict) and step.get("wait_ui"):
                self._wait_ui(event.get("id", ""), float(step["wait_ui"]))
            if "run_end" in step:
                # custom final frame after the event (e.g. an error run_end)
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
            return httpx.Response(200, json={"status": "ok", "pi": "1.4.0"})

        if method == "GET" and path == "/sessions" and self.not_found:
            return httpx.Response(404, json={"error": "no route"})

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
                "state": {"sessionId": new_sid, "isStreaming": False,
                          "model": {"provider": "mock", "id": "mock-model"}},
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
                # getOrResume: spawn + switch_session — the mock re-opens the
                # same state (proves the map survived the close).
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
            steps = self.next_runs.get(sid, [])
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=self._stream(sid, steps),
            )

        return httpx.Response(405, json={"error": "method not allowed"})

    def _stream(self, sid: str, steps: list[dict]):
        """Yield SSE chunks lazily so waits overlap with concurrent POSTs."""
        yield from self._sse_chunks(sid, steps)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------
@pytest.fixture
def logger():
    return logging.getLogger("test-pi")


@pytest.fixture
def server():
    return MockPiSidecar()


@pytest.fixture
def transport(server):
    return httpx.MockTransport(server.handler)


SETTING = {
    "PI_API_URL": "http://pi.test",
    "PI_SECRET_KEY": "k1",
    "PI_CWD": "/workspace",
    # fail fast if a scripted run never arrives
    "PI_STREAM_TIMEOUT": "25",
}

AGENT_CONFIG = {
    "agent_id": "pi-agent",
    "metadata": {"module_name": "a2a_protocol_plugin.handlers.pi_handler"},
}


def _handler(logger, transport, setting=None, metadata=None, rows=None, server=None):
    """Build a PiAgentHandler over the mock transport with an in-memory
    partition-scoped a2a_settings repo shared via ``rows``."""
    cfg = dict(AGENT_CONFIG)
    cfg["metadata"] = {**(AGENT_CONFIG["metadata"]), **(metadata or {})}
    h = PiAgentHandler(logger=logger, agent_config=cfg,
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


def _resume_ctx(h, sid: str, partition: str = "ep#x") -> dict:
    """Store a mapping for ``sid`` and return a resume context (this is
    exactly what the executor does between turns with a live repo)."""
    endpoint_id, _, part_id = partition.partition("#")
    h._store_session_mapping(partition, endpoint_id, part_id, "ctx-1", sid)
    return {"partition_key": partition, "thread_uuid": "ctx-1"}


def _mapped(h, sid: str, partition: str = "ep#x") -> dict:
    """Store a mapping for a pre-scripted session and return a context that
    makes the handler reuse it (the executor's between-turns behavior)."""
    endpoint_id, _, part_id = partition.partition("#")
    h._store_session_mapping(partition, endpoint_id, part_id, "ctx-fixed", sid)
    return {"partition_key": partition, "thread_uuid": "ctx-fixed"}


def _drain(q: _queue.Queue, timeout: float = 6.0) -> list[dict]:
    """Drain the stream queue at the end of a run."""
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
        client = PiSidecarClient(logger, "http://pi.test", secret_key="k1",
                                 http_transport=transport)
        health = client.health()
        assert health["status"] == "ok"
        session = client.create_session(cwd="/workspace", model="anthropic/m1")
        assert session["sessionId"].startswith("pi-sid-")
        assert session["sessionFile"].endswith(".jsonl")
        assert server.creates == [{"cwd": "/workspace", "model": "anthropic/m1"}]
        client.close()

    def test_create_session_omits_empty_model(self, logger, server, transport):
        client = PiSidecarClient(logger, "http://pi.test", secret_key="k1",
                                 http_transport=transport)
        client.create_session(cwd="/workspace")
        assert server.creates == [{"cwd": "/workspace"}]

    def test_auth_rejected_401(self, logger):
        server = MockPiSidecar()
        server.auth_reject = True
        client = PiSidecarClient(logger, "http://pi.test", secret_key="wrong",
                                 http_transport=httpx.MockTransport(server.handler))
        with pytest.raises(PiSidecarError) as exc:
            client.health()
        assert exc.value.is_auth_error()
        client.close()

    def test_wrong_secret_401(self, logger, server, transport):
        client = PiSidecarClient(logger, "http://pi.test", secret_key="nope",
                                 http_transport=transport)
        with pytest.raises(PiSidecarError) as exc:
            client.create_session(cwd="/workspace")
        assert exc.value.is_auth_error()

    def test_unknown_session_404(self, logger, server, transport):
        client = PiSidecarClient(logger, "http://pi.test", secret_key="k1",
                                 http_transport=transport)
        with pytest.raises(PiSidecarError) as exc:
            client.get_session("missing")
        assert exc.value.is_not_found()
        client.close()

    def test_prompt_busy_409(self, logger, server, transport):
        sid = "pi-sid-busy"
        server.sessions[sid] = {"sessionFile": "/data/sessions/x.jsonl",
                                "state": {}}
        server.busy_once = True
        client = PiSidecarClient(logger, "http://pi.test", secret_key="k1",
                                 http_transport=transport)
        with pytest.raises(PiSidecarError) as exc:
            client.stream_prompt(sid, "hi")
        assert exc.value.is_busy()
        client.close()

    def test_stream_without_run_end_treated_as_drop(self, logger):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=b"data: " + json.dumps(_text_delta("partial")).encode()
                        + b"\n\n",
            )

        client = PiSidecarClient(logger, "http://pi.test", secret_key="k1",
                                 http_transport=httpx.MockTransport(handler))
        run_end, events = client.stream_prompt("pi-sid-drop", "hi")
        assert "without run_end" in run_end["error"]
        assert len(events) == 1
        client.close()


# ---------------------------------------------------------------------------
# Non-streaming
# ---------------------------------------------------------------------------
class TestNonStreaming:
    def test_first_turn_collects_deltas_only(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/data/sessions/x.jsonl",
                                "state": {}}
        server.script_run(sid, [
            {"event": _thinking_delta("Let me think")},
            {"event": _thinking_end("Let me think fully")},
            {"event": _text_delta("Hello")},
            {"event": _text_delta(" world")},
            {"event": _message_end("Hello world")},
        ])
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        result = h.ask_model(
            [{"role": "user", "content": "Say hi"}],
            ctx,
        )
        # Only text_delta reaches the answer; *_end / message_end full-content
        # copies are never re-emitted (no duplicated answer).
        assert result["content"] == "Hello world"
        assert result["role"] == "agent"
        assert result["metadata"]["thread_uuid"] == sid
        assert result["metadata"]["run_id"].startswith(f"{sid}:")
        assert result["metadata"]["pi"]["usage"]["totalTokens"] == 18
        assert result["metadata"]["pi"]["stopReason"] == "end_turn"

    def test_only_newest_user_turn_is_sent(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/data/sessions/x.jsonl",
                                "state": {}}
        server.script_run(sid, [{"event": _text_delta("ok")}])
        h = _handler(logger, transport, server=server)
        h.ask_model(
            [
                {"role": "user", "content": "older question"},
                {"role": "agent", "content": "older answer"},
                {"role": "user", "content": "newest question"},
            ],
            {"partition_key": "ep#x"},
        )
        assert len(server.prompts) == 1
        # The sidecar prompt payload carries ONLY the newest user text.
        assert server.prompts[0][1]["message"] == "newest question"

    def test_provider_error_message_end_is_surfaced(self, logger, server,
                                                    transport):
        """P1 spike: a provider failure (wrong API key) rides message_end with
        stopReason=error + errorMessage (no error event); the turn ends FAILED
        with the cause, not an empty success."""
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/data/sessions/x.jsonl",
                                "state": {}}
        server.script_run(sid, [
            {"event": {
                "type": "message_end",
                "message": {
                    "role": "assistant", "content": [],
                    "stopReason": "error",
                    "errorMessage": "OpenAI API error (401): Incorrect API key",
                },
            }},
        ])
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        q = _queue.Queue()
        result = h.ask_model([{"role": "user", "content": "go"}], ctx,
                             stream_queue=q, stream_event=threading.Event())
        chunks = _drain(q)
        errors = [c for c in chunks if c["name"] == "error"]
        assert len(errors) == 1
        assert "Incorrect API key" in errors[0]["value"]
        assert result["error"] == "OpenAI API error (401): Incorrect API key"
        assert result["content"] == ""

    def test_error_run_end_is_surfaced(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/data/sessions/x.jsonl",
                                "state": {}}
        server.script_run(sid, [{
            "event": _text_delta("partial…"),
            "run_end": {"type": "run_end", "aborted": False,
                        "error": "Provider stream failed: overloaded"},
        }])
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        result = h.ask_model([{"role": "user", "content": "hi"}], ctx)
        assert result["content"] == "partial…"
        assert "overloaded" in result["error"]

    def test_images_mapped_to_prompt_payload(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/data/sessions/x.jsonl",
                                "state": {}}
        server.script_run(sid, [{"event": _text_delta("got it")}])
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        h.ask_model(
            [{"role": "user", "content": [
                {"text": "what is this?"},
                {"file": {"bytes": "aGk=", "mimeType": "image/png",
                          "name": "shot.png"}},
                {"file": {"uri": "https://example.com/doc.pdf",
                          "mimeType": "application/pdf"}},
            ]}],
            ctx,
        )
        payload = server.prompts[0][1]
        assert payload["message"] == "what is this?\n[file: https://example.com/doc.pdf]"
        assert payload["images"] == [
            {"type": "image", "data": "aGk=", "mimeType": "image/png"},
        ]


# ---------------------------------------------------------------------------
# Conversation-key → sessionId mapping
# ---------------------------------------------------------------------------
class TestContextMapping:
    def test_first_turn_stores_mapping(self, logger, server, transport):
        h = _handler(logger, transport, server=server)
        result = h.ask_model([{"role": "user", "content": "hi"}],
                             {"partition_key": "ep#x", "context_id": "ctx-a"})
        stored = h._rows[("ep#x", h._setting_id("ctx-a"))]["setting"]["session_id"]
        assert stored == result["metadata"]["thread_uuid"]

    def test_resume_probes_and_reuses_mapped_session(
        self, logger, server, transport
    ):
        h = _handler(logger, transport, server=server)
        first = h.ask_model([{"role": "user", "content": "hi"}],
                            {"partition_key": "ep#x", "context_id": "ctx-r"})
        sid = first["metadata"]["thread_uuid"]
        server.script_run(sid, [{"event": _text_delta("resumed")},
                                {"event": _settled()}])
        second = h.ask_model([{"role": "user", "content": "again?"}],
                             {"partition_key": "ep#x", "context_id": "ctx-r"})
        assert second["content"] == "resumed"
        assert second["metadata"]["thread_uuid"] == sid
        # The mapped session was probed on resume; still exactly ONE create.
        assert server.gets.count(sid) >= 1
        assert len(server.creates) == 1

    def test_client_supplied_session_path_never_used(self, logger, server,
                                                     transport):
        """A tenant-supplied id in metadata must not select the pi session."""
        h = _handler(logger, transport, server=server)
        h.ask_model(
            [{"role": "user", "content": "hi"}],
            {"partition_key": "ep#x",
             "thread_uuid": "/data/sessions/rogue.jsonl"},
        )
        # exactly one NEW sidecar-minted session; the rogue path was never
        # passed to create/get.
        assert len(server.creates) == 1
        assert server.gets == []

    def test_lost_session_falls_back_to_new(self, logger, server, transport):
        h = _handler(logger, transport, server=server)
        h.ask_model([{"role": "user", "content": "hi"}],
                    {"partition_key": "ep#x", "context_id": "ctx-lost"})
        sid = h._rows[("ep#x", h._setting_id("ctx-lost"))]["setting"]["session_id"]
        # The per-turn close moved it to the resume index; the sidecar
        # "forgets" the session entirely (e.g. operator wiped the volume).
        server.sessions.pop(sid, None)
        server.resume_index.pop(sid, None)
        second = h.ask_model([{"role": "user", "content": "back"}],
                             {"partition_key": "ep#x", "context_id": "ctx-lost"})
        assert len(server.creates) == 2  # a fresh session replaced the lost one
        assert second["metadata"]["thread_uuid"] != sid
        stored = h._rows[("ep#x", h._setting_id("ctx-lost"))]["setting"]["session_id"]
        assert stored == second["metadata"]["thread_uuid"]

    def test_cross_tenant_key_is_isolated(self, logger, server, transport):
        h = _handler(logger, transport, server=server)
        h.ask_model([{"role": "user", "content": "hi"}],
                    {"partition_key": "ep#one", "context_id": "shared"})
        h.ask_model([{"role": "user", "content": "hi"}],
                    {"partition_key": "ep#two", "context_id": "shared"})
        assert len(server.creates) == 2  # same key, different partitions


# ---------------------------------------------------------------------------
# Streaming mapping
# ---------------------------------------------------------------------------
class TestStreamingMapping:
    def test_run_id_chunk_comes_first(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [{"event": _text_delta("hi")}])
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        q = _queue.Queue()
        h.ask_model([{"role": "user", "content": "go"}], ctx, stream_queue=q,
                    stream_event=threading.Event())
        chunks = _drain(q)
        assert chunks[0]["name"] == "run_id"
        assert chunks[0]["value"].startswith(f"{sid}:")

    def test_tokens_and_reasoning_map(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _thinking_delta("plan the work")},
            {"event": _text_delta("answer part 1, ")},
            {"event": _text_delta("part 2")},
            {"event": _message_end("answer part 1, part 2")},
        ])
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        q = _queue.Queue()
        result = h.ask_model([{"role": "user", "content": "go"}], ctx,
                             stream_queue=q, stream_event=threading.Event())
        chunks = _drain(q)
        names = _names(chunks)
        assert "tool_call" not in names and "error" not in names
        tokens = [c for c in chunks if c["name"] == "token" and "meta" not in c]
        reasoning = [c for c in chunks
                     if c.get("meta", {}).get("data_format") == "reasoning"]
        assert "".join(c["value"] for c in tokens) == "answer part 1, part 2"
        assert result["content"] == "answer part 1, part 2"
        # reasoning tagged and kept OUT of the answer
        assert [c["value"] for c in reasoning] == ["plan the work"]
        # the authoritative *_end copies never re-streamed
        assert "answer part 1, part 2" not in "".join(
            c["value"] for c in reasoning)

    def test_tool_call_and_result_chunks(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _tool_start("call_1", "bash", {"command": "ls"})},
            {"event": _tool_end("call_1", "bash", "file.txt")},
            {"event": _text_delta("done")},
        ])
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        q = _queue.Queue()
        h.ask_model([{"role": "user", "content": "go"}], ctx, stream_queue=q,
                    stream_event=threading.Event())
        chunks = _drain(q)
        calls = [c for c in chunks if c["name"] == "tool_call"]
        results = [c for c in chunks if c["name"] == "tool_result"]
        assert len(calls) == 1 and len(results) == 1
        assert json.loads(calls[0]["value"])["toolName"] == "bash"
        assert json.loads(results[0]["value"])["output"] == "file.txt"

    def test_provider_error_event_becomes_error_chunk(self, logger, server,
                                                      transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [{"event": _error_event("overloaded_provider")}])
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        q = _queue.Queue()
        result = h.ask_model([{"role": "user", "content": "go"}], ctx,
                             stream_queue=q, stream_event=threading.Event())
        chunks = _drain(q)
        errors = [c for c in chunks if c["name"] == "error"]
        assert len(errors) == 1
        assert "overloaded_provider" in errors[0]["value"]

    def test_stream_drop_yields_error_result(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.next_runs[sid] = [{"run_end": None}]  # stream drops w/o run_end
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        q = _queue.Queue()
        result = h.ask_model([{"role": "user", "content": "go"}], ctx,
                             stream_queue=q, stream_event=threading.Event())
        _drain(q)
        # Stream drop → partial-content result carrying the error (HTTP
        # fallback keeps whatever streamed; same shape as goose partial stops).
        assert "without run_end" in result["error"]
        assert "pi_error" not in result.get("metadata", {})


# ---------------------------------------------------------------------------
# Busy policy
# ---------------------------------------------------------------------------
class TestBusyPolicy:
    def _busy_test(self, logger, transport, behavior):
        server = MockPiSidecar()
        transport = httpx.MockTransport(server.handler)
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [{"event": _text_delta("queued")},
                                {"event": _settled()}])
        server.busy_once = True
        h = _handler(logger, transport,
                     metadata={"pi_busy_behavior": behavior}, server=server)
        ctx = _mapped(h, sid)
        q = _queue.Queue()
        result = h.ask_model([{"role": "user", "content": "go"}], ctx,
                             stream_queue=q, stream_event=threading.Event())
        chunks = _drain(q)
        return result, chunks, server

    def test_busy_follow_up_resubmits_with_streaming_behavior(
        self, logger
    ):
        result, _chunks, server = self._busy_test(logger, None, "followUp")
        assert result["content"] == "queued"
        assert len(server.prompts) == 2
        assert server.prompts[0][1].get("streamingBehavior") is None
        assert server.prompts[1][1]["streamingBehavior"] == "followUp"

    def test_busy_steer_resubmits_with_steer(self, logger):
        _result, _chunks, server = self._busy_test(logger, None, "steer")
        assert len(server.prompts) == 2
        assert server.prompts[1][1]["streamingBehavior"] == "steer"

    def test_busy_reject_surfaces_error(self, logger):
        result, chunks, server = self._busy_test(logger, None, "reject")
        errors = [c for c in chunks if c["name"] == "error"]
        assert len(errors) == 1
        assert "409" in result["error"]

    def test_busy_steer_json_ack_is_a_clean_empty_turn(self, logger):
        """The real sidecar answers a steering/followUp during an active run
        with a plain JSON acknowledgement (no SSE run window — the reply
        streams on the FIRST caller's window). The client must return a
        synthetic run_end instead of a transport error."""
        server = MockPiSidecar()
        transport = httpx.MockTransport(server.handler)

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST" and request.url.path.endswith("/prompt"):
                return httpx.Response(
                    200,
                    headers={"content-type": "application/json"},
                    json={"disposition": "steer", "sessionId": "pi-sid-1"},
                )
            return server.handler(request)

        h = _handler(logger, httpx.MockTransport(handler),
                     metadata={"pi_busy_behavior": "steer"})
        q = _queue.Queue()
        result = h.ask_model([{"role": "user", "content": "go"}],
                             {"partition_key": "ep#x", "context_id": "ctx-ack"},
                             stream_queue=q, stream_event=threading.Event())
        chunks = _drain(q)
        assert not [c for c in chunks if c["name"] == "error"]
        assert "without run_end" not in str(result.get("error"))
        assert result["metadata"]["pi"]["stopReason"] == "queued"
        # no events streamed on this window (the reply rides the first caller's)
        assert result["content"] == ""


# ---------------------------------------------------------------------------
# Extension-UI permission modes
# ---------------------------------------------------------------------------
class TestPermissionModes:
    def test_auto_allow_confirms_and_selects_first(self, logger, server,
                                                   transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _dialog("confirm", "d1"), "wait_ui": 5.0},
            {"event": _dialog("select", "d2", options=["Allow", "Deny"]),
             "wait_ui": 5.0},
            {"event": _dialog("input", "d3"), "wait_ui": 5.0},
            {"event": _text_delta("done after dialogs")},
        ])
        h = _handler(logger, transport,
                     metadata={"pi_permission_mode": "auto_allow"}, server=server)
        ctx = _mapped(h, sid)
        result = h.ask_model([{"role": "user", "content": "go"}], ctx,
                             stream_queue=_queue.Queue(),
                             stream_event=threading.Event())
        assert result["content"] == "done after dialogs"
        answers = {a["payload"]["id"]: a["payload"] for a in server.ui_answers}
        # confirm → confirmed:true, select → first option, input → cancelled
        assert answers["d1"]["confirmed"] is True
        assert answers["d2"]["value"] == "Allow"
        assert answers["d3"]["cancelled"] is True

    def test_deny_cancels_everything(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _dialog("confirm", "d1"), "wait_ui": 5.0},
            {"event": _dialog("select", "d2", options=["Allow"]),
             "wait_ui": 5.0},
            {"event": _text_delta("worked without approvals")},
        ])
        h = _handler(logger, transport,
                     metadata={"pi_permission_mode": "deny"}, server=server)
        ctx = _mapped(h, sid)
        result = h.ask_model([{"role": "user", "content": "go"}], ctx,
                             stream_queue=_queue.Queue(),
                             stream_event=threading.Event())
        answers = {a["payload"]["id"]: a["payload"] for a in server.ui_answers}
        assert answers["d1"]["confirmed"] is False
        assert answers["d2"]["cancelled"] is True
        assert result["content"] == "worked without approvals"

    def test_ask_emits_approval_then_resumes(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _dialog("confirm", "d1",
                              message="rm -rf /tmp/spike-tmp"),
             "wait_ui": 8.0},
            {"event": _text_delta("resumed after approval")},
        ])
        h = _handler(logger, transport,
                     metadata={"pi_permission_mode": "ask",
                               "pi_approval_timeout": "5"},
                     server=server)
        ctx = _mapped(h, sid)
        q = _queue.Queue()
        ev = threading.Event()

        def _run():
            return h.ask_model([{"role": "user", "content": "go"}], ctx,
                               stream_queue=q, stream_event=ev)

        worker = threading.Thread(target=_run, daemon=True)
        worker.start()
        # approval chunk arrives before the dialog is answered
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
        assert approval["method"] == "confirm"
        # the dialog message (what is being approved) rides the chunk, both
        # flat and inside the bridge's `tool_call` details blob
        assert approval["message"] == "rm -rf /tmp/spike-tmp"
        assert approval["tool_call"]["input"] == "rm -rf /tmp/spike-tmp"

        run_id = next(c["value"] for c in chunks if c["name"] == "run_id")
        assert h.resolve_approval(run_id, True) is True
        worker.join(timeout=12)
        answers = {a["payload"]["id"]: a["payload"] for a in server.ui_answers}
        assert answers["d1"]["confirmed"] is True

    def test_ask_timeout_cancels_dialog(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _dialog("confirm", "d1"), "wait_ui": 6.0},
            {"event": _text_delta("cancelled through")},
        ])
        h = _handler(logger, transport,
                     metadata={"pi_permission_mode": "ask",
                               "pi_approval_timeout": "0.3"},
                     server=server)
        ctx = _mapped(h, sid)
        q = _queue.Queue()
        h.ask_model([{"role": "user", "content": "go"}], ctx, stream_queue=q,
                    stream_event=threading.Event())
        chunks = _drain(q)
        approvals = [c for c in chunks if c["name"] == "approval"]
        assert len(approvals) == 1
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline and not server.ui_answers:
            time.sleep(0.05)
        answers = {a["payload"]["id"]: a["payload"] for a in server.ui_answers}
        assert answers["d1"]["confirmed"] is False

    def test_fire_and_forget_ui_ignored(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": {"type": "extension_ui_request", "id": "n1",
                       "method": "notify", "message": "hello"}},
            {"event": {"type": "extension_ui_request", "id": "s1",
                       "method": "setStatus", "statusKey": "k",
                       "statusText": "running"}},
            {"event": _text_delta("ok")},
        ])
        h = _handler(logger, transport, server=server)
        h.ask_model([{"role": "user", "content": "go"}],
                    {"partition_key": "ep#x"})
        time.sleep(0.3)
        assert server.ui_answers == []  # nothing answered for fire-and-forget


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------
class TestCancel:
    def test_cancel_run_aborts_session(self, logger, server, transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _text_delta("start")},
            {"sleep": 0.5},
            {"event": _text_delta("late")},
        ])
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        q = _queue.Queue()
        ev = threading.Event()

        def _run():
            return h.ask_model([{"role": "user", "content": "long"}], ctx,
                               stream_queue=q, stream_event=ev)

        worker = threading.Thread(target=_run, daemon=True)
        worker.start()
        # wait for the run_id chunk
        run_id = None
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                c = q.get(timeout=0.3)
                if c["name"] == "run_id":
                    run_id = c["value"]
                    break
            except _queue.Empty:
                pass
        assert run_id and run_id.startswith(f"{sid}:")

        assert h.cancel_run(run_id) is True
        assert server.aborts == [sid]
        ev.set()
        worker.join(timeout=8)

    def test_turn_closes_the_process(self, logger, server, transport):
        """Goose parity: the turn deletes the sidecar session (frees the pi
        process); the session FILE + mapping survive for the next turn."""
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [{"event": _text_delta("hi")}])
        h = _handler(logger, transport, server=server)
        ctx = _mapped(h, sid)
        h.ask_model([{"role": "user", "content": "go"}], ctx)
        assert server.deletes == [sid]
        assert h._client is None
        # Second turn re-opens the SAME mapped session (probe + prompt; the
        # real sidecar resumes from its persisted index — modeled by the mock).
        server.script_run(sid, [{"event": _text_delta("again")}])
        second = h.ask_model([{"role": "user", "content": "go 2"}], ctx)
        assert second["content"] == "again"
        # mapping unchanged; no session was ever (re)created
        stored = h._rows[("ep#x", h._setting_id("ctx-fixed"))]["setting"]["session_id"]
        assert stored == sid
        assert server.creates == []
        assert server.aborts == []  # DELETE, not abort: process close each turn

    def test_turn_close_skipped_while_approval_pending(self, logger, server,
                                                       transport):
        sid = server.new_session_id()
        server.sessions[sid] = {"sessionFile": "/x.jsonl", "state": {}}
        server.script_run(sid, [
            {"event": _dialog("confirm", "d1"), "wait_ui": 6.0},
            {"event": _text_delta("late")},
        ])
        h = _handler(logger, transport,
                     metadata={"pi_permission_mode": "ask",
                               "pi_approval_timeout": "8"},
                     server=server)
        ctx = _mapped(h, sid)
        q = _queue.Queue()
        ev = threading.Event()

        def _run():
            return h.ask_model([{"role": "user", "content": "go"}], ctx,
                               stream_queue=q, stream_event=ev)

        worker = threading.Thread(target=_run, daemon=True)
        worker.start()
        run_id = None
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                c = q.get(timeout=0.3)
                if c["name"] == "run_id":
                    run_id = c["value"]
                    break
            except _queue.Empty:
                pass
        assert run_id
        h.resolve_approval(run_id, True)
        worker.join(timeout=15)
        # The dialog was answered via the still-open run connection; once the
        # pending state clears, the turn close deletes the session process.
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and h._pending_approvals:
            time.sleep(0.05)
        assert not h._pending_approvals
        assert server.deletes == [sid]

    def test_cancel_unknown_session_is_done(self, logger, server, transport):
        h = _handler(logger, transport, server=server)
        # unknown session (sidecar restart / reaped): abort → 404 → True
        assert h.cancel_run("pi-sid-gone:1") is True

    def test_cancel_unparseable_run_id_is_noop(self, logger, server, transport):
        h = _handler(logger, transport, server=server)
        assert h.cancel_run("") is False
        assert server.aborts == []

    def test_resolve_approval_without_pending_is_false(self, logger, server,
                                                       transport):
        h = _handler(logger, transport, server=server)
        assert h.resolve_approval("pi-sid-x:1", True) is False


# ---------------------------------------------------------------------------
# Bridge-level streaming contract (reasoning switch — shared with goose etc.)
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
    """The pi handler feeds the shared bridge; verify reasoning stays out of
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
        # The bridge passes thread_uuid="t-1" as the conversation key — map
        # that exact key so the handler probes and reuses the scripted session.
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
            result = await execute_ai_agent_streaming(
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
        return result

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