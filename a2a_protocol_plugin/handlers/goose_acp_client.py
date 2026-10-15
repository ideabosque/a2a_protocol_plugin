#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Goose ACP-over-HTTP client — wire-protocol isolation layer.

Implements the transport half of the goose integration (Phase G2 of
docs/GOOSE_INTEGRATION_PLAN.md) against ``goose serve`` (goose >= 1.53.0),
which exposes the Agent Client Protocol (ACP) over Streamable HTTP + SSE.

Every wire-level detail lives here so ``goose_handler.py`` speaks ACP
methods only:

- ``POST /acp`` with headers ``X-Secret-Key`` and ``Acp-Connection-Id``
  (client-generated for ``initialize``; the server returns its canonical
  connection id in the ``acp-connection-id`` response header and every
  later request must use THAT id — a foreign id is answered with 404).
- ``POST /acp .../initialize`` returns its result inline (HTTP 200).
- Every other POST returns ``202 Accepted`` with an empty body; results
  arrive as JSON-RPC messages on SSE streams opened with
  ``GET /acp`` (``Accept: text/event-stream``), matched by request id.
- Two streams: the connection-scoped stream (``Acp-Connection-Id`` only)
  and the session-scoped one (adds ``Acp-Session-Id``). In practice goose
  1.53.0 delivers ``session/new`` / ``session/load`` results and all
  ``session/update`` notifications on either stream; this client waits
  for a response id on both, so the stream split stays an internal detail.
- Server -> client requests (``session/request_permission``, ``fs/*``)
  are answered by POSTing the JSON-RPC response with both headers.
- ``session/cancel`` is a session-scoped notification (no request id).
- ``DELETE /acp`` closes the connection and its sessions (202).

Behaviour confirmed against the pinned goose 1.53.0 (see
``tests/goose_acp_transcript.json``):

- ``session/load`` replays the conversation as ``session/update``
  notifications. Callers must ignore updates that arrive before the
  ``session/prompt`` POST (replay guard, see ``prompt()``).
- A second ``session/prompt`` while a run is active fails with
  JSON-RPC ``-32602`` ("session already has active run").
- Unauthorized requests (missing/wrong secret) get HTTP 401; unknown
  connection ids get HTTP 404.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from typing import Any, Callable

import httpx

__author__ = "bibow"

__all__ = ["GooseAcpClient", "GooseAcpError"]


class GooseAcpError(Exception):
    """ACP transport / protocol failure with optional HTTP and RPC codes."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        rpc_code: Any = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.rpc_code = rpc_code

    def is_auth_error(self) -> bool:
        return self.status_code in (401, 403)

    def is_connection_error(self) -> bool:
        return self.status_code == 404

    def is_transport_bug(self) -> bool:
        return self.status_code in (406, 415)

    def is_active_run_error(self) -> bool:
        return self.rpc_code == -32602


class GooseAcpClient:
    """One ACP connection to ``goose serve`` (Streamable HTTP + SSE).

    The client owns its background SSE reader threads and exposes a
    synchronous API. A connection is meant to be created per handler run
    (gateway processes are not sticky) and closed with :meth:`close` in a
    ``finally`` block — goose persists sessions, so process affinity is
    never required.

    ``http_transport`` accepts an ``httpx.BaseTransport`` (typically
    ``httpx.MockTransport`` in tests) which is injected into the
    synchronous clients; pass ``client_factory`` instead to fully control
    client construction.
    """

    def __init__(
        self,
        logger: logging.Logger,
        base_url: str,
        secret_key: str | None = None,
        protocol_version: int = 1,
        client_capabilities: dict[str, Any] | None = None,
        request_timeout: float = 120.0,
        request_permission_callback: Callable[[str, dict[str, Any]], Any] | None = None,
        server_request_callback: Callable[[str, dict[str, Any]], Any] | None = None,
        http_transport: Any = None,
        client_factory: Callable[[Any], httpx.Client] | None = None,
    ) -> None:
        self.logger = logger
        if base_url.endswith("/"):
            base_url = base_url.rstrip("/")
        self.base_url = base_url
        self.secret_key = secret_key or ""
        self.protocol_version = protocol_version
        self.client_capabilities = (
            client_capabilities
            if client_capabilities is not None
            else {
                "fs": {"readTextFile": False, "writeTextFile": False},
                "terminal": False,
            }
        )
        self.request_timeout = float(request_timeout)
        self.request_permission_callback = request_permission_callback
        self.server_request_callback = server_request_callback
        self._http_transport = http_transport
        self._client_factory = client_factory

        # Server-assigned connection id (set by initialize()).
        self.connection_id: str | None = None
        # ACP agent capabilities as advertised by the initialize result.
        self.agent_capabilities: dict[str, Any] = {}
        self.agent_info: dict[str, Any] = {}

        self._lock = threading.RLock()
        self._next_request_id = 100
        self._response_events: dict[Any, threading.Event] = {}
        self._responses: dict[Any, dict[str, Any]] = {}
        self._notifications: dict[str, list[dict[str, Any]]] = {}
        # Update epochs: the reader stamps every buffered session/update with
        # the epoch current at processing time; prompt() bumps the epoch
        # before POSTing and only returns updates stamped with its own epoch.
        # (Wall-clock ordering between the reader and the waiting thread is
        # not reliable — epoch marking, protected by the FIFO stream order,
        # is: load-replays are always processed before the load result that
        # unblocks the caller.)
        self._update_epoch = 0
        self._prompt_epoch = 0
        self._prompt_started: float | None = None
        self._on_update_cb: Callable[[dict[str, Any]], None] | None = None
        # id -> (session_id, request_payload) for server -> client requests
        self._server_requests: dict[Any, dict[str, Any]] = {}
        self._stop = threading.Event()
        self._conn_stop = self._stop
        self._conn_thread: threading.Thread | None = None
        self._sess_stop: threading.Event | None = None
        self._sess_thread: threading.Thread | None = None
        self._sess_session_id: str | None = None
        self._initialized = False
        self._closed = False

    # ------------------------------------------------------------------
    # HTTP plumbing
    # ------------------------------------------------------------------

    def _make_client(self) -> httpx.Client:
        if self._client_factory is not None:
            return self._client_factory(self._http_transport)
        return httpx.Client(
            timeout=self.request_timeout,
            transport=self._http_transport,
        )

    def _headers(self, with_connection: bool = True, session_id: str | None = None) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.secret_key:
            headers["X-Secret-Key"] = self.secret_key
        if with_connection and self.connection_id:
            headers["Acp-Connection-Id"] = self.connection_id
        if session_id:
            headers["Acp-Session-Id"] = session_id
        return headers

    def _post(
        self,
        payload: dict[str, Any],
        with_connection: bool = True,
        session_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], Any]:
        send = self._headers(with_connection=with_connection, session_id=session_id)
        send.update(headers or {})
        try:
            client = self._make_client()
            try:
                resp = client.post(f"{self.base_url}/acp", json=payload, headers=send)
            finally:
                client.close()
        except httpx.HTTPError as e:
            raise GooseAcpError(f"goose ACP transport error: {e}") from e

        if resp.status_code in (401, 403, 404, 406, 415):
            raise GooseAcpError(
                f"goose ACP POST failed with HTTP {resp.status_code}",
                status_code=resp.status_code,
            )

        body: Any = None
        if resp.status_code == 200:
            try:
                body = resp.json()
            except ValueError:
                body = None
        acp_headers = {k.lower(): v for k, v in resp.headers.items() if k.lower().startswith("acp")}
        return resp.status_code, acp_headers, body

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def initialize(self) -> dict[str, Any]:
        """POST ``initialize``; returns the ACP ``initialize`` result inline.

        The client supplies its own ``Acp-Connection-Id``; the server
        returns its canonical connection id in the ``acp-connection-id``
        response header and that id is used for everything else.
        """
        payload = {
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": {
                "protocolVersion": self.protocol_version,
                "clientCapabilities": self.client_capabilities,
            },
            "id": self._alloc_request_id(),
        }
        client_conn_id = f"a2a-{uuid.uuid4()}"
        status, acp_headers, body = self._post(
            payload, with_connection=False, headers={"Acp-Connection-Id": client_conn_id}
        )
        if status != 200 or not isinstance(body, dict) or "result" not in body:
            raise GooseAcpError(
                f"goose ACP initialize failed (HTTP {status}, body={body!r})",
                status_code=status,
            )
        result = body["result"]
        self.connection_id = acp_headers.get("acp-connection-id") or client_conn_id
        self.agent_capabilities = result.get("agentCapabilities", {}) or {}
        self.agent_info = result.get("agentInfo", {}) or {}
        self._initialized = True
        return result

    # ------------------------------------------------------------------
    # Stream management
    # ------------------------------------------------------------------
    # goose routes messages: the connection-scoped stream carries the
    # ``session/new`` result; the session-scoped stream (opened with an
    # ``Acp-Session-Id`` header) carries ``session/load`` results,
    # ``session/update`` notifications, prompt results, and server -> client
    # requests. Each message is delivered on exactly one stream, so the
    # shared buffers see no duplicates.

    def start_streams(self) -> None:
        """Open the connection-scoped SSE stream (needed by ``new_session``)."""
        if not self._initialized:
            raise GooseAcpError("initialize() must complete before opening streams")
        with self._lock:
            self._stop = threading.Event()
            if self._conn_thread is None or not self._conn_thread.is_alive():
                self._conn_thread = threading.Thread(
                    target=self._reader,
                    args=("conn", None, self._conn_stop),
                    name="goose-acp-conn",
                    daemon=True,
                )
                self._conn_thread.start()

    def _ensure_session_stream(self, session_id: str) -> None:
        """Open the session-scoped SSE stream (needed by load/prompt)."""
        if self._sess_session_id == session_id and (
            self._sess_thread is not None and self._sess_thread.is_alive()
        ):
            return
        self._stop_sess_reader()
        self._sess_stop = threading.Event()
        self._sess_session_id = session_id
        self._sess_thread = threading.Thread(
            target=self._reader,
            args=("session", session_id, self._sess_stop),
            name="goose-acp-sess",
            daemon=True,
        )
        self._sess_thread.start()
        # give the reader a moment to connect so early updates are not missed
        time.sleep(0.3)

    def _stop_sess_reader(self) -> None:
        if self._sess_stop is not None:
            self._sess_stop.set()
        self._sess_session_id = None
        self._sess_thread = None

    def close(self) -> None:
        """DELETE /acp (closes the connection and its sessions) + stop readers."""
        if self._closed:
            return
        if self.connection_id:
            try:
                client = self._make_client()
                try:
                    resp = client.delete(
                        f"{self.base_url}/acp",
                        headers=self._headers(with_connection=True),
                    )
                    self.logger.debug("goose ACP DELETE /acp -> HTTP %s", resp.status_code)
                finally:
                    client.close()
            except Exception as e:  # best effort
                self.logger.debug("goose ACP DELETE /acp failed: %s", e)
        self._closed = True
        with self._lock:
            self._stop_sess_reader()
            if self._conn_stop is not None:
                self._conn_stop.set()

    # ------------------------------------------------------------------
    # Requests
    # ------------------------------------------------------------------

    def _alloc_request_id(self) -> int:
        with self._lock:
            rid = self._next_request_id
            self._next_request_id += 1
            return rid

    def _register(self, request_id: Any) -> threading.Event:
        event = threading.Event()
        with self._lock:
            self._response_events[request_id] = event
        return event

    def _wait_response(self, request_id: Any, timeout: float) -> dict[str, Any]:
        with self._lock:
            event = self._response_events.get(request_id)
        if event is None:
            raise GooseAcpError(f"No pending request id={request_id}")
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                with self._lock:
                    self._response_events.pop(request_id, None)
                raise GooseAcpError(
                    f"Timed out waiting for ACP response id={request_id}"
                )
            if event.wait(min(0.2, remaining)):
                break
        with self._lock:
            self._response_events.pop(request_id, None)
            return self._responses.pop(request_id, None) or {}

    def _call(
        self,
        method: str,
        params: dict[str, Any],
        timeout: float,
    ) -> dict[str, Any]:
        request_id = self._alloc_request_id()
        event = self._register(request_id)
        payload: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": request_id,
        }
        try:
            self._post(payload, with_connection=True)
        except Exception:
            with self._lock:
                self._response_events.pop(request_id, None)
            raise
        msg = self._wait_response(request_id, timeout)
        if "error" in msg:
            err = msg["error"] or {}
            raise GooseAcpError(
                f"ACP {method} failed: {err.get('message', err)}",
                rpc_code=err.get("code"),
            )
        return msg.get("result") or {}

    def new_session(self, cwd: str, mcp_servers: Any = None, timeout: float | None = None) -> str:
        """``session/new`` — creates a goose session and returns its id."""
        result = self._call(
            "session/new",
            {"cwd": cwd, "mcpServers": mcp_servers if mcp_servers is not None else []},
            timeout if timeout is not None else self.request_timeout,
        )
        session_id = result.get("sessionId")
        if not session_id:
            raise GooseAcpError(f"goose session/new returned no sessionId: {result!r}")
        return session_id

    def load_session(
        self, session_id: str, cwd: str, mcp_servers: Any = None, timeout: float | None = None
    ) -> dict[str, Any]:
        """``session/load`` — resumes a goose session (replays history)."""
        # session/load results ride the session-scoped stream.
        self._ensure_session_stream(session_id)
        try:
            return self._call(
                "session/load",
                {"sessionId": session_id, "cwd": cwd,
                 "mcpServers": mcp_servers if mcp_servers is not None else []},
                timeout if timeout is not None else self.request_timeout,
            )
        except GooseAcpError as e:
            # Unknown session ids (e.g. server reset) surface as an error;
            # treat it like a 404 so callers can fall back to session/new.
            self.logger.warning("goose session/load failed for %s: %s", session_id, e)
            raise GooseAcpError(
                f"goose session/load failed: {e}", status_code=404
            ) from e

    def set_mode(self, session_id: str, mode_id: str, timeout: float = 30.0) -> None:
        """``session/set_mode`` — e.g. ``approve`` for interactive permissions."""
        self._call(
            "session/set_mode",
            {"sessionId": session_id, "modeId": mode_id},
            timeout,
        )

    def prompt(
        self,
        session_id: str,
        content_blocks: list[dict[str, Any]],
        timeout: float | None = None,
        on_update: Callable[[dict[str, Any]], None] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """``session/prompt`` — runs one agent turn and returns the result.

        Returns ``(result, updates)`` where ``result`` is the prompt
        response message (``stopReason``, ``usage``) and ``updates`` lists
        every ``session/update`` notification observed after the prompt
        POST. Updates from earlier activity (e.g. the ``session/load``
        history replay) are excluded. ``on_update`` (optional) is invoked
        with each raw notification message as it arrives, in real time, on
        the SSE reader thread — callbacks must stay fast.
        """
        request_id = self._alloc_request_id()
        self._register(request_id)
        payload = {
            "jsonrpc": "2.0",
            "method": "session/prompt",
            "params": {"sessionId": session_id, "prompt": content_blocks},
            "id": request_id,
        }
        # The session-scoped stream carries the prompt result + updates.
        self._ensure_session_stream(session_id)
        # Epoch bump: only updates the reader stamps with THIS epoch (i.e.
        # processed after this POST) are surfaced — this excludes the
        # session/load history replay.
        with self._lock:
            self._update_epoch += 1
            epoch = self._update_epoch
            self._prompt_epoch = epoch
            self._on_update_cb = on_update
        try:
            self._post(payload, with_connection=True)
            msg = self._wait_response(
                request_id, timeout if timeout is not None else self.request_timeout
            )
        finally:
            self._on_update_cb = None
            self._prompt_epoch = 0
        with self._lock:
            received = self._notifications.pop("session_update", [])
        updates = [dict(n) for n in received if n.get("_epoch") == epoch]
        for u in updates:
            u.pop("_epoch", None)
        if "error" in msg:
            err = msg["error"] or {}
            raise GooseAcpError(
                f"ACP session/prompt failed: {err.get('message', err)}",
                rpc_code=err.get("code"),
            )
        return msg, updates

    def cancel(self, session_id: str) -> None:
        """``session/cancel`` — session-scoped notification (no response)."""
        self._post(
            {"jsonrpc": "2.0", "method": "session/cancel",
             "params": {"sessionId": session_id}},
            with_connection=True,
            session_id=session_id,
        )

    def respond(
        self,
        response_id: Any,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> None:
        """POST a JSON-RPC response to a server -> client request."""
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": response_id}
        if error is not None:
            payload["error"] = error
        else:
            payload["result"] = result or {}
        self._post(
            payload,
            with_connection=True,
            session_id=session_id,
        )

    # ------------------------------------------------------------------
    # Server -> client request helpers
    # ------------------------------------------------------------------

    def take_permission_request(self) -> dict[str, Any] | None:
        """Pop the oldest pending ``session/request_permission`` (if any)."""
        with self._lock:
            for rid in list(self._server_requests.keys()):
                pending = self._server_requests.pop(rid, None)
                if pending and pending.get("method") == "session/request_permission":
                    return pending
            return None

    def answer_permission(
        self,
        request_id: Any,
        session_id: str,
        option_id: str | None = None,
        cancelled: bool = False,
    ) -> None:
        """Answer a permissions request from outside the prompt drain loop."""
        if cancelled:
            self.respond(request_id, result={"outcome": {"outcome": "cancelled"}},
                         session_id=session_id)
            return
        self.respond(
            request_id,
            result={"outcome": {"outcome": "selected", "optionId": option_id}},
            session_id=session_id,
        )

    def _handle_server_request(self, message: dict[str, Any]) -> None:
        method = message.get("method", "")
        params = message.get("params", {}) or {}
        request_id = message.get("id")
        session_id = params.get("sessionId") or ""
        with self._lock:
            self._server_requests[request_id] = {
                "request_id": request_id,
                "method": method,
                "session_id": session_id,
                "params": params,
            }
        if method == "session/request_permission":
            callback = self.request_permission_callback
            if callback is not None:
                try:
                    option_id = callback(session_id, {
                        "request_id": request_id,
                        "method": method,
                        "session_id": session_id,
                        "params": params,
                    })
                except Exception as e:  # callback decides; errors reject
                    self.logger.warning("goose permission callback failed: %s", e)
                    option_id = None
                if option_id is not None:
                    try:
                        self.answer_permission(request_id, session_id, option_id)
                        self._pop_server_request(request_id)
                    except Exception as e:
                        self.logger.warning(
                            "goose permission answer failed for %s: %s", request_id, e
                        )
                # option_id None -> deferred (ask policy); the caller must
                # answer via answer_permission() before the run completes.
                return
            # No policy: reject politely so the run proceeds.
            try:
                self.answer_permission(request_id, session_id, option_id="reject_once")
                self._pop_server_request(request_id)
            except Exception as e:
                self.logger.warning("goose rejection failed: %s", e)
            return
        if self.server_request_callback is not None:
            try:
                self.server_request_callback(session_id, {
                    "request_id": request_id,
                    "method": method,
                    "params": params,
                })
                return
            except Exception as e:
                self.logger.warning("goose server request callback failed: %s", e)
        # fs/* / terminal requests are refused (we never advertise them).
        try:
            self.respond(
                request_id,
                error={"code": -32601,
                       "message": f"goose ACP client does not support {method}"},
                session_id=session_id,
            )
        except Exception as e:
            self.logger.warning("goose server request refusal failed: %s", e)

    def _pop_server_request(self, request_id: Any) -> dict[str, Any] | None:
        with self._lock:
            return self._server_requests.pop(request_id, None)

    # ------------------------------------------------------------------
    # SSE reader
    # ------------------------------------------------------------------

    def _reader(self, tag: str, session_id: str | None, stop: threading.Event) -> None:
        headers = self._headers(with_connection=True, session_id=session_id)
        try:
            client = httpx.Client(timeout=None, transport=self._http_transport)
            try:
                with client.stream(
                    "GET", f"{self.base_url}/acp",
                    headers={"Accept": "text/event-stream", **headers},
                ) as resp:
                    if resp.status_code >= 400:
                        body = resp.read().decode("utf-8", errors="replace")
                        self.logger.warning(
                            "goose ACP %s stream rejected: HTTP %s %s",
                            tag, resp.status_code, body[:200],
                        )
                        return
                    for line in resp.iter_lines():
                        if stop.is_set():
                            return
                        if not line or line.startswith(":"):
                            continue
                        if not line.startswith("data:"):
                            continue
                        data = line[5:].strip()
                        if data == "[DONE]":
                            return
                        try:
                            message = json.loads(data)
                        except ValueError:
                            continue
                        self._route_message(tag, message)
            finally:
                client.close()
        except httpx.HTTPError as e:
            if not stop.is_set():
                self.logger.warning("goose ACP %s stream error: %s", tag, e)

    def _route_message(self, tag: str, message: dict[str, Any]) -> None:
        method = message.get("method")
        if method == "session/update":
            # Stamp with the update epoch at processing time (see prompt()).
            # FIFO stream order makes this race-free: replayed history is
            # always processed before the session/load result unblocks the
            # caller, which is when the epoch becomes stale.
            with self._lock:
                stamped_epoch = self._update_epoch
                self._notifications.setdefault("session_update", []).append(
                    {"_epoch": stamped_epoch, **message}
                )
            callback = self._on_update_cb
            if (
                callback is not None
                and self._prompt_epoch == stamped_epoch
            ):
                try:
                    callback(message)
                except Exception as e:
                    self.logger.warning("goose update callback failed: %s", e)
            return
        if "id" in message and ("result" in message or "error" in message):
            with self._lock:
                response_event = self._response_events.get(message["id"])
                if response_event is not None:
                    self._responses[message["id"]] = message
                    response_event.set()
                    return
            return
        if method and "id" in message:
            self._handle_server_request(message)
            return

    def drain_updates(self) -> list[dict[str, Any]]:
        """Return buffered ``session/update`` notifications (raw messages)."""
        with self._lock:
            return [
                {k: v for k, v in n.items() if k != "_epoch"}
                for n in self._notifications.pop("session_update", [])
            ]