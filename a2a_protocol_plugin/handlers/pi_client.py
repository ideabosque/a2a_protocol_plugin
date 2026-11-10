#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Pi sidecar client — transport isolation layer for the pi bridge.

Implements the transport half of the pi integration (Phase P3 of
docs/PI_INTEGRATION_PLAN.md) against the pi sidecar service shipped in
docker-a2a-gateway (``pi/`` — a small Node service that spawns
``pi --mode rpc`` child processes over stdin/stdout JSONL, per the official
pi RPC docs).

pi itself has NO network server; the sidecar is the HTTP+SSE boundary:

- ``GET  /health``                    → liveness + pi version.
- ``POST /sessions``                  → start a session
  (body ``{cwd?, model?, thinking?}``) → ``{sessionId, sessionFile}``.
- ``GET  /sessions/{id}``             → ``get_state`` + ``get_session_stats``.
- ``POST /sessions/{id}/prompt``      → body ``{message, images?,
  streamingBehavior?}``; response is ``text/event-stream`` of pi events for
  the run (sidecar-managed run window) terminated by a
  ``{"type": "run_end", ...}`` frame, then the stream closes.
- ``POST /sessions/{id}/abort``       → abort the running prompt.
- ``POST /sessions/{id}/ui-response`` → answer a pending extension UI dialog
  (``extension_ui_response`` payload: ``id`` + ``value``/``confirmed``/
  ``cancelled``).
- ``DELETE /sessions/{id}``           → close the live pi process (the
  session file stays on the sidecar's session volume and can be resumed).

Wire details:

- Auth is a shared secret sent as ``X-Secret-Key`` (same pattern as goose
  serve); wrong/missing key → HTTP 401.
- A prompt for a session that is already streaming is rejected with HTTP 409
  unless the body carries ``streamingBehavior`` (``steer`` | ``followUp``).
- An unknown session id → HTTP 404 (callers typically fall back to creating
  a new session).
- The session FILE survives process exit; ``POST /sessions`` again + the
  sidecar re-opening the stored file resumes a conversation. The client
  never passes pi session paths from request data — session ids are minted
  by the sidecar.

Only pi EVENT forwarding (``message_update`` deltas, tool execution events,
``extension_ui_request`` dialogs, ``agent_settled``) and run bookkeeping
happen here; meaning conversion to A2A stream chunks lives in
``sidecar_handler.py`` (the shared bridge of the pi and omp dialects).

``http_transport`` accepts an ``httpx.BaseTransport`` (typically
``httpx.MockTransport`` in tests), injected into the synchronous clients; pass
``client_factory`` instead to fully control client construction.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable

import httpx

__author__ = "bibow"

__all__ = ["PiSidecarClient", "PiSidecarError"]

# SSE frames are ``data: <json>``; the sidecar ends each run stream with a
# ``run_end`` record and then closes it.
_RUN_END_TYPE = "run_end"


class PiSidecarError(Exception):
    """pi sidecar transport failure with optional HTTP status."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code

    def is_auth_error(self) -> bool:
        """Wrong or missing ``X-Secret-Key``."""
        return self.status_code in (401, 403)

    def is_not_found(self) -> bool:
        """Unknown session id (sidecar restarted, or session reaped)."""
        return self.status_code == 404

    def is_busy(self) -> bool:
        """A prompt is already streaming for this session."""
        return self.status_code == 409


class PiSidecarClient:
    """One connection to the pi sidecar (HTTP + SSE).

    The client is synchronous and safe to use from the handler thread while
    its SSE pump reads a run stream (the shared ``httpx.Client`` pools a
    second connection). Created per handler run — gateway processes are not
    sticky — and closed with :meth:`close` in a ``finally`` block. Sessions
    live on the sidecar's volume, so process affinity is never required.
    """

    def __init__(
        self,
        logger: logging.Logger,
        base_url: str,
        secret_key: str | None = None,
        request_timeout: float = 120.0,
        http_transport: Any = None,
        client_factory: Callable[[Any], httpx.Client] | None = None,
        label: str = "pi",
    ) -> None:
        self.logger = logger
        self.base_url = base_url.rstrip("/")
        self.secret_key = secret_key or ""
        self.request_timeout = float(request_timeout)
        self._http_transport = http_transport
        self._client_factory = client_factory
        # Agent label of the dialect this client talks to ("pi", "omp") —
        # it words the transport errors and derives the env prefix of the
        # sidecar secret variables.
        self.label = label
        self.env_prefix = label.upper()
        # Shared client (pool) — the SSE pump and helper POSTs (ui-response,
        # abort) can safely overlap on separate pooled connections.
        self._client: httpx.Client | None = None
        self._closed = False

    # ------------------------------------------------------------------
    # HTTP plumbing
    # ------------------------------------------------------------------

    def _make_client(self) -> httpx.Client:
        if self._client is not None:
            return self._client
        if self._client_factory is not None:
            self._client = self._client_factory(self._http_transport)
        else:
            self._client = httpx.Client(
                timeout=self.request_timeout,
                transport=self._http_transport,
            )
        return self._client

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.secret_key:
            headers["X-Secret-Key"] = self.secret_key
        return headers

    def _check_status(self, resp: httpx.Response, action: str) -> None:
        if resp.status_code in (401, 403):
            raise PiSidecarError(
                f"{self.label} sidecar {action} rejected the A2A gateway "
                f"credentials (HTTP {resp.status_code}); verify "
                f"{self.env_prefix}_SIDECAR_SECRET_KEY and "
                f"{self.env_prefix}_SECRET_KEY",
                status_code=resp.status_code,
            )
        # DEF-PI-005 fix (2026-10-25): only successful HTTP responses
        # may be interpreted as a run or a queue acknowledgement.
        if not 200 <= resp.status_code < 300:
            detail = ""
            try:
                body = resp.json()
                detail = str(body.get("error") or "") if isinstance(body, dict) else ""
            except ValueError:
                detail = resp.text[:200]
            raise PiSidecarError(
                f"{self.label} sidecar {action} failed with HTTP "
                f"{resp.status_code}"
                + (f": {detail}" if detail else ""),
                status_code=resp.status_code,
            )

    def _post_json(
        self,
        path: str,
        payload: dict[str, Any] | None = None,
        action: str = "request",
        timeout: float | None = None,
    ) -> httpx.Response:
        client = self._make_client()
        try:
            resp = client.post(
                f"{self.base_url}{path}",
                json=payload if payload is not None else {},
                headers=self._headers(),
                timeout=timeout if timeout is not None else self.request_timeout,
            )
        except httpx.HTTPError as e:
            raise PiSidecarError(
                f"{self.label} sidecar transport error: {e}"
            ) from e
        self._check_status(resp, action)
        return resp

    def _delete(self, path: str, action: str = "request") -> httpx.Response:
        client = self._make_client()
        try:
            resp = client.delete(
                f"{self.base_url}{path}",
                headers=self._headers(),
                timeout=self.request_timeout,
            )
        except httpx.HTTPError as e:
            raise PiSidecarError(
                f"{self.label} sidecar transport error: {e}"
            ) from e
        self._check_status(resp, action)
        return resp

    @staticmethod
    def _json_or_none(resp: httpx.Response) -> Any:
        try:
            return resp.json()
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Lifecycle / session management
    # ------------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        """``GET /health`` — liveness + pi version."""
        client = self._make_client()
        try:
            resp = client.get(
                f"{self.base_url}/health",
                headers=self._headers(),
                timeout=self.request_timeout,
            )
        except httpx.HTTPError as e:
            raise PiSidecarError(
                f"{self.label} sidecar transport error: {e}"
            ) from e
        self._check_status(resp, "health check")
        return self._json_or_none(resp) or {}

    def create_session(
        self,
        cwd: str | None = None,
        model: str | None = None,
        thinking: str | None = None,
    ) -> dict[str, Any]:
        """``POST /sessions`` — start (or re-open) a pi session.

        Returns ``{sessionId, sessionFile, resumed?}``. ``model`` is
        ``provider/modelId`` (e.g. ``anthropic/claude-sonnet-4-5``);
        ``thinking`` one of pi's levels (``off``…``max``).
        """
        payload: dict[str, Any] = {}
        if cwd:
            payload["cwd"] = cwd
        if model:
            payload["model"] = model
        if thinking:
            payload["thinking"] = thinking
        resp = self._post_json("/sessions", payload, "create_session")
        data = self._json_or_none(resp) or {}
        if not data.get("sessionId"):
            raise PiSidecarError(
                f"{self.label} sidecar create_session returned no sessionId: "
                f"{data!r}"
            )
        return data

    def get_session(self, session_id: str) -> dict[str, Any]:
        """``GET /sessions/{id}`` — ``get_state`` + ``get_session_stats``.

        Also doubles as a liveness/known-session probe: a 404 here is the
        signal to discard a stored mapping and start a fresh session.
        """
        client = self._make_client()
        try:
            resp = client.get(
                f"{self.base_url}/sessions/{session_id}",
                headers=self._headers(),
                timeout=min(30.0, self.request_timeout),
            )
        except httpx.HTTPError as e:
            raise PiSidecarError(
                f"{self.label} sidecar transport error: {e}"
            ) from e
        self._check_status(resp, "get_session")
        return self._json_or_none(resp) or {}

    def close_session(self, session_id: str) -> None:
        """``DELETE /sessions/{id}`` — stop the live pi process for a session."""
        self._delete(f"/sessions/{session_id}", "close_session")

    def abort(self, session_id: str) -> bool:
        """``POST /sessions/{id}/abort`` — abort the running prompt."""
        resp = self._post_json(
            f"/sessions/{session_id}/abort", None, "abort", timeout=30.0
        )
        data = self._json_or_none(resp) or {}
        return bool(data.get("aborted", True))

    def respond_ui(self, session_id: str, response: dict[str, Any]) -> None:
        """``POST /sessions/{id}/ui-response`` — answer a pending dialog.

        ``response`` is the ``extension_ui_response`` payload: ``{"id": ...,
        "value": ...}`` / ``{"id": ..., "confirmed": true|false}`` /
        ``{"id": ..., "cancelled": true}``.
        """
        self._post_json(
            f"/sessions/{session_id}/ui-response",
            response,
            "ui-response",
            timeout=30.0,
        )

    # ------------------------------------------------------------------
    # Streaming prompt
    # ------------------------------------------------------------------

    def stream_prompt(
        self,
        session_id: str,
        message: str,
        images: list[dict[str, Any]] | None = None,
        streaming_behavior: str | None = None,
        on_event: Callable[[dict[str, Any]], None] | None = None,
        timeout: float | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """``POST /sessions/{id}/prompt`` — run one agent turn.

        Consumes the sidecar's event stream until its ``run_end`` frame.
        ``on_event`` (optional) is invoked with each raw pi event as it
        arrives, in real time, on this thread — callbacks must stay fast
        (or offload to their own workers).

        Returns ``(run_end, events)`` where ``run_end`` is the final
        ``{"type": "run_end", "aborted": bool, "stopReason": ...,
        "usage": ..., "error": ...}`` frame and ``events`` lists every
        forwarded pi event in order.

        :raises PiSidecarError: HTTP 401 (auth), 404 (unknown session),
            409 (busy — retry with ``streaming_behavior``), transport.
        """
        payload: dict[str, Any] = {"message": message}
        if images:
            payload["images"] = images
        if streaming_behavior:
            payload["streamingBehavior"] = streaming_behavior

        total_deadline: float | None = (
            time.monotonic() + float(timeout) if timeout is not None else None
        )
        events: list[dict[str, Any]] = []
        run_end: dict[str, Any] = {}

        # Read timeout between SSE frames: pi keeps a run alive with periodic
        # events; a quiet gap longer than this means the stream is gone.
        read_timeout = self.request_timeout if self.request_timeout else 120.0
        client = self._make_client()
        try:
            with client.stream(
                "POST",
                f"{self.base_url}/sessions/{session_id}/prompt",
                json=payload,
                headers={**self._headers(), "Accept": "text/event-stream"},
                timeout=httpx.Timeout(
                    self.request_timeout,
                    connect=min(30.0, read_timeout),
                    read=read_timeout,
                    write=30.0,
                ),
            ) as resp:
                if not 200 <= resp.status_code < 300:
                    resp.read()
                    self._check_status(resp, "prompt")
                content_type = resp.headers.get("content-type", "")
                if "text/event-stream" not in content_type:
                    # A steering/followUp during the session's active run is
                    # accepted with a plain JSON acknowledgement (the reply
                    # streams on the FIRST request's run window).
                    body = resp.read().decode("utf-8", errors="replace")
                    try:
                        ack = json.loads(body) if body else {}
                    except ValueError:
                        ack = {}
                    if (not isinstance(ack, dict)
                            or ack.get("disposition") not in ("steer", "followUp", "queued")):
                        raise PiSidecarError(
                            f"{self.label} sidecar returned an invalid prompt "
                            "acknowledgement"
                        )
                    run_end = {
                        "type": _RUN_END_TYPE,
                        "sessionId": session_id,
                        "aborted": False,
                        "disposition": ack.get("disposition") or "queued",
                        "stopReason": "queued",
                    }
                    return run_end, []
                for line in resp.iter_lines():
                    if total_deadline is not None and time.monotonic() > total_deadline:
                        self._abort_quietly(session_id)
                        raise PiSidecarError(
                            f"{self.label} sidecar prompt timed out after "
                            f"{timeout:.0f}s"
                        )
                    if not line or line.startswith(":"):
                        continue
                    if not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        break
                    try:
                        event = json.loads(data)
                    except ValueError:
                        continue
                    if not isinstance(event, dict):
                        continue
                    events.append(event)
                    if event.get("type") == _RUN_END_TYPE:
                        run_end = event
                        break
                    if on_event is not None:
                        try:
                            on_event(event)
                        except Exception as e:  # callback decides; never kill the pump
                            self.logger.warning(
                                "%s sidecar event callback failed: %s",
                                self.label, e,
                            )
        except httpx.HTTPError as e:
            raise PiSidecarError(
                f"{self.label} sidecar transport error: {e}"
            ) from e

        if not run_end:
            # Stream ended without a run_end frame — transport dropped or the
            # sidecar crashed; callers treat this like a partial run.
            self.logger.warning(
                "%s sidecar prompt stream ended without a run_end frame "
                "(session=%s, %d events observed)",
                self.label, session_id, len(events),
            )
            run_end = {
                "type": _RUN_END_TYPE,
                "aborted": False,
                "error": f"{self.label} sidecar stream ended without run_end",
            }
        return run_end, events

    def _abort_quietly(self, session_id: str) -> None:
        try:
            self.abort(session_id)
        except Exception as e:  # best effort during timeout handling
            self.logger.warning(
                "%s sidecar abort during timeout failed: %s", self.label, e
            )

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the pooled HTTP client (sessions live on the sidecar)."""
        if self._closed:
            return
        self._closed = True
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    def __enter__(self) -> "PiSidecarClient":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()