#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Banyanos Engine Client — authenticated GraphQL / WebSocket transport.

Implements the backend-native transport contract between the plugin and the
BanyanOS ``agent_engine`` deployed behind ``docker-banyanos-gateway``
(routes: ``/{endpoint_id}/agent_engine_graphql`` HTTP GraphQL and
``/{endpoint_id}/banyan_ws`` WebSocket, see
``silvaengine_gateway/routes.yaml`` / ``module_routes/banyan_ws.yaml``).

Transport contracts (verified against banyanos sources; docs/BANYANOS_INTEGRATION_PLAN.md
"Backend operation contracts"):

Non-streaming (HTTP GraphQL, ``agent_engine_graphql`` route):

    mutation InvokeAgent($input: InvokeAgentInput!, $idempotencyKey: ID!) {
        invokeAgent(input: $input, idempotencyKey: $idempotencyKey) {
            id agentId status outputPayload tokenUsage latencyMs errorMessage startedAt
        }
    }

Streaming (banyan_ws channel, one WS connection per streaming invocation):

1. Connect ``{ws_base}/{endpoint_id}/banyan_ws?token=<JWT>&part_id=<tenant>``
   (browsers cannot set headers on the upgrade request; the gateway feeds the
   token to ``perm_engine.PermAuthorizer`` as a Bearer header).
2. Wait for the gateway welcome frame
   ``{"type": "connection_ack", "connection_id": "..."}``.
3. Trigger the subscription over HTTP — the trigger is an HTTP POST of the
   ``agentInvokeStream`` subscription document with a top-level
   ``connection_id`` field (the gateway merges it into dispatch params). Do
   not open a second invocation for the same logical message.
4. Consume WS frames ``{"data": {"agentInvokeStream": {event}}}`` until the
   terminal ``completed`` (full final content) or ``error`` event. ``chunk`` /
   ``reasoning`` contents are deltas; ``completed.content`` is authoritative.

Error model: backend HTTP auth errors, GraphQL execution errors and caller
deadlines are separate categories (``BanyanosClientError.category``). GraphQL
HTTP 200 responses carrying ``errors`` are failures; a missing expected
``data`` field is also an error.

Both transports accept injectable factories (``http_transport`` /
``ws_connect``) so tests script the wire without network access.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid as _uuid
from urllib.parse import urlencode
from dataclasses import dataclass, field
from typing import Any, Callable

import httpx

__author__ = "bibow"

# Error categories mirror the plan's separation of failure kinds. They map to
# distinct A2A semantics upstream (auth -> REJECTED/AUTH_REQUIRED, deadline ->
# retryable FAILED, execution -> FAILED).
ERROR_AUTH = "auth"
ERROR_NOT_FOUND = "not_found"
ERROR_EXECUTION = "execution"
ERROR_TRANSPORT = "transport"
ERROR_TIMEOUT = "timeout"
ERROR_PROTOCOL = "protocol"


class BanyanosClientError(Exception):
    """Typed transport/protocol error raised by BanyanosClient."""

    def __init__(self, message: str, category: str = ERROR_EXECUTION,
                 status_code: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.category = category
        self.status_code = status_code


@dataclass
class BanyanosStreamEvent:
    """One ``agentInvokeStream`` event, decoded from a WS frame."""

    event_type: str  # started/chunk/reasoning/tool_started/tool_finished/completed/error
    content: str = ""
    tool_name: str = ""
    tool_input: str = ""
    tool_output: str = ""
    error: str = ""
    iteration: int = 0
    raw: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_payload(payload: Any) -> "BanyanosStreamEvent | None":
        """Decode one ``agentInvokeStream`` payload dict, or None if unusable."""
        if not isinstance(payload, dict):
            return None
        event_type = payload.get("eventType") or payload.get("event_type") or ""
        return BanyanosStreamEvent(
            event_type=str(event_type),
            content=str(payload.get("content") or ""),
            tool_name=str(payload.get("toolName") or payload.get("tool_name") or ""),
            tool_input=str(payload.get("toolInput") or payload.get("tool_input") or ""),
            tool_output=str(payload.get("toolOutput") or payload.get("tool_output") or ""),
            error=str(payload.get("error") or ""),
            iteration=int(payload.get("iteration") or 0),
            raw=payload,
        )

    @property
    def is_terminal(self) -> bool:
        return self.event_type in ("completed", "error")


class BanyanosClient:
    """Authenticated client for the Banyanos agent_engine gateway surface."""

    def __init__(
        self,
        logger: logging.Logger,
        graphql_url: str,
        ws_url: str,
        endpoint_id: str,
        token: str = "",
        part_id: str = "",
        stream_timeout: float = 180.0,
        http_timeout: float = 60.0,
        http_transport: Any = None,
        ws_connect: Callable[..., Any] | None = None,
    ) -> None:
        """
        ``http_transport`` — optional ``httpx.BaseTransport`` (test injection;
        wrapped into an ``httpx.Client`` for the GraphQL transport; otherwise
        a fresh ``httpx.Client`` per request.

        ``ws_connect`` — optional callable ``connect(uri) -> ws`` returning an
        object with ``recv(timeout) -> frame|None`` (delta semantics), and
        ``send(text)``, ``close()``. Injected transports also receive an
        ``on_close`` callback through ``close()``. When omitted the real
        ``websockets`` library drives a dedicated event loop, mirroring
        ``CoreEngineAgentHandler``.
        """
        self.logger = logger
        self.graphql_url = graphql_url.rstrip("/")
        self.ws_url = ws_url.rstrip("/")
        self.endpoint_id = endpoint_id
        self.token = token
        self.part_id = part_id
        self.stream_timeout = float(stream_timeout)
        self.http_timeout = float(http_timeout)
        self._http_transport = http_transport
        self._ws_connect = ws_connect
        self._ws = None
        self._ws_loop = None
        self._connection_id = None

    # ------------------------------------------------------------------
    # GraphQL (HTTP) transport
    # ------------------------------------------------------------------

    @staticmethod
    def new_idempotency_key(prefix: str = "banyanos") -> str:
        """Fresh idempotency key for one logical invocation.

        The Banyanos mutations require an ``idempotencyKey``; a generated key is
        only uniqueness, not dedup — the handler resolves durable keys from
        execution map so retries of the *same logical message* reuse theirs.
        """
        return f"{prefix}-{_uuid.uuid4().hex}"

    def create_session(
        self,
        agent_id: str,
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        """Create (or reuse, via the backend idempotency key) a Banyanos session.

        Banyanos sessions are purely logical groupings of agent messages; the
        gateway-side ``createSession`` mutation mints and returns the id. The
        key must be stable per (partition, conversation) so a retried turn
        reuses the same session instead of forking the conversation.
        """
        query = """
            mutation CreateSession($input: SessionCreateInput!, $idempotencyKey: ID!) {
                createSession(input: $input, idempotencyKey: $idempotencyKey) {
                    id
                    status
                }
            }
        """
        variables: dict[str, Any] = {
            "input": {"agentId": agent_id},
            "idempotencyKey": idempotency_key or self.new_idempotency_key("banyanos-sess"),
        }
        if isinstance(metadata, dict) and metadata:
            variables["input"]["metadata"] = metadata
        session = self._post_graphql(query, variables, ("createSession",))
        return str(session.get("id") or "")

    def send_message(
        self,
        agent_id: str,
        session_id: str,
        content: str,
        chat_history: list[dict[str, Any]] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Continue a conversation on an existing session (``SendMessage``).

        The Banyanos backend's continuation API: it persists the USER and
        ASSISTANT messages in the session and returns the reply content
        directly. ``chat_history`` is the caller-provided history contract -
        the SendMessage path does NOT load session history itself
        (live-verified 2026-10-28: only the agentInvokeStream subscription
        loads it), so the A2A bridge passes the history it already loaded
        for the conversation. Returns
        ``{success, messageId, sessionId, role, content, latencyMs,
        continuationQueued, taskId}`` (``taskId`` is the backend task row
        that the execution map and dedup observation query).
        """
        query = """
            mutation SendMessage($input: SendMessageInput!, $idempotencyKey: ID!) {
                sendMessage(input: $input, idempotencyKey: $idempotencyKey) {
                    success
                    messageId
                    sessionId
                    role
                    content
                    latencyMs
                    continuationQueued
                    taskId
                }
            }
        """
        variables: dict[str, Any] = {
            "input": {"agentId": agent_id, "sessionId": session_id, "content": content, "asyncExecution": True},
            "idempotencyKey": idempotency_key or self.new_idempotency_key("banyanos-msg"),
        }
        if chat_history:
            variables["input"]["chatHistory"] = json.dumps(
                chat_history, ensure_ascii=False)
        return self._post_graphql(query, variables, ("sendMessage",))

    def invoke_agent(
        self,
        agent_id: str,
        message: str,
        session_id: str | None = None,
        idempotency_key: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        context_variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Invoke an agent synchronously (``invokeAgent`` mutation).

        With ``stream=False`` the Banyanos mutate returns after execution. The
        mutation result is an ``AgentInvocation`` whose ``outputPayload``
        carries ``content``. Returns the invocation dict.
        """
        key = idempotency_key or self.new_idempotency_key()
        query = """
            mutation InvokeAgent($input: InvokeAgentInput!, $idempotencyKey: ID!) {
                invokeAgent(input: $input, idempotencyKey: $idempotencyKey) {
                    id
                    agentId
                    status
                    outputPayload
                    tokenUsage
                    latencyMs
                    errorMessage
                    startedAt
                }
            }
        """
        variables: dict[str, Any] = {
            "input": {"agentId": agent_id, "message": message, "stream": False},
            "idempotencyKey": key,
        }
        if session_id:
            variables["input"]["sessionId"] = session_id
        if temperature is not None:
            variables["input"]["temperature"] = temperature
        if max_tokens is not None:
            variables["input"]["maxTokens"] = max_tokens
        if context_variables is not None:
            variables["input"]["contextVariables"] = context_variables

        invocation = self._post_graphql(query, variables, ("invokeAgent",))
        invocation["idempotency_key"] = key
        # Preserve the caller's idempotency key on the result so the execution
        # map can correlate a retried logical message to one backend task.
        return invocation

    def task_status(self, task_id: str) -> dict[str, Any]:
        """Query an existing agent task (``taskStatus``) for reconciliation."""
        query = """
            query TaskStatus($taskId: ID!) {
                taskStatus(taskId: $taskId) {
                    id
                    agentId
                    status
                    outputPayload
                    errorMessage
                    startedAt
                    finishedAt
                }
            }
        """
        return self._post_graphql(
            query, {"taskId": task_id}, ("taskStatus",)
        )

    def cancel_task(self, task_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
        """Cancel a running/pending agent task (``cancelTask`` mutation)."""
        query = """
            mutation CancelTask($taskId: ID!, $idempotencyKey: ID!) {
                cancelTask(taskId: $taskId, idempotencyKey: $idempotencyKey) {
                    id
                    status
                    errorMessage
                }
            }
        """
        return self._post_graphql(
            query,
            {
                "taskId": task_id,
                "idempotencyKey": idempotency_key or self.new_idempotency_key("banyanos-cancel"),
            },
            ("cancelTask",),
        )

    def _post_graphql(self, query: str, variables: dict[str, Any],
                      expected_path: tuple[str, ...]) -> dict[str, Any]:
        """POST a GraphQL document; return the inner data object at keypath."""
        url = f"{self.graphql_url}/{self.endpoint_id}/agent_engine_graphql"
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.part_id:
            headers["Part-Id"] = self.part_id

        try:
            if self._http_transport is not None:
                with httpx.Client(transport=self._http_transport,
                                  timeout=self.http_timeout) as client:
                    resp = client.post(url, json={"query": query, "variables": variables},
                                       headers=headers)
            else:
                resp = httpx.post(url, json={"query": query, "variables": variables},
                                  headers=headers, timeout=self.http_timeout)
        except httpx.TimeoutException as exc:
            raise BanyanosClientError("Backend request timed out",
                                    category=ERROR_TIMEOUT) from exc
        except httpx.HTTPStatusError as exc:
            raise BanyanosClientError(
                f"Transport error: {exc}", category=ERROR_TRANSPORT
            ) from exc
        except Exception as exc:
            raise BanyanosClientError(
                f"Transport error: {exc}", category=ERROR_TRANSPORT
            ) from exc

        if resp.status_code == 401 or resp.status_code == 403:
            raise BanyanosClientError(
                "Backend authentication failed",
                category=ERROR_AUTH,
                status_code=resp.status_code,
            )
        if resp.status_code >= 400:
            raise BanyanosClientError(
                "Backend HTTP error",
                category=ERROR_TRANSPORT,
                status_code=resp.status_code,
            )
        try:
            payload = resp.json()
        except Exception as exc:
            raise BanyanosClientError("Non-JSON GraphQL response",
                                    category=ERROR_PROTOCOL) from exc

        # DEF-BNY-001 (2026-10-27): invalid JSON shapes are protocol errors.
        if not isinstance(payload, dict):
            raise BanyanosClientError("GraphQL response must be an object",
                                    category=ERROR_PROTOCOL)
        errors = payload.get("errors")
        if errors:
            first = errors[0] if isinstance(errors, list) else None
            message = (first or {}).get("message", "GraphQL error") \
                if isinstance(first, dict) else str(first)
            code = (first or {}).get("extensions", {}).get("code", "") \
                if isinstance(first, dict) else ""
            category = ERROR_AUTH if code in ("UNAUTHENTICATED", "FORBIDDEN") \
                else ERROR_EXECUTION
            raise BanyanosClientError(f"GraphQL error: {message}", category=category)

        data = payload.get("data")
        if not isinstance(data, dict):
            raise BanyanosClientError(
                "GraphQL response missing data field", category=ERROR_PROTOCOL
            )
        for key in expected_path:
            data = data.get(key)
            if data is None:
                raise BanyanosClientError(
                    f"GraphQL response missing data.{key}", category=ERROR_PROTOCOL
                )
        return data

    # ------------------------------------------------------------------
    # Streaming transport (banyan_ws channel)
    # ------------------------------------------------------------------

    def invoke_agent_stream(
        self,
        agent_id: str,
        message: str,
        session_id: str | None = None,
        include_reasoning: bool = False,
        on_event: Callable[[BanyanosStreamEvent], None] | None = None,
    ) -> BanyanosStreamEvent:
        """Streaming invocation over the banyan_ws channel.

        Drives the four-step contract (connect, ack, HTTP subscription trigger,
        WS consume) and returns the terminal event. Every event is also handed
        to ``on_event`` as it arrives, so the handler can forward deltas while
        the stream is live. Exactly one invocation per connection: the frames
        carry no request identity, so concurrent invokes on one socket cannot
        be attributed (mirrors the front-end client's serial queue).
        """
        return self._stream(
            subscription_document=(
                "subscription AgentInvokeStream(\n"
                "    $agentId: ID!\n"
                "    $message: String!\n"
                "    $sessionId: ID\n"
                "    $includeReasoning: Boolean\n"
                "  ) {\n"
                "    agentInvokeStream(\n"
                "      agentId: $agentId\n"
                "      message: $message\n"
                "      sessionId: $sessionId\n"
                "      includeReasoning: $includeReasoning\n"
                "    ) {\n"
                "      eventType\n"
                "      content\n"
                "      toolName\n"
                "      toolInput\n"
                "      toolOutput\n"
                "      error\n"
                "      iteration\n"
                "      timestamp\n"
                "    }\n"
                "  }\n"
            ),
            variables={
                "agentId": agent_id,
                "message": message,
                "sessionId": session_id or None,
                "includeReasoning": bool(include_reasoning),
            },
            on_event=on_event,
        )

    def connect_ws(self) -> str:
        """Open the banyan_ws connection and return its ``connection_id``."""
        uri = self._build_ws_uri()
        ws = self._connect_ws(uri)
        self._ws = ws

        ack = self._ws_recv(ws, timeout=15.0)
        if not isinstance(ack, dict) or ack.get("type") != "connection_ack":
            self._close_ws()
            raise BanyanosClientError(
                f"banyan_ws handshake failed, expected connection_ack frame, got: {ack}",
                category=ERROR_PROTOCOL,
            )
        connection_id = ack.get("connection_id") or ""
        if not connection_id:
            self._close_ws()
            raise BanyanosClientError(
                "banyan_ws connection_ack without connection_id",
                category=ERROR_PROTOCOL,
            )
        self._connection_id = connection_id
        return connection_id

    def trigger_subscription(self, document: str, variables: dict[str, Any]) -> None:
        """Trigger an already-connected subscription over HTTP GraphQL.

        The gateway merges the top-level ``connection_id`` into dispatch
        params; subsequent subscription events arrive on the WS channel.
        """
        if not self._connection_id:
            raise BanyanosClientError(
                "cannot trigger subscription before banyan_ws connect",
                category=ERROR_PROTOCOL,
            )
        url = f"{self.graphql_url}/{self.endpoint_id}/agent_engine_graphql"
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.part_id:
            headers["Part-Id"] = self.part_id

        body: dict[str, Any] = {
            "query": document,
            "variables": variables,
            "connection_id": self._connection_id,
        }
        if self._http_transport is not None:
            with httpx.Client(transport=self._http_transport,
                              timeout=self.http_timeout) as client:
                resp = client.post(url, json=body, headers=headers)
        else:
            resp = httpx.post(url, json=body, headers=headers,
                              timeout=self.http_timeout)

        if resp.status_code in (401, 403):
            self._close_ws()
            raise BanyanosClientError("Backend authentication failed",
                                    category=ERROR_AUTH,
                                    status_code=resp.status_code)
        if resp.status_code >= 400:
            self._close_ws()
            raise BanyanosClientError(
                "Subscription trigger failed",
                category=ERROR_TRANSPORT,
                status_code=resp.status_code,
            )
        try:
            payload = resp.json()
        except Exception as exc:
            self._close_ws()
            raise BanyanosClientError("Non-JSON subscription trigger response",
                                    category=ERROR_PROTOCOL) from exc
        # DEF-BNY-001 (2026-10-27): invalid JSON shapes are protocol errors.
        if not isinstance(payload, dict):
            raise BanyanosClientError("GraphQL response must be an object",
                                    category=ERROR_PROTOCOL)
        errors = payload.get("errors")
        if errors:
            self._close_ws()
            first = errors[0] if isinstance(errors, list) else errors
            message = first.get("message", "GraphQL error") \
                if isinstance(first, dict) else str(first)
            # A rejected subscription cannot deliver a terminal event; the
            # socket is closed so late frames cannot join the next invocation.
            raise BanyanosClientError(f"Subscription rejected: {message}",
                                    category=ERROR_EXECUTION)

    def _stream(self, subscription_document: str, variables: dict[str, Any],
                on_event: Callable[[BanyanosStreamEvent], None] | None) -> BanyanosStreamEvent:
        """Connect, trigger, and drain — returning the terminal event."""
        terminal: BanyanosStreamEvent | None = None
        try:
            self.connect_ws()
            self.trigger_subscription(subscription_document, variables)

            import time

            deadline = time.monotonic() + self.stream_timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    terminal = BanyanosStreamEvent(
                        event_type="error",
                        error=(f"banyan_ws stream timeout after "
                               f"{self.stream_timeout}s without terminal event"),
                    )
                    break
                try:
                    frame = self._ws_recv(self._ws, timeout=min(remaining, 30.0))
                except TimeoutError:
                    # DEF-BNY-002 (2026-10-27): an idle receive interval
                    # is not the configured invocation deadline.
                    continue
                if frame is None:
                    terminal = BanyanosStreamEvent(
                        event_type="error",
                        error="banyan_ws closed before terminal event",
                    )
                    break

                event = self._decode_frame(frame)
                if event is None:
                    continue
                if on_event is not None:
                    try:
                        on_event(event)
                    except Exception:  # consumer errors must not kill the stream
                        self.logger.exception("on_event consumer failed")
                if event.is_terminal:
                    terminal = event
                    break
        except BanyanosClientError:
            raise
        finally:
            # Always close observation; a terminal event may already be set.
            if terminal is None:
                terminal = BanyanosStreamEvent(
                    event_type="error", error="banyan_ws stream ended unexpectedly"
                )
            self._close_ws()
        self.logger.info(
            "banyanos stream terminal: %s%s",
            terminal.event_type,
            f" ({terminal.error})" if terminal.error else "",
        )
        return terminal

    @staticmethod
    def _decode_frame(frame: Any) -> BanyanosStreamEvent | None:
        """Decode ``{"data": {"agentInvokeStream": {...}}}`` subscription frames."""
        if not isinstance(frame, dict):
            return None
        if frame.get("type") == "error":
            # Gateway-level error frame (auth/partition issues).
            return BanyanosStreamEvent(
                event_type="error", error=str(frame.get("detail") or "gateway error")
            )
        data = frame.get("data")
        if not isinstance(data, dict):
            return None
        payload = data.get("agentInvokeStream")
        return BanyanosStreamEvent.from_payload(payload)

    # ------------------------------------------------------------------
    # WebSocket plumbings (mirrors CoreEngineAgentHandler)
    # ------------------------------------------------------------------

    def _build_ws_uri(self) -> str:
        """banyan_ws URI with token and part_id as query params (browsers
        cannot add headers to an upgrade; the gateway feeds the token to
        PermAuthorizer as the Bearer header)."""
        uri = f"{self.ws_url}/{self.endpoint_id}/banyan_ws"
        params: dict[str, str] = {}
        if self.token:
            params["token"] = self.token
        if self.part_id:
            params["part_id"] = self.part_id
        if params:
            uri += "?" + urlencode(params)
        return uri

    def _connect_ws(self, uri: str) -> Any:
        if self._ws_connect is not None:
            ws = self._ws_connect(uri)
            self._ws_loop = None
            return ws

        import websockets

        # ask_model runs in a worker thread; give the connection its own loop.
        self._ws_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._ws_loop)
        self._ws = self._ws_loop.run_until_complete(
            websockets.connect(uri, max_size=2**20)
        )
        return self._ws

    def _ws_recv(self, ws: Any, timeout: float = 30.0) -> dict[str, Any] | None:
        if self._ws_connect is not None:
            raw = ws.recv(timeout=timeout)
            if raw is None:
                return None
            return json.loads(raw) if isinstance(raw, str) else raw


        async def _recv():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                return json.loads(raw)
            except asyncio.TimeoutError:
                raise TimeoutError(f"banyan_ws recv timeout after {timeout}s")

        return self._ws_loop.run_until_complete(_recv())

    def _close_ws(self) -> None:
        """Close the WS and tear down the private event loop. Idempotent."""
        self._connection_id = None
        ws = getattr(self, "_ws", None)
        if ws is None and getattr(self, "_ws_loop", None) is None:
            return
        if ws is not None and self._ws_connect is not None:
            close = getattr(ws, "close", None)
            if close:
                close()
            self._ws = None
            return
        loop = getattr(self, "_ws_loop", None)
        if ws is not None and loop is not None and not loop.is_closed():
            try:
                loop.run_until_complete(ws.close())
            except Exception:
                pass
        if loop is not None:
            try:
                loop.close()
            except Exception:
                pass
        self._ws = None
        self._ws_loop = None
        self._connection_id = None


__all__ = [
    "BanyanosClient",
    "BanyanosClientError",
    "BanyanosStreamEvent",
    "ERROR_AUTH",
    "ERROR_EXECUTION",
    "ERROR_NOT_FOUND",
    "ERROR_PROTOCOL",
    "ERROR_TIMEOUT",
    "ERROR_TRANSPORT",
]