#!/usr/bin/python
"""
Goose Agent Handler — A2A Bridge to a ``goose serve`` ACP server

This handler implements the Phase 10 bridge contract (``ask_model``) for
goose (https://github.com/aaif-goose/goose), fronting an ACP-over-HTTP
``goose serve`` instance the same way ``HermesAgentHandler`` /
``OpenClawAgentHandler`` front their APIs.

Endpoints used (wire details in handlers/goose_acp_client.py):

- ``POST /acp``  (initialize / session/new / session/load / session/prompt
                  / session/set_mode / notification session/cancel / responses)
- ``GET  /acp``  (SSE streams delivering results and session updates)
- ``DELETE /acp`` (closes the connection and its sessions)

Conversation design:
- goose keeps the conversation history, so unlike Hermes/OpenClaw we send
  only the newest user turn on every call.
- The A2A ``context_id`` (``context["thread_uuid"]``) is mapped to a goose
  ``sessionId`` through a partition-scoped ``a2a_settings`` record
  (``setting_id = "goose_session#{context_id}"``) via the repository
  dispatch layer — the same pattern as ``DurablePushNotificationConfigStore``.
  Client-supplied ids are NEVER passed to ``session/load`` unverified:
  goose sessions are not tenant-aware, so handing over any client-supplied
  id would let one tenant resume another's conversation. Unknown ids and
  failed loads simply start a new session.
- The handler returns ``metadata.thread_uuid = goose sessionId`` so the
  executor adopts the goose session as the conversation's ``context_id``
  (``effective_context_id`` in ``a2a_executor.py``).

Streaming chunk names pushed to ``stream_queue``: ``run_id``, ``token``,
``tool_call``, ``tool_result``, ``approval``, ``error``. Reasoning deltas
(``agent_thought_chunk``) arrive tagged ``meta.data_format = "reasoning"``.

Config (per-agent metadata -> setting -> Config -> default):
- goose_api_url           / GOOSE_API_URL            http://localhost:3284
- goose_secret_key        / GOOSE_SECRET_KEY         (X-Secret-Key header)
- goose_cwd               / GOOSE_CWD                /workspace
- goose_mcp_servers       / GOOSE_MCP_SERVERS        (JSON, default [])
- goose_permission_mode   / GOOSE_PERMISSION_MODE    auto_allow|deny|ask
- goose_timeout           / GOOSE_STREAM_TIMEOUT     300
- goose_approval_timeout  / GOOSE_APPROVAL_TIMEOUT   120

Permission policies for ``session/request_permission``:
- ``auto_allow``: answer ``allow_once`` (default for sandboxed goose containers).
- ``deny``: answer ``reject_once``.
- ``ask``: emit an ``approval`` chunk (executor replies INPUT_REQUIRED), then
  block on ``resolve_approval(run_id, approved)`` or
  ``goose_approval_timeout`` (then reject). Requires the same process to hold
  the session stream (daemon mode) — serverless deployments should use
  ``auto_allow``/``deny``.

Provider/model selection is owned by the goose server itself (GOOSE_PROVIDER,
GOOSE_MODEL, its credentials, GOOSE_MODE for server-side tool approval,
GOOSE_DISABLE_KEYRING=1). The secret key is never logged or persisted in
agent metadata (env/setting take priority over metadata for it).
"""

import json
import logging
import os
import threading
from typing import Any

from .config import Config
from .goose_acp_client import GooseAcpClient, GooseAcpError

__author__ = "bibow"

# Reasoning chunks are tagged so the bridge keeps them OUT of the answer
# (same pattern as the Hermes reasoning fix).
REASONING_META = {"data_format": "reasoning"}

# goose session modes surfaced by session/new (see session/set_mode).
ACP_MODE_FOR_PERMISSION = {
    "auto_allow": "auto",
    "deny": "auto",
    "ask": "approve",
}

# Prompt-result stop reasons that mean a partial/failed turn.
_PARTIAL_STOP_REASONS = {
    "cancelled": "Run cancelled",
    "refusal": "Goose refused the request",
    "max_tokens": "Goose hit the token limit (partial content)",
    "max_turn_requests": "Goose stopped: too many turn requests",
}


class _FakeInfo:
    """Minimal ResolveInfo stand-in for the repository dispatch calls."""

    def __init__(self, context: dict[str, Any]) -> None:
        self.context = context


class GooseAgentHandler:
    """A2A handler bridging tasks to a ``goose serve`` ACP server."""

    def __init__(
        self,
        logger: logging.Logger,
        agent_config: dict[str, Any],
        setting: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
        http_transport: Any = None,
    ) -> None:
        self.logger = logger
        self.agent_config = agent_config or {}
        self.setting = setting or {}
        self.http_transport = http_transport  # test injection (httpx.MockTransport)

        # Priority: agent metadata -> setting -> Config -> default.
        metadata = self.agent_config.get("metadata") or self.agent_config
        if not isinstance(metadata, dict):
            metadata = {}

        self.goose_api_url = str(
            metadata.get("goose_api_url")
            or self.setting.get("GOOSE_API_URL")
            or os.environ.get("GOOSE_API_URL")
            or getattr(Config, "goose_api_url", None)
            or "http://localhost:3284"
        )
        # Prefer env/setting for the secret over per-agent metadata (avoid
        # storing the goose server secret in tenant-writable metadata).
        self.goose_secret_key = (
            self.setting.get("GOOSE_SECRET_KEY")
            or os.environ.get("GOOSE_SECRET_KEY")
            or metadata.get("goose_secret_key")
            or getattr(Config, "goose_secret_key", None)
            or ""
        )
        self.goose_cwd = str(
            metadata.get("goose_cwd")
            or self.setting.get("GOOSE_CWD")
            or os.environ.get("GOOSE_CWD")
            or getattr(Config, "goose_cwd", None)
            or "/workspace"
        )
        self.goose_mcp_servers = self._parse_mcp_servers(
            metadata.get("goose_mcp_servers")
            or self.setting.get("GOOSE_MCP_SERVERS")
            or os.environ.get("GOOSE_MCP_SERVERS")
            or getattr(Config, "goose_mcp_servers", None)
            or []
        )
        self.goose_permission_mode = (
            metadata.get("goose_permission_mode")
            or self.setting.get("GOOSE_PERMISSION_MODE")
            or os.environ.get("GOOSE_PERMISSION_MODE")
            or getattr(Config, "goose_permission_mode", None)
            or "auto_allow"
        )
        if self.goose_permission_mode not in ("auto_allow", "deny", "ask"):
            self.goose_permission_mode = "auto_allow"
        self.timeout = float(
            metadata.get("goose_timeout")
            or self.setting.get("GOOSE_STREAM_TIMEOUT")
            or os.environ.get("GOOSE_STREAM_TIMEOUT")
            or getattr(Config, "goose_stream_timeout", None)
            or 300.0
        )
        self.approval_timeout = float(
            metadata.get("goose_approval_timeout")
            or self.setting.get("GOOSE_APPROVAL_TIMEOUT")
            or os.environ.get("GOOSE_APPROVAL_TIMEOUT")
            or getattr(Config, "goose_approval_timeout", None)
            or 120.0
        )

        # The live ACP connection for the current run (set by _new_client);
        # cancel_run()/resolve_approval() on the executor thread reuse it.
        self._client: GooseAcpClient | None = None
        # Pending approvals: session_id -> permission state (ask mode).
        self._pending_approvals: dict[str, dict[str, Any]] = {}
        # The active ask_model run's stream queue (approval chunk target).
        self._stream_queue: Any = None
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_mcp_servers(raw: Any) -> Any:
        """Accept a JSON string or an already-parsed list of MCP servers."""
        if raw is None or raw == "":
            return []
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except ValueError:
                return []
            return parsed if isinstance(parsed, list) else []
        return raw if isinstance(raw, list) else []

    # ------------------------------------------------------------------
    # Phase 10 bridge contract
    # ------------------------------------------------------------------

    def ask_model(
        self,
        input_messages: list[dict[str, Any]],
        context: dict[str, Any],
        stream_queue: Any = None,
        stream_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Execute the agent turn (non-streaming or streaming)."""
        if stream_queue is not None:
            return self._ask_streaming(
                input_messages, context, stream_queue,
                stream_event or threading.Event(),
            )
        return self._ask_non_streaming(input_messages, context)

    def _ask_non_streaming(
        self, input_messages: list[dict[str, Any]], context: dict[str, Any]
    ) -> dict[str, Any]:
        content_blocks = self._to_prompt_blocks(input_messages)
        chunks: list[str] = []
        client: GooseAcpClient | None = None
        try:
            client, session_id = self._open_session(context)
            result, updates = self._prompt_with_retry(
                client, session_id, content_blocks, _on_update=None
            )
            for wrapped in updates:
                self._process_update(
                    (wrapped.get("params") or {}).get("update") or {}, chunks, None
                )
            return self._build_result(
                client, session_id, result, "".join(chunks)
            )
        except GooseAcpError as e:
            return self._error_result(e)
        except Exception as e:
            self.logger.warning(f"GooseAgentHandler non-streaming error: {e}")
            return {"content": "", "role": "agent", "error": str(e)}
        finally:
            self._close_session_run(client)

    def _ask_streaming(
        self,
        input_messages: list[dict[str, Any]],
        context: dict[str, Any],
        stream_queue: Any,
        stream_event: threading.Event,
    ) -> dict[str, Any]:
        content_blocks = self._to_prompt_blocks(input_messages)
        chunks: list[str] = []
        client: GooseAcpClient | None = None
        self._stream_queue = stream_queue

        def _on_update(wrapped: dict[str, Any]) -> None:
            if stream_event.is_set():
                return
            update = (wrapped.get("params") or {}).get("update") or {}
            self._process_update(update, chunks, stream_queue)

        try:
            client, session_id = self._open_session(context)
            run_id = f"{client.connection_id}:{session_id}"
            # Publish the run id BEFORE any update chunk so the executor can
            # register cancel/approval routing for the in-flight run.
            stream_queue.put({"name": "run_id", "value": run_id})

            result, updates = self._prompt_with_retry(
                client, session_id, content_blocks, _on_update=_on_update
            )
            return self._build_result(
                client, session_id, result, "".join(chunks)
            )
        except GooseAcpError as e:
            self.logger.warning(f"GooseAgentHandler streaming error: {e}")
            return self._error_result(e, stream_queue=stream_queue)
        except Exception as e:
            self.logger.warning(f"GooseAgentHandler streaming error: {e}")
            stream_queue.put({"name": "error", "value": str(e)})
            return {"content": "".join(chunks), "role": "agent", "error": str(e)}
        finally:
            stream_event.set()
            self._close_session_run(client)

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def _open_session(
        self, context: dict[str, Any]
    ) -> tuple[GooseAcpClient, str]:
        """initialize -> streams -> session/(new|load) -> set_mode."""
        client = self._new_client()
        try:
            client.initialize()
            client.start_streams()

            session_id = self._resolve_session_id(client, context)
            # session/set_mode results ride the session-scoped stream; open it
            # before issuing set_mode (prompt() uses the same stream).
            client._ensure_session_stream(session_id)
            mode = ACP_MODE_FOR_PERMISSION.get(self.goose_permission_mode)
            if mode:
                try:
                    client.set_mode(session_id, mode)
                except GooseAcpError as e:
                    self.logger.warning(
                        "goose set_mode(%s) failed: %s", self.goose_permission_mode, e
                    )
            return client, session_id
        except Exception:
            try:
                client.close()
            except Exception:
                pass
            if self._client is client:
                self._client = None
            raise

    def _prompt_with_retry(
        self,
        client: GooseAcpClient,
        session_id: str,
        content_blocks: list[dict[str, Any]],
        _on_update: Any,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Run the prompt, retrying once when a previous run is still active.

        Concurrent prompts fail with JSON-RPC -32602 ("session already has
        active run ..."); a session/cancel followed by one retry recovers a
        run orphaned by a previous failure.
        """
        try:
            return client.prompt(
                session_id, content_blocks,
                on_update=_on_update, timeout=self.timeout,
            )
        except GooseAcpError as e:
            if not e.is_active_run_error():
                raise
            self.logger.warning(
                "goose session %s had an active run; cancelling and retrying once",
                session_id,
            )
            self._cancel_session(client, session_id)
            # Wait for any pending ask-mode approval to settle (max 10 s).
            import time

            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                with self._lock:
                    pending = self._pending_approvals.pop(session_id, None)
                if pending is None:
                    break
                time.sleep(0.2)
            return client.prompt(
                session_id, content_blocks,
                on_update=_on_update, timeout=self.timeout,
            )

    def _close_session_run(self, client: GooseAcpClient | None) -> None:
        """DELETE /acp and clear the live client — unless an ask-mode approval
        is still pending (the permission worker needs the connection to post
        its answer; the outstanding connection is bounded by that one run)."""
        if client is None:
            return
        try:
            with self._lock:
                approval_pending = any(
                    pending.get("client") is client
                    for pending in self._pending_approvals.values()
                )
            if approval_pending:
                self.logger.info(
                    "goose connection kept open: ask-mode approval pending"
                )
                return
            client.close()
        finally:
            if self._client is client:
                self._client = None

    # ------------------------------------------------------------------
    # Result assembly
    # ------------------------------------------------------------------

    def _build_result(
        self,
        client: GooseAcpClient,
        session_id: str,
        result: dict[str, Any],
        content: str,
    ) -> dict[str, Any]:
        result_payload = result.get("result") or {}
        stop_reason = result_payload.get("stopReason") or ""
        run_id = f"{client.connection_id}:{session_id}"
        metadata: dict[str, Any] = {
            "run_id": run_id,
            "thread_uuid": session_id,
            "goose": {
                "connectionId": client.connection_id,
                "sessionId": session_id,
                "stopReason": stop_reason or None,
                "usage": result_payload.get("usage") or {},
                "model": client.agent_info.get("name"),
            },
        }
        output: dict[str, Any] = {
            "content": content,
            "role": "agent",
            "metadata": metadata,
        }
        if stop_reason in _PARTIAL_STOP_REASONS:
            output["error"] = _PARTIAL_STOP_REASONS[stop_reason]
        elif stop_reason != "end_turn" and result.get("error"):
            err = result.get("error") or {}
            output["error"] = err.get("message") or f"goose ended with {stop_reason}"
        return output

    def _error_result(
        self,
        error: GooseAcpError | Exception,
        stream_queue: Any = None,
    ) -> dict[str, Any]:
        message = str(error)
        if isinstance(error, GooseAcpError) and error.is_auth_error():
            message = (
                f"goose server rejected the A2A gateway credentials "
                f"(HTTP {error.status_code}); verify GOOSE_SERVER__SECRET_KEY "
                "and GOOSE_SECRET_KEY"
            )
        elif isinstance(error, GooseAcpError) and error.is_transport_bug():
            message = (
                f"goose server refused the A2A gateway transport "
                f"(HTTP {error.status_code}); transport bug — see "
                "handlers/goose_acp_client.py"
            )
        elif isinstance(error, GooseAcpError) and error.is_connection_error():
            message = (
                f"goose ACP connection not found (HTTP 404): {message}"
            )
        if stream_queue is not None:
            stream_queue.put({"name": "error", "value": message})
        return {
            "content": "",
            "role": "agent",
            "error": message,
            "metadata": {"goose_error": str(error)},
        }

    # ------------------------------------------------------------------
    # Cancel / approval passthrough
    # ------------------------------------------------------------------

    def cancel_run(self, run_id: str) -> bool:
        """``session/cancel`` on the run's goose session.

        ``run_id`` is ``"{connectionId}:{sessionId}"`` from the ``run_id``
        stream chunk. The pending prompt then resolves with
        ``stopReason: cancelled`` and the task ends CANCELED.
        """
        session_id = self._session_id_from_run_id(run_id)
        if not session_id:
            self.logger.warning(
                "GooseAgentHandler.cancel_run: unparseable run_id %s", run_id
            )
            return False
        try:
            client = self._client
            if client is None or not client.connection_id:
                client = self._new_client()
                client.initialize()
                client.start_streams()
            client.cancel(session_id)
        except Exception as e:
            self.logger.warning(
                f"Failed to cancel goose session {session_id}: {e}"
            )
            return False
        # Reject a pending ask-mode approval so the run cannot hang on it.
        with self._lock:
            pending = self._pending_approvals.pop(session_id, None)
        if pending is not None:
            pending["approved"] = False
            pending["resolved"] = True
            pending["event"].set()
        return True

    def resolve_approval(self, run_id: str, approved: bool, reason: str = "") -> bool:
        """Resolve a pending ``session/request_permission`` (ask mode)."""
        session_id = self._session_id_from_run_id(run_id)
        with self._lock:
            pending = self._pending_approvals.get(session_id or "")
        if pending is None:
            self.logger.warning(
                "GooseAgentHandler.resolve_approval: no pending approval for %s",
                run_id,
            )
            return False
        pending["approved"] = bool(approved)
        pending["resolved"] = True
        pending["event"].set()
        return True

    # ------------------------------------------------------------------
    # Ask-mode permission handling
    # ------------------------------------------------------------------

    def _on_permission_request(
        self, session_id: str, request: dict[str, Any]
    ) -> Any:
        """Permission policy (goose_permission_mode) for request_permission.

        Returns the goose option id immediately for ``auto_allow``/``deny``;
        for ``ask`` emits an ``approval`` chunk and defers (returns None) —
        the worker answers after ``resolve_approval()` or the timeout.
        """
        mode = self.goose_permission_mode
        if mode == "auto_allow":
            return "allow_once"
        if mode == "deny":
            return "reject_once"

        with self._lock:
            pending: dict[str, Any] = {
                "request": request,
                "resolved": False,
                "approved": None,
                "event": threading.Event(),
                "client": self._client,
            }
            self._pending_approvals[session_id] = pending
        # Surface the pending approval to the bridge (executor replies
        # INPUT_REQUIRED; resolve_approval(run_id, approved) resumes goose).
        if self._stream_queue is not None:
            client = self._client
            run_id = (
                f"{client.connection_id}:{session_id}"
                if client and client.connection_id
                else session_id
            )
            params = request.get("params") or {}
            self._stream_queue.put({
                "name": "approval",
                "value": json.dumps({
                    "run_id": run_id,
                    "session_id": session_id,
                    "tool_call": params.get("toolCall", {}),
                    "options": params.get("options", []),
                }),
            })
        worker = threading.Thread(
            target=self._permission_worker, args=(pending,), daemon=True
        )
        worker.start()
        return None  # defer — the worker answers goose later

    def _permission_worker(self, pending: dict[str, Any]) -> None:
        """Wait for resolve_approval() (or the timeout) then answer goose."""
        approved = pending["event"].wait(self.approval_timeout) and bool(
            pending["approved"]
        )
        option_id = "allow_once" if approved else "reject_once"
        request = pending["request"]
        client = pending["client"] or self._client
        if client is None:
            self.logger.warning("goose approval worker: client already closed")
            return
        try:
            client.answer_permission(
                request["request_id"], request["session_id"], option_id=option_id
            )
        except Exception as e:
            self.logger.warning("goose approval answer failed: %s", e)
        finally:
            with self._lock:
                current = self._pending_approvals.get(request["session_id"])
                if current is pending:
                    self._pending_approvals.pop(request["session_id"], None)

    def _cancel_session(self, client: GooseAcpClient, session_id: str) -> None:
        try:
            client.cancel(session_id)
        except GooseAcpError as e:
            self.logger.warning(
                "goose session/cancel failed for %s: %s", session_id, e
            )

    # ------------------------------------------------------------------
    # context_id -> sessionId mapping (partition-scoped a2a_settings)
    # ------------------------------------------------------------------

    def _resolve_session_id(
        self, client: GooseAcpClient, context: dict[str, Any]
    ) -> str:
        """Map the A2A context_id to a goose sessionId, or start a new one."""
        partition_key = (
            context.get("partition_key") if isinstance(context, dict) else None
        )
        thread_uuid = (
            context.get("thread_uuid") if isinstance(context, dict) else None
        )

        if thread_uuid and partition_key:
            persisted = self._load_session_mapping(partition_key, thread_uuid)
            if persisted:
                try:
                    client.load_session(
                        persisted, self.goose_cwd, self.goose_mcp_servers
                    )
                    return persisted
                except GooseAcpError:
                    self.logger.warning(
                        "goose session %s no longer exists on the server; "
                        "starting a new session for context %s",
                        persisted, thread_uuid,
                    )
            else:
                self.logger.info(
                    "No goose session mapping for context %s; "
                    "starting a new session",
                    thread_uuid,
                )

        session_id = client.new_session(self.goose_cwd, self.goose_mcp_servers)
        if thread_uuid and partition_key:
            self._store_session_mapping(
                partition_key,
                context.get("endpoint_id"),
                context.get("part_id"),
                thread_uuid,
                session_id,
            )
        return session_id

    def _load_session_mapping(self, partition_key: str, context_id: str) -> str | None:
        try:
            repo = self._setting_repo()
            if repo is None:
                return None
            self._set_rls(partition_key)
            row = repo.get(
                partition_key=partition_key,
                setting_id=self._setting_id(context_id),
            )
            if not row:
                return None
            setting = row.get("setting") if isinstance(row, dict) else None
            if isinstance(setting, dict):
                session_id = setting.get("session_id")
                if isinstance(session_id, str) and session_id:
                    return session_id
        except Exception as e:
            self.logger.warning("goose session mapping lookup failed: %s", e)
        return None

    def _store_session_mapping(
        self,
        partition_key: str,
        endpoint_id: Any,
        part_id: Any,
        context_id: str,
        session_id: str,
    ) -> None:
        try:
            repo = self._setting_repo()
            if repo is None:
                self.logger.warning(
                    "goose session mapping not persisted (no a2a_setting repo)"
                )
                return
            self._set_rls(partition_key)
            repo.insert_update(
                _FakeInfo({"partition_key": partition_key, "logger": self.logger}),
                partition_key=partition_key,
                setting_id=self._setting_id(context_id),
                endpoint_id=endpoint_id or str(partition_key).partition("#")[0],
                part_id=part_id or "",
                setting={
                    "context_id": context_id,
                    "session_id": session_id,
                },
                updated_by="goose_handler",
            )
        except Exception as e:
            self.logger.warning(
                "goose session mapping persistence failed (the session still "
                "works for this turn; the next turn starts a fresh one): %s", e
            )

    @staticmethod
    def _setting_id(context_id: str) -> str:
        return f"goose_session#{context_id}"

    @staticmethod
    def _setting_repo() -> Any:
        try:
            from silvaengine_daemon.repositories import get_repo

            return get_repo("a2a_setting")
        except Exception:
            return None

    @staticmethod
    def _set_rls(partition_key: str) -> None:
        # No-op in DynamoDB mode; sets the tenant row-security context in PG.
        if (
            getattr(Config, "DB_BACKEND", None) == "postgresql"
            and getattr(Config, "db_session", None)
        ):
            Config._set_rls_context(partition_key)

    # ------------------------------------------------------------------
    # ACP updates -> A2A stream chunks
    # ------------------------------------------------------------------

    def _process_update(
        self,
        update: dict[str, Any],
        chunks: list[str] | None,
        stream_queue: Any | None,
    ) -> None:
        kind = update.get("sessionUpdate")
        if kind == "agent_message_chunk":
            text = ((update.get("content") or {}).get("text")) or ""
            if text:
                if chunks is not None:
                    chunks.append(text)
                if stream_queue is not None:
                    stream_queue.put({"name": "token", "value": text})
        elif kind == "agent_thought_chunk":
            # Reasoning — tagged, kept out of the answer content.
            text = ((update.get("content") or {}).get("text")) or ""
            if text and stream_queue is not None:
                stream_queue.put({
                    "name": "token", "value": text, "meta": dict(REASONING_META)
                })
        elif kind == "tool_call":
            if stream_queue is not None:
                stream_queue.put({
                    "name": "tool_call",
                    "value": json.dumps({
                        "toolCallId": update.get("toolCallId"),
                        "title": update.get("title"),
                        "kind": update.get("kind"),
                        "rawInput": update.get("rawInput"),
                        "status": update.get("status"),
                    }),
                })
        elif kind == "tool_call_update":
            if stream_queue is not None:
                content_blocks = update.get("content") or []
                text_parts = []
                for block in content_blocks if isinstance(content_blocks, list) else []:
                    inner = (block or {}).get("content") or {}
                    text_parts.append(inner.get("text", "") or "")
                stream_queue.put({
                    "name": "tool_result",
                    "value": json.dumps({
                        "toolCallId": update.get("toolCallId"),
                        "status": update.get("status"),
                        "output": "".join(text_parts),
                        "error": update.get("error"),
                    }),
                })
        elif kind == "plan":
            pass  # status bookkeeping only
        elif kind in ("user_message_chunk",):
            pass  # replayed history must never become answer tokens
        return

    # ------------------------------------------------------------------
    # Content conversion
    # ------------------------------------------------------------------

    def _to_prompt_blocks(
        self, input_messages: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Convert the newest user turn into ACP content blocks.

        goose keeps the conversation history; resending it duplicates every
        turn. Only the newest user message becomes the prompt.
        """
        messages = input_messages or []
        user_msgs = [
            m for m in messages if (m.get("role") or "user") != "agent"
        ] or [messages[-1] if messages else {}]
        newest = user_msgs[-1]
        content = newest.get("content", "")

        blocks: list[dict[str, Any]] = []
        if isinstance(content, str):
            text = content.strip()
            if text:
                blocks.append({"type": "text", "text": text})
            return blocks or [{"type": "text", "text": ""}]

        for part in content if isinstance(content, list) else []:
            if not isinstance(part, dict):
                blocks.append({"type": "text", "text": str(part)})
                continue
            if part.get("text"):
                blocks.append({"type": "text", "text": part["text"]})
                continue
            file_part = part.get("file")
            if isinstance(file_part, dict):
                if file_part.get("bytes"):
                    mime = file_part.get("mimeType") or "application/octet-stream"
                    if mime.startswith("image/") and self._images_supported():
                        blocks.append({
                            "type": "image",
                            "data": file_part["bytes"],
                            "mimeType": mime,
                        })
                    else:
                        blocks.append({
                            "type": "resource",
                            "uri": file_part.get("uri")
                            or f"data:{mime};base64,{file_part['bytes']}",
                            "blob": file_part.get("bytes"),
                            "mimeType": mime,
                            "name": file_part.get("name") or "file",
                        })
                elif file_part.get("uri"):
                    blocks.append({
                        "type": "resource_link",
                        "uri": file_part["uri"],
                        "name": file_part.get("name") or "resource",
                    })
                continue
            data_part = part.get("data")
            if isinstance(data_part, dict):
                blocks.append({
                    "type": "text", "text": json.dumps(data_part, default=str)
                })
        return blocks or [{"type": "text", "text": ""}]

    def _images_supported(self) -> bool:
        client = self._client
        if client is None:
            # No live capabilities handshake yet; allow and let the server
            # reject unsupported blocks if it must.
            return True
        prompt_caps = (client.agent_capabilities or {}).get("promptCapabilities") or {}
        return bool(prompt_caps.get("image", True))

    # ------------------------------------------------------------------
    # Client construction
    # ------------------------------------------------------------------

    def _new_client(self) -> GooseAcpClient:
        client = GooseAcpClient(
            logger=self.logger,
            base_url=self.goose_api_url,
            secret_key=self.goose_secret_key,
            request_timeout=self.timeout,
            request_permission_callback=self._on_permission_request,
            http_transport=self.http_transport,
        )
        self._client = client
        return client

    @staticmethod
    def _session_id_from_run_id(run_id: str) -> str | None:
        if not run_id:
            return None
        if ":" in run_id:
            return run_id.split(":", 1)[1] or None
        return run_id


__all__ = ["GooseAgentHandler"]