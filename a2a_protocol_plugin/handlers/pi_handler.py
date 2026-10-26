#!/usr/bin/python
"""
pi Agent Handler — A2A Bridge to the pi sidecar service

This handler implements the Phase 10 bridge contract (``ask_model``) for
pi (https://github.com/earendil-works/pi, pi.dev) — a minimal, self-extensible
coding agent with a multi-provider LLM API. pi has NO network server; the
docker-a2a-gateway ``pi/`` compose sibling is a small Node sidecar that spawns
``pi --mode rpc`` child processes over stdin/stdout JSONL (the official pi RPC
protocol, see pi docs rpc.md / rpc-commands.md / json.md /
rpc-extension-ui.md) and exposes HTTP + SSE. This handler fronts the sidecar
the same way ``GooseAgentHandler`` fronts ``goose serve``.

Endpoints used (wire details in handlers/pi_client.py):

- ``GET    /health``                    — liveness (healthcheck scripts only)
- ``POST   /sessions``                  — start (or re-open) a pi session
- ``POST   /sessions/{id}/prompt``      — run one turn (SSE of pi events)
- ``POST   /sessions/{id}/abort``       — cancel the running turn
- ``POST   /sessions/{id}/ui-response`` — answer an extension UI dialog
- ``DELETE /sessions/{id}``             — stop the live pi process

Conversation design (identical to goose):
- pi keeps the conversation history in its session file, so, unlike
  Hermes/OpenClaw, only the newest user turn is sent on every call.
- The conversation key — the caller's explicit ``context["thread_uuid"]``,
  else the A2A ``context["context_id"]`` (the reply's ``contextId``, echoed
  back by the client) — is mapped to a pi ``sessionId`` through a
  partition-scoped ``a2a_settings`` record
  (``setting_id = "pi_session#{key}"``, see ``conversation_map.py``).
  Client-supplied session paths are NEVER sent to the sidecar: pi sessions
  are not tenant-aware, so any client-supplied id could resume another
  tenant's conversation. Unknown keys and failed resumes simply start a new
  session.
- The handler reports ``metadata.thread_uuid = pi sessionId`` for
  diagnostics; the reply's ``contextId`` stays the conversation key.

Streaming chunk names pushed to ``stream_queue``: ``run_id``, ``token``,
``tool_call``, ``tool_result``, ``approval``, ``error``. pi reasoning deltas
(``assistantMessageEvent.thinking_delta``) arrive tagged
``meta.data_format = "reasoning"``; ``text_delta`` is the answer;
``thinking_end`` / ``message_end`` full-content copies are NEVER re-emitted
(no duplicated answer as reasoning — the Hermes ``reasoning.available`` bug).

Events of the current run only are forwarded: the sidecar streams a run
window (prompt → ``run_end``), so a resumed session's history never becomes
tokens (no epoch/replay guard needed, unlike goose's ACP ``session/load``).

pi built-in tools run WITHOUT approval prompts (the pi process's own OS
permission is the boundary — we own the container; ``pi_permission_mode``
gates only extension UI dialogs, see docs/rpc-extension-ui.md).

Config (per-agent metadata -> setting -> Config -> default):
- pi_api_url             / PI_API_URL             http://localhost:3300
- pi_secret_key          / PI_SECRET_KEY          (X-Secret-Key header;
    fallback only — env/setting win over tenant-writable agent metadata)
- pi_cwd                 / PI_CWD                 /workspace
- pi_model               / PI_MODEL               (provider/modelId; sidecar default)
- pi_thinking            / PI_THINKING            (off…max; sidecar default)
- pi_permission_mode     / PI_PERMISSION_MODE     auto_allow|deny|ask
- pi_busy_behavior       / PI_BUSY_BEHAVIOR       followUp|steer|reject
- pi_timeout             / PI_STREAM_TIMEOUT      300
- pi_approval_timeout    / PI_APPROVAL_TIMEOUT    120
"""

import json
import logging
import os
import threading
from typing import Any

from .config import Config
from .conversation_map import default_setting_repo, load_mapping, store_mapping
from .pi_client import PiSidecarClient, PiSidecarError

__author__ = "bibow"

# Reasoning chunks are tagged so the bridge keeps them OUT of the answer
# (same pattern as the goose / Hermes reasoning handling).
REASONING_META = {"data_format": "reasoning"}

# pi assistantMessageEvent subtypes that stream content (docs/json.md).
# start/*_end/done carry authoritative full-block copies — they must never be
# re-emitted as tokens (they would duplicate already-streamed deltas).
_TEXT_EVENT = "text_delta"
_THINKING_EVENT = "thinking_delta"

# Dialog methods of the RPC extension UI that BLOCK until answered
# (docs/rpc-extension-ui.md). Everything else (notify/setStatus/setWidget/
# setTitle/set_editor_text) is fire-and-forget and ignored.
_DIALOG_METHODS = ("select", "confirm", "input", "editor")

BUSY_BEHAVIORS = ("followUp", "steer", "reject")
# JSON-RPC-facing names for stream chunks (lowercase), mapped to pi's
# camelCase streamingBehavior parameter.
_BUSY_STREAMING_BEHAVIOR = {"followUp": "followUp", "steer": "steer"}


class PiAgentHandler:
    """A2A handler bridging tasks to the pi sidecar (HTTP + SSE)."""

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

        self.pi_api_url = str(
            metadata.get("pi_api_url")
            or self.setting.get("PI_API_URL")
            or os.environ.get("PI_API_URL")
            or getattr(Config, "pi_api_url", None)
            or "http://localhost:3300"
        )
        # Prefer env/setting for the secret over per-agent metadata (avoid
        # storing the sidecar secret in tenant-writable metadata).
        self.pi_secret_key = (
            self.setting.get("PI_SECRET_KEY")
            or os.environ.get("PI_SECRET_KEY")
            or metadata.get("pi_secret_key")
            or getattr(Config, "pi_secret_key", None)
            or ""
        )
        self.pi_cwd = str(
            metadata.get("pi_cwd")
            or self.setting.get("PI_CWD")
            or os.environ.get("PI_CWD")
            or getattr(Config, "pi_cwd", None)
            or "/workspace"
        )
        self.pi_model = str(
            metadata.get("pi_model")
            or self.setting.get("PI_MODEL")
            or os.environ.get("PI_MODEL")
            or getattr(Config, "pi_model", None)
            or ""
        )
        self.pi_thinking = str(
            metadata.get("pi_thinking")
            or self.setting.get("PI_THINKING")
            or os.environ.get("PI_THINKING")
            or getattr(Config, "pi_thinking", None)
            or ""
        )
        self.pi_permission_mode = (
            metadata.get("pi_permission_mode")
            or self.setting.get("PI_PERMISSION_MODE")
            or os.environ.get("PI_PERMISSION_MODE")
            or getattr(Config, "pi_permission_mode", None)
            or "auto_allow"
        )
        if self.pi_permission_mode not in ("auto_allow", "deny", "ask"):
            self.pi_permission_mode = "auto_allow"
        self.pi_busy_behavior = (
            metadata.get("pi_busy_behavior")
            or self.setting.get("PI_BUSY_BEHAVIOR")
            or os.environ.get("PI_BUSY_BEHAVIOR")
            or getattr(Config, "pi_busy_behavior", None)
            or "followUp"
        )
        if self.pi_busy_behavior not in BUSY_BEHAVIORS:
            self.pi_busy_behavior = "followUp"
        self.timeout = float(
            metadata.get("pi_timeout")
            or self.setting.get("PI_STREAM_TIMEOUT")
            or os.environ.get("PI_STREAM_TIMEOUT")
            or getattr(Config, "pi_stream_timeout", None)
            or 300.0
        )
        self.approval_timeout = float(
            metadata.get("pi_approval_timeout")
            or self.setting.get("PI_APPROVAL_TIMEOUT")
            or os.environ.get("PI_APPROVAL_TIMEOUT")
            or getattr(Config, "pi_approval_timeout", None)
            or 120.0
        )

        # Live sidecar client of the current run (set by _new_client);
        # cancel_run()/resolve_approval() on the executor thread reuse it.
        self._client: PiSidecarClient | None = None
        # Pending extension-UI approvals: dialog request id -> state (ask mode).
        self._pending_approvals: dict[str, dict[str, Any]] = {}
        # The active ask_model run's stream queue (approval chunk target).
        self._stream_queue: Any = None
        # Provider-level failure of the current run (message_end errorMessage
        # — pi reports these without a dedicated error event, P1 spike).
        self._run_error: str | None = None
        # One-up run counter per handler run (run_id = "sessionId:N").
        self._run_seq = 0
        # The live run's pi session id (dialogs carry no sessionId in pi's
        # RPC wire format — the handler knows the current session).
        self._current_session_id: str | None = None
        # Set when the run window's finally ran (possibly deferring the turn
        # close because an ask-mode approval was still pending).
        self._run_done = threading.Event()
        self._lock = threading.RLock()

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
        message, images = self._to_prompt(input_messages)
        chunks: list[str] = []
        client: PiSidecarClient | None = None
        session_id_holder: list[str] = []
        self._run_error = None
        try:
            client, session_id = self._open_session(context)
            session_id_holder.append(session_id)
            self._current_session_id = session_id
            run_end, events = self._prompt_with_busy_policy(
                client, session_id, message, images,
                on_event=lambda ev: self._process_event(ev, chunks, None),
            )
            return self._build_result(client, session_id, run_end, "".join(chunks))
        except PiSidecarError as e:
            return self._error_result(e)
        except Exception as e:
            self.logger.warning(f"PiAgentHandler non-streaming error: {e}")
            return {"content": "", "role": "agent", "error": str(e)}
        finally:
            self._close_session_run(
                client, context,
                session_id=session_id_holder[0] if session_id_holder else None,
            )

    def _ask_streaming(
        self,
        input_messages: list[dict[str, Any]],
        context: dict[str, Any],
        stream_queue: Any,
        stream_event: threading.Event,
    ) -> dict[str, Any]:
        message, images = self._to_prompt(input_messages)
        chunks: list[str] = []
        client: PiSidecarClient | None = None
        session_id_holder: list[str] = []
        self._stream_queue = stream_queue
        self._run_error = None

        def _on_event(event: dict[str, Any]) -> None:
            if stream_event.is_set():
                return
            self._process_event(event, chunks, stream_queue)

        try:
            client, session_id = self._open_session(context)
            session_id_holder.append(session_id)
            self._current_session_id = session_id
            # Publish the run id BEFORE any event chunk so the executor can
            # register cancel/approval routing for the in-flight run.
            run_id = self._next_run_id(session_id)
            stream_queue.put({"name": "run_id", "value": run_id})

            run_end, _events = self._prompt_with_busy_policy(
                client, session_id, message, images, on_event=_on_event
            )
            return self._build_result(client, session_id, run_end, "".join(chunks))
        except PiSidecarError as e:
            self.logger.warning(f"PiAgentHandler streaming error: {e}")
            return self._error_result(e, stream_queue=stream_queue)
        except Exception as e:
            self.logger.warning(f"PiAgentHandler streaming error: {e}")
            stream_queue.put({"name": "error", "value": str(e)})
            return {"content": "".join(chunks), "role": "agent", "error": str(e)}
        finally:
            stream_event.set()
            self._close_session_run(
                client, context,
                session_id=session_id_holder[0] if session_id_holder else None,
            )
            self._stream_queue = None

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def _open_session(
        self, context: dict[str, Any]
    ) -> tuple[PiSidecarClient, str]:
        """Create (or re-open) a pi session via the sidecar."""
        client = self._new_client()
        try:
            session_id = self._resolve_session_id(client, context)
            return client, session_id
        except Exception:
            try:
                client.close()
            except Exception:
                pass
            if self._client is client:
                self._client = None
            raise

    def _prompt_with_busy_policy(
        self,
        client: PiSidecarClient,
        session_id: str,
        message: str,
        images: list[dict[str, Any]] | None,
        on_event: Any,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Run the prompt, applying ``pi_busy_behavior`` on HTTP 409.

        pi (and therefore the sidecar) rejects a prompt while a run is
        streaming unless it carries a ``streamingBehavior``. ``followUp``
        (default) queues until the agent stops; ``steer`` delivers before
        the next LLM call; ``reject`` surfaces the busy state as an error.
        """
        try:
            return client.stream_prompt(
                session_id, message, images=images,
                on_event=on_event, timeout=self.timeout,
            )
        except PiSidecarError as e:
            if not e.is_busy() or self.pi_busy_behavior == "reject":
                raise
            streaming_behavior = _BUSY_STREAMING_BEHAVIOR[self.pi_busy_behavior]
            self.logger.info(
                "pi session %s is busy with a run; re-submitting with "
                "streamingBehavior=%s (pi_busy_behavior)", session_id,
                streaming_behavior,
            )
            return client.stream_prompt(
                session_id, message, images=images,
                streaming_behavior=streaming_behavior,
                on_event=on_event, timeout=self.timeout,
            )

    def _clear_client(self, client: PiSidecarClient | None) -> None:
        if client is not None and self._client is client:
            self._client = None

    def _close_session_run(
        self,
        client: PiSidecarClient | None,
        context: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> None:
        """Close the turn: DELETE /sessions/{id} (frees the pi process) + drop
        the HTTP client — goose-parity bookkeeping.

        The session FILE survives the process (the sidecar re-opens it via the
        resume index), so multi-turn conversations keep working; a per-turn
        process keeps the one-process-per-session pool effectively flat
        (docked E2E: sessions pile up to the PI_MAX_PROCESSES cap otherwise).
        Kept open only while an ask-mode approval is pending — the pending
        approval worker posts the deferred close once it resolves (the
        ``_run_done`` signal tells it this run window is settled).
        """
        self._run_done.set()
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
                    "pi process kept open: ask-mode approval pending"
                )
                return
            if session_id:
                try:
                    client.close_session(session_id)
                except Exception as e:  # best effort; the reaper covers rest
                    self.logger.warning(
                        "pi close_session(%s) failed: %s", session_id, e
                    )
        finally:
            client.close()
            if self._client is client:
                self._client = None
            # The run window ran its bookkeeping; only an ask-mode approval
            # worker may still hold the deferred close.
            self._run_done.set()

    # ------------------------------------------------------------------
    # Result assembly
    # ------------------------------------------------------------------

    def _build_result(
        self,
        client: PiSidecarClient,
        session_id: str,
        run_end: dict[str, Any],
        content: str,
    ) -> dict[str, Any]:
        aborted = bool(run_end.get("aborted"))
        metadata: dict[str, Any] = {
            "run_id": self._run_id_for(session_id),
            "thread_uuid": session_id,
            "pi": {
                "sessionId": session_id,
                "sessionFile": run_end.get("sessionFile")
                or run_end.get("session_file"),
                "aborted": aborted,
                "stopReason": run_end.get("stopReason") or None,
                "usage": run_end.get("usage") or {},
                "model": run_end.get("model") or None,
            },
        }
        output: dict[str, Any] = {
            "content": content,
            "role": "agent",
            "metadata": metadata,
        }
        error_text = run_end.get("error") or self._run_error
        if aborted:
            output["error"] = "Run aborted"
        elif isinstance(error_text, str) and error_text:
            # Provider-level failure after the prompt was accepted (pi reports
            # these through the message stream / run_end, not the HTTP status).
            output["error"] = error_text
        return output

    def _error_result(
        self,
        error: PiSidecarError | Exception,
        stream_queue: Any = None,
    ) -> dict[str, Any]:
        message = str(error)
        if isinstance(error, PiSidecarError) and error.is_auth_error():
            message = (
                f"pi sidecar rejected the A2A gateway credentials "
                f"(HTTP {error.status_code}); verify PI_SIDECAR_SECRET_KEY "
                "and PI_SECRET_KEY"
            )
        elif isinstance(error, PiSidecarError) and error.is_not_found():
            message = f"pi sidecar session not found (HTTP 404): {message}"
        if stream_queue is not None:
            stream_queue.put({"name": "error", "value": message})
        return {
            "content": "",
            "role": "agent",
            "error": message,
            "metadata": {"pi_error": str(error)},
        }

    # ------------------------------------------------------------------
    # Cancel / approval passthrough
    # ------------------------------------------------------------------

    def cancel_run(self, run_id: str) -> bool:
        """Abort the run's pi session via ``POST /sessions/{id}/abort``.

        ``run_id`` is ``"{sessionId}:{seq}"`` from the ``run_id`` stream
        chunk. The sidecar aborts the pi run; the run stream ends with
        ``aborted: true`` and the task ends CANCELED (the executor sets
        ``stream_event.cancelled`` on the A2A cancel call).
        """
        session_id = self._session_id_from_run_id(run_id)
        if not session_id:
            self.logger.warning(
                "PiAgentHandler.cancel_run: unparseable run_id %s", run_id
            )
            return False
        try:
            client = self._client
            if client is None:
                client = self._new_client()
            client.abort(session_id)
        except PiSidecarError as e:
            if e.is_not_found():
                # Unknown session: nothing is running — the cancel is done.
                self.logger.info(
                    "pi session %s no longer exists; cancel complete",
                    session_id,
                )
            else:
                self.logger.warning(
                    f"Failed to abort pi session {session_id}: {e}"
                )
                return False
        except Exception as e:
            self.logger.warning(
                f"Failed to abort pi session {session_id}: {e}"
            )
            return False
        # Cancel any pending ask-mode dialog so the run cannot hang on it.
        with self._lock:
            pending = self._pending_approvals.pop(session_id, None)
        if pending is not None:
            pending["approved"] = False
            pending["resolved"] = True
            pending["event"].set()
        return True

    def resolve_approval(self, run_id: str, approved: bool, reason: str = "") -> bool:
        """Resolve a pending extension-UI dialog approval (ask mode).

        ``approved=False`` cancels the dialog (pi receives ``cancelled``);
        ``approved=True`` confirms (confirm → ``confirmed: true``, select →
        the first option) — the same auto_allow answers.
        """
        session_id = self._session_id_from_run_id(run_id)
        with self._lock:
            pending = self._pending_approvals.get(session_id or "")
        if pending is None:
            self.logger.warning(
                "PiAgentHandler.resolve_approval: no pending approval for %s",
                run_id,
            )
            return False
        pending["approved"] = bool(approved)
        pending["resolved"] = True
        pending["event"].set()
        return True

    # ------------------------------------------------------------------
    # Extension-UI dialog handling (pi_permission_mode)
    # ------------------------------------------------------------------

    def _on_extension_dialog(self, event: dict[str, Any]) -> None:
        """Permission policy for ``extension_ui_request`` dialogs.

        Built-in pi tools never ask; only extensions open these dialogs.
        ``auto_allow`` (default, sandboxed pi containers) confirms; ``deny``
        cancels everything; ``ask`` emits an ``approval`` chunk (the executor
        replies INPUT_REQUIRED) and answers after ``resolve_approval()`` or
        the timeout.
        """
        method = event.get("method", "")
        request_id = event.get("id", "")
        if method not in _DIALOG_METHODS or not request_id:
            return  # fire-and-forget UI (notify/setStatus/...): ignore

        session_id = self._session_id_from_event(event) or self._current_session_id
        mode = self.pi_permission_mode
        if mode == "ask":
            # Pending approvals are keyed by SESSION id (like goose) when the
            # run's session is known, AND by the dialog request id — pi does
            # not stamp UI records with a session id (P1 spike), so the
            # run_id-keyed lookup needs the current session; the dialog-id
            # key covers sessions-less late answers.
            with self._lock:
                pending: dict[str, Any] = {
                    "event_obj": event,
                    "session_id": session_id,
                    "approved": None,
                    "event": threading.Event(),
                    "client": self._client,
                }
                if session_id:
                    self._pending_approvals[session_id] = pending
                self._pending_approvals[request_id] = pending
            # Surface the pending dialog to the bridge (executor replies
            # INPUT_REQUIRED; resolve_approval(run_id, approved) resumes pi).
            if self._stream_queue is not None:
                run_id = self._run_id_for(session_id or request_id)
                self._stream_queue.put({
                    "name": "approval",
                    "value": json.dumps({
                        "run_id": run_id,
                        "session_id": session_id,
                        "method": method,
                        "title": event.get("title"),
                        # The dialog message carries WHAT is being approved
                        # (the tool-approval extension summarizes the tool
                        # input, e.g. the exact bash command).
                        "message": event.get("message"),
                        "options": event.get("options"),
                        # The gateway bridge renders this key as the SSE
                        # task_permission `details` (shared with the Hermes/
                        # goose shapes) — shows the operator the tool call.
                        "tool_call": {
                            "method": method,
                            "title": event.get("title") or method,
                            "input": event.get("message"),
                            "dialog_id": request_id,
                        },
                        "dialog_id": request_id,
                    }),
                })
            worker = threading.Thread(
                target=self._approval_worker, args=(pending,), daemon=True
            )
            worker.start()
            return

        # auto_allow / deny: answer immediately (pooled HTTP — the SSE pump
        # keeps reading on its own connection).
        answer = self._policy_answer(event, mode)
        try:
            client = self._client
            if client is None:
                client = self._new_client()
            client.respond_ui(session_id, {
                "type": "extension_ui_response",
                "id": request_id,
                **answer,
            })
        except Exception as e:
            self.logger.warning("pi ui-response failed for %s: %s", request_id, e)

    @staticmethod
    def _policy_answer(event: dict[str, Any], mode: str) -> dict[str, Any]:
        """The dialog answer for auto_allow / deny (docs/rpc-extension-ui.md).

        auto_allow: confirm → confirmed:true, select → first option,
        input/editor → cancelled (we never type free-form text for the agent).
        deny: every dialog is cancelled (confirm → confirmed:false).
        """
        method = event.get("method", "")
        if mode == "deny":
            if method == "confirm":
                return {"confirmed": False}
            return {"cancelled": True}
        # auto_allow
        if method == "confirm":
            return {"confirmed": True}
        if method == "select":
            options = event.get("options") or []
            first = options[0] if isinstance(options, list) and options else None
            if first is None:
                return {"cancelled": True}
            return {"value": first}
        return {"cancelled": True}

    def _approval_worker(self, pending: dict[str, Any]) -> None:
        """Wait for resolve_approval() (or the timeout) then answer the dialog."""
        approved = pending["event"].wait(self.approval_timeout) and bool(
            pending["approved"]
        )
        event_obj = pending["event_obj"]
        request_id = event_obj.get("id", "")
        session_id = pending.get("session_id")
        # Same answers as auto_allow when approved; cancelled otherwise.
        answer = (
            self._policy_answer(event_obj, "auto_allow")
            if approved
            else ({"confirmed": False} if event_obj.get("method") == "confirm"
                  else {"cancelled": True})
        )
        client = pending.get("client") or self._client
        if client is not None and client._closed:
            # The run client was closed in the meantime — answer through a
            # fresh connection so the dialog never blocks pi forever.
            client = self._new_client()
        if client is None:
            self.logger.warning("pi approval worker: client already closed")
            return
        try:
            client.respond_ui(session_id or "", {
                "type": "extension_ui_response",
                "id": request_id,
                **answer,
            })
        except Exception as e:
            self.logger.warning("pi approval answer failed: %s", e)
        finally:
            with self._lock:
                for key in {session_id or "", request_id}:
                    if not key:
                        continue
                    current = self._pending_approvals.get(key)
                    if current is pending:
                        self._pending_approvals.pop(key, None)
                no_more_pending = not self._pending_approvals
            # Deferred turn close: the run window could not close (approval
            # pending). When this worker was the last pending one and the
            # run's finally already ran, finish the bookkeeping here.
            if (
                no_more_pending
                and self._run_done.is_set()
                and client is not None
            ):
                try:
                    client.close_session(session_id or "")
                except Exception as e:
                    self.logger.debug("deferred pi close_session failed: %s", e)
                client.close()
                if self._client is client:
                    self._client = None

    # ------------------------------------------------------------------
    # context key -> sessionId mapping (partition-scoped a2a_settings)
    # ------------------------------------------------------------------

    def _resolve_session_id(
        self, client: PiSidecarClient, context: dict[str, Any]
    ) -> str:
        """Map the conversation key to a pi sessionId, or start a new one.

        The key is the caller's explicit ``thread_uuid`` when given, otherwise
        the A2A ``context_id`` — the ``contextId`` the client echoes back from
        the previous reply. The sidecar re-opens the session file (or its
        live process) when the client asks for the stored sessionId.
        """
        partition_key = (
            context.get("partition_key") if isinstance(context, dict) else None
        )
        thread_uuid = (
            (context.get("thread_uuid") or context.get("context_id"))
            if isinstance(context, dict)
            else None
        )

        if thread_uuid and partition_key:
            persisted = self._load_session_mapping(partition_key, thread_uuid)
            if persisted:
                # Verify the sidecar still knows the session (it re-opens the
                # session file automatically); a 404 → start a fresh session.
                try:
                    client.get_session(persisted)
                    return persisted
                except PiSidecarError as e:
                    if not e.is_not_found():
                        # 401/409/transport: surface — retrying with a new
                        # session would hide real misconfiguration.
                        raise
                    self.logger.warning(
                        "pi session %s no longer exists on the sidecar; "
                        "starting a new session for context %s",
                        persisted, thread_uuid,
                    )
            else:
                self.logger.info(
                    "No pi session mapping for context %s; "
                    "starting a new session", thread_uuid,
                )

        session = client.create_session(
            cwd=self.pi_cwd,
            model=self.pi_model or None,
            thinking=self.pi_thinking or None,
        )
        session_id = session["sessionId"]
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
        return load_mapping(
            self._setting_repo(),
            partition_key,
            self._setting_id(context_id),
            "session_id",
            self.logger,
        )

    def _store_session_mapping(
        self,
        partition_key: str,
        endpoint_id: Any,
        part_id: Any,
        context_id: str,
        session_id: str,
    ) -> None:
        store_mapping(
            self._setting_repo(),
            partition_key,
            endpoint_id,
            part_id,
            self._setting_id(context_id),
            {"context_id": context_id, "session_id": session_id},
            "pi_handler",
            self.logger,
        )

    @staticmethod
    def _setting_id(context_id: str) -> str:
        return f"pi_session#{context_id}"

    @staticmethod
    def _setting_repo() -> Any:
        return default_setting_repo()

    # ------------------------------------------------------------------
    # pi events -> A2A stream chunks
    # ------------------------------------------------------------------

    def _process_event(
        self,
        event: dict[str, Any],
        chunks: list[str] | None,
        stream_queue: Any | None,
    ) -> None:
        etype = event.get("type", "")

        if etype == "extension_ui_request":
            self._on_extension_dialog(event)
            return

        if etype == "message_update":
            update = event.get("assistantMessageEvent") or {}
            kind = update.get("type", "")
            if kind == _TEXT_EVENT:
                text = update.get("delta") or ""
                if text:
                    if chunks is not None:
                        chunks.append(text)
                    if stream_queue is not None:
                        stream_queue.put({"name": "token", "value": text})
            elif kind == _THINKING_EVENT:
                # Reasoning — tagged, kept out of the answer content. Bridge's
                # show_reasoning switch applies downstream; always emit.
                text = update.get("delta") or ""
                if text and stream_queue is not None:
                    stream_queue.put({
                        "name": "token", "value": text,
                        "meta": dict(REASONING_META),
                    })
            elif kind == "error":
                # Provider stream ended in error/abort (docs/json.md).
                reason = update.get("reason") or "provider stream error"
                if stream_queue is not None:
                    stream_queue.put({
                        "name": "error",
                        "value": json.dumps({"reason": reason}),
                    })
            # text_end / thinking_end / toolcall_* / done / start: authoritative
            # full-block copies — never re-emitted (no duplicated answer).
            return

        if etype == "message_end":
            # P1 spike finding: a provider failure (e.g. wrong API key) arrives
            # as a message_end with stopReason="error" + errorMessage and
            # NO error event; pi then settles normally. Surface it so the
            # turn ends FAILED with the cause instead of an empty success.
            message = event.get("message") or {}
            error_message = (
                message.get("errorMessage")
                if isinstance(message, dict) else None
            )
            if isinstance(error_message, str) and error_message:
                if stream_queue is not None:
                    stream_queue.put({"name": "error", "value": error_message})
                self._run_error = error_message
            return

        if etype == "tool_execution_start":
            if stream_queue is not None:
                stream_queue.put({
                    "name": "tool_call",
                    "value": json.dumps({
                        "toolCallId": event.get("toolCallId"),
                        "toolName": event.get("toolName"),
                        "args": event.get("args"),
                    }),
                })
            return
        if etype == "tool_execution_end":
            if stream_queue is not None:
                result = event.get("result")
                content_text = self._tool_result_text(result)
                stream_queue.put({
                    "name": "tool_result",
                    "value": json.dumps({
                        "toolCallId": event.get("toolCallId"),
                        "toolName": event.get("toolName"),
                        "output": content_text,
                        "error": bool(event.get("isError")),
                    }),
                })
            return
        # tool_execution_update (partial results), agent_start / turn_* /
        # agent_end, auto_retry_*, compaction_*, queue_update,
        # bash_execution_update, extension_error: run bookkeeping only.
        return

    @staticmethod
    def _tool_result_text(result: Any) -> str:
        """Flatten a pi tool result's content blocks to text."""
        if isinstance(result, dict):
            blocks = result.get("content")
            parts = []
            for block in blocks if isinstance(blocks, list) else []:
                if isinstance(block, dict) and block.get("text"):
                    parts.append(block["text"])
                elif isinstance(block, str):
                    parts.append(block)
            return "".join(parts)
        if isinstance(result, str):
            return result
        return ""

    # ------------------------------------------------------------------
    # Content conversion
    # ------------------------------------------------------------------

    def _to_prompt(
        self, input_messages: list[dict[str, Any]]
    ) -> tuple[str, list[dict[str, Any]]]:
        """Convert the newest user turn into the sidecar's prompt payload.

        pi keeps the conversation history in its session; resending it would
        duplicate every turn. Only the newest user message becomes the
        prompt. File parts become base64 images when the model takes them.
        """
        messages = input_messages or []
        user_msgs = [
            m for m in messages if (m.get("role") or "user") != "agent"
        ] or [messages[-1] if messages else {}]
        newest = user_msgs[-1]
        content = newest.get("content", "")

        text_parts: list[str] = []
        images: list[dict[str, Any]] = []

        if isinstance(content, str):
            text_parts.append(content.strip())
        else:
            for part in content if isinstance(content, list) else []:
                if not isinstance(part, dict):
                    text_parts.append(str(part))
                    continue
                if part.get("text"):
                    text_parts.append(str(part["text"]))
                    continue
                file_part = part.get("file")
                if isinstance(file_part, dict):
                    mime = file_part.get("mimeType") or "application/octet-stream"
                    if file_part.get("bytes") and mime.startswith("image/"):
                        images.append({
                            "type": "image",
                            "data": file_part["bytes"],
                            "mimeType": mime,
                        })
                    elif file_part.get("uri"):
                        text_parts.append(f"[file: {file_part['uri']}]")
                    elif file_part.get("bytes"):
                        text_parts.append(
                            f"[file: {file_part.get('name') or mime}]"
                        )
                data_part = part.get("data")
                if isinstance(data_part, dict):
                    text_parts.append(json.dumps(data_part, default=str))

        message = "\n".join(t for t in text_parts if t)
        return message, images

    # ------------------------------------------------------------------
    # Client construction / run ids
    # ------------------------------------------------------------------

    def _new_client(self) -> PiSidecarClient:
        client = PiSidecarClient(
            logger=self.logger,
            base_url=self.pi_api_url,
            secret_key=self.pi_secret_key,
            request_timeout=self.timeout,
            http_transport=self.http_transport,
        )
        self._client = client
        return client

    def _next_run_id(self, session_id: str) -> str:
        with self._lock:
            self._run_seq += 1
            return f"{session_id}:{self._run_seq}"

    def _run_id_for(self, session_id: str) -> str:
        """The live run id when the counter was already bumped; else a stable
        identifier from the session (cancel/cleanup paths)."""
        if self._run_seq:
            return f"{session_id}:{self._run_seq}"
        return f"{session_id}:0"

    @staticmethod
    def _session_id_from_run_id(run_id: str) -> str | None:
        if not run_id:
            return None
        if ":" in run_id:
            return run_id.rsplit(":", 1)[0] or None
        return run_id

    @staticmethod
    def _session_id_from_event(event: dict[str, Any]) -> str:
        """The sidecar stamps dialog events with the session id."""
        sid = event.get("sessionId") or event.get("session_id")
        return str(sid) if sid else ""


__all__ = ["PiAgentHandler"]