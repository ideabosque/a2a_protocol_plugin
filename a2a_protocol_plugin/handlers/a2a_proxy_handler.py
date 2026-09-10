#!/usr/bin/python
"""
A2A Proxy Handler — Forward A2A requests to A2A-compliant backends

This handler implements the Phase 10 bridge contract (``ask_model``) but routes
requests to any A2A-compliant backend (Hermes Agent :9900, LangChain, CrewAI,
Google ADK, or anything built on the a2a-sdk) by forwarding native A2A
JSON-RPC requests — no protocol translation needed.

Wire format is the **standard A2A JSON-RPC transport** (the same dialect this
project's own server speaks in ``a2a_server.py``): requests are JSON-RPC 2.0
envelopes POSTed to the backend, using the canonical method names and
``camelCase`` field aliases defined by the a2a-sdk.

A2A JSON-RPC methods used:
- ``message/send``    — send a message (non-streaming)
- ``message/stream``  — send a message and receive SSE updates (streaming)
- ``tasks/cancel``    — cancel an in-flight task

Streaming frames follow the A2A event contract: each SSE ``data:`` line is a
JSON-RPC envelope whose ``result`` is a ``Message``, a ``Task``, a
``TaskStatusUpdateEvent`` (``kind: "status-update"``) or a
``TaskArtifactUpdateEvent`` (``kind: "artifact-update"``).

Configuration is per-agent metadata only:
- ``a2a_proxy_url``
- ``a2a_proxy_token``
- ``a2a_proxy_timeout``
"""

import json
import logging
import threading
import uuid as _uuid
from typing import Any

import httpx

__author__ = "bibow"


class A2AProxyHandler:
    """A2A handler that proxies to any standard A2A JSON-RPC backend."""

    def __init__(
        self,
        logger: logging.Logger,
        agent_config: dict[str, Any],
        setting: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
        http_transport: Any = None,
    ) -> None:
        self.logger = logger
        self.agent_config = agent_config
        self.setting = setting or {}
        self.http_transport = http_transport  # Optional test injection

        # Resolve A2A backend connection details from per-agent metadata only.
        # Each agent proxies to its own A2A backend — there are no global
        # defaults, since proxy targets are inherently per-agent.
        metadata = agent_config.get("metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {}

        self.proxy_url = metadata.get("a2a_proxy_url") or ""
        self.proxy_token = metadata.get("a2a_proxy_token") or ""
        self.timeout = float(metadata.get("a2a_proxy_timeout") or 120.0)

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
        """Execute the LLM call via A2A protocol forwarding."""
        context = context if isinstance(context, dict) else {}
        agent_uuid = (
            context.get("agent_uuid")
            or self.agent_config.get("agent_id")
            or ""
        )
        thread_uuid = context.get("thread_uuid")

        user_query = ""
        if input_messages:
            user_query = input_messages[-1].get("content", "") or ""

        if stream_queue is not None:
            # ``_ask_streaming`` drives the event unconditionally (drain-loop
            # guard + ``finally: set()``); guarantee a real Event even if the
            # caller supplied a queue without one.
            return self._ask_streaming(
                agent_uuid, thread_uuid, user_query, context,
                stream_queue, stream_event or threading.Event(),
            )
        return self._ask_non_streaming(
            agent_uuid, thread_uuid, user_query, context,
        )

    # ------------------------------------------------------------------
    # Non-streaming path (A2A message/send)
    # ------------------------------------------------------------------

    def _ask_non_streaming(
        self,
        agent_uuid: str,
        thread_uuid: str | None,
        user_query: str,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """Forward a message/send JSON-RPC to the backend A2A endpoint."""
        if not self.proxy_url:
            return {"content": "", "role": "agent", "error": "No a2a_proxy_url configured"}

        message_id = f"msg-{_uuid.uuid4().hex}"
        request = self._build_jsonrpc(
            method="message/send",
            params={
                "message": self._build_message(message_id, user_query, thread_uuid, context),
                "metadata": self._request_metadata(agent_uuid, context),
            },
        )

        try:
            with httpx.Client(timeout=self.timeout, transport=self.http_transport) as client:
                resp = client.post(
                    self._endpoint(),
                    json=request,
                    headers=self._headers(),
                )
                resp.raise_for_status()
                data = resp.json()

            # Parse JSON-RPC response
            if "error" in data:
                err = data["error"]
                return {"content": "", "role": "agent", "error": str(err.get("message", err))}

            result = data.get("result") or {}
            kind = result.get("kind")

            # A2A message/send returns either a Message or a Task.
            if kind == "message" or "parts" in result:
                content = self._extract_text(result.get("parts", []))
                return {
                    "content": content,
                    "role": "agent",
                    "message_id": result.get("messageId", message_id),
                    "metadata": {
                        "thread_uuid": result.get("contextId") or thread_uuid or "",
                        "task_id": result.get("taskId", ""),
                    },
                }
            if kind == "task" or "status" in result or "artifacts" in result:
                content = "".join(
                    self._extract_text(artifact.get("parts", []))
                    for artifact in result.get("artifacts", [])
                )
                # A completed task may also carry its final text in status.message.
                status = result.get("status") or {}
                status_msg = status.get("message") or {}
                if not content and status_msg:
                    content = self._extract_text(status_msg.get("parts", []))
                return {
                    "content": content,
                    "role": "agent",
                    "message_id": message_id,
                    "metadata": {
                        "thread_uuid": result.get("contextId") or thread_uuid or "",
                        "task_id": result.get("id", ""),
                    },
                }
            return {"content": "", "role": "agent", "error": "Unexpected A2A response shape"}

        except Exception as e:
            self.logger.warning(f"A2AProxyHandler non-streaming error: {e}")
            return {"content": "", "role": "agent", "error": str(e)}

    # ------------------------------------------------------------------
    # Streaming path (A2A message/stream via SSE)
    # ------------------------------------------------------------------

    def _ask_streaming(
        self,
        agent_uuid: str,
        thread_uuid: str | None,
        user_query: str,
        context: dict[str, Any],
        stream_queue: Any,
        stream_event: threading.Event,
    ) -> dict[str, Any]:
        """Forward a message/stream JSON-RPC and drain A2A SSE events."""
        if not self.proxy_url:
            stream_queue.put({"name": "error", "value": "No a2a_proxy_url configured"})
            stream_event.set()
            return {"content": "", "role": "agent", "error": "No a2a_proxy_url configured"}

        message_id = f"msg-{_uuid.uuid4().hex}"
        request = self._build_jsonrpc(
            method="message/stream",
            params={
                "message": self._build_message(message_id, user_query, thread_uuid, context),
                "metadata": self._request_metadata(agent_uuid, context),
            },
        )

        chunks: list[str] = []
        stream_error: str | None = None
        resolved_context = ""  # backend-assigned contextId (the conversation id)
        backend_task_id = ""   # backend-assigned task id (for cancel/approval)

        def _remember_ids(obj: dict[str, Any]) -> None:
            """Capture contextId / task id and surface the run id once."""
            nonlocal resolved_context, backend_task_id
            ctx = obj.get("contextId")
            if ctx:
                resolved_context = ctx
            tid = obj.get("taskId") or (obj.get("id") if obj.get("kind") == "task" else "")
            if tid and tid != backend_task_id:
                backend_task_id = tid
                # Let the drain loop register the external run so cancel and
                # approval passthrough can reach this backend task.
                stream_queue.put({"name": "run_id", "value": backend_task_id})

        try:
            with httpx.Client(timeout=self.timeout, transport=self.http_transport) as client:
                with client.stream(
                    "POST",
                    self._endpoint(),
                    json=request,
                    headers=self._headers(),
                ) as sse_resp:
                    sse_resp.raise_for_status()

                    for line in sse_resp.iter_lines():
                        if stream_event.is_set():
                            break
                        if not line or line.startswith(":"):
                            continue  # keep-alive / blank line

                        # A2A SSE frames are JSON-RPC envelopes on ``data:`` lines.
                        # ``event:`` / ``id:`` / ``retry:`` lines carry no payload.
                        if line.startswith("data:"):
                            data_str = line[5:].lstrip()
                        else:
                            continue

                        if data_str == "[DONE]":
                            break

                        try:
                            frame = json.loads(data_str)
                        except json.JSONDecodeError:
                            continue

                        if "error" in frame:
                            stream_error = str(frame["error"].get("message", frame["error"]))
                            stream_queue.put({"name": "error", "value": stream_error})
                            break

                        result = frame.get("result")
                        if not isinstance(result, dict):
                            continue
                        _remember_ids(result)
                        kind = result.get("kind")

                        # --- Message frame: an incremental / final agent message
                        if kind == "message" or (kind is None and "parts" in result and "role" in result):
                            text = self._extract_text(result.get("parts", []))
                            if text:
                                chunks.append(text)
                                stream_queue.put({"name": "token", "value": text})
                            continue

                        # --- Artifact update: the primary streaming delta channel
                        if kind == "artifact-update":
                            artifact = result.get("artifact") or {}
                            text = self._extract_text(artifact.get("parts", []))
                            if text:
                                chunks.append(text)
                                stream_queue.put({"name": "token", "value": text})
                            continue

                        # --- Hermes A2A statusUpdate: lifecycle transitions
                        if "statusUpdate" in result:
                            su = result["statusUpdate"]
                            state = self._get_task_state(su)
                            if state == "input_required":
                                stream_queue.put(
                                    {"name": "approval", "value": json.dumps(su)}
                                )
                            elif state in ("failed", "rejected"):
                                stream_error = "Backend task failed"
                                stream_queue.put({"name": "error", "value": stream_error})
                                break
                            elif state in ("completed", "canceled"):
                                # Terminal state — stop draining
                                break
                            # WORKING / SUBMITTED — keep draining
                            continue

                        # --- Hermes A2A artifactUpdate: streaming text deltas
                        if "artifactUpdate" in result:
                            au = result["artifactUpdate"]
                            artifact = au.get("artifact") or {}
                            text = self._extract_text(artifact.get("parts", []))
                            if text:
                                chunks.append(text)
                                stream_queue.put({"name": "token", "value": text})
                            continue

                        # --- Status update / full Task: lifecycle transitions
                        if kind in ("status-update", "task") or "task" in result or "status" in result:
                            task_like = result.get("task") or result
                            state = self._get_task_state(task_like)
                            is_final = bool(result.get("final"))

                            if state == "input_required":
                                stream_queue.put(
                                    {"name": "approval", "value": json.dumps(task_like)}
                                )
                            elif state == "auth_required":
                                stream_queue.put(
                                    {"name": "auth_required", "value": json.dumps(task_like)}
                                )
                            elif state in ("failed", "rejected"):
                                stream_error = self._status_message(task_like) or (
                                    "Backend task rejected" if state == "rejected"
                                    else "Backend task failed"
                                )
                                stream_queue.put({"name": "error", "value": stream_error})
                                break
                            elif state in ("completed", "canceled"):
                                # Emit any final text carried on a full Task frame.
                                for artifact in task_like.get("artifacts", []):
                                    text = self._extract_text(artifact.get("parts", []))
                                    if text:
                                        chunks.append(text)
                                        stream_queue.put({"name": "token", "value": text})
                                break

                            if is_final:
                                break
                            # otherwise WORKING / SUBMITTED — keep draining

            full_content = "".join(chunks)
            if stream_error:
                return {"content": full_content, "role": "agent", "error": stream_error}
            return {
                "content": full_content,
                "role": "agent",
                "metadata": {
                    # The backend owns the conversation id; prefer the id it
                    # resolved this turn, falling back to the inbound one.
                    "thread_uuid": resolved_context or thread_uuid or "",
                    "run_id": backend_task_id,
                },
            }

        except Exception as e:
            self.logger.warning(f"A2AProxyHandler streaming error: {e}")
            stream_queue.put({"name": "error", "value": str(e)})
            return {"content": "", "role": "agent", "error": str(e)}
        finally:
            stream_event.set()

    # ------------------------------------------------------------------
    # Optional methods: cancel / approval passthrough
    # ------------------------------------------------------------------

    def cancel_run(self, run_id: str) -> bool:
        """Forward tasks/cancel JSON-RPC to the backend."""
        if not self.proxy_url or not run_id:
            return False

        request = self._build_jsonrpc(method="tasks/cancel", params={"id": run_id})

        try:
            with httpx.Client(timeout=10.0, transport=self.http_transport) as client:
                resp = client.post(
                    self._endpoint(),
                    json=request,
                    headers=self._headers(),
                )
                resp.raise_for_status()
            return True
        except Exception as e:
            self.logger.warning(f"Failed to cancel A2A task {run_id}: {e}")
            return False

    def resolve_approval(self, run_id: str, approved: bool, reason: str = "") -> bool:
        """Send an approval continuation via message/send to the backend.

        In A2A, approval continuation is just a multi-turn message — the client
        sends a new message/send with the same contextId containing the
        approval response.
        """
        if not self.proxy_url:
            return False

        approval_text = "APPROVE" if approved else "REJECT"
        if reason:
            approval_text += f": {reason}"

        message_id = f"msg-{_uuid.uuid4().hex}"
        request = self._build_jsonrpc(
            method="message/send",
            params={
                "message": self._build_message(message_id, approval_text, run_id, None),
            },
        )

        try:
            with httpx.Client(timeout=30.0, transport=self.http_transport) as client:
                resp = client.post(
                    self._endpoint(),
                    json=request,
                    headers=self._headers(),
                )
                resp.raise_for_status()
            return True
        except Exception as e:
            self.logger.warning(f"Failed to resolve approval for A2A task {run_id}: {e}")
            return False

    # ------------------------------------------------------------------
    # Request builders
    # ------------------------------------------------------------------

    def _endpoint(self) -> str:
        return self.proxy_url.rstrip("/") + "/"

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.proxy_token:
            headers["Authorization"] = f"Bearer {self.proxy_token}"
        return headers

    def _build_jsonrpc(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Build an A2A JSON-RPC 2.0 request envelope."""
        return {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": f"a2a-proxy-{_uuid.uuid4().hex[:8]}",
        }

    def _build_message(
        self,
        message_id: str,
        text: str,
        thread_uuid: str | None,
        context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Build a standard A2A Message with camelCase aliases.

        ``role`` is the JSON-RPC enum value ``"user"``; ``contextId`` is only
        included when known (A2A permits omitting it for the first message of a
        new conversation). Any file/data parts already extracted onto the
        bridge context are forwarded as spec ``FilePart`` / ``DataPart`` so a
        multimodal turn survives the proxy hop.
        """
        message: dict[str, Any] = {
            "messageId": message_id,
            "role": "user",
            "parts": self._build_parts(text, context),
        }
        if thread_uuid:
            message["contextId"] = thread_uuid
        return message

    def _build_parts(
        self, text: str, context: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        """Assemble A2A Parts: a TextPart plus any inbound file/data parts."""
        parts: list[dict[str, Any]] = [{"kind": "text", "text": text}]
        context = context if isinstance(context, dict) else {}

        for file_info in context.get("input_files") or []:
            if not isinstance(file_info, dict):
                continue
            file_obj: dict[str, Any] = {}
            if file_info.get("url"):
                file_obj["uri"] = file_info["url"]
            elif file_info.get("uri"):
                file_obj["uri"] = file_info["uri"]
            elif file_info.get("bytes"):
                file_obj["bytes"] = file_info["bytes"]
            else:
                continue
            if file_info.get("filename") or file_info.get("name"):
                file_obj["name"] = file_info.get("filename") or file_info.get("name")
            if file_info.get("media_type") or file_info.get("mimeType"):
                file_obj["mimeType"] = file_info.get("media_type") or file_info.get("mimeType")
            parts.append({"kind": "file", "file": file_obj})

        for data_part in context.get("input_data_parts") or []:
            if isinstance(data_part, dict) and data_part:
                parts.append({"kind": "data", "data": data_part})

        return parts

    def _request_metadata(
        self, agent_uuid: str, context: dict[str, Any]
    ) -> dict[str, Any]:
        """Extension metadata carried alongside the message (params.metadata)."""
        return {
            "agent_uuid": agent_uuid,
            "endpoint_id": context.get("endpoint_id", ""),
            "part_id": context.get("part_id", ""),
        }

    # ------------------------------------------------------------------
    # Response parsing helpers
    # ------------------------------------------------------------------

    def _extract_text(self, parts: list[dict[str, Any]]) -> str:
        """Extract text content from A2A Parts (TextPart / bare text)."""
        text = ""
        for part in parts:
            if isinstance(part, dict):
                if part.get("kind") == "text" and part.get("text"):
                    text += part["text"]
                elif part.get("text"):
                    text += part["text"]
            elif isinstance(part, str):
                text += part
        return text

    def _get_task_state(self, task_like: dict[str, Any]) -> str:
        """Normalize a task/status state across dialects.

        Handles the JSON-RPC wire form (``input-required``), the protobuf enum
        form (``TASK_STATE_INPUT_REQUIRED``) and plain strings, collapsing them
        all to snake_case (e.g. ``input_required``).
        """
        status = task_like.get("status", task_like)
        if isinstance(status, dict):
            state = status.get("state", "")
        else:
            state = str(status)
        return state.lower().replace("task_state_", "").replace("-", "_")

    def _status_message(self, task_like: dict[str, Any]) -> str:
        """Best-effort human-readable text from a task's status message."""
        status = task_like.get("status") or {}
        message = status.get("message") or {}
        return self._extract_text(message.get("parts", []))


__all__ = ["A2AProxyHandler"]
