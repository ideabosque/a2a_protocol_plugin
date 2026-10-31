#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Banyanos Agent Handler — A2A bridge to the BanyanOS agent_engine.

Implements docs/BANYANOS_INTEGRATION_PLAN.md's ``BanyanosAgentHandler``
(Phase 2 direct publication + Phase 3 streaming/cancellation slice), the same
narrow bridge contract as ``CoreEngineAgentHandler`` (``ask_model`` /
``cancel_run``), so the executor and ``a2a_ai_agent_utility.py`` streaming,
persistence and push machinery are reused unchanged.

Backend transports (docker-banyanos-gateway routes):

| Mode | Transport | Route / operation |
|------|-----------|-------------------|
| Non-streaming | HTTP GraphQL ``invokeAgent`` | ``POST /{endpoint_id}/agent_engine_graphql`` |
| Streaming     | WebSocket subscription ``agentInvokeStream`` | ``ws://{gateway}/{endpoint_id}/banyan_ws`` + HTTP trigger |
| Cancel        | HTTP GraphQL ``cancelTask`` | same GraphQL endpoint |

Lifecycle translation (subset of the plan's state table; Banyanos's synchronous
``invokeAgent`` and event stream expose SUBMITTED/WORKING/COMPLETED/FAILED/
CANCELED semantics; INPUT_REQUIRED / AUTH_REQUIRED await the backend exposing
those states — see the plan §"Task lifecycle and bridge changes"):

- ``started``              -> WORKING (nonterminal; executor keeps task open)
- ``chunk``                -> token chunk (reasoning tagged, default off)
- ``tool_started/finished``-> structured stream metadata, not answer text
- ``completed``            -> COMPLETED; ``completed.content`` is authoritative
- ``error``                -> FAILED with the backend's safe error text

Handler contract rules honored:

- Identity, URLs, credential, and the backend agent id come from trusted
  configuration only (``banyanos_identity.resolve_banyanos_identity``); request
  metadata can never point the handler at another agent or URL.
- The A2A ``contextId`` is mapped to a Banyanos ``session_id`` per tenant via
  ``banyanos_execution_map`` (backed by ``a2a_settings``); the contextId itself
  is never sent to the backend, and history is not replayed — Banyanos owns the
  conversation for the mapped session.
- Streaming reasoning is pushed with ``{"data_format": "reasoning"}`` meta
  only when the executor resolves ``show_reasoning`` on; here reasoning events
  are forwarded tagged and the bridge strips them when disclosure is off
  (``_is_reasoning_meta``). No answer copy is ever re-labeled as reasoning.
- Queue/event cleanup happens in ``finally`` on success, failure and cancel.
- ``cancel_run(task_id)`` calls the durable backend ``cancelTask`` mutation —
  not merely a local stream close; closing the observation socket does not
  cancel backend work.
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

from . import banyanos_execution_map as execution_map
from .banyanos_client import (
    BanyanosClientError,
    BanyanosClient,
    BanyanosStreamEvent,
)
from .banyanos_identity import BanyanosIdentityError, resolve_banyanos_identity
from .config import Config

__author__ = "bibow"


class BanyanosAgentHandler:
    """A2A handler bridging to the Banyanos agent_engine via the gateway."""

    def __init__(
        self,
        logger: logging.Logger,
        agent_config: dict[str, Any],
        setting: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
        http_transport: Any = None,
        ws_connect: Any = None,
        token_resolver: Any = None,
    ) -> None:
        """``http_transport`` / ``ws_connect`` are test injections for the
        underlying ``BanyanosClient``; ``token_resolver`` plugs a secret
        reference resolver for ``banyanos_credential_ref``."""
        self.logger = logger
        self.agent_config = agent_config or {}
        self.setting = setting or {}
        self._http_transport = http_transport
        self._ws_connect = ws_connect
        self._token_resolver = token_resolver

        metadata = self.agent_config.get("metadata") or self.agent_config
        if not isinstance(metadata, dict):
            metadata = {}
        self.show_tool_progress = bool(
            metadata.get("banyanos_show_tool_progress")
            or self.setting.get("BANYANOS_SHOW_TOOL_PROGRESS")
            or getattr(Config, "banyanos_show_tool_progress", False)
        )
        self.default_stream_timeout = float(
            metadata.get("banyanos_stream_timeout")
            or self.setting.get("BANYANOS_STREAM_TIMEOUT")
            or getattr(Config, "banyanos_stream_timeout", None)
            or 180.0
        )

        # Identity resolved per call (partition scope arrives with context),
        # but fail fast at construction when trusted defaults are absent
        # altogether (no partition yet -> identity not resolvable here).
        self._identity = None
        # Process-local registry of live streaming clients keyed by the
        # reserved logical invocation id; backs cancel_run for stream runs.
        self._active_clients: dict[str, BanyanosClient] = {}
        self._registry_lock = threading.RLock()

    # ------------------------------------------------------------------
    # Bridge contract
    # ------------------------------------------------------------------

    def ask_model(
        self,
        input_messages: list[dict[str, Any]],
        context: dict[str, Any],
        stream_queue: Any = None,
        stream_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Execute one turn against the Banyanos agent.

        With ``stream_queue`` the handler drives the banyan_ws subscription
        and pushes token chunks; without it, it POSTs the synchronous
        ``invokeAgent`` mutation. Returns the normalized handler result
        ``{content, role, metadata, error?}``.
        """
        ctx = context if isinstance(context, dict) else {}
        partition_key = str(ctx.get("partition_key") or "")
        endpoint_id, _, part_id = partition_key.partition("#")

        # 1. Trusted identity. The backend agent id is authoritative from
        # configuration — request/message metadata must not override it.
        self._current_context = ctx
        try:
            identity = resolve_banyanos_identity(
                self.agent_config, self.setting, ctx,
                logger=self.logger, token_resolver=self._token_resolver,
            )
        except BanyanosIdentityError as exc:
            self.logger.warning("Banyanos identity rejected: %s", exc)
            return {"content": "", "role": "agent", "error": str(exc)}

        # 2. Conversation mapping: A2A contextId -> Banyanos session. The
        # contextId is never sent to the backend as an id; history is owned
        # by Banyanos for the mapped session (no plugin-side history replay).
        # Without a mapping yet, one is created via the gateway-side
        # ``createSession`` mutation with a stable idempotency key, so a
        # retried turn reuses the same backend conversation.
        context_id = ctx.get("context_id")
        context_id = str(context_id) if context_id else ""
        session_id = execution_map.load_session_id(
            partition_key, context_id, self.logger
        )

        user_query = ""
        if input_messages:
            user_query = input_messages[-1].get("content", "")

        # 3. Reserve the logical invocation (stable idempotency key scoped to
        # partition + conversation + agent + message). A concurrent duplicate
        # of the same logical submission resolves to the reserved one.
        reservation = execution_map.resolve_or_reserve_execution(
            partition_key, identity.endpoint_id, part_id,
            str(context_id or ""), identity.banyanos_agent_id,
            user_query, self.logger,
        )

        client = BanyanosClient(
            logger=self.logger,
            graphql_url=identity.graphql_url,
            ws_url=identity.ws_url,
            endpoint_id=identity.endpoint_id,
            token=identity.token,
            part_id=part_id,
            stream_timeout=self.default_stream_timeout,
            http_transport=self._http_transport,
            ws_connect=self._ws_connect,
        )

        try:
            if context_id and not session_id:
                # No mapped session yet: mint one via the gateway-side
                # ``createSession`` mutation with a stable per-conversation
                # idempotency key (a retried turn reuses the same session).
                session_id = client.create_session(
                    agent_id=identity.banyanos_agent_id,
                    metadata={
                        "a2a_context_id": context_id,
                        "partition_key": partition_key,
                    },
                    idempotency_key=(
                        "banyanos-sess-"
                        + execution_map.invocation_digest(
                            partition_key, context_id, identity.banyanos_agent_id, "")
                    ),
                )
                execution_map.store_session_id(
                    partition_key, identity.endpoint_id, part_id,
                    context_id, session_id, self.logger,
                )

            if stream_queue is not None and stream_event is not None:
                result = self._ask_streaming(
                    client, identity, reservation, user_query, session_id,
                    ctx, stream_queue, stream_event,
                )
            else:
                result = self._ask_non_streaming(
                    client, identity, reservation, user_query, session_id,
                    input_messages=input_messages,
                )
        except BanyanosClientError as exc:
            result = {
                "content": "",
                "role": "agent",
                "error": f"Banyanos {exc.category} error: {exc.message}",
                "metadata": {
                    "banyanos_error_category": exc.category,
                    "banyanos_task_id": (reservation.get("backend_ref") or {}).get("task_id") or "",
                    "idempotency_key": reservation["idempotency_key"],
                },
            }
        except Exception as exc:  # narrow bridge contract: never raise
            self.logger.error("Banyanos handler invocation failed: %s", exc, exc_info=True)
            result = {"content": "", "role": "agent", "error": str(exc)}

        # 4. Persist correlation (for non-deduplicated executions) and the
        # resolved session mapping, so retries and following turns reconcile.
        if not reservation.get("deduplicated"):
            execution_map.record_execution(
                partition_key, identity.endpoint_id, part_id,
                reservation["setting_id"],
                (result.get("metadata") or {}).get("banyanos_invocation"),
                self.logger,
                session_id=(result.get("metadata") or {}).get("session_id"),
            )
        resolved_session = (result.get("metadata") or {}).get("session_id")
        if context_id and resolved_session and resolved_session != session_id:
            execution_map.store_session_id(
                partition_key, identity.endpoint_id, part_id,
                str(context_id), str(resolved_session), self.logger,
            )
        return result

    def cancel_run(self, run_id: str) -> bool:
        """Cancel an in-flight Banyanos run (best effort, per-transport).

        Backend reality (verified in agent_engine.execute_agent_streaming):
        the ``agentInvokeStream`` subscription creates NO AgentTask row — the
        backend treats a subscriber disconnect as the cancel signal (writes a
        Redis cancel key, terminates the ReAct loop at the next checkpoint).
        So for streaming runs the durable way to cancel is closing the
        observation socket (done here via the live client registry); for an
        async, task-backed run (if exposed later), ``cancelTask`` is used.

        ``run_id`` is the value drained from the handler's ``run_id`` chunk
        (the reserved logical invocation id). The registry is process-local:
        after a worker restart the reservation row lets the caller reconcile
        state instead of the handler reattaching (plan: stream disconnect
        stops observation by default; the backend disconnect policy makes
        observation termination durable here).
        """
        if not run_id:
            return False
        cancelled = False
        with self._registry_lock:
            client = self._active_clients.pop(run_id, None)
        if client is not None:
            try:
                client._close_ws()
                cancelled = True
                self.logger.info(
                    "Banyanos streaming run %s: observation socket closed "
                    "(backend writes cancel key at next checkpoint)", run_id
                )
            except Exception as exc:
                self.logger.warning(
                    "Failed to close Banyanos stream for run %s: %s", run_id, exc
                )
        else:
            # Not streaming anymore (or a task-backed run): fall back to the
            # durable agent-level mutation, which only applies to a real
            # AgentTask id — a logical reservation key is not one.
            cancelled = self._cancel_task_if_task_id(run_id)
        return cancelled

    def _cancel_task_if_task_id(self, run_id: str) -> bool:
        """Attempt the durable ``cancelTask`` mutation for a task-backed run."""
        ctx = getattr(self, "_current_context", {}) or {}
        try:
            identity = resolve_banyanos_identity(
                self.agent_config, self.setting, ctx,
                logger=self.logger, token_resolver=self._token_resolver,
            )
            client = BanyanosClient(
                logger=self.logger,
                graphql_url=identity.graphql_url,
                ws_url=identity.ws_url,
                endpoint_id=identity.endpoint_id,
                token=identity.token,
                part_id=identity.part_id,
                http_transport=self._http_transport,
            )
            task = client.cancel_task(run_id)
            status = str(task.get("status") or "")
            self.logger.info(
                "Banyanos cancel acknowledged for task %s (status=%s)", run_id, status
            )
            return status not in ("FAILED", "")
        except Exception as exc:
            self.logger.warning("Failed to cancel Banyanos task %s: %s", run_id, exc)
            return False

    # ------------------------------------------------------------------
    # Non-streaming path (HTTP GraphQL invokeAgent)
    # ------------------------------------------------------------------

    def _ask_non_streaming(
        self,
        client: BanyanosClient,
        identity: Any,
        reservation: dict[str, Any],
        user_query: str,
        session_id: str | None,
        input_messages: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Continuation turn via the Banyanos ``SendMessage`` mutation.

        Live-verified contract (2026-10-28): `invokeAgent` executes
        synchronously but does NOT persist session messages, and
        `SendMessage` loads history only from its caller-provided
        `chatHistory` argument (only the agentInvokeStream subscription
        loads server-side history). So a non-streaming conversation turn
        goes through `SendMessage` with the history the A2A bridge already
        loaded for this conversation (`input_messages` minus the current
        message); each turn then persists USER/ASSISTANT messages on the
        Banyanos side too.
        """
        # 观察优先（发现 #4/幂等规则）：已有非终态后端执行 → 绝不重复
        # 投递副作用动作 —— 只观察其终态；终态则直接回放结果（A2A 任务
        # 与幂等预留收敛）。
        dedup_task_id = str(
            (reservation.get("backend_ref") or {}).get("task_id") or ""
        )
        if reservation.get("deduplicated") and dedup_task_id:
            return self._observe_deduplicated(
                client, reservation, dedup_task_id,
            )

        chat_history: list[dict[str, Any]] = []
        for message in (input_messages or [])[:-1]:
            role = str(message.get("role", "user")).upper()
            if role == "SYSTEM":
                # Dropped: the published agent's own system prompt is
                # authoritative on the backend (execute_agent drops it too).
                continue
            if role in ("USER", "ASSISTANT", "TOOL") and message.get("content"):
                chat_history.append({"role": role, "content": message["content"]})


        result_message = client.send_message(
            agent_id=identity.banyanos_agent_id,
            session_id=session_id or "",
            content=user_query,
            chat_history=chat_history,
            idempotency_key=reservation["idempotency_key"],
        )
        status = "SUCCESS" if result_message.get("success") else "FAILED"
        message_id = str(result_message.get("messageId")
                          or result_message.get("message_id") or "")
        # 发现 #4/Phase 3 配套修复：SendMessage 落库了真实后端任务行 ——
        # 这里取出其对后端引用，供续跑观察/对账层（reconciliation）查询终态。
        backend_task_id = str(result_message.get("taskId")
                              or result_message.get("task_id") or "")
        latency = (result_message.get("latencyMs")
                   or result_message.get("latency_ms"))

        metadata: dict[str, Any] = {
            "banyanos_task_id": message_id,
            "session_id": session_id or "",
            "idempotency_key": reservation["idempotency_key"],
            "banyanos_status": status,
            "banyanos_agent_id": identity.banyanos_agent_id,
            "latency_ms": latency,
        }
        if backend_task_id:
            metadata["banyanos_backend_task_id"] = backend_task_id
        metadata["banyanos_invocation"] = {
            "id": message_id,
            # 后端任务行引用优先（观察/对账层的权威后端 ID）。
            "task_id": backend_task_id,
            "agent_id": metadata["banyanos_agent_id"],
            "status": status,
            "idempotency_key": reservation["idempotency_key"],
        }
        content = str(result_message.get("content") or "")
        result: dict[str, Any] = {
            "content": content,
            "role": "agent",
            "metadata": metadata,
        }
        # 发现 #4（DEF-B002，2026-10-29）：SendMessage 已投递异步续跑时
        # 本轮为部分产物、任务保持非终态 —— 不是失败也不是完成。置标记，
        # 执行器据此返回 WORKING 任务（观察继续），不提前终态。
        continuation_queued = bool(
            result_message.get("continuationQueued")
            or result_message.get("continuation_queued")
        )
        if continuation_queued:
            metadata["banyanos_continuation"] = "queued"
        elif status == "FAILED":
            result["error"] = ("Banyanos SendMessage failed: "
                               + str(content or "unspecified backend error"))
        return result

    def _observe_deduplicated(
        self,
        client: BanyanosClient,
        reservation: dict[str, Any],
        backend_task_id: str,
    ) -> dict[str, Any]:
        """Observe an already-reserved backend execution (no new side effect).

        幂等规则（计划 §Invocation）：同一逻辑操作的重复提交（重试/恢复）
        通过预留行解析既有后端任务，仅查询其生命周期：

        - 已终态：直接回放结果（content 取自后端任务 outputPayload），
          执行器可安全完成任务；
        - 仍非终态：返回 continuation 标记 —— 执行器保任务 WORKING，
          等待 reconciliation（``banyanos_continuation_poller``）。
        """
        try:
            backend = client.task_status(backend_task_id)
        except BanyanosClientError as exc:
            # 观察失败 ≠ 后端失败：保持非终态语义（WORKING）等待下一轮
            # 对账，绝不降级为失败或重新投递。
            self.logger.warning(
                "banyanos dedup observe failed (backend=%s): %s",
                backend_task_id, exc,
            )
            return {
                "content": "",
                "role": "agent",
                "metadata": {
                    "banyanos_backend_task_id": backend_task_id,
                    "idempotency_key": reservation["idempotency_key"],
                    "banyanos_status": "RUNNING",
                    "banyanos_continuation": "queued",
                },
            }

        backend_status = str(backend.get("status") or "").upper()
        content = str((backend.get("outputPayload") or {}).get("content") or "")
        metadata: dict[str, Any] = {
            "banyanos_backend_task_id": backend_task_id,
            "idempotency_key": reservation["idempotency_key"],
            "banyanos_agent_id": (reservation.get("backend_ref") or {}).get(
                "agent_id"
            ) or "",
            "banyanos_status": backend_status or "RUNNING",
            "deduplicated": True,
        }
        if backend_status in ("SUCCESS", "COMPLETED"):
            metadata["banyanos_invocation"] = {
                "task_id": backend_task_id,
                "status": backend_status,
                "idempotency_key": reservation["idempotency_key"],
            }
            return {"content": content, "role": "agent", "metadata": metadata}
        if backend_status in ("FAILED", "CANCELED", "CANCELLED"):
            return {
                "content": "",
                "role": "agent",
                "error": ("Banyanos dedup-observed failure: "
                          + str(backend.get("errorMessage") or backend_status)),
                "metadata": metadata,
            }
        # 非终态：保持 WORKING。
        metadata["banyanos_continuation"] = "queued"
        return {"content": content, "role": "agent", "metadata": metadata}

    # Streaming path (banyan_ws agentInvokeStream subscription)
    # ------------------------------------------------------------------

    def _ask_streaming(
        self,
        client: BanyanosClient,
        identity: Any,
        reservation: dict[str, Any],
        user_query: str,
        session_id: str | None,
        context: dict[str, Any],
        stream_queue: Any,
        stream_event: threading.Event,
    ) -> dict[str, Any]:
        from .a2a_ai_agent_utility import resolve_show_reasoning

        self._current_context = context
        include_reasoning = resolve_show_reasoning(
            self.agent_config, context.get("show_reasoning")
        )
        chunks: list[str] = []
        stream_error: str | None = None
        task_id = ""
        resolved_session = session_id or ""

        def _on_event(event: BanyanosStreamEvent) -> None:
            nonlocal task_id, resolved_session
            if event.event_type == "started":
                # Register the live client so the executor's on_run_id drain
                # (queued from the run_id chunk emitted below) can reach
                # cancel_run mid-stream, not only after completion.
                task_id = (reservation.get("backend_ref") or {}).get("task_id") \
                    or reservation["idempotency_key"]
                with self._registry_lock:
                    self._active_clients[task_id] = client
                stream_queue.put({"name": "run_id", "value": task_id})
            if event.event_type == "chunk":
                chunks.append(event.content)
                stream_queue.put({"name": "token", "value": event.content})
            elif event.event_type == "reasoning" and include_reasoning:
                # Tagged reasoning token — the bridge keeps it out of the
                # answer and tags the SSE chunk (dropped when show_reasoning
                # resolves off; never relabel answer text as reasoning).
                stream_queue.put({
                    "name": "token",
                    "value": event.content,
                    "meta": {"data_format": "reasoning"},
                })
            elif event.event_type in ("tool_started", "tool_finished") \
                    and self.show_tool_progress:
                stream_queue.put({
                    "name": "tool_progress",
                    "value": json.dumps({
                        "tool_name": event.tool_name,
                        "phase": event.event_type,
                        "iteration": event.iteration,
                    }),
                    "meta": {"data_format": "tool_progress"},
                })
            elif event.is_terminal:
                # Terminal — deregister; nothing more to cancel here.
                with self._registry_lock:
                    self._active_clients.pop(
                        task_id or reservation["idempotency_key"], None
                    )
                if event.event_type == "completed":
                    # The reservation row is the correlation record; update it
                    # (below, via record_execution) once invoke returns.
                    pass

        try:
            terminal = client.invoke_agent_stream(
                agent_id=identity.banyanos_agent_id,
                message=user_query,
                session_id=session_id,
                include_reasoning=include_reasoning,
                on_event=_on_event,
            )
        except BanyanosClientError as exc:
            terminal = BanyanosStreamEvent(event_type="error", error=exc.message)
            stream_error = f"Banyanos {exc.category} error: {exc.message}"
        with self._registry_lock:
            self._active_clients.pop(reservation["idempotency_key"], None)

        # ``completed.content`` is the full final answer; prefer it over the
        # re-assembled deltas (authoritative per the WS event contract).
        if terminal.event_type == "completed" and terminal.content:
            full_content = terminal.content
        else:
            full_content = "".join(chunks)

        if terminal.event_type == "error":
            stream_error = stream_error or terminal.error or "Banyanos stream error"
            stream_queue.put({"name": "error", "value": stream_error})

        metadata: dict[str, Any] = {
            "banyanos_task_id": task_id,
            "session_id": resolved_session,
            "idempotency_key": reservation["idempotency_key"],
            "banyanos_status": "COMPLETED" if terminal.event_type == "completed" else "FAILED",
            "banyanos_agent_id": identity.banyanos_agent_id,
        }
        result: dict[str, Any] = {
            "content": full_content,
            "role": "agent",
            "metadata": metadata,
        }
        if stream_error:
            result["error"] = stream_error
        return result

    def resolve_approval(self, run_id: str, approved: bool, reason: str = "") -> bool:
        """Not yet supported — Banyanos agent tasks do not expose an approval
        continuation mutation through this handler's scope (INPUT_REQUIRED
        awaits backend state exposure; see plan §"Task lifecycle")."""
        return False


__all__ = ["BanyanosAgentHandler"]