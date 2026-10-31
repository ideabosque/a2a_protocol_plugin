#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Banyanos execution map — durable public/backend task correlation.

docs/BANYANOS_INTEGRATION_PLAN.md §"Durable correlation and history". All
mappings are persisted in the ``a2a_settings`` table (through the repository
dispatch layer, both DynamoDB and PostgreSQL) under the request's
``partition_key``, mirroring ``handlers/conversation_map.py``:

    partition_key = "{endpoint_id}#{part_id}"
    setting_id    = "banyanos_session#{context_id}"
                    -> {"context_id": ..., "session_id": backend session}
    setting_id    = "banyanos_exec#{context_id}#{invocation_digest}"
                    -> {"backend_ref": {"kind": "agent_task", "task_id": ...,
                        "invocation_id": ..., "agent_id": ...},
                        "idempotency_key": ..., "state": ...}

The execution row is written **before** invoking the backend (reserve, then
invoke) so concurrent retries of the same logical message can resolve to the
existing invocation instead of submitting a duplicate side effect. After an
ambiguous outcome the caller reconciles with ``task_status`` (see
``BanyanosClient.task_status``) rather than blindly resubmitting.
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

from .conversation_map import (
    default_setting_repo,
    load_mapping,
    store_mapping,
)

__author__ = "bibow"

# Nonterminal statuses of a Banyanos agent invocation/task.
NONTERMINAL_STATUSES = ("PENDING", "RUNNING")

_SAFE_ID = re.compile(r"[^A-Za-z0-9_#-]")


def _safe(part: str) -> str:
    """Keep setting ids stably encodable (ids are opaque strings in Banyanos)."""
    return _SAFE_ID.sub("_", part)[:128]


def invocation_digest(
    partition_key: str,
    context_id: str,
    agent_id: str,
    message: str,
) -> str:
    """Stable digest identifying one logical message submission.

    Includes the caller scope (partition) and target (agent id) so distinct
    nodes/tenants invoking the same agent never share an idempotency key.
    """
    return hashlib.sha256(
        f"{partition_key}\x1f{context_id}\x1f{agent_id}\x1f{message}".encode("utf-8")
    ).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Session mapping (context_id -> Banyanos session_id)
# ---------------------------------------------------------------------------


def load_session_id(
    partition_key: str,
    context_id: str | None,
    logger: logging.Logger,
) -> str | None:
    """Return the Banyanos session id mapped for this A2A context, or None."""
    if not context_id:
        return None
    repo = default_setting_repo()
    if repo is None:
        return None
    return load_mapping(
        repo, partition_key,
        f"banyanos_session#{_safe(context_id)}", "session_id", logger,
    )


def store_session_id(
    partition_key: str,
    endpoint_id: str,
    part_id: str,
    context_id: str | None,
    session_id: str,
    logger: logging.Logger,
) -> bool:
    """Persist context_id -> Banyanos session_id. Durable but non-fatal on error."""
    if not context_id or not session_id:
        return False
    repo = default_setting_repo()
    if repo is None:
        logger.warning("banyanos session mapping not persisted (no a2a_setting repo)")
        return False
    return store_mapping(
        repo, partition_key, endpoint_id, part_id,
        f"banyanos_session#{_safe(context_id)}",
        {"context_id": context_id, "session_id": session_id},
        "banyanos_agent_handler", logger,
    )


# ---------------------------------------------------------------------------
# Execution mapping (logical invocation -> backend task) + deduplication
# ---------------------------------------------------------------------------


def _execution_row(repo: Any, partition_key: str, setting_id: str) -> dict[str, Any] | None:
    try:
        row = repo.get(partition_key=partition_key, setting_id=setting_id)
        if not row:
            return None
        setting = row.get("setting") if isinstance(row, dict) else None
        return setting if isinstance(setting, dict) else None
    except Exception:
        return None


def resolve_or_reserve_execution(
    partition_key: str,
    endpoint_id: str,
    part_id: str,
    context_id: str,
    agent_id: str,
    message: str,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Resolve an existing nonterminal execution, or reserve a new logical one.

    Returns a dict::

        {
            "deduplicated": bool,    # an existing nonterminal execution exists
            "backend_ref": {...},    # agent_task ref when deduplicated
            "idempotency_key": str,  # stable per logical invocation
            "setting_id": str,
        }

    Best effort: when no repository is available (e.g. degraded settings
    backend) each submission gets a fresh idempotency key and no dedup
    guarantee is claimed — callers must reconcile after ambiguous timeouts.
    """
    digest = invocation_digest(partition_key, context_id or "", agent_id, message)
    setting_id = f"banyanos_exec#{_safe(context_id or 'none')}#{digest}"
    idempotency_key = f"banyanos-{digest}"

    repo = default_setting_repo()
    if repo is None:
        logger.warning(
            "banyanos execution map unavailable; proceeding without durable "
            "dedup (invoke-after-ambiguity must reconcile via task_status)"
        )
        return {
            "deduplicated": False,
            "backend_ref": None,
            "idempotency_key": idempotency_key,
            "setting_id": setting_id,
        }

    existing = _execution_row(repo, partition_key, setting_id)
    if isinstance(existing, dict) and existing.get("backend_ref"):
        backend_ref = existing["backend_ref"]
        if isinstance(backend_ref, dict) and backend_ref.get("task_id"):
            return {
                "deduplicated": True,
                "backend_ref": backend_ref,
                "idempotency_key": idempotency_key,
                "setting_id": setting_id,
            }

    # Reserve before invoke: a concurrent retry reading the row finds the
    # reservation rather than submitting a second backend invocation.
    store_mapping(
        repo, partition_key, endpoint_id, part_id, setting_id,
        {
            "backend_ref": {"kind": "agent_task", "task_id": None,
                            "invocation_id": None, "agent_id": agent_id},
            "idempotency_key": idempotency_key,
            "state": "RESERVED",
            "context_id": context_id or "",
        },
        "banyanos_agent_handler", logger,
    )
    return {
        "deduplicated": False,
        "backend_ref": None,
        "idempotency_key": idempotency_key,
        "setting_id": setting_id,
    }


def record_execution(
    partition_key: str,
    endpoint_id: str,
    part_id: str,
    setting_id: str,
    invocation: dict[str, Any] | None,
    logger: logging.Logger,
    session_id: str | None = None,
) -> None:
    """Persist the backend task/invocation ref for a reserved execution."""
    repo = default_setting_repo()
    if repo is None or not setting_id:
        return
    # 后端任务行 ID 优先（task_id 键，Phase 3 续跑对账引用）；fallback:
    # invocation id（assistant 消息 id，兼容旧行为）。
    backend_task_id = str(
        (invocation or {}).get("task_id")
        or (invocation or {}).get("id") or ""
    )
    payload = {
        "backend_ref": {
            "kind": "agent_task",
            "task_id": backend_task_id,
            "invocation_id": backend_task_id,
            "agent_id": str((invocation or {}).get("agent_id") or ""),
        },
        "idempotency_key": (invocation or {}).get("idempotency_key", ""),
        "state": (invocation or {}).get("status", ""),
        "session_id": session_id or "",
    }
    store_mapping(
        repo, partition_key, endpoint_id, part_id, setting_id,
        payload, "banyanos_agent_handler", logger,
    )


def _store_reconcile_record(
    partition_key: str,
    reply_task_id: str,
    backend_task_id: str,
    context_id: str,
    logger: logging.Logger,
) -> bool:
    """Persist the continuation reconciliation record (DEF-B002 companion).

    Reply (A2A) task id -> backend agent task id + context correlation, in
    ``a2a_settings`` under key ``banyanos_reconcile#<reply_task_id>``. The
    continuation poller (``banyanos_continuation_poller``) reads these to
    resolve terminal backend states for A2A tasks left WORKING.
    """
    if not reply_task_id or not backend_task_id:
        logger.warning(
            "banyanos reconcile record skipped (reply_task_id=%r, "
            "backend_task_id=%r)", reply_task_id, backend_task_id,
        )
        return False
    repo = default_setting_repo()
    if repo is None:
        logger.warning("banyanos reconcile record not persisted (no repo)")
        return False
    return bool(store_mapping(
        repo, partition_key, partition_key.split("#", 1)[0] if "#" in partition_key else "",
        partition_key.split("#", 1)[1] if "#" in partition_key else "",
        f"banyanos_reconcile#{_safe(reply_task_id)}",
        {
            "backend_task_id": backend_task_id,
            "context_id": context_id or "",
            "state": "POLLING",
        },
        "banyanos_reconciler", logger,
    ))


__all__ = [
    "invocation_digest",
    "load_session_id",
    "store_session_id",
    "resolve_or_reserve_execution",
    "record_execution",
    "NONTERMINAL_STATUSES",
]