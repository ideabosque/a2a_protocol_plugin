#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Banyanos continuation poller — reconcile A2A tasks left WORKING.

Plan Phase 3 ("observe-only task updates", pull variant): a queued backend
continuation leaves the public A2A task nonterminal (DEF-B002 emits a
``WORKING`` Task and stores a reconcile record). This poller closes the loop
without push infrastructure:

    a2a_tasks (status=in_progress, output_data.banyanos_continuation=queued)
      -> reconcile record ``banyanos_reconcile#<reply_task_id>``
         (backend_task_id — durable correlation)
      -> backend ``taskStatus`` query (``BanyanosClient.task_status``)
      -> terminal backend state -> public task row terminal update

Rules honored (a2a_integration_development_plan.md):

- Observation, not resubmission: the backend executes exactly once (queued);
  this module only observes and projects terminal states. Ambiguous lookups
  (missing backend ref or query failure) are logged and retried next pass;
  the mapping row is marked ``ERROR`` only after a bounded retry budget is
  exhausted, so state remains inspectable rather than silently mutated.
- Idempotent completion: terminal re-check before writing (a row already
  terminal is skipped); mapping row state records the terminal projection.
- Durable correlation: backend task ids come only from the reconcile record
  (trusted, server-written) — never message metadata.
- Single worker: started next to the a2a scheduler (one gateway worker),
  gated by ``BANYANOS_CONTINUATION_POLL_ENABLED`` (default enabled).

PostgreSQL-only slice: the in_progress scan reads the SQLA model directly
(``Config.db_session``); other backends log a single info line and idle.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Optional

from .conversation_map import (  # noqa: F401 — re-exported helpers
    default_setting_repo,
    store_mapping,
)
from .banyanos_execution_map import _safe
from .config import Config
from .banyanos_client import BanyanosClient, BanyanosClientError

__author__ = "bibow"

# Backend states treated as terminal, mapped to public task statuses.
_BACKEND_TERMINAL = {
    "SUCCESS": "completed",
    "FAILED": "failed",
    "CANCELED": "failed",
    "CANCELLED": "failed",
}
# A reconcile attempt budget before mapping rows stop mutating state
# (kept inspectable for ops; the poller keeps polling cheaply regardless).
_MAX_LOOKUP_FAILURES = 50

_SCAN_LIMIT_DEFAULT = 20
_INTERVAL_DEFAULT = 15.0

logger = logging.getLogger(__name__)

_instance: Optional["ContinuationPoller"] = None
_instance_lock = threading.Lock()


def _default_endpoint_part(partition_key: str) -> tuple[str, str]:
    endpoint_id, _, part_id = partition_key.partition("#")
    return (endpoint_id or "", part_id or "")


def _client_from_config() -> Optional[BanyanosClient]:
    """Build a Banyanos read client from trusted deployment configuration.

    Reuses Config's banyanos_* bridge settings (URLs + delegated credential);
    returns None when the deployment has not configured them (poller stays
    idle, matching fail-closed identity rules — no untrusted fallbacks).
    """
    graphql_url = getattr(Config, "banyanos_graphql_url", None)
    token = getattr(Config, "banyanos_token", None)
    if not graphql_url or not token:
        logger.info(
            "banyanos continuation poller idle (banyanos_graphql_url/token "
            "not configured)"
        )
        return None
    return BanyanosClient(
        logger=logger,
        graphql_url=graphql_url,
        ws_url=getattr(Config, "banyanos_ws_url", None) or "",
        endpoint_id=getattr(Config, "banyanos_endpoint_id", None) or "banyan",
        token=token,
        part_id="",
    )


class ContinuationPoller:
    """Poll WORKING continuation tasks and project backend terminal states."""

    def __init__(
        self,
        client: Any,
        scan_limit: int = _SCAN_LIMIT_DEFAULT,
        max_lookup_failures: int = _MAX_LOOKUP_FAILURES,
        logger_: logging.Logger | None = None,
    ) -> None:
        self.client = client
        self.scan_limit = int(scan_limit)
        self.max_lookup_failures = int(max_lookup_failures)
        self.logger = logger_ or logger

    # -- public entry -------------------------------------------------------

    def run_forever(self, interval: float = _INTERVAL_DEFAULT) -> None:
        """Poll loop (runs in a daemon thread started by ``start_poller``)."""
        while True:
            try:
                reconciled = self.poll_once()
                if reconciled:
                    self.logger.info(
                        "banyanos continuation poller: %d task(s) reconciled.",
                        reconciled,
                    )
            except Exception as exc:  # noqa: BLE001 — poller must never die
                self.logger.error("banyanos continuation poll failed: %s", exc)
            time.sleep(max(1.0, float(interval)))

    # -- scan ---------------------------------------------------------------

    def _scan_working_rows(self) -> list[dict[str, Any]]:
        """One cross-partition page of in_progress continuation task rows."""
        if getattr(Config, "DB_BACKEND", None) != "postgresql":
            return []
        db_session = getattr(Config, "db_session", None)
        if db_session is None:
            return []
        from a2a_protocol_plugin.models.postgresql.a2a_task import A2ATaskModel

        rows = (
            db_session.query(A2ATaskModel)
            .filter(
                A2ATaskModel.status == "in_progress",
                A2ATaskModel.task_type == "message_response",
            )
            .order_by(A2ATaskModel.updated_at.asc())
            .limit(self.scan_limit)
            .all()
        )
        working: list[dict[str, Any]] = []
        for row in rows:
            output_data = row.output_data if isinstance(row.output_data, dict) \
                else None
            if not isinstance(output_data, dict):
                continue
            if output_data.get("banyanos_continuation") != "queued":
                continue
            working.append(
                {
                    "partition_key": row.partition_key or "",
                    "task_id": row.task_id or "",
                    "context_id": row.context_id or "",
                    "output_data": output_data,
                    "created_at": row.created_at,
                    "updated_at": row.updated_at,
                }
            )
        return working

    # -- reconcile ----------------------------------------------------------

    def _load_reconcile_record(
        self, partition_key: str, reply_task_id: str
    ) -> Optional[dict[str, Any]]:
        repo = default_setting_repo()
        if repo is None:
            return None
        try:
            from .conversation_map import _set_rls

            _set_rls(partition_key)
            row = repo.get(
                partition_key=partition_key,
                setting_id=f"banyanos_reconcile#{_safe(reply_task_id)}",
            )
            setting = row.get("setting") if isinstance(row, dict) else None
            return setting if isinstance(setting, dict) else None
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "reconcile record lookup failed (task=%s): %s", reply_task_id, exc
            )
            return None

    @staticmethod
    def _extract_content(status_result: dict[str, Any]) -> str:
        output = status_result.get("outputPayload") or status_result.get(
            "output_payload"
        )
        if isinstance(output, dict):
            return str(output.get("content") or "")
        if isinstance(status_result.get("output"), dict):
            return str(status_result["output"].get("content") or "")
        return ""

    def _reconcile_one(self, task: dict[str, Any]) -> bool:
        """Project one backend terminal state onto the public task row."""
        partition_key = task["partition_key"]
        reply_task_id = task["task_id"]

        record = self._load_reconcile_record(partition_key, reply_task_id)
        if not record:
            self.logger.warning(
                "banyanos reconciliation: no record for task=%s (will retry)",
                reply_task_id,
            )
            self._record_lookup_failure(partition_key, reply_task_id)
            return False
        backend_task_id = str(record.get("backend_task_id") or "")
        if not backend_task_id:
            self.logger.warning(
                "banyanos reconciliation: record without backend ref "
                "(task=%s)", reply_task_id,
            )
            self._record_lookup_failure(partition_key, reply_task_id)
            return False

        # Backend auth scope = the task's own partition tenant (Part-Id).
        endpoint_id, part_id = _default_endpoint_part(partition_key)
        try:
            self.client.part_id = part_id
            backend = self.client.task_status(backend_task_id)
        except BanyanosClientError as exc:
            self.logger.warning(
                "banyanos taskStatus query failed (backend=%s): %s",
                backend_task_id, exc,
            )
            self._record_lookup_failure(partition_key, reply_task_id)
            return False

        backend_status = str(
            backend.get("status") or backend.get("taskStatus") or ""
        ).upper()
        terminal = _BACKEND_TERMINAL.get(backend_status)
        if not terminal:
            return False  # still running (or unknown) — next pass

        final_content = self._extract_content(backend)
        error_message = backend.get("errorMessage") or ""
        payload = (
            {"content": final_content}
            if terminal == "completed" and final_content
            else {"content": "", "error_message": str(error_message)}
        )
        self._write_terminal(
            partition_key, reply_task_id, terminal, payload, backend_task_id
        )
        self.logger.info(
            "banyanos continuation reconciled: a2a_task=%s backend=%s -> %s",
            reply_task_id, backend_task_id, terminal,
        )
        return True

    def _write_terminal(
        self,
        partition_key: str,
        reply_task_id: str,
        terminal: str,
        payload: dict[str, Any],
        backend_task_id: str,
    ) -> None:
        """Idempotent terminal write onto the public task row + map row."""
        from .conversation_map import _set_rls

        repo = default_setting_repo()
        try:
            if repo is not None:
                _set_rls(partition_key)
                existing = repo.get(
                    partition_key=partition_key, task_id=reply_task_id
                )
                if isinstance(existing, dict) and existing.get("status") in (
                    "completed",
                    "failed",
                ):
                    return  # terminal re-check — idempotent write
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "terminal re-check failed (task=%s): %s", reply_task_id, exc
            )

        from .conversation_map import _set_rls

        # Public row update through the a2a_core async bridge (same writer
        # the executor uses; RLS via _set_rls above when applicable).
        import asyncio as _asyncio

        async def _write() -> None:
            if hasattr(Config, "a2a_core") and Config.a2a_core is not None:
                if hasattr(Config.a2a_core, "insert_update_a2a_task"):
                    await Config.a2a_core.insert_update_a2a_task(
                        partition_key=partition_key,
                        task_id=reply_task_id,
                        task_type="message_response",
                        status=terminal,
                        output_data=payload,
                        context_id=None,
                        updated_by="banyanos_reconciler",
                    )

        try:
            from silvaengine_daemon.async_utils import run_async as _run_async

            _run_async(_write())
        except Exception as exc:  # noqa: BLE001
            try:
                _asyncio.get_event_loop().run_until_complete(_write())
            except Exception:
                self.logger.error(
                    "terminal write failed (task=%s): %s", reply_task_id, exc
                )
                return

        if repo is not None:
            try:
                _set_rls(partition_key)
                record = self._load_reconcile_record(partition_key, reply_task_id)
                if isinstance(record, dict):
                    record["state"] = terminal.upper()
                    record["backend_task_id"] = backend_task_id
                    endpoint_id, part_id = _default_endpoint_part(partition_key)
                    store_mapping(
                        repo, partition_key, endpoint_id, part_id,
                        f"banyanos_reconcile#{_safe(reply_task_id)}",
                        record, "banyanos_reconciler", self.logger,
                    )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "reconcile map terminal update failed (task=%s): %s",
                    reply_task_id, exc,
                )

    def _record_lookup_failure(
        self, partition_key: str, reply_task_id: str
    ) -> None:
        repo = default_setting_repo()
        if repo is None:
            return
        try:
            record = self._load_reconcile_record(partition_key, reply_task_id)
            if not isinstance(record, dict):
                return
            failures = int(record.get("lookup_failures") or 0) + 1
            record["lookup_failures"] = failures
            if failures >= self.max_lookup_failures:
                record["state"] = "ERROR"
                self.logger.error(
                    "banyanos reconciliation stale (task=%s): %d lookup "
                    "failures — inspect records manually.",
                    reply_task_id, failures,
                )
            endpoint_id, part_id = _default_endpoint_part(partition_key)
            store_mapping(
                repo, partition_key, endpoint_id, part_id,
                f"banyanos_reconcile#{_safe(reply_task_id)}",
                record, "banyanos_reconciler", self.logger,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("lookup-failure record failed: %s", exc)

    # -- one pass -----------------------------------------------------------

    def poll_once(self) -> int:
        """One scan+reconcile pass; returns the number of reconciled tasks."""
        if self.client is None:
            return 0
        try:
            working = self._scan_working_rows()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("in_progress scan failed: %s", exc)
            return 0
        reconciled = 0
        for task in working:
            try:
                if self._reconcile_one(task):
                    reconciled += 1
            except Exception as exc:  # noqa: BLE001
                self.logger.error(
                    "reconcile failed (task=%s): %s",
                    task.get("task_id"), exc,
                )
        return reconciled


def start_poller() -> threading.Thread | None:
    """Gateway on_startup companion for the a2a scheduler (one worker only)."""
    global _instance
    if os.environ.get("BANYANOS_CONTINUATION_POLL_ENABLED", "true").strip().lower() in (
        "0", "false", "no", "off",
    ):
        logger.info("banyanos continuation poller disabled (env flag)")
        return None
    with _instance_lock:
        if _instance is not None:
            return None
        client = _client_from_config()
        _instance = ContinuationPoller(client)
        if client is None:
            return None
        interval = float(
            os.environ.get("BANYANOS_CONTINUATION_POLL_INTERVAL", str(_INTERVAL_DEFAULT))
        )
        thread = threading.Thread(
            target=_instance.run_forever,
            args=(interval,),
            name="banyanos-continuation-poller",
            daemon=True,
        )
        thread.start()
        logger.info(
            "banyanos continuation poller started (every %.0fs).", interval
        )
        return thread


def stop_poller() -> None:
    """Daemon thread — no join contract; kept for symmetry with scheduler."""
    global _instance
    with _instance_lock:
        _instance = None


__all__ = ["ContinuationPoller", "start_poller", "stop_poller"]