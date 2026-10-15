#!/usr/bin/python
"""
Partition-scoped conversation mapping for stateful agent backends.

Backends that keep the conversation on their own side (goose sessions, core
engine threads) cannot use the A2A ``contextId`` directly: their ids are
minted by the backend, and the backend is not tenant-aware. These helpers map
an A2A conversation key to the backend's id in the ``a2a_settings`` table,
under the request's ``partition_key``, through the repository dispatch layer
(same pattern as ``DurablePushNotificationConfigStore``) — so it works on both
DynamoDB and PostgreSQL without a migration.

Because every lookup is scoped by ``partition_key``, a conversation key used
by another tenant never resolves to this tenant's backend id.

Storage layout — one settings row per conversation:
    partition_key = "{endpoint_id}#{part_id}"
    setting_id    = "{prefix}#{conversation_key}"
    setting       = {"context_id": conversation_key, <value_key>: backend_id}
"""

import logging
from typing import Any

from .config import Config

__author__ = "bibow"


class _FakeInfo:
    """Minimal GraphQL-style info object the repositories expect."""

    def __init__(self, context: dict[str, Any]) -> None:
        self.context = context


def default_setting_repo() -> Any:
    """Return the ``a2a_setting`` repository, or None when unavailable."""
    try:
        from silvaengine_daemon.repositories import get_repo

        return get_repo("a2a_setting")
    except Exception:
        return None


def _set_rls(partition_key: str) -> None:
    # No-op in DynamoDB mode; sets the tenant row-security context in PG.
    if (
        getattr(Config, "DB_BACKEND", None) == "postgresql"
        and getattr(Config, "db_session", None)
    ):
        Config._set_rls_context(partition_key)


def load_mapping(
    repo: Any,
    partition_key: str,
    setting_id: str,
    value_key: str,
    logger: logging.Logger,
) -> str | None:
    """Return the mapped backend id, or None when absent or on any error."""
    if repo is None:
        return None
    try:
        _set_rls(partition_key)
        row = repo.get(partition_key=partition_key, setting_id=setting_id)
        if not row:
            return None
        setting = row.get("setting") if isinstance(row, dict) else None
        if isinstance(setting, dict):
            value = setting.get(value_key)
            if isinstance(value, str) and value:
                return value
    except Exception as e:
        logger.warning("conversation mapping lookup failed (%s): %s", setting_id, e)
    return None


def store_mapping(
    repo: Any,
    partition_key: str,
    endpoint_id: Any,
    part_id: Any,
    setting_id: str,
    setting: dict[str, Any],
    updated_by: str,
    logger: logging.Logger,
) -> bool:
    """Upsert a mapping row. Returns False (and logs) instead of raising."""
    if repo is None:
        logger.warning(
            "conversation mapping %s not persisted (no a2a_setting repo)", setting_id
        )
        return False
    try:
        _set_rls(partition_key)
        repo.insert_update(
            _FakeInfo({"partition_key": partition_key, "logger": logger}),
            partition_key=partition_key,
            setting_id=setting_id,
            endpoint_id=endpoint_id or str(partition_key).partition("#")[0],
            part_id=part_id or "",
            setting=setting,
            updated_by=updated_by,
        )
        return True
    except Exception as e:
        logger.warning(
            "conversation mapping persistence failed (this turn still works; "
            "the next turn starts a new conversation): %s", e
        )
        return False
