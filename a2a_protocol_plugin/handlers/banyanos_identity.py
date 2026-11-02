#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Banyanos identity mapping — trusted gateway context to Banyanos tenant/actor.

docs/BANYANOS_INTEGRATION_PLAN.md §"Identity and tenant isolation":

The plugin partitions by ``endpoint_id#part_id``; Banyanos gates access by its
own validated tenant/operator identity (``perm_engine`` authorizer, JWT
claims ``tenant_id`` / ``sub`` operator). This module resolves the trusted,
deployment-controlled view of that identity:

- Banyanos connection details (URLs, credential) come ONLY from trusted
  sources: the agent record's admin-configured metadata allowlist, the
  ``a2a_setting``-backed Config defaults, or environment overrides. Values
  carried in request/message metadata (``context``) are untrusted and are
  never used for target URLs, credentials, or backend agent ids.
- The configured ``banyanos_agent_id`` is authoritative: public client input
  cannot override it to invoke an unrelated/unpublished agent.

Raises :class:`BanyanosIdentityError` when a required trusted value is missing
(fail closed on protected execution paths).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from .config import Config

__author__ = "bibow"


class BanyanosIdentityError(Exception):
    """Trusted Banyanos identity could not be established (fail closed)."""


@dataclass
class BanyanosIdentity:
    """Deployment-validated Banyanos identity for one agent execution.

    ``endpoint_id`` is the BANYANOS GATEWAY's own endpoint id - its deployed
    route prefix (``BANYANOS_ENDPOINT_ID``, default ``banyan``), used to
    build backend URLs. It is NOT the A2A partition's endpoint id; the A2A
    partition scope is kept in ``scope_endpoint_id`` / ``part_id``.
    """

    endpoint_id: str    # Banyanos gateway endpoint id (backend routes)
    part_id: str
    graphql_url: str
    ws_url: str
    scope_endpoint_id: str = ""   # A2A partition endpoint id (mapping rows)
    token: str = ""
    banyanos_agent_id: str = ""
    service_ref: str = ""
    credential_ref: str = ""

    def redacted(self) -> dict[str, Any]:
        """Log-safe projection (never include the credential)."""
        return {
            "endpoint_id": self.endpoint_id,
            "part_id": self.part_id,
            "graphql_url": self.graphql_url,
            "ws_url": self.ws_url,
            "banyanos_agent_id": self.banyanos_agent_id,
            "service_ref": self.service_ref,
            "has_token": bool(self.token),
        }


# Metadata keys the handler may consume from the agent record's metadata.
# Everything else in that dict is ignored; request metadata is never read.
METADATA_ALLOWLIST = (
    # Credential references flow only through the token resolver hook;
    "banyanos_service_ref",
    "banyanos_agent_id",
    "banyanos_graphql_url",
    "banyanos_ws_url",
)


def resolve_banyanos_identity(
    agent_config: dict[str, Any] | None,
    setting: dict[str, Any] | None,
    context: dict[str, Any] | None,
    logger: logging.Logger | None = None,
    token_resolver: Any = None,
) -> BanyanosIdentity:
    """Resolve trusted Banyanos identity for a handler instance.

    Resolution order (trusted values only; request/message metadata from
    ``context`` is deliberately NOT consulted):

    1. agent metadata (admin-configured allowlist — per-agent overrides),
    2. ``setting`` (gateway/configdata forwarding),
    3. Config attributes (``handlers/config.py``, with env fallbacks),
    4. defaults.

    ``token_resolver(service_ref) -> str`` is the optional credential hook
    (secret reference lookup). When absent, ``BANYANOS_TOKEN``/``banyanos_token``
    settings and ``Config.banyanos_token`` are used.

    Raises :class:`BanyanosIdentityError` when URLs or the backend agent id
    cannot be established.
    """
    metadata: dict[str, Any] = {}
    if isinstance(agent_config, dict):
        candidate = agent_config.get("metadata")
        metadata = candidate if isinstance(candidate, dict) else (agent_config or {})
    setting = setting if isinstance(setting, dict) else {}
    context = context if isinstance(context, dict) else {}
    _log = logger or logging.getLogger(__name__)

    def _pick(*names: str) -> str:
        """First non-empty value across allowed sources."""
        for name in names:
            value = metadata.get(name)
            if isinstance(value, str) and value.strip():
                return value.strip()
            value = setting.get(name)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    # Partition / endpoint scope — trusted only from the executor context
    # (gateway-derived), never from message metadata.
    partition_key = str(context.get("partition_key") or "")
    endpoint_id, _, part_id = partition_key.partition("#")
    endpoint_id = endpoint_id or str(context.get("endpoint_id") or "")
    part_id = part_id or str(context.get("part_id") or "")
    if not endpoint_id:
        raise BanyanosIdentityError(
            "Banyanos identity requires a gateway-derived partition_key; "
            "refusing to execute without tenant scope"
        )

    service_ref = _pick("banyanos_service_ref")
    credential_ref = _pick("banyanos_credential_ref")

    # Endpoints: trusted agent metadata overrides, then deployment settings.
    graphql_url = (
        _pick("banyanos_graphql_url", "BANYANOS_GRAPHQL_URL")
        or getattr(Config, "banyanos_graphql_url", None)
        or ""
    )
    ws_url = (
        _pick("banyanos_ws_url", "BANYANOS_WS_URL")
        or getattr(Config, "banyanos_ws_url", None)
        or ""
    )
    banyanos_agent_id = (
        _pick("banyanos_agent_id")
        or getattr(Config, "banyanos_agent_id", None)
        or ""
    )
    # The Banyanos gateway's OWN endpoint id (its route prefix) -
    # deployment config, defaulting to the standard deployed value "banyan".
    # Deliberately NOT derived from the A2A partition's endpoint id: the two
    # deployments are independent.
    banyanos_endpoint_id = (
        _pick("banyanos_endpoint_id", "BANYANOS_ENDPOINT_ID")
        or getattr(Config, "banyanos_endpoint_id", None)
        or "banyan"
    )

    # Credential. The caller's own gateway bearer (forwarded by the A2A
    # server; never taken from request metadata) wins — but only towards the
    # deployment's own Banyanos endpoints. A per-agent metadata override that
    # points elsewhere keeps the configured credential for that target, so a
    # user's token never leaves the deployment.
    authorization = context.get("_gateway_authorization")
    forwarded = ""
    if authorization is not None:
        if not isinstance(authorization, str) or not authorization.lower().startswith("bearer "):
            raise BanyanosIdentityError("Invalid gateway caller authorization")
        forwarded = authorization.split(" ", 1)[1].strip()
        if not forwarded:
            raise BanyanosIdentityError("Missing gateway caller bearer token")

    def _deployment_value(*names: str, attr: str) -> str:
        for name in names:
            value = setting.get(name)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return str(getattr(Config, attr, None) or "").strip()

    deployment_graphql_url = _deployment_value(
        "banyanos_graphql_url", "BANYANOS_GRAPHQL_URL", attr="banyanos_graphql_url"
    )
    deployment_ws_url = _deployment_value(
        "banyanos_ws_url", "BANYANOS_WS_URL", attr="banyanos_ws_url"
    )
    targets_deployment = bool(graphql_url) and (
        graphql_url == deployment_graphql_url and ws_url == deployment_ws_url
    )

    # Otherwise: resolver hook (secret reference from trusted agent metadata,
    # never from request metadata), then settings.
    token = ""
    if forwarded and targets_deployment:
        token = forwarded
    elif forwarded:
        _log.warning(
            "Banyanos endpoint overridden by agent metadata; not forwarding the "
            "caller's token (using the configured credential instead)"
        )
    if not token and token_resolver is not None:
        try:
            resolved = token_resolver(service_ref, credential_ref)
        except Exception as exc:
            raise BanyanosIdentityError(
                f"Banyanos credential resolution failed for service_ref "
                f"{service_ref or '<default>'}: {exc}"
            ) from exc
        if isinstance(resolved, str):
            token = resolved.strip()
    if not token:
        token = _pick("BANYANOS_TOKEN", "banyanos_token") or             (getattr(Config, "banyanos_token", None) or "")

    if not graphql_url:
        raise BanyanosIdentityError(
            "No trusted banyanos_graphql_url configured (agent metadata "
            "banyanos_graphql_url / BANYANOS_GRAPHQL_URL / Config)"
        )
    if not ws_url:
        raise BanyanosIdentityError(
            "No trusted banyanos_ws_url configured (agent metadata banyanos_ws_url "
            "/ BANYANOS_WS_URL / Config)"
        )
    if not banyanos_agent_id:
        raise BanyanosIdentityError(
            "No authoritative banyanos_agent_id configured; the published "
            "backend agent id must come from trusted configuration"
        )
    if not token:
        raise BanyanosIdentityError(
            "No Banyanos credential resolved (BANYANOS_TOKEN / banyanos_token / "
            "Config.banyanos_token); refusing to invoke"
        )

    _log.info(
        "Banyanos identity resolved: %s",
        BanyanosIdentity(
            endpoint_id=banyanos_endpoint_id, part_id=part_id,
            graphql_url=graphql_url, ws_url=ws_url, token="<redacted>",
            banyanos_agent_id=banyanos_agent_id,
            service_ref=service_ref, scope_endpoint_id=endpoint_id,
        ).redacted(),
    )
    return BanyanosIdentity(
        endpoint_id=banyanos_endpoint_id,
        part_id=part_id,
        graphql_url=graphql_url,
        ws_url=ws_url,
        token=token,
        banyanos_agent_id=banyanos_agent_id,
        service_ref=service_ref,
        credential_ref=credential_ref,
        scope_endpoint_id=endpoint_id,
    )


__all__ = ["BanyanosIdentity", "BanyanosIdentityError", "resolve_banyanos_identity",
           "METADATA_ALLOWLIST"]