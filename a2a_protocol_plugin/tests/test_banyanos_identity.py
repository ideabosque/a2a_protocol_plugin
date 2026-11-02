#!/usr/bin/python
"""
Banyanos Identity — Unit Test Suite

Verifies the trusted-identity resolution contract of
``handlers/banyanos_identity.py`` (docs/BANYANOS_INTEGRATION_PLAN.md
§"Identity and tenant isolation"):

- Resolution sources are trusted only: agent-record metadata allowlist,
  gateway setting, Config (env fallbacks). Request/message metadata carried
  in ``context`` is ignored except for the gateway-derived partition scope.
- The configured backend agent id is authoritative; client input cannot
  override it.
- Fail closed: missing partition, URLs, agent id, or credential raise
  ``BanyanosIdentityError``.
- ``redacted()`` never leaks the credential.

Run with: python -m pytest a2a_protocol_plugin/tests/test_banyan_identity.py -v -o log_cli=false
"""

from __future__ import annotations

import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from a2a_protocol_plugin.handlers.banyanos_identity import (
    BanyanosIdentityError,
    METADATA_ALLOWLIST,
    resolve_banyanos_identity,
)

__author__ = "bibow"


@pytest.fixture
def logger():
    return logging.getLogger("test-banyanos-identity")


def _config(**fields):
    defaults = {
        "banyanos_graphql_url": "http://config-gateway.test",
        "banyanos_ws_url": "ws://config-gateway.test",
        "banyanos_token": "config-token",
        "banyanos_agent_id": "agent-config-default",
    }
    defaults.update(fields)
    return type("Config", (), defaults)()


def _metadata(**fields):
    base = {
        "banyanos_graphql_url": "http://meta-gateway.test",
        "banyanos_ws_url": "ws://meta-gateway.test",
        "banyanos_agent_id": "agent-meta-1",
        "banyanos_service_ref": "banyanos-staging",
    }
    base.update(fields)
    return {"metadata": base}


def _context(partition="banyan#t1", **extra):
    ctx = {
        "partition_key": partition,
        "endpoint_id": partition.split("#")[0],
        "part_id": partition.partition("#")[2],
    }
    ctx.update(extra)
    return ctx


@pytest.mark.unit
def test_resolves_from_config_and_partition(logger, monkeypatch):
    monkeypatch.setattr(
        "a2a_protocol_plugin.handlers.banyanos_identity.Config", _config()
    )
    identity = resolve_banyanos_identity({}, {}, _context(), logger)
    assert identity.graphql_url == "http://config-gateway.test"
    assert identity.ws_url == "ws://config-gateway.test"
    assert identity.banyanos_agent_id == "agent-config-default"
    assert identity.token == "config-token"
    assert identity.endpoint_id == "banyan"
    assert identity.part_id == "t1"


@pytest.mark.unit
def test_agent_metadata_overrides_config(logger, monkeypatch):
    monkeypatch.setattr(
        "a2a_protocol_plugin.handlers.banyanos_identity.Config", _config()
    )
    identity = resolve_banyanos_identity(
        _metadata(), {}, _context(), logger
    )
    assert identity.graphql_url == "http://meta-gateway.test"
    assert identity.banyanos_agent_id == "agent-meta-1"
    assert identity.service_ref == "banyanos-staging"


@pytest.mark.unit
def test_token_resolver_wins_over_static_token(logger, monkeypatch):
    monkeypatch.setattr(
        "a2a_protocol_plugin.handlers.banyanos_identity.Config", _config()
    )
    identity = resolve_banyanos_identity(
        _metadata(banyanos_credential_ref="secret/banyanos-staging"),
        {}, _context(), logger,
        token_resolver=lambda service_ref, credential_ref:
            f"resolved:{service_ref}:{credential_ref}",
    )
    assert identity.token == "resolved:banyanos-staging:secret/banyanos-staging"


@pytest.mark.unit
def test_request_metadata_is_never_a_trusted_source(logger, monkeypatch):
    """ spoofed context keys (banyanos_* / any URL) must be ignored. """
    monkeypatch.setattr(
        "a2a_protocol_plugin.handlers.banyanos_identity.Config", _config()
    )
    identity = resolve_banyanos_identity(
        {}, {}, _context(
            banyanos_graphql_url="http://attacker.invalid",
            banyanos_ws_url="ws://attacker.invalid",
            banyanos_token="attacker-token",
            banyanos_agent_id="agent-attacker",
            agent_uuid="agent-attacker",
        ),
        logger,
    )
    assert identity.graphql_url == "http://config-gateway.test"
    assert identity.banyanos_agent_id == "agent-config-default"
    assert identity.token == "config-token"


@pytest.mark.unit
def test_missing_partition_fails_closed(logger, monkeypatch):
    monkeypatch.setattr(
        "a2a_protocol_plugin.handlers.banyanos_identity.Config", _config()
    )
    with pytest.raises(BanyanosIdentityError) as exc_info:
        resolve_banyanos_identity({}, {}, {}, logger)
    assert "partition_key" in str(exc_info.value)


@pytest.mark.unit
def test_missing_token_fails_closed(logger, monkeypatch):
    monkeypatch.setattr(
        "a2a_protocol_plugin.handlers.banyanos_identity.Config", _config(
            banyanos_token=None)
    )
    with pytest.raises(BanyanosIdentityError) as exc_info:
        resolve_banyanos_identity(_metadata(), {}, _context(), logger)
    assert "credential" in str(exc_info.value).lower()


@pytest.mark.unit
def test_missing_agent_id_fails_closed(logger, monkeypatch):
    monkeypatch.setattr(
        "a2a_protocol_plugin.handlers.banyanos_identity.Config", _config(
            banyanos_agent_id=None)
    )
    meta = _metadata()
    meta["metadata"].pop("banyanos_agent_id")
    with pytest.raises(BanyanosIdentityError) as exc_info:
        resolve_banyanos_identity(meta, {}, _context(), logger)
    assert "banyanos_agent_id" in str(exc_info.value)


@pytest.mark.unit
def test_resolver_failure_fails_closed(logger, monkeypatch):
    monkeypatch.setattr(
        "a2a_protocol_plugin.handlers.banyanos_identity.Config", _config()
    )

    def _boom(service_ref, credential_ref):
        raise RuntimeError("vault unreachable")

    with pytest.raises(BanyanosIdentityError) as exc_info:
        resolve_banyanos_identity(_metadata(), {}, _context(), logger,
                                token_resolver=_boom)
    assert "vault unreachable" in str(exc_info.value)


@pytest.mark.unit
def test_redacted_never_includes_token(logger, monkeypatch):
    monkeypatch.setattr(
        "a2a_protocol_plugin.handlers.banyanos_identity.Config", _config()
    )
    identity = resolve_banyanos_identity({}, {}, _context(), logger)
    projection = identity.redacted()
    assert "token" not in projection and "has_token" in projection
    assert projection["has_token"] is True


@pytest.mark.unit
def test_metadata_allowlist_excludes_credential_and_dynamic_values(logger):
    # The allowlist governs values the handler consumes directly; credential
    # references flow only through the token resolver hook.
    assert "banyanos_graphql_url" in METADATA_ALLOWLIST
    assert "banyanos_agent_id" in METADATA_ALLOWLIST
    assert "banyanos_service_ref" in METADATA_ALLOWLIST
    assert "banyanos_token" not in METADATA_ALLOWLIST
def _deployment_metadata(**fields):
    """Agent metadata without endpoint overrides (deployment URLs apply)."""
    meta = _metadata(**fields)["metadata"]
    meta.pop("banyanos_graphql_url")
    meta.pop("banyanos_ws_url")
    return {"metadata": meta}


@pytest.mark.unit
def test_gateway_user_token_wins_over_static_and_resolver(logger, monkeypatch):
    monkeypatch.setattr("a2a_protocol_plugin.handlers.banyanos_identity.Config", _config())
    identity = resolve_banyanos_identity(
        _deployment_metadata(), {},
        _context(_gateway_authorization="Bearer original-user-token"),
        logger, token_resolver=lambda *_: "service-token",
    )
    assert identity.graphql_url == "http://config-gateway.test"
    assert identity.token == "original-user-token"
    assert "original-user-token" not in str(identity.redacted())


@pytest.mark.unit
def test_user_token_not_forwarded_to_agent_overridden_endpoint(logger, monkeypatch):
    """Per-agent metadata pointing elsewhere keeps the configured credential."""
    monkeypatch.setattr("a2a_protocol_plugin.handlers.banyanos_identity.Config", _config())
    identity = resolve_banyanos_identity(
        _metadata(), {},
        _context(_gateway_authorization="Bearer original-user-token"),
        logger, token_resolver=lambda *_: "service-token",
    )
    assert identity.graphql_url == "http://meta-gateway.test"
    assert identity.token == "service-token"


@pytest.mark.unit
def test_invalid_gateway_authorization_never_falls_back(logger, monkeypatch):
    monkeypatch.setattr("a2a_protocol_plugin.handlers.banyanos_identity.Config", _config())
    with pytest.raises(BanyanosIdentityError):
        resolve_banyanos_identity(
            _metadata(), {}, _context(_gateway_authorization="Basic spoofed"), logger,
        )
