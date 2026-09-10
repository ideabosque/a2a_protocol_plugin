# -*- coding: utf-8 -*-
"""A2A Protocol Plugin repository registration.

The base ``EntityRepository`` contract, dispatch ``get_repo``/``register_repo``,
and shared helpers (_normalize, normalize_row) live in ``silvaengine_daemon``.
This package only registers A2A-specific repo classes with the daemon's
dispatch registry and sets the backend resolver to use this plugin's
``Config.DB_BACKEND``.
"""
from __future__ import print_function

__author__ = "bibow"

from silvaengine_daemon.repositories import (
    DependencyExistsError,
    EntityNotFoundError,
    EntityRepository,
    RepositoryError,
    clear_registry,
    get_loaders,
    get_repo,
    register_entities,
    register_repo,
    set_backend_resolver,
)

__all__ = [
    "EntityRepository",
    "RepositoryError",
    "EntityNotFoundError",
    "DependencyExistsError",
    "get_repo",
    "get_loaders",
    "register_repo",
    "clear_registry",
    "set_backend_resolver",
    "register_entities",
]


# A2A entity repo specs for each backend.  The silvaengine_daemon dispatch
# lazily imports + instantiates these on first ``get_repo()`` call.
_DYNAMODB_ENTITY_SPECS = [
    ("a2a_protocol_plugin.models.repositories.dynamodb.a2a_agent_repo", "A2AAgentRepository"),
    ("a2a_protocol_plugin.models.repositories.dynamodb.a2a_task_repo", "A2ATaskRepository"),
    ("a2a_protocol_plugin.models.repositories.dynamodb.a2a_message_repo", "A2AMessageRepository"),
    ("a2a_protocol_plugin.models.repositories.dynamodb.a2a_setting_repo", "A2ASettingRepository"),
]

_POSTGRESQL_ENTITY_SPECS = [
    ("a2a_protocol_plugin.models.repositories.postgresql.a2a_agent_repo", "A2AAgentPGRepository"),
    ("a2a_protocol_plugin.models.repositories.postgresql.a2a_task_repo", "A2ATaskPGRepository"),
    ("a2a_protocol_plugin.models.repositories.postgresql.a2a_message_repo", "A2AMessagePGRepository"),
    ("a2a_protocol_plugin.models.repositories.postgresql.a2a_setting_repo", "A2ASettingPGRepository"),
]


def register_a2a_repos() -> None:
    """Register A2A entity repos and backend resolver with silvaengine_daemon.

    Called once at plugin startup (Config.initialize) so ``get_repo()`` can
    lazily instantiate the correct A2A repository for the active backend.
    """
    from ...handlers.config import Config

    set_backend_resolver(lambda: Config.DB_BACKEND)
    register_entities("dynamodb", _DYNAMODB_ENTITY_SPECS)
    register_entities("postgresql", _POSTGRESQL_ENTITY_SPECS)