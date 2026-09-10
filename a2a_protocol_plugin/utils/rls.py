# -*- coding: utf-8 -*-
"""Row-Level Security (RLS) helpers for the A2A Protocol Plugin.

Re-exports ``set_rls_context`` and ``create_rls_policies`` from
``silvaengine_daemon.rls`` with A2A-specific table names.
"""
from __future__ import print_function

__author__ = "bibow"

from silvaengine_daemon.rls import set_rls_context, create_rls_policies

# A2A table names for RLS policy creation
_RLS_TABLES = [
    "a2a_agents",
    "a2a_messages",
    "a2a_settings",
    "a2a_tasks",
]

__all__ = ["set_rls_context", "create_rls_policies", "_RLS_TABLES"]