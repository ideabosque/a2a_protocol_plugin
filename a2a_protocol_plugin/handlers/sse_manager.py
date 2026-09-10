#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SSE manager compatibility shim for the A2A Protocol Plugin.

Re-exports ``SSEManager``, ``sse_manager``, and ``cleanup_sse`` from
``silvaengine_daemon.sse`` so the gateway's module routes can resolve
``a2a_protocol_plugin.handlers.sse_manager:sse_manager`` and
``a2a_protocol_plugin.handlers.sse_manager:cleanup_sse``.
"""
from __future__ import annotations

__author__ = "bibow"

from silvaengine_daemon.sse import SSEManager, cleanup_sse, sse_manager

__all__ = ["SSEManager", "sse_manager", "cleanup_sse"]