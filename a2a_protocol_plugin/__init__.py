#!/usr/bin/python
"""A2A Protocol Plugin — ports A2A-protocol-specific code from a2a_daemon_engine,
importing shared daemon plumbing from silvaengine_daemon.
"""
from __future__ import print_function

__author__ = "bibow"
__version__ = "0.0.1"

# Lazy import to avoid circular import issues during testing
def __getattr__(name):
    if name == "A2ADaemonEngine":
        from .main import A2ADaemonEngine
        return A2ADaemonEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = ["A2ADaemonEngine"]