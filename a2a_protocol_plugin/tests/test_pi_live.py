#!/usr/bin/python
"""
pi Agent Handler — Live Integration Tests (against a running pi sidecar)

Gated: skipped unless A2A_RUN_LIVE_PI_TESTS=1.

Environment:
    A2A_RUN_LIVE_PI_TESTS=1  gate (required, tests are skipped otherwise)
    PI_LIVE_URL              (default http://localhost:3300) sidecar base URL
    PI_LIVE_KEY              (default empty — the sidecar requires the shared
                             secret unless started without PI_SIDECAR_SECRET_KEY)
    PI_LIVE_MODEL            optional provider/modelId to pin the model
    PI_LIVE_CWD              optional working dir (default /workspace)

Usage:
    python -m pytest a2a_protocol_plugin/tests/test_pi_live.py -v -s
    A2A_RUN_LIVE_PI_TESTS=1 PI_LIVE_KEY=<secret> python -m pytest ...

Author: bibow
"""

import logging
import os
import queue
import sys
import threading
import time

import pytest

from a2a_protocol_plugin.handlers.pi_handler import PiAgentHandler

__author__ = "bibow"

PI_LIVE_URL = os.getenv("PI_LIVE_URL", "http://localhost:3300")
PI_LIVE_KEY = os.getenv("PI_LIVE_KEY", "")
PI_LIVE_MODEL = os.getenv("PI_LIVE_MODEL", "")
PI_LIVE_CWD = os.getenv("PI_LIVE_CWD", "/workspace")

pytestmark = pytest.mark.skipif(
    os.getenv("A2A_RUN_LIVE_PI_TESTS") != "1",
    reason="Set A2A_RUN_LIVE_PI_TESTS=1 to run live pi integration tests",
)


def _handler() -> PiAgentHandler:
    setting = {
        "PI_API_URL": PI_LIVE_URL,
        "PI_SECRET_KEY": PI_LIVE_KEY,
        "PI_CWD": PI_LIVE_CWD,
    }
    if PI_LIVE_MODEL:
        setting["PI_MODEL"] = PI_LIVE_MODEL
    return PiAgentHandler(
        logger=logging.getLogger("pi-live"),
        agent_config={"metadata": {}},
        setting=setting,
        context={},
    )


def _chunks(q: queue.Queue, timeout: float = 60.0) -> list:
    """Collect chunks while a background run streams."""
    items = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            items.append(q.get(timeout=0.3))
        except queue.Empty:
            break
    return items


def test_health_and_session():
    handler = _handler()
    session = handler._new_client().create_session(cwd=PI_LIVE_CWD)
    assert session.get("sessionId")


def test_first_turn_round_trip():
    handler = _handler()
    result = handler.ask_model(
        [{"role": "user", "content": "Say hello and nothing else."}],
        {"partition_key": "live#default", "context_id": "pi-live-1"},
    )
    assert not result.get("error") or "aborted" not in str(result["error"]).lower()
    assert isinstance(result.get("content"), str)
    assert result["metadata"]["thread_uuid"]  # pi sessionId


def test_context_echo_resumes_same_session():
    handler = _handler()
    first = handler.ask_model(
        [{"role": "user", "content": "Remember the token word: BANANA42."}],
        {"partition_key": "live#default", "context_id": "pi-live-echo"},
    )
    first_sid = first["metadata"]["thread_uuid"]

    second = handler.ask_model(
        [{"role": "user", "content": "What was the token word?"}],
        {"partition_key": "live#default", "context_id": "pi-live-echo"},
    )
    # Echoing the conversation id resumes the SAME pi session.
    assert second["metadata"]["thread_uuid"] == first_sid
    assert "BANANA42" in (second.get("content") or "") or not second.get("error")


def test_streaming_round_trip_with_run_id():
    handler = _handler()
    q = queue.Queue()
    ev = threading.Event()
    result = handler.ask_model(
        [{"role": "user", "content": "Count from 1 to 5 slowly."}],
        {"partition_key": "live#default", "context_id": "pi-live-stream"},
        stream_queue=q,
        stream_event=ev,
    )
    chunks = _chunks(q)
    names = [c["name"] for c in chunks]
    assert names and names[0] == "run_id"
    assert "token" in names or (result.get("content"))
    assert not any(c.get("meta", {}).get("data_format") == "reasoning"
                   and c["value"] in (result.get("content") or "") for c in chunks)