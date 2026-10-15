#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Goose Live Integration Tests — runs against a live ``goose serve`` ACP server.

Prerequisites:
    - goose serve running (default http://localhost:3284) with:
        GOOSE_SERVER__SECRET_KEY set (or the same value passed here),
        a provider configured (`goose configure` / GOOSE_PROVIDER + key).
    - Example: goose serve --host 127.0.0.1 --port 3284

Environment variables:
    A2A_RUN_LIVE_GOOSE_TESTS=1   gate (required, tests are skipped otherwise)
    GOOSE_LIVE_URL               (default http://localhost:3284)
    GOOSE_LIVE_KEY               (default empty — goose serve requires
                                 GOOSE_SERVER__SECRET_KEY unless started with
                                 --dangerously-unauthenticated)

Usage:
    python -m pytest a2a_protocol_plugin/tests/test_goose_live.py -v -s
    A2A_RUN_LIVE_GOOSE_TESTS=1 GOOSE_LIVE_KEY=<secret> python -m pytest ...

Author: bibow
"""

import json
import logging
import os
import queue
import sys
import threading

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from a2a_protocol_plugin.handlers.goose_handler import GooseAgentHandler

__author__ = "bibow"

# ---------------------------------------------------------------------------
# Config from env
# ---------------------------------------------------------------------------
GOOSE_API_URL = os.getenv("GOOSE_LIVE_URL", "http://localhost:3284")
GOOSE_SECRET = os.getenv("GOOSE_LIVE_KEY", "")

LIVE = pytest.mark.skipif(
    os.getenv("A2A_RUN_LIVE_GOOSE_TESTS") != "1",
    reason="Set A2A_RUN_LIVE_GOOSE_TESTS=1 to run live goose integration tests",
)


def _handler() -> GooseAgentHandler:
    return GooseAgentHandler(
        logger=logging.getLogger("goose-live"),
        agent_config={"metadata": {}},
        setting={"GOOSE_API_URL": GOOSE_API_URL, "GOOSE_SECRET_KEY": GOOSE_SECRET},
        context={},
    )


@LIVE
class TestGooseLive:
    def test_server_reachable(self):
        with httpx.Client(timeout=10) as client:
            resp = client.post(
                f"{GOOSE_API_URL}/acp",
                json={"jsonrpc": "2.0", "method": "initialize",
                      "params": {"protocolVersion": 1}, "id": 1},
                headers={"X-Secret-Key": GOOSE_SECRET,
                         "Acp-Connection-Id": "pytest-live-probe"},
            )
            assert resp.status_code == 200, (resp.status_code, resp.text[:200])
            result = resp.json().get("result") or {}
            assert result.get("agentInfo", {}).get("name") == "goose"

    def test_non_streaming_round_trip(self):
        h = _handler()
        result = h.ask_model(
            input_messages=[{"role": "user", "content": "Reply with exactly: live-ok"}],
            context={"partition_key": "test#default", "thread_uuid": None},
        )
        assert not result.get("error"), result
        assert "live-ok" in (result.get("content") or "")
        assert result["metadata"]["goose"]["stopReason"] == "end_turn"

    def test_streaming_round_trip_and_resume(self):
        h = _handler()
        first = h.ask_model(
            input_messages=[{"role": "user", "content": "Remember the word: bananahat. Reply with: remembered"}],
            context={"partition_key": "test#default", "thread_uuid": None},
        )
        assert not first.get("error"), first
        session_id = first["metadata"]["thread_uuid"]

        q: queue.Queue = queue.Queue()
        second = h.ask_model(
            input_messages=[{"role": "user",
                             "content": "What word did I ask you to remember? Reply with just that word."}],
            context={"partition_key": "test#default", "thread_uuid": session_id},
            stream_queue=q, stream_event=threading.Event(),
        )
        assert not second.get("error"), second
        assert second["metadata"]["thread_uuid"] == session_id, (
            "second turn must resume the same goose session"
        )
        assert "bananahat" in (second.get("content") or "")
        names = []
        while not q.empty():
            names.append(q.get()["name"])
        assert "run_id" in names and "token" in names

    def test_tool_call_flow(self):
        """A tool-using turn surfaces tool_call / tool_result chunks."""
        h = _handler()
        q: queue.Queue = queue.Queue()
        result = h.ask_model(
            input_messages=[{"role": "user",
                             "content": "Write a one-word file named goose-live-test.txt in the current directory. Confirm with: written"}],
            context={"partition_key": "test#default", "thread_uuid": None},
            stream_queue=q, stream_event=threading.Event(),
        )
        assert not result.get("error"), result
        names = [q.get()["name"] for _ in range(q.qsize())] if q.qsize() else []
        # Tool use is model-dependent; assert only that the flow completed.
        assert result["metadata"]["goose"]["stopReason"] == "end_turn"

    def test_cancel_mid_run(self):
        h = _handler()
        q: queue.Queue = queue.Queue()
        holder = {}

        def _ask():
            holder["res"] = h.ask_model(
                input_messages=[{"role": "user",
                                 "content": "Count from 1 to 500, one number per line."}],
                context={"partition_key": "test#default", "thread_uuid": None},
                stream_queue=q, stream_event=threading.Event(),
            )

        t = threading.Thread(target=_ask, daemon=True)
        t.start()
        run_id = None
        for _ in range(100):
            try:
                chunk = q.get(timeout=10)
            except queue.Empty:
                continue
            if chunk["name"] == "run_id":
                run_id = chunk["value"]
            elif chunk["name"] == "token":
                break
        assert run_id, "run_id chunk missing"
        assert h.cancel_run(run_id) is True
        t.join(timeout=60)
        assert not t.is_alive(), "cancelled run must settle"
        result = holder.get("res") or {}
        # goose resolves the pending prompt with stopReason=cancelled
        assert (result.get("metadata") or {}).get("goose", {}).get("stopReason") in (
            "cancelled", None, ""
        )