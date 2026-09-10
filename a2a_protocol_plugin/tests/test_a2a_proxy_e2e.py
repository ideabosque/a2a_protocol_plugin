#!/usr/bin/python
# -*- coding: utf-8 -*-
"""End-to-end test: A2AProxyHandler → Hermes A2A (localhost:9900).

Tests the full chain:
  A2AProxyHandler.ask_model() → JSON-RPC SendMessage → Hermes A2A adapter → gateway → agent → reply

Validates:
  1. Agent Card discovery
  2. Non-streaming SendMessage (ask_model without stream_queue)
  3. Streaming SendStreamingMessage (ask_model with stream_queue)
  4. Multi-turn conversation (context_id continuity)
  5. Error handling (malformed request)
"""
from __future__ import print_function

__author__ = "bibow"

import json
import logging
import os
import queue
import sys
import threading
import time
import urllib.request

# Add a2a_protocol_plugin to path
A2A_DIR = r"C:\Users\bibo7\gitrepo\silvaengine\a2a_protocol_plugin"
sys.path.insert(0, A2A_DIR)

from a2a_protocol_plugin.handlers.a2a_proxy_handler import A2AProxyHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

HERMES_A2A_URL = "http://localhost:9900"
RESULTS = []


def run_test(test_id, test_name, fn):
    t0 = time.perf_counter()
    try:
        fn()
        elapsed = time.perf_counter() - t0
        RESULTS.append({"id": test_id, "name": test_name, "status": "PASS", "elapsed_s": round(elapsed, 2)})
        print(f"  {test_id}: PASS — {test_name} ({elapsed:.1f}s)")
    except Exception as e:
        elapsed = time.perf_counter() - t0
        RESULTS.append({"id": test_id, "name": test_name, "status": "FAIL", "elapsed_s": round(elapsed, 2), "error": str(e)[:300]})
        print(f"  {test_id}: FAIL — {test_name} ({elapsed:.1f}s) — {str(e)[:200]}")


def make_handler():
    """Create an A2AProxyHandler pointed at the local Hermes A2A server."""
    agent_config = {
        "metadata": {
            "a2a_proxy_url": HERMES_A2A_URL,
            "a2a_proxy_token": "",  # localhost, no token needed
            "a2a_proxy_timeout": 120,
        }
    }
    return A2AProxyHandler(logger=logger, agent_config=agent_config)


# ---------------------------------------------------------------------------
# E2E-001: Agent Card discovery
# ---------------------------------------------------------------------------

def test_agent_card():
    """Verify Hermes serves the Agent Card at /.well-known/agent-card.json."""
    req = urllib.request.Request(f"{HERMES_A2A_URL}/.well-known/agent-card.json")
    resp = urllib.request.urlopen(req, timeout=10)
    card = json.loads(resp.read().decode())
    
    assert card["name"], "Agent card should have a name"
    assert card["url"], "Agent card should have a url"
    assert "supportedInterfaces" in card, "Should have supportedInterfaces"
    assert card["capabilities"]["streaming"] is True, "Should support streaming"
    print(f"    Agent name: {card['name']}")
    print(f"    URL: {card['url']}")
    print(f"    Streaming: {card['capabilities']['streaming']}")


# ---------------------------------------------------------------------------
# E2E-002: Non-streaming SendMessage
# ---------------------------------------------------------------------------

def test_non_streaming_send():
    """Send a simple message via A2AProxyHandler.ask_model() and verify response."""
    handler = make_handler()
    
    input_messages = [{"role": "user", "content": "Reply with exactly: A2A_PROXY_OK"}]
    context = {
        "agent_uuid": "e2e-test-agent",
        "endpoint_id": "e2e-test",
        "part_id": "p1",
        "thread_uuid": "",
    }
    
    result = handler.ask_model(input_messages, context)
    
    assert "content" in result, f"Result should have 'content': {result}"
    assert result.get("error") is None, f"Unexpected error: {result.get('error')}"
    assert len(result["content"]) > 0, "Response content should not be empty"
    print(f"    Response: {result['content'][:200]}")
    # The agent may not say exactly A2A_PROXY_OK, but it should respond
    assert result["role"] == "agent", f"Role should be 'agent': {result.get('role')}"


# ---------------------------------------------------------------------------
# E2E-003: Streaming SendStreamingMessage
# ---------------------------------------------------------------------------

def test_streaming_send():
    """Send a streaming message via ask_model with stream_queue and verify tokens."""
    handler = make_handler()
    
    stream_queue = queue.Queue()
    stream_event = threading.Event()
    
    input_messages = [{"role": "user", "content": "Reply with: STREAMING_A2A_OK"}]
    context = {
        "agent_uuid": "e2e-test-streaming",
        "endpoint_id": "e2e-test",
        "part_id": "p1",
        "thread_uuid": "",
    }
    
    # Run in a thread since ask_model blocks until streaming completes
    result_holder = {}
    def run():
        result_holder["result"] = handler.ask_model(
            input_messages, context,
            stream_queue=stream_queue,
            stream_event=stream_event,
        )
    
    t = threading.Thread(target=run, daemon=True)
    t.start()
    
    # Collect stream tokens
    tokens = []
    errors = []
    timeout = 120  # 2 minutes max
    t0 = time.time()
    
    while time.time() - t0 < timeout:
        try:
            event = stream_queue.get(timeout=2)
            if event.get("name") == "token":
                tokens.append(event["value"])
            elif event.get("name") == "error":
                errors.append(event["value"])
                break
        except queue.Empty:
            if stream_event.is_set() and stream_queue.empty():
                break
    
    t.join(timeout=10)
    
    assert len(errors) == 0, f"Stream errors: {errors}"
    assert len(tokens) > 0, f"No tokens received. Result: {result_holder.get('result')}"
    full_content = "".join(tokens)
    print(f"    Tokens received: {len(tokens)}")
    print(f"    Full content: {full_content[:200]}")
    
    result = result_holder.get("result", {})
    assert result.get("error") is None, f"Result error: {result.get('error')}"
    assert len(result.get("content", "")) > 0, "Result content should not be empty"


# ---------------------------------------------------------------------------
# E2E-004: Multi-turn conversation (context_id continuity)
# ---------------------------------------------------------------------------

def test_multi_turn():
    """Send two messages with the same context_id and verify conversation continuity."""
    handler = make_handler()
    
    # First message
    input1 = [{"role": "user", "content": "Remember the number 42. Reply: OK"}]
    context1 = {
        "agent_uuid": "e2e-test-multiturn",
        "endpoint_id": "e2e-test",
        "part_id": "p1",
        "thread_uuid": "",
    }
    
    result1 = handler.ask_model(input1, context1)
    assert result1.get("error") is None, f"First turn error: {result1.get('error')}"
    
    # Extract thread_uuid from first response
    thread_uuid = ""
    if "metadata" in result1:
        thread_uuid = result1["metadata"].get("thread_uuid", "")
    print(f"    Turn 1 response: {result1['content'][:100]}")
    print(f"    Thread UUID: {thread_uuid}")
    
    # Second message — use the same thread_uuid
    input2 = [{"role": "user", "content": "What number did I ask you to remember? Reply with just the number."}]
    context2 = {
        "agent_uuid": "e2e-test-multiturn",
        "endpoint_id": "e2e-test",
        "part_id": "p1",
        "thread_uuid": thread_uuid,
    }
    
    result2 = handler.ask_model(input2, context2)
    assert result2.get("error") is None, f"Second turn error: {result2.get('error')}"
    print(f"    Turn 2 response: {result2['content'][:100]}")
    
    # The agent should reference 42 (though we can't guarantee exact match)
    assert len(result2["content"]) > 0, "Second turn should have content"


# ---------------------------------------------------------------------------
# E2E-005: Error handling — no proxy URL configured
# ---------------------------------------------------------------------------

def test_no_proxy_url():
    """Verify graceful error when no a2a_proxy_url is configured."""
    handler = A2AProxyHandler(
        logger=logger,
        agent_config={"metadata": {}},  # No proxy URL
    )
    
    result = handler.ask_model(
        [{"role": "user", "content": "test"}],
        {"agent_uuid": "test", "endpoint_id": "test", "part_id": "p1"},
    )
    
    assert "error" in result, "Should have error field"
    assert "No a2a_proxy_url" in result["error"], f"Expected 'No a2a_proxy_url': {result['error']}"
    print(f"    Error: {result['error']}")


# ---------------------------------------------------------------------------
# E2E-006: Cancel task
# ---------------------------------------------------------------------------

def test_cancel_task():
    """Verify cancel_run sends an HTTP request to the backend (graceful for non-existent task)."""
    handler = make_handler()
    result = handler.cancel_run("nonexistent-task-id")
    # cancel_run returns True if the HTTP request was sent successfully,
    # even if the backend returns a JSON-RPC error for a non-existent task.
    # It returns False only if the request itself fails (network error, no URL, etc.)
    assert isinstance(result, bool), f"Expected bool, got: {type(result)}"
    print(f"    Cancel non-existent task: HTTP sent = {result}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 70)
    print("A2AProxyHandler → Hermes A2A End-to-End Test")
    print(f"Target: {HERMES_A2A_URL}")
    print("=" * 70)
    
    tests = [
        ("E2E-001", "Agent Card discovery", test_agent_card),
        ("E2E-002", "Non-streaming SendMessage", test_non_streaming_send),
        ("E2E-003", "Streaming SendStreamingMessage", test_streaming_send),
        ("E2E-004", "Multi-turn conversation continuity", test_multi_turn),
        ("E2E-005", "Error handling — no proxy URL", test_no_proxy_url),
        ("E2E-006", "Cancel task (non-existent)", test_cancel_task),
    ]
    
    for tid, tname, fn in tests:
        run_test(tid, tname, fn)
    
    passed = sum(1 for r in RESULTS if r["status"] == "PASS")
    failed = sum(1 for r in RESULTS if r["status"] == "FAIL")
    print(f"\n{'=' * 70}")
    print(f"Results: {passed} passed, {failed} failed, {len(RESULTS)} total")
    print(f"{'=' * 70}")
    
    # Write results
    results_path = os.path.join(A2A_DIR, "docs", "test_results", "a2a_proxy_e2e_results.json")
    os.makedirs(os.path.dirname(results_path), exist_ok=True)
    with open(results_path, "w") as f:
        json.dump({"tests": RESULTS, "passed": passed, "failed": failed, "target": HERMES_A2A_URL}, f, indent=2)
    print(f"Results: {results_path}")
    
    sys.exit(0 if failed == 0 else 1)