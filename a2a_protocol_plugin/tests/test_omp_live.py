#!/usr/bin/python
"""
omp Agent Handler — Live Integration Tests (against a running omp sidecar)

Gated: skipped unless A2A_RUN_LIVE_OMP_TESTS=1.

Environment:
    A2A_RUN_LIVE_OMP_TESTS=1  gate (required, tests are skipped otherwise)
    OMP_LIVE_URL              (default http://localhost:3310) sidecar base URL
    OMP_LIVE_KEY              (default empty — the sidecar requires the shared
                             secret unless started without OMP_SIDECAR_SECRET_KEY)
    OMP_LIVE_MODEL            optional provider/modelId to pin the model
    OMP_LIVE_CWD              optional working dir (default /workspace)

Usage:
    python -m pytest a2a_protocol_plugin/tests/test_omp_live.py -v -s
    A2A_RUN_LIVE_OMP_TESTS=1 OMP_LIVE_KEY=<secret> python -m pytest ...

Author: bibow
"""

import logging
import os
import queue
import threading
import time
import uuid

import pytest

from a2a_protocol_plugin.handlers.config import Config
from a2a_protocol_plugin.handlers.omp_handler import OmpAgentHandler
from a2a_protocol_plugin.handlers.pi_client import PiSidecarError

__author__ = "bibow"

OMP_LIVE_URL = os.getenv("OMP_LIVE_URL", "http://localhost:3310")
OMP_LIVE_KEY = os.getenv("OMP_LIVE_KEY", "")
OMP_LIVE_MODEL = os.getenv("OMP_LIVE_MODEL", "")
OMP_LIVE_CWD = os.getenv("OMP_LIVE_CWD", "/workspace")

pytestmark = pytest.mark.skipif(
    os.getenv("A2A_RUN_LIVE_OMP_TESTS") != "1",
    reason="Set A2A_RUN_LIVE_OMP_TESTS=1 to run live omp integration tests",
)


@pytest.fixture(scope="module", autouse=True)
def live_persistence():
    """Use the configured test database for conversation mapping, never AWS defaults."""
    if os.getenv("OMP_LIVE_PG_HOST"):
        Config.DB_BACKEND = "postgresql"
        Config._initialize_db_session({
            "db_host": os.environ["OMP_LIVE_PG_HOST"],
            "db_port": os.getenv("OMP_LIVE_PG_PORT", "5432"),
            "db_user": os.environ["OMP_LIVE_PG_USER"],
            "db_password": os.environ["OMP_LIVE_PG_PASSWORD"],
            "db_schema": os.environ["OMP_LIVE_PG_DB"],
        })
        from a2a_protocol_plugin.models.repositories import register_a2a_repos
        register_a2a_repos()
    yield
    if Config.DB_BACKEND == "postgresql" and Config.db_session is not None:
        Config.db_session.remove()


@pytest.fixture(autouse=True)
def release_live_db_session():
    yield
    if Config.DB_BACKEND == "postgresql" and Config.db_session is not None:
        Config.db_session.remove()


def _handler() -> OmpAgentHandler:
    setting = {
        "OMP_API_URL": OMP_LIVE_URL,
        "OMP_SECRET_KEY": OMP_LIVE_KEY,
        "OMP_CWD": OMP_LIVE_CWD,
    }
    if OMP_LIVE_MODEL:
        setting["OMP_MODEL"] = OMP_LIVE_MODEL
    return OmpAgentHandler(
        logger=logging.getLogger("omp-live"),
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
    client = handler._new_client()
    session = None
    try:
        session = client.create_session(cwd=OMP_LIVE_CWD,
                                        model=OMP_LIVE_MODEL or None)
        assert session.get("sessionId")
    finally:
        if session:
            client.close_session(session["sessionId"])
        client.close()


def test_first_turn_round_trip():
    handler = _handler()
    result = handler.ask_model(
        [{"role": "user", "content": "Say hello and nothing else."}],
        {"partition_key": "live#default", "context_id": "omp-live-1"},
    )
    assert not result.get("error"), result.get("error")
    assert result.get("content")
    assert result["metadata"]["thread_uuid"]  # omp sessionId


def test_context_echo_resumes_same_session():
    if not os.getenv("OMP_LIVE_PG_HOST") and Config.db_session is None:
        pytest.skip("Configure OMP_LIVE_PG_* for durable conversation mapping")
    handler = _handler()
    first = handler.ask_model(
        [{"role": "user", "content": "Remember the token word: BANANA42."}],
        {"partition_key": "live#default", "context_id": "omp-live-echo"},
    )
    first_sid = first["metadata"]["thread_uuid"]

    second = handler.ask_model(
        [{"role": "user", "content": "What was the token word?"}],
        {"partition_key": "live#default", "context_id": "omp-live-echo"},
    )
    # Echoing the conversation id resumes the SAME omp session.
    assert second["metadata"]["thread_uuid"] == first_sid
    assert not first.get("error"), first.get("error")
    assert not second.get("error"), second.get("error")
    assert "BANANA42" in (second.get("content") or "")


def test_streaming_round_trip_with_run_id():
    handler = _handler()
    q = queue.Queue()
    ev = threading.Event()
    result = handler.ask_model(
        [{"role": "user", "content": "Count from 1 to 5 slowly."}],
        {"partition_key": "live#default", "context_id": "omp-live-stream"},
        stream_queue=q,
        stream_event=ev,
    )
    chunks = _chunks(q)
    names = [c["name"] for c in chunks]
    assert names and names[0] == "run_id"
    assert "token" in names or result.get("content")
    assert not any(c.get("meta", {}).get("data_format") == "reasoning"
                   and c["value"] in (result.get("content") or "") for c in chunks)


def test_prompt_validation_error_is_reported():
    handler = _handler()
    client = handler._new_client()
    sid = None
    try:
        sid = client.create_session(cwd=OMP_LIVE_CWD,
                                    model=OMP_LIVE_MODEL or None)["sessionId"]
        with pytest.raises(PiSidecarError) as error:
            client.stream_prompt(sid, "")
        assert error.value.status_code == 400
    finally:
        if sid:
            client.close_session(sid)
        client.close()


def test_busy_follow_up_preserves_original_run():
    """A second handler must not DELETE the session owned by the first stream."""
    handler = _handler()
    client = handler._new_client()
    sid = client.create_session(cwd=OMP_LIVE_CWD,
                                model=OMP_LIVE_MODEL or None)["sessionId"]
    started = threading.Event()
    outcome = {}
    marker = f"OMP_QUEUED_{uuid.uuid4().hex[:8]}"

    def original_run():
        try:
            outcome["result"] = client.stream_prompt(
                sid, "Write a detailed 1000-word history of computing, with "
                     "numbered sections. Do not use tools.",
                on_event=lambda event: started.set()
                if event.get("type") == "agent_start" else None,
                timeout=120,
            )
        except Exception as error:
            outcome["error"] = str(error)

    thread = threading.Thread(target=original_run, daemon=True)
    thread.start()
    try:
        assert started.wait(30), "Original run did not start"
        handler._resolve_session_id = lambda client, context: sid
        queued = handler.ask_model(
            [{"role": "user", "content":
              f"After completing the previous answer, say exactly {marker}."}],
            {"partition_key": "live#default",
             "context_id": f"queue-{uuid.uuid4().hex}"},
        )
        assert not queued.get("error"), queued.get("error")
        assert queued["metadata"]["omp"]["stopReason"] == "queued"
        assert thread.is_alive(), "Queue acknowledgement terminated the original run"
        thread.join(120)
        assert not thread.is_alive(), "Original run did not finish"
        assert "error" not in outcome, outcome.get("error")
        run_end, events = outcome["result"]
        assert not run_end.get("error"), run_end
        assert run_end.get("stopReason") == "end_turn"
        answer = "".join((event.get("assistantMessageEvent") or {}).get("delta", "")
                         for event in events if event.get("type") == "message_update"
                         and (event.get("assistantMessageEvent") or {}).get("type")
                         == "text_delta")
        assert marker in answer, "Queued answer never reached the original stream"
    finally:
        if thread.is_alive():
            client.abort(sid)
            thread.join(15)
        client.close_session(sid)
        client.close()
