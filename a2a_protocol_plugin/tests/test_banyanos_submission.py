"""Atomic public reservation, replay, ownership and cancellation contracts."""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from types import SimpleNamespace

import pytest
from a2a.types import SendMessageRequest
from a2a.utils.errors import InvalidParamsError

from a2a_protocol_plugin.handlers import banyanos_submission as sub
from a2a_protocol_plugin.handlers.a2a_executor import _agent_error_task_event
from a2a_protocol_plugin.handlers.a2a_jsonrpc_bridge import build_jsonrpc_sdk_request


class Repo:
    def __init__(self):
        self.rows = {}
        self.lock = Lock()

    def claim(self, info, **values):
        key = values["partition_key"], values["setting_id"]
        with self.lock:
            if key in self.rows:
                return False
            self.rows[key] = values
            return True

    def get(self, **keys):
        return self.rows.get((keys["partition_key"], keys.get("setting_id") or keys.get("task_id")))

    def insert_update(self, info, **values):
        with self.lock:
            self.rows[values["partition_key"], values["task_id"]] = values
        return values


@pytest.fixture
def env(monkeypatch):
    settings, tasks = Repo(), Repo()
    monkeypatch.setattr(sub, "get_repo", lambda name: settings if name == "a2a_setting" else tasks)
    monkeypatch.setattr(sub, "_set_rls", lambda _: None)

    async def config(*args, **kwargs):
        return {"class_name": "BanyanosAgentHandler"}

    monkeypatch.setattr(sub, "resolve_agent", config)

    def read(task_id, context):
        row = tasks.get(partition_key=context.state["partition_key"], task_id=task_id)
        if row:
            state = {"completed": "COMPLETED", "cancelled": "CANCELED", "failed": "FAILED"}.get(row["status"], "WORKING")
            return _agent_error_task_event(task_id, row["context_id"], state)

    monkeypatch.setattr(sub, "_read_task", read)
    calls = []

    async def send(request, context):
        calls.append(request.message.task_id)
        await asyncio.sleep(0.02)
        return read(request.message.task_id, context)

    return settings, tasks, calls, SimpleNamespace(on_message_send=send)


def context(partition="banyan#tenant-a", actor="user-a"):
    return SimpleNamespace(state={"partition_key": partition, "agentId": "published", "_gateway_actor_id": actor})


def request(message_id="same", text="hello", context_id="conversation"):
    return build_jsonrpc_sdk_request(SendMessageRequest, {"params": {"message": {
        "messageId": message_id, "contextId": context_id, "role": "user", "parts": [{"text": text}],
    }}})


def send(env, req=None, ctx=None):
    return sub.send_once(req or request(), ctx or context(), env[3], logging.getLogger(__name__))


def test_concurrent_duplicates_share_one_public_task_and_dispatch(env):
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: send(env), range(2)))
    assert len(env[2]) == 1
    assert results[0].id == results[1].id
    assert len(env[0].rows) == len(env[1].rows) == 1


def test_terminal_replay_does_not_dispatch(env):
    first = send(env)
    env[1].rows["banyan#tenant-a", first.id]["status"] = "completed"
    replay = send(env)
    assert first.id == replay.id
    assert len(env[2]) == 1


def test_new_message_in_same_conversation_dispatches_again(env):
    first = send(env)
    second = send(env, request("next"))
    assert first.id != second.id
    assert len(env[2]) == 2


@pytest.mark.parametrize("changed", [{"text": "different"}, {"context_id": "other"}])
def test_same_message_id_with_changed_input_is_rejected(env, changed):
    send(env)
    with pytest.raises(InvalidParamsError):
        send(env, request(**changed))
    assert len(env[2]) == 1


def test_other_actor_cannot_replay_reserved_task(env):
    send(env)
    with pytest.raises(InvalidParamsError, match="owned"):
        send(env, ctx=context(actor="other"))


def test_other_tenant_gets_a_distinct_reservation(env):
    first = send(env)
    second = send(env, ctx=context(partition="banyan#tenant-b"))
    assert first.id != second.id


def test_storage_failure_never_dispatches(env, monkeypatch):
    def unavailable(*args, **kwargs):
        raise RuntimeError("storage unavailable")
    monkeypatch.setattr(env[0], "claim", unavailable)
    with pytest.raises(RuntimeError, match="storage unavailable"):
        send(env)
    assert not env[2]


def test_expired_abandoned_reservation_becomes_failed(env, monkeypatch):
    first = send(env)
    record = next(iter(env[0].rows.values()))["setting"]
    monkeypatch.setattr(sub.time, "time", lambda: record["deadline"] + 1)
    sub.observe(first.id, context(), logging.getLogger(__name__))
    assert env[1].rows["banyan#tenant-a", first.id]["status"] == "failed"
    assert len(env[2]) == 1
