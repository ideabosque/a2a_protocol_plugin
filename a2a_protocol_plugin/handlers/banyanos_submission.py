"""DEF-B003 (2026-11-02): reserve public task IDs before backend dispatch.

The ledger uses existing partition-scoped settings, with an atomic insert in
both repositories. Request credentials are never put into this ledger.
"""
from __future__ import annotations

import hashlib
import json
import time
from types import SimpleNamespace

from silvaengine_daemon.async_utils import run_async
from silvaengine_daemon.repositories import get_repo

from .a2a_ai_agent_utility import resolve_agent
from .a2a_executor import _agent_error_task_event
from .a2a_jsonrpc_bridge import sdk_response_to_dict
from .config import Config
from .conversation_map import _set_rls

PREFIX = "a2a-submit-"


def _repo():
    return get_repo("a2a_setting")


def _info(partition):
    return SimpleNamespace(context={"partition_key": partition, "logger": Config.logger})


def _read_task(task_id, context):
    return run_async(Config.a2a_server.task_store.get(task_id, context))


def _write_task(partition, task_id, context_id, agent_id, status, output=None):
    endpoint, _, part = partition.partition("#")
    _set_rls(partition)
    get_repo("a2a_task").insert_update(
        _info(partition), partition_key=partition, task_id=task_id,
        endpoint_id=endpoint, part_id=part, task_type="message_response",
        assigned_agent_id=agent_id, status=status, context_id=context_id,
        output_data=output, updated_by="banyanos_submission",
    )


def _ledger(task_id, context):
    if not task_id.startswith(PREFIX):
        return None
    partition = context.state["partition_key"]
    _set_rls(partition)
    row = _repo().get(partition_key=partition, setting_id="a2a_submit#" + task_id[len(PREFIX):])
    record = row.get("setting") if row else None
    if record and record.get("actor_id") != context.state.get("_gateway_actor_id", ""):
        from a2a.utils.errors import InvalidParamsError
        raise InvalidParamsError(message="Task is not owned by this caller")
    return record


def send_once(request, context, handler, logger):
    """Return None for other backends; Banyanos sends return a stable Task."""
    state = context.state
    partition = state["partition_key"]
    agent_id = state.get("agent_uuid") or state.get("agentId") or state.get("agent_id")
    if not agent_id:
        agent_id = dict(getattr(request.message, "metadata", {}) or {}).get("agentId")
    config = run_async(resolve_agent(partition, agent_id, logger=logger))
    if not config or config.get("class_name") != "BanyanosAgentHandler":
        return None
    from a2a.utils.errors import InvalidParamsError
    message = request.message
    message_id = message.message_id
    if not message_id:
        raise InvalidParamsError(message="messageId is required")
    digest = hashlib.sha256(f"{partition}\x1f{agent_id}\x1f{message_id}".encode()).hexdigest()
    task_id = PREFIX + digest
    context_id = message.context_id or "ctx-" + digest
    content_hash = hashlib.sha256(json.dumps(
        sdk_response_to_dict(message).get("parts", []), sort_keys=True,
    ).encode()).hexdigest()
    record = {"task_id": task_id, "context_id": context_id, "agent_id": agent_id,
              "actor_id": state.get("_gateway_actor_id", ""), "content_hash": content_hash,
              "deadline": time.time() + 600}
    endpoint, _, part = partition.partition("#")
    _set_rls(partition)
    reserved = _repo().claim(
        _info(partition), partition_key=partition, setting_id="a2a_submit#" + digest,
        endpoint_id=endpoint, part_id=part, setting=record, updated_by="banyanos_submission",
    )
    if not reserved:
        existing = _ledger(task_id, context)
        if not existing or existing["content_hash"] != content_hash:
            raise InvalidParamsError(message="messageId was reused with different input")
        if message.context_id and message.context_id != existing["context_id"]:
            raise InvalidParamsError(message="messageId was reused in another context")
        task = observe(task_id, context, logger)
        return task or _agent_error_task_event(task_id, existing["context_id"], "SUBMITTED")
    _write_task(partition, task_id, context_id, agent_id, "in_progress")
    # The SDK uses the durable ID for every persistence/event path.
    message.task_id = task_id
    message.context_id = context_id
    state["agent_uuid"] = agent_id
    try:
        return run_async(handler.on_message_send(request, context))
    except Exception:
        _write_task(partition, task_id, context_id, agent_id, "failed",
                    {"content": "", "error_message": "Backend dispatch failed; inspect task before retry"})
        raise


def observe(task_id, context, logger, cancel=False):
    record = _ledger(task_id, context)
    if not record:
        return None
    partition = context.state["partition_key"]
    _set_rls(partition)
    row = get_repo("a2a_task").get(partition_key=partition, task_id=task_id)
    if row and str(row.get("status", "")).lower() in ("completed", "failed", "cancelled", "canceled"):
        return _read_task(task_id, context)
    from .banyanos_execution_map import _safe
    ref_row = _repo().get(partition_key=partition,
                          setting_id="banyanos_reconcile#" + _safe(task_id))
    ref = (ref_row or {}).get("setting") or {}
    backend_id = ref.get("backend_task_id") or ((row or {}).get("output_data") or {}).get("backend_task_id")
    if backend_id:
        from .banyanos_identity import resolve_banyanos_identity
        from .banyanos_client import BanyanosClient
        config = run_async(resolve_agent(partition, record["agent_id"], logger=logger))
        identity = resolve_banyanos_identity(config, {}, {
            "partition_key": partition,
            "_gateway_authorization": context.state.get("_gateway_authorization"),
        }, logger=logger)
        client = BanyanosClient(logger=logger, graphql_url=identity.graphql_url,
                               ws_url=identity.ws_url, endpoint_id=identity.endpoint_id,
                               token=identity.token, part_id=identity.part_id)
        backend = client.task_status(backend_id)
        status = str(backend.get("status", "")).upper()
        if (cancel or record["deadline"] < time.time()) and status in ("PENDING", "RUNNING"):
            backend = client.cancel_task(backend_id)
            status = str(backend.get("status", "")).upper()
        mapping = {"SUCCESS": "completed", "COMPLETED": "completed", "FAILED": "failed",
                   "CANCELLED": "cancelled", "CANCELED": "cancelled", "TIMEOUT": "failed"}
        if status in mapping:
            output = backend.get("outputPayload") or {}
            if isinstance(output, str):
                output = json.loads(output)
            _write_task(partition, task_id, record["context_id"], record["agent_id"], mapping[status],
                        {"content": output.get("content", ""), "error_message": backend.get("errorMessage", ""),
                         "backend_task_id": backend_id})
            return _read_task(task_id, context)
    elif cancel:
        from a2a.utils.errors import InvalidParamsError
        raise InvalidParamsError(message="Task acceptance is pending; observe before cancellation")
    if record["deadline"] < time.time():
        _write_task(partition, task_id, record["context_id"], record["agent_id"], "failed",
                    {"content": "", "error_message": "Task observation deadline exceeded; no resubmission performed"})
    return _read_task(task_id, context)
