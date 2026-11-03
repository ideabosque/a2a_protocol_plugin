"""Public task IDs must retain the context required by SDK resume validation."""
import asyncio
from types import SimpleNamespace

from a2a_protocol_plugin.handlers import a2a_utility


def test_task_read_selects_and_preserves_context_id(monkeypatch):
    queries = []

    def query(**kwargs):
        queries.append(kwargs["query"])
        return {"data": {"a2aTask": {"taskId": "task", "contextId": "conversation",
                "status": "IN_PROGRESS", "inputData": {}, "outputData": {"backend_task_id": "backend"}}}}

    monkeypatch.setattr(a2a_utility.Config, "a2a_core", SimpleNamespace(a2a_core_graphql=query))
    row = asyncio.run(a2a_utility.get_a2a_task("banyan#tenant", "task"))
    assert "contextId" in queries[0]
    assert row["context_id"] == "conversation"
    assert row["output_data"]["backend_task_id"] == "backend"


def test_signed_tenant_overrides_forged_partition():
    from a2a_protocol_plugin.main import apply_partition_defaults
    params = {"endpoint_id": "banyan", "part_id": "foreign", "partition_key": "banyan#foreign",
              "context": {"user": {"tenant_id": "own"}, "partition_key": "banyan#foreign"}}
    apply_partition_defaults(params)
    assert params["partition_key"] == params["context"]["partition_key"] == "banyan#own"


def test_platform_identity_can_select_partition():
    from a2a_protocol_plugin.main import apply_partition_defaults
    params = {"endpoint_id": "banyan", "part_id": "selected",
              "context": {"user": {"tenant_id": None}}}
    apply_partition_defaults(params)
    assert params["partition_key"] == "banyan#selected"


def test_backend_status_query_validates_against_real_agent_schema():
    import json
    import httpx
    import pytest
    pytest.importorskip("agent_engine")
    from agent_engine.main import AgentEngine
    from graphql import parse, validate
    from a2a_protocol_plugin.handlers.banyanos_client import BanyanosClient
    import logging

    def respond(request):
        query = json.loads(request.content)["query"]
        assert not validate(AgentEngine.build_graphql_schema().graphql_schema, parse(query))
        return httpx.Response(200, json={"data": {"taskStatus": {"id": "task", "status": "RUNNING"}}})

    client = BanyanosClient(logger=logging.getLogger(__name__), graphql_url="http://local.test",
                           ws_url="ws://local.test", endpoint_id="banyan", http_transport=httpx.MockTransport(respond))
    assert client.task_status("task")["status"] == "RUNNING"
