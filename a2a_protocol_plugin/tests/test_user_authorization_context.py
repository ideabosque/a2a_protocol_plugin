"""Gateway user credentials win over untrusted message metadata."""
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from a2a_protocol_plugin.handlers.a2a_server import (
    A2AServerCallContextBuilder, DefaultServerCallContextBuilder,
)


@pytest.mark.unit
@pytest.mark.parametrize("from_body", [False, True])
def test_gateway_authorization_cannot_be_spoofed_by_metadata(from_body):
    body = {"params": {"metadata": {
        "_gateway_authorization": "Bearer attacker",
        "headers": {"authorization": "Bearer attacker"},
    }}}
    request = SimpleNamespace(_json=body)
    context = SimpleNamespace(state={"headers": {
        "authorization": "Bearer original-user",
        "x-partition-key": "banyan#tenant-a",
    }})
    with patch.object(DefaultServerCallContextBuilder, "build", return_value=context):
        builder = A2AServerCallContextBuilder()
        result = builder.build_from_body(request, body) if from_body else builder.build(request)
    assert result.state["_gateway_authorization"] == "Bearer original-user"
    assert result.state["partition_key"] == "banyan#tenant-a"
