# -*- coding: utf-8 -*-
"""tasks/get must surface a message_response task's stored reply (output_data)."""

import logging

import pytest

from a2a_protocol_plugin.handlers.a2a_taskstore import DynamoDBA2ATaskStore


def _store() -> DynamoDBA2ATaskStore:
    store = DynamoDBA2ATaskStore.__new__(DynamoDBA2ATaskStore)
    store.logger = logging.getLogger(__name__)
    return store


@pytest.mark.unit
def test_completed_task_carries_output_as_status_message():
    task = _store()._dict_to_task({
        "taskId": "t1",
        "status": "COMPLETED",
        "task_type": "message_response",
        "output_data": {"content": "late answer"},
    })
    assert task.status.message.parts[0].text == "late answer"


@pytest.mark.unit
def test_task_without_output_has_no_status_message():
    task = _store()._dict_to_task({"taskId": "t2", "status": "WORKING",
                                   "output_data": None})
    assert not task.status.HasField("message")
