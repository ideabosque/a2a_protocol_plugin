"""A2A scheduler: cron parsing, due logic, claim-before-run and run recording."""
import json
from datetime import datetime, timezone

import pytest

from a2a_protocol_plugin.handlers.a2a_cron import CronError, parse_cron
from a2a_protocol_plugin.handlers.a2a_scheduler import (
    A2AScheduler,
    due_reason,
    next_run_iso,
    read_send_result,
    scheduled_prompt,
    validate_schedule,
)

pytestmark = pytest.mark.unit


def schedule(**overrides):
    base = {
        "kind": "schedule",
        "name": "Inbox to requests",
        "prompt": "Check the QA/QC inbox for replies to open requests.",
        "agent_uuid": "a2a-goose-scheduler",
        "cron": "0 8 * * 1-5",
        "timezone": "America/Los_Angeles",
        "enabled": True,
        "runtime": {},
    }
    base.update(overrides)
    return base


# -- cron -------------------------------------------------------------------


def test_cron_matches_weekday_morning():
    expr = parse_cron("0 8 * * 1-5")
    assert expr.matches(datetime(2026, 10, 16, 8, 0))  # Friday
    assert not expr.matches(datetime(2026, 10, 17, 8, 0))  # Saturday
    assert not expr.matches(datetime(2026, 10, 16, 8, 1))


def test_cron_steps_lists_and_sunday_as_7():
    assert parse_cron("*/15 * * * *").minutes == frozenset({0, 15, 30, 45})
    assert parse_cron("0 9,17 * * 7").weekdays == frozenset({0})


def test_cron_day_or_weekday_when_both_restricted():
    expr = parse_cron("0 0 1 * 1")  # the 1st, or any Monday
    assert expr.matches(datetime(2026, 10, 1, 0, 0))  # Thursday the 1st
    assert expr.matches(datetime(2026, 10, 12, 0, 0))  # a Monday


@pytest.mark.parametrize("bad", ["", "* * * *", "60 * * * *", "* * * * 8", "*/0 * * * *", "a * * * *"])
def test_cron_rejects_bad_expressions(bad):
    with pytest.raises(CronError):
        parse_cron(bad)


def test_cron_next_after():
    nxt = parse_cron("30 8 * * 1-5").next_after(datetime(2026, 10, 16, 9, 0))
    assert nxt == datetime(2026, 10, 19, 8, 30)  # Friday 09:00 -> Monday 08:30


# -- validation and due logic -------------------------------------------------


def test_validate_reports_the_problem():
    assert validate_schedule(schedule()) is None
    assert "prompt" in validate_schedule(schedule(prompt="  "))
    assert "cron" in validate_schedule(schedule(cron="61 * * * *"))
    assert "timezone" in validate_schedule(schedule(timezone="Mars/Base"))


def test_due_in_the_schedule_timezone_once_per_minute():
    now = datetime(2026, 10, 16, 15, 0, 20, tzinfo=timezone.utc)  # 08:00 in Los Angeles (PDT)
    assert due_reason(schedule(), now) == "cron"
    fired = schedule(runtime={"last_fired_minute": "2026-10-16T08:00:00-07:00"})
    assert due_reason(fired, now) is None
    assert due_reason(schedule(enabled=False), now) is None


def test_run_now_fires_even_when_paused():
    now = datetime(2026, 10, 16, 3, 0, tzinfo=timezone.utc)
    assert due_reason(schedule(enabled=False, run_now=True), now) == "run_now"


def test_next_run_is_none_when_paused():
    now = datetime(2026, 10, 16, 16, 0, tzinfo=timezone.utc)
    assert next_run_iso(schedule(), now) == "2026-10-19T08:00:00-07:00"
    assert next_run_iso(schedule(enabled=False), now) is None


def test_prompt_header_states_unattended_and_no_decisions():
    text = scheduled_prompt(schedule(), "inbox", "cron", datetime(2026, 10, 16, 15, 0, tzinfo=timezone.utc))
    assert text.startswith("[Scheduled run · Inbox to requests (inbox)")
    assert "never approve, resolve or decide a request" in text
    assert text.endswith("Check the QA/QC inbox for replies to open requests.")


def test_read_send_result_task_and_errors():
    ok = {"result": {"task": {"status": {"state": "TASK_STATE_COMPLETED"},
                              "artifacts": [{"parts": [{"text": "2 replies relayed"}]},
                                            {"metadata": {"data_format": "reasoning"}, "parts": [{"text": "thinking"}]}]}}}
    assert read_send_result(json.dumps(ok)) == ("completed", "2 replies relayed")
    assert read_send_result({"error": {"message": "Agent not found"}}) == ("failed", "Agent not found")


# -- the dispatcher --------------------------------------------------------------


class FakeStore:
    def __init__(self, rows):
        self.rows = dict(rows)
        self.saves = []

    def graphql(self, query, variables, endpoint_id, part_id):
        if "a2aSetting(settingId" in query:
            row = self.rows.get(variables["sid"])
            return {"a2aSetting": {"settingId": variables["sid"], "setting": row} if row else None}
        if "insertUpdateA2aSetting" in query:
            self.rows[variables["sid"]] = json.loads(json.dumps(variables["setting"]))
            self.saves.append((variables["sid"], variables["setting"]))
            return {}
        return {"a2aSettingList": {"a2aSettingList": [
            {"settingId": sid, "setting": setting} for sid, setting in self.rows.items()
        ] + [{"settingId": "goose_session#abc", "setting": {"x": 1}}]}}


def make(store, sent, now, state=("completed", "done")):
    def send(run, endpoint_id, part_id, task_id):
        sent.append(run)
        return state
    return A2AScheduler(store.graphql, send, [("a2a", "default")], clock=lambda: now)


def test_tick_claims_then_records_the_run():
    now = datetime(2026, 10, 16, 15, 0, 5, tzinfo=timezone.utc)
    store, sent = FakeStore({"schedule#inbox": schedule()}), []
    scheduler = make(store, sent, now)

    assert scheduler.tick() == 1
    scheduler._pool.shutdown(wait=True)

    claim_sid, claim = store.saves[0]
    assert claim_sid == "schedule#inbox" and claim["runtime"]["last_status"] == "running"
    assert sent[0]["agent_uuid"] == "a2a-goose-scheduler"
    assert sent[0]["task_id"].startswith("sched-inbox-")
    final = store.rows["schedule#inbox"]["runtime"]
    assert final["last_status"] == "completed" and final["last_result"] == "done"
    assert final["history"][0]["trigger"] == "cron"
    assert final["next_run_at"] == "2026-10-19T08:00:00-07:00"


def test_same_minute_does_not_fire_twice():
    now = datetime(2026, 10, 16, 15, 0, 5, tzinfo=timezone.utc)
    store, sent = FakeStore({"schedule#inbox": schedule()}), []
    scheduler = make(store, sent, now)
    scheduler.tick()
    scheduler._pool.shutdown(wait=True)
    scheduler._pool = type(scheduler._pool)(max_workers=1)
    assert scheduler.tick() == 0
    assert len(sent) == 1


def test_run_now_is_cleared_and_invalid_schedules_are_marked():
    now = datetime(2026, 10, 16, 3, 0, tzinfo=timezone.utc)
    store, sent = FakeStore({
        "schedule#now": schedule(enabled=False, run_now=True),
        "schedule#bad": schedule(cron="99 * * * *"),
    }), []
    scheduler = make(store, sent, now)
    assert scheduler.tick() == 1
    scheduler._pool.shutdown(wait=True)
    assert store.rows["schedule#now"]["run_now"] is False
    assert store.rows["schedule#now"]["runtime"]["last_trigger"] == "run_now"
    assert store.rows["schedule#bad"]["runtime"]["last_status"].startswith("invalid: cron")


def test_dispatch_error_is_recorded_not_raised():
    now = datetime(2026, 10, 16, 15, 0, 5, tzinfo=timezone.utc)
    store = FakeStore({"schedule#inbox": schedule()})

    def boom(run, endpoint_id, part_id, task_id):
        raise RuntimeError("gateway down")

    scheduler = A2AScheduler(store.graphql, boom, [("a2a", "default")], clock=lambda: now)
    scheduler.tick()
    scheduler._pool.shutdown(wait=True)
    runtime = store.rows["schedule#inbox"]["runtime"]
    assert runtime["last_status"] == "failed" and "gateway down" in runtime["last_result"]


def test_camel_cased_graphql_rows_are_read_as_stored():
    """The JSON scalar camel-cases keys on output; the scheduler must still fire."""
    now = datetime(2026, 10, 16, 3, 0, tzinfo=timezone.utc)
    camel = {"kind": "schedule", "name": "x", "prompt": "p", "agentUuid": "a2a-goose-scheduler",
             "cron": "0 8 * * 1-5", "timezone": "UTC", "enabled": False, "runNow": True,
             "runtime": {"lastStatus": "invalid: no agent is set", "nextRunAt": None}}
    store, sent = FakeStore({"schedule#x": camel}), []
    scheduler = make(store, sent, now)
    assert scheduler.tick() == 1
    scheduler._pool.shutdown(wait=True)
    assert sent[0]["agent_uuid"] == "a2a-goose-scheduler"
    saved = store.rows["schedule#x"]
    assert saved["agent_uuid"] == "a2a-goose-scheduler" and saved["run_now"] is False
    assert "agentUuid" not in saved


class StaleListStore(FakeStore):
    """The list resolver is cached: it keeps returning the first snapshot."""

    def __init__(self, rows):
        super().__init__(rows)
        self.snapshot = json.loads(json.dumps(self.rows))

    def graphql(self, query, variables, endpoint_id, part_id):
        if "a2aSettingList" in query:
            return {"a2aSettingList": {"a2aSettingList": [
                {"settingId": sid, "setting": s} for sid, s in self.snapshot.items()]}}
        return super().graphql(query, variables, endpoint_id, part_id)


def test_run_now_fires_once_even_when_the_list_is_stale():
    now = datetime(2026, 10, 16, 3, 0, tzinfo=timezone.utc)
    store, sent = StaleListStore({"schedule#x": schedule(enabled=False, run_now=True)}), []
    scheduler = make(store, sent, now)
    scheduler.tick()
    scheduler._pool.shutdown(wait=True)
    scheduler._pool = type(scheduler._pool)(max_workers=1)
    assert scheduler.tick() == 0  # stale list still says run_now; the fresh read does not
    assert len(sent) == 1
