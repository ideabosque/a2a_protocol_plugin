#!/usr/bin/python
"""A2A scheduler — sends a stored prompt to an agent on a cron cadence.

A schedule is an ``a2a_setting`` record (no new table, both backends) whose
``setting_id`` is ``schedule#<id>`` and whose ``setting`` JSON holds::

    {"kind": "schedule", "name": str, "prompt": str, "agent_uuid": str,
     "cron": "m h dom mon dow", "timezone": "Area/City", "enabled": bool,
     "run_now": bool, "owner": str,
     "runtime": {"last_fired_minute", "last_run_at", "last_task_id",
                 "last_status", "last_result", "next_run_at", "history": [...]}}

A background thread (started by the gateway's module ``on_startup`` hook,
see ``start_scheduler``) wakes every ``A2A_SCHEDULER_INTERVAL`` seconds, and
for each due schedule sends ``message/send`` in-process to the plugin itself.
Each run is therefore an ordinary A2A task, visible like any chat task.

Guarantees and limits:
- A schedule fires at most once per cron minute; the claim is written before
  the run starts. Missed minutes while the gateway is down are skipped, not
  replayed.
- One run per schedule at a time; a due minute while the previous run is still
  going is skipped and recorded.
- Runs are agent execution only. The agent's own permissions and the engines'
  server-side role checks decide what a run may do; the scheduler never
  approves or resolves anything itself.
- Single-process: with several gateway workers each would run a dispatcher, so
  enable it on one worker only (A2A_SCHEDULER_ENABLED).
"""
from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .a2a_cron import CronError, parse_cron

__author__ = "bibow"

SCHEDULE_PREFIX = "schedule#"
HISTORY_LIMIT = 10
RESULT_EXCERPT = 600

logger = logging.getLogger(__name__)

GraphqlFn = Callable[[str, Dict[str, Any], str, str], Dict[str, Any]]
SendFn = Callable[[Dict[str, Any], str, str, str], Tuple[str, str]]


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def snake_keys(value: Any) -> Any:
    """Recursively turn camelCase keys into snake_case.

    The plugin's GraphQL JSON scalar camel-cases keys on output (``agent_uuid``
    comes back as ``agentUuid``), while schedules are stored snake_case."""
    import re

    if isinstance(value, dict):
        return {re.sub(r"(?<!^)(?=[A-Z])", "_", str(k)).lower(): snake_keys(v) for k, v in value.items()}
    if isinstance(value, list):
        return [snake_keys(v) for v in value]
    return value


def validate_schedule(setting: Dict[str, Any]) -> Optional[str]:
    """Reason the schedule cannot run, or None when it is valid."""
    if not isinstance(setting, dict) or setting.get("kind") != "schedule":
        return "not a schedule record"
    if not str(setting.get("prompt") or "").strip():
        return "the prompt is empty"
    if not str(setting.get("agent_uuid") or "").strip():
        return "no agent is set"
    try:
        parse_cron(str(setting.get("cron") or ""))
    except CronError as exc:
        return f"cron: {exc}"
    try:
        ZoneInfo(str(setting.get("timezone") or "UTC"))
    except (ZoneInfoNotFoundError, ValueError):
        return f"unknown timezone '{setting.get('timezone')}'"
    return None


def local_minute(now_utc: datetime, tz_name: str) -> datetime:
    """``now_utc`` as wall-clock time in the schedule's timezone, at minute resolution."""
    return now_utc.astimezone(ZoneInfo(tz_name or "UTC")).replace(second=0, microsecond=0)


def due_reason(setting: Dict[str, Any], now_utc: datetime) -> Optional[str]:
    """Why the schedule should fire now ('cron' or 'run_now'), or None."""
    if validate_schedule(setting) is not None:
        return None
    if setting.get("run_now"):
        return "run_now"
    if not setting.get("enabled", False):
        return None
    minute = local_minute(now_utc, setting.get("timezone") or "UTC")
    runtime = setting.get("runtime") or {}
    if runtime.get("last_fired_minute") == minute.isoformat():
        return None
    return "cron" if parse_cron(setting["cron"]).matches(minute) else None


def next_run_iso(setting: Dict[str, Any], now_utc: datetime) -> Optional[str]:
    """Next firing time (ISO, schedule timezone) for display; None when disabled/invalid."""
    if not setting.get("enabled", False) or validate_schedule(setting) is not None:
        return None
    nxt = parse_cron(setting["cron"]).next_after(
        local_minute(now_utc, setting.get("timezone") or "UTC")
    )
    return nxt.isoformat() if nxt else None


def scheduled_prompt(setting: Dict[str, Any], schedule_id: str, reason: str, now_utc: datetime) -> str:
    """The text sent to the agent: a short provenance header, then the stored prompt."""
    trigger = "run on request" if reason == "run_now" else f"cron {setting.get('cron')}"
    header = (
        f"[Scheduled run · {setting.get('name') or schedule_id} ({schedule_id}) · {trigger} · "
        f"{now_utc.strftime('%Y-%m-%d %H:%M UTC')}. Unattended: nobody is watching this chat; "
        "do not wait for answers, and never approve, resolve or decide a request — those are a "
        "person's, recorded in the workbench.]"
    )
    return f"{header}\n\n{str(setting.get('prompt') or '').strip()}"


def read_send_result(raw: Any) -> Tuple[str, str]:
    """(state, reply excerpt) from the plugin's message/send JSON-RPC response."""
    try:
        body = json.loads(raw) if isinstance(raw, (str, bytes)) else (raw or {})
    except (TypeError, ValueError):
        return "failed", "unreadable response from the A2A handler"
    if isinstance(body, dict) and isinstance(body.get("body"), str):
        try:
            body = json.loads(body["body"])
        except ValueError:
            pass
    if not isinstance(body, dict):
        return "failed", "unexpected response from the A2A handler"
    if body.get("error"):
        error = body["error"]
        message = error.get("message") if isinstance(error, dict) else str(error)
        return "failed", (message or "error")[:RESULT_EXCERPT]
    result = body.get("result") or {}
    task = result.get("task", result) if isinstance(result, dict) else {}
    status = task.get("status") or {}
    state = str(status.get("state") or task.get("state") or "completed")
    state = state.replace("TASK_STATE_", "").lower()

    def texts(parts: Any) -> str:
        if not isinstance(parts, list):
            return ""
        return "".join(
            part if isinstance(part, str) else str((part or {}).get("text") or "")
            for part in parts
        )

    answer = "".join(
        texts((artifact or {}).get("parts"))
        for artifact in (task.get("artifacts") or [])
        if not any("reason" in str(v).lower() for v in ((artifact or {}).get("metadata") or {}).values())
    )
    answer = answer or texts((status.get("message") or {}).get("parts")) or texts(task.get("parts"))
    return state, answer.strip()[:RESULT_EXCERPT]


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


class A2AScheduler:
    """Dispatch loop over the schedules of the configured partitions."""

    def __init__(
        self,
        graphql: GraphqlFn,
        send: SendFn,
        partitions: List[Tuple[str, str]],
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        max_workers: int = 2,
    ) -> None:
        self._graphql = graphql
        self._send = send
        self._partitions = partitions
        self._clock = clock
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="a2a-sched")
        self._running: set[Tuple[str, str]] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._seen_counts: Dict[str, int] = {}

    # -- storage (through the plugin's own GraphQL, partition-scoped) --------

    _LIST = """query Schedules($sid: String) {
      a2aSettingList(pageNumber: 1, limit: 200, settingId: $sid) {
        a2aSettingList { settingId setting }
      }
    }"""

    # Single-record read: not behind the list resolver's method_cache, so the
    # dispatcher decides on the current row (the cached list can be minutes
    # stale after a write, which made a run_now fire repeatedly).
    _GET = """query Schedule($sid: String!) {
      a2aSetting(settingId: $sid) { settingId setting }
    }"""

    _SAVE = """mutation SaveSchedule($sid: String!, $ep: String!, $part: String!, $setting: JSON!, $by: String!) {
      insertUpdateA2aSetting(settingId: $sid, endpointId: $ep, partId: $part, setting: $setting, updatedBy: $by) {
        a2aSetting { settingId }
      }
    }"""

    def _load(self, endpoint_id: str, part_id: str) -> List[Tuple[str, Dict[str, Any]]]:
        data = self._graphql(self._LIST, {"sid": SCHEDULE_PREFIX}, endpoint_id, part_id)
        rows = ((data or {}).get("a2aSettingList") or {}).get("a2aSettingList") or []
        out = []
        for row in rows:
            sid = str((row or {}).get("settingId") or "")
            setting = (row or {}).get("setting")
            if isinstance(setting, str):
                try:
                    setting = json.loads(setting)
                except ValueError:
                    setting = None
            if sid.startswith(SCHEDULE_PREFIX) and isinstance(setting, dict):
                out.append((sid, snake_keys(setting)))
        return out

    def _save(self, sid: str, setting: Dict[str, Any], endpoint_id: str, part_id: str) -> None:
        self._graphql(
            self._SAVE,
            {"sid": sid, "ep": endpoint_id, "part": part_id, "setting": setting, "by": "scheduler"},
            endpoint_id,
            part_id,
        )

    def _reload(self, sid: str, endpoint_id: str, part_id: str) -> Optional[Dict[str, Any]]:
        data = self._graphql(self._GET, {"sid": sid}, endpoint_id, part_id)
        setting = ((data or {}).get("a2aSetting") or {}).get("setting")
        if isinstance(setting, str):
            try:
                setting = json.loads(setting)
            except ValueError:
                return None
        return snake_keys(setting) if isinstance(setting, dict) else None

    # -- loop -----------------------------------------------------------------

    def tick(self) -> int:
        """One pass over all partitions; returns how many runs were started."""
        started = 0
        now = self._clock()
        for endpoint_id, part_id in self._partitions:
            try:
                schedules = self._load(endpoint_id, part_id)
            except Exception as exc:  # storage unavailable: try again next tick
                logger.warning(f"scheduler: cannot list schedules in {endpoint_id}#{part_id}: {exc}")
                continue
            partition = f"{endpoint_id}#{part_id}"
            if self._seen_counts.get(partition) != len(schedules):
                self._seen_counts[partition] = len(schedules)
                logger.info(f"scheduler: {len(schedules)} schedule(s) in {partition}")
            for sid, listed in schedules:
                try:
                    # The list (cached) finds the ids; decide on the current row.
                    setting = self._reload(sid, endpoint_id, part_id)
                    if setting is None:
                        continue  # deleted since the list was cached
                    started += self._consider(sid, setting, now, endpoint_id, part_id)
                except Exception as exc:
                    logger.warning(f"scheduler: {sid} skipped this tick: {exc}")
        return started

    def _consider(self, sid: str, setting: Dict[str, Any], now: datetime, endpoint_id: str, part_id: str) -> int:
        runtime = dict(setting.get("runtime") or {})
        problem = validate_schedule(setting)
        if problem:
            if runtime.get("last_status") != f"invalid: {problem}":
                runtime.update(last_status=f"invalid: {problem}", next_run_at=None)
                self._save(sid, {**setting, "runtime": runtime}, endpoint_id, part_id)
            return 0

        reason = due_reason(setting, now)
        if reason is None:
            nxt = next_run_iso(setting, now)
            if runtime.get("next_run_at") != nxt:
                runtime["next_run_at"] = nxt
                self._save(sid, {**setting, "runtime": runtime}, endpoint_id, part_id)
            return 0

        key = (f"{endpoint_id}#{part_id}", sid)
        with self._lock:
            busy = key in self._running
            if not busy:
                self._running.add(key)
        minute = local_minute(now, setting.get("timezone") or "UTC").isoformat()
        if busy:
            runtime.update(last_fired_minute=minute, last_status="skipped: previous run still going")
            self._save(sid, {**setting, "run_now": False, "runtime": runtime}, endpoint_id, part_id)
            return 0

        task_id = f"sched-{sid[len(SCHEDULE_PREFIX):]}-{uuid.uuid4().hex[:8]}"
        runtime.update(
            last_fired_minute=minute,
            last_run_at=now.isoformat(),
            last_task_id=task_id,
            last_status="running",
            last_trigger=reason,
        )
        claimed = {**setting, "run_now": False, "runtime": runtime}
        try:
            self._save(sid, claimed, endpoint_id, part_id)  # claim before running
        except Exception:
            with self._lock:
                self._running.discard(key)
            raise
        logger.info(f"scheduler: {sid} fired ({reason}) as task {task_id} -> {setting['agent_uuid']}")
        self._pool.submit(self._run, sid, claimed, reason, task_id, now, endpoint_id, part_id, key)
        return 1

    def _run(self, sid, setting, reason, task_id, started_at, endpoint_id, part_id, key) -> None:
        try:
            try:
                state, excerpt = self._send(
                    {
                        "text": scheduled_prompt(setting, sid[len(SCHEDULE_PREFIX):], reason, started_at),
                        "agent_uuid": setting["agent_uuid"],
                        "task_id": task_id,
                        "schedule_id": sid,
                    },
                    endpoint_id,
                    part_id,
                    task_id,
                )
            except Exception as exc:
                state, excerpt = "failed", f"dispatch error: {exc}"[:RESULT_EXCERPT]
            finished = self._clock()
            latest = self._reload(sid, endpoint_id, part_id) or setting
            runtime = dict(latest.get("runtime") or {})
            history = list(runtime.get("history") or [])
            history.insert(
                0,
                {
                    "task_id": task_id,
                    "trigger": reason,
                    "started_at": started_at.isoformat(),
                    "finished_at": finished.isoformat(),
                    "status": state,
                    "result": excerpt,
                },
            )
            runtime.update(
                last_status=state,
                last_result=excerpt,
                last_finished_at=finished.isoformat(),
                next_run_at=next_run_iso(latest, finished),
                history=history[:HISTORY_LIMIT],
            )
            self._save(sid, {**latest, "runtime": runtime}, endpoint_id, part_id)
            logger.info(f"scheduler: {sid} task {task_id} finished: {state}")
        except Exception as exc:
            logger.warning(f"scheduler: recording the result of {sid} failed: {exc}")
        finally:
            with self._lock:
                self._running.discard(key)

    def run_forever(self, interval: float) -> None:
        logger.info(f"A2A scheduler started (every {interval:.0f}s, partitions={self._partitions})")
        while not self._stop.wait(interval):
            try:
                self.tick()
            except Exception as exc:
                logger.warning(f"scheduler tick failed: {exc}")

    def stop(self) -> None:
        self._stop.set()
        self._pool.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Gateway wiring
# ---------------------------------------------------------------------------

_instance: Optional[A2AScheduler] = None
_instance_lock = threading.Lock()


def _partitions_from_env() -> List[Tuple[str, str]]:
    raw = os.environ.get("A2A_SCHEDULER_PARTITIONS", "a2a#default")
    out = []
    for item in raw.split(","):
        endpoint_id, _, part_id = item.strip().partition("#")
        if endpoint_id:
            out.append((endpoint_id, part_id or "default"))
    return out


def _in_process_graphql(query: str, variables: Dict[str, Any], endpoint_id: str, part_id: str) -> Dict[str, Any]:
    from ..main import dispatch_graphql

    raw = dispatch_graphql(
        query=query,
        variables=variables,
        endpoint_id=endpoint_id,
        part_id=part_id,
        context={"user_id": "scheduler", "roles": ["scheduler"]},
    )
    body = json.loads(raw) if isinstance(raw, (str, bytes)) else (raw or {})
    if isinstance(body, dict) and isinstance(body.get("body"), str):
        body = json.loads(body["body"])
    if body.get("errors"):
        raise RuntimeError(str(body["errors"])[:300])
    return body.get("data") or {}


def _in_process_send(run: Dict[str, Any], endpoint_id: str, part_id: str, task_id: str) -> Tuple[str, str]:
    from ..main import dispatch_a2a

    raw = dispatch_a2a(
        jsonrpc="2.0",
        id=task_id,
        method="message/send",
        params={
            "message": {"role": "ROLE_USER", "parts": [{"text": run["text"]}]},
            "metadata": {
                "operation": "task_execution",
                "agent_uuid": run["agent_uuid"],
                "stream": False,
                "task_data": {"task_id": task_id, "task_type": "scheduled"},
                "schedule_id": run["schedule_id"],
            },
        },
        endpoint_id=endpoint_id,
        part_id=part_id,
        context={"user_id": "scheduler", "roles": ["scheduler"]},
    )
    return read_send_result(raw)


def start_scheduler() -> None:
    """Gateway ``on_startup`` hook: start the dispatcher thread once per process."""
    global _instance
    if os.environ.get("A2A_SCHEDULER_ENABLED", "true").strip().lower() in ("0", "false", "no", "off"):
        logger.info("A2A scheduler disabled (A2A_SCHEDULER_ENABLED)")
        return
    with _instance_lock:
        if _instance is not None:
            return
        _instance = A2AScheduler(_in_process_graphql, _in_process_send, _partitions_from_env())
        interval = float(os.environ.get("A2A_SCHEDULER_INTERVAL", "30"))
        threading.Thread(
            target=_instance.run_forever, args=(interval,), name="a2a-scheduler", daemon=True
        ).start()


def stop_scheduler() -> None:
    """Gateway ``on_shutdown`` hook."""
    global _instance
    with _instance_lock:
        if _instance is not None:
            _instance.stop()
            _instance = None
