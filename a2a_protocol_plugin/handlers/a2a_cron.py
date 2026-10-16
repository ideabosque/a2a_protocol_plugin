#!/usr/bin/python
"""Five-field cron expressions for the A2A scheduler (no third-party deps).

Fields: minute hour day-of-month month day-of-week, each ``*``, a number, a
range ``a-b``, a list ``a,b`` and/or a step ``*/n`` / ``a-b/n``. Day-of-week
is 0-6 with 0 = Sunday (7 is accepted as Sunday too). As in standard cron,
when both day-of-month and day-of-week are restricted, a day matches if
either matches.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

__author__ = "bibow"

_FIELDS = (
    ("minute", 0, 59),
    ("hour", 0, 23),
    ("day of month", 1, 31),
    ("month", 1, 12),
    ("day of week", 0, 7),
)


class CronError(ValueError):
    """The expression is not a valid five-field cron expression."""


@dataclass(frozen=True)
class CronExpr:
    minutes: frozenset
    hours: frozenset
    days: frozenset
    months: frozenset
    weekdays: frozenset
    day_restricted: bool
    weekday_restricted: bool

    def matches(self, moment: datetime) -> bool:
        """True when ``moment`` (wall-clock, minute resolution) is a firing minute."""
        if moment.minute not in self.minutes or moment.hour not in self.hours:
            return False
        if moment.month not in self.months:
            return False
        weekday = (moment.weekday() + 1) % 7  # Python: Monday=0 → cron: Sunday=0
        day_ok = moment.day in self.days
        weekday_ok = weekday in self.weekdays
        if self.day_restricted and self.weekday_restricted:
            return day_ok or weekday_ok
        return day_ok and weekday_ok

    def next_after(self, moment: datetime, horizon_days: int = 366) -> datetime | None:
        """The first firing minute strictly after ``moment``, or None within the horizon."""
        candidate = moment.replace(second=0, microsecond=0) + timedelta(minutes=1)
        end = candidate + timedelta(days=horizon_days)
        while candidate < end:
            if candidate.month not in self.months:
                # Jump to the first minute of the next month.
                year = candidate.year + (1 if candidate.month == 12 else 0)
                month = 1 if candidate.month == 12 else candidate.month + 1
                candidate = candidate.replace(year=year, month=month, day=1, hour=0, minute=0)
                continue
            if candidate.hour not in self.hours:
                candidate = candidate.replace(minute=0) + timedelta(hours=1)
                continue
            if self.matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        return None


def _parse_field(text: str, name: str, low: int, high: int) -> tuple[frozenset, bool]:
    values: set[int] = set()
    restricted = text.strip() != "*"
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise CronError(f"empty value in the {name} field")
        step = 1
        if "/" in part:
            part, step_text = part.split("/", 1)
            if not step_text.isdigit() or int(step_text) < 1:
                raise CronError(f"bad step '{step_text}' in the {name} field")
            step = int(step_text)
        if part == "*":
            start, end = low, high
        elif "-" in part:
            a, b = part.split("-", 1)
            if not (a.isdigit() and b.isdigit()):
                raise CronError(f"bad range '{part}' in the {name} field")
            start, end = int(a), int(b)
        elif part.isdigit():
            start = end = int(part)
            if step != 1:
                end = high
        else:
            raise CronError(f"bad value '{part}' in the {name} field")
        if start < low or end > high or start > end:
            raise CronError(f"'{part}' is outside {low}-{high} in the {name} field")
        values.update(range(start, end + 1, step))
    return frozenset(values), restricted


def parse_cron(expression: str) -> CronExpr:
    """Parse a five-field cron expression; raises CronError with a readable reason."""
    if not isinstance(expression, str):
        raise CronError("the cron expression must be text")
    fields = expression.split()
    if len(fields) != 5:
        raise CronError("a cron expression has 5 fields: minute hour day-of-month month day-of-week")
    parsed = [_parse_field(text, *spec) for text, spec in zip(fields, _FIELDS)]
    weekdays = frozenset(0 if day == 7 else day for day in parsed[4][0])
    return CronExpr(
        minutes=parsed[0][0],
        hours=parsed[1][0],
        days=parsed[2][0],
        months=parsed[3][0],
        weekdays=weekdays,
        day_restricted=parsed[2][1],
        weekday_restricted=parsed[4][1],
    )
