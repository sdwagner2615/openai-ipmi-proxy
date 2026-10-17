"""Cron off-time evaluation (D11b).

The per-server shutdown engine fires a scheduled power-off at a 5-field cron
off-time. This module is a thin, clock-injectable wrapper over ``croniter`` so
the engine (and its unit tests) can ask "when does this schedule next fire" and
"what is the most recent fire at or before a given instant" without touching
the wall clock.

Cron is a calendar time, so it is evaluated on the wall clock; the idle-off
half of the shutdown engine stays on the monotonic clock (P11).
"""

from __future__ import annotations

from datetime import datetime

from croniter import croniter

__all__ = ["CronSchedule"]


class CronSchedule:
    """A parsed 5-field cron expression with fire-time queries."""

    def __init__(self, cron: str):
        self.cron = cron
        # Parsing validates the expression (config already validates at load;
        # this guards runtime use and surfaces a clear error otherwise).
        croniter(cron)

    def next_fire(self, after: datetime) -> datetime:
        """The first scheduled fire strictly after ``after``."""
        return croniter(self.cron, after).get_next(datetime)

    def last_fire_before(self, before: datetime) -> datetime:
        """The most recent scheduled fire at or before ``before``."""
        return croniter(self.cron, before).get_prev(datetime)
