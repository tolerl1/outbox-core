"""Shared SQL expression helpers for the relay."""

from __future__ import annotations

from datetime import timedelta

from sqlalchemy import func
from sqlalchemy.sql.elements import ColumnElement


def seconds_interval(duration: timedelta) -> ColumnElement[timedelta]:
    """Build a Postgres INTERVAL from a timedelta via make_interval.

    Positional args are (years, months, weeks, days, hours, mins, secs); only
    secs is set, and fractional seconds survive. Mirrors the textual
    make_interval(secs => :lease_seconds) in the claim SQL template — keep the
    two in sync.

    Args:
        duration (timedelta): the interval length

    Returns:
        ColumnElement[timedelta]: a make_interval() SQL expression
    """
    return func.make_interval(0, 0, 0, 0, 0, 0, duration.total_seconds())
