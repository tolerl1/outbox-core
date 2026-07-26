from __future__ import annotations

import importlib.resources

_CURRENT_MIGRATION = "0001_initial.sql"


def ddl() -> str:
    """Returns the full schema DDL for the current migration.

    Reads the shipped SQL file via `importlib.resources`, so this works from a
    pip install too — not just a source checkout where `src/outbox/migrations/sql/`
    is on disk relative to the repo root. See `python -m outbox.migrations` for
    a CLI wrapper around this.
    """
    return (
        importlib.resources.files("outbox.migrations")
        .joinpath("sql", _CURRENT_MIGRATION)
        .read_text(encoding="utf-8")
    )
