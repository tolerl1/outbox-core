# Versioning

The full policy lives in
[VERSIONING.md](https://github.com/tolerl1/outbox-core/blob/main/VERSIONING.md).
The parts consumers most need:

- **SemVer, currently `0.x`** - a breaking change bumps *minor* until 1.0
  (standard SemVer major-zero rules). Pin `>=0.a.b,<0.(a+1)` during `0.x`;
  standard caret pinning once at `1.0+`.
- **Two public surfaces version independently**: the Python API (what the
  root `outbox` package exports) and the database schema (the shipped DDL).
  A schema change can break you with zero Python code touched, so schema
  changes always ship with a migration file and an "Upgrading from vX"
  changelog note.
- **`MessageProvider` protocol changes are always breaking**, no exceptions -
  every provider package implements it.
- **Python `>=3.12` is a floor, not a sliding window** - never dropped just
  for being old; raising it is a deliberate, changelog-flagged breaking
  change.
