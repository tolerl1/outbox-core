# CLAUDE.md

The contributor guide for this repository lives in `AGENTS.md` (imported
below) — repo map, commands, invariants, style, versioning rules, and the
list of docs that must stay in sync with code changes.

@AGENTS.md

Claude-specific notes:

- The skills in `src/outbox/.agents/skills/` are consumer-facing artifacts
  this repo *ships*; they are not skills for working on this repo. Treat
  them as documentation to keep in sync, not instructions to follow here.
- Run all four checks before finishing: `uv run ruff check .`,
  `uv run ruff format --check .`, `uv run pyright`, `uv run pytest`. If you
  touched `schemas.py`, the migration SQL, or anything under `src/outbox/relay/`,
  also run the integration suite against a real Postgres
  (see AGENTS.md → Commands).
