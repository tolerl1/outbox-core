# Copilot instructions for outbox-core

Full contributor guide: `AGENTS.md` at the repo root. Condensed rules:

## Project

Broker-agnostic transactional outbox for SQLAlchemy (async) + Postgres.
Contracts that must never be weakened: at-least-once delivery (dedupe on the
row `id`), **no ordering guarantee**, transport specifics live in
`MessageProvider` implementations — never in the core.

## Workflow

- `uv` only: `uv sync`, `uv run pytest`, `uv run ruff check .`,
  `uv run ruff format .`, `uv run pyright` (strict — zero errors).
- Integration tests (`uv run pytest -m integration`) need a real Postgres via
  `OUTBOX_TEST_DATABASE_URL`; they skip silently without it, so run them
  whenever you touch `src/outbox/schemas.py`, the migration SQL, or
  `src/outbox/relay/`.
- Conventional Commits; PR titles must be conventional-commit-shaped
  (enforced by CI; the repo squash-merges).

## Hard invariants

- `src/outbox/schemas.py` and `src/outbox/migrations/sql/0001_initial.sql`
  mirror each other exactly — change both or neither.
- `attempts` is incremented by the claim query only; outcome writes never
  touch it (poison-message guard).
- Outcome updates keep their fence (`status = 'claimed' AND worker_id = :worker_id`);
  see the ABA note in `src/outbox/relay/outcomes.py::_fenced` before reusing them.
- No caller data ever interpolated into SQL text — bind parameters only.
- `OutboxWriter.enqueue()` never commits; `MessageProvider.send()` is a
  single attempt that raises on failure (no internal retries, no swallowed
  exceptions).
- Any change to the `MessageProvider` protocol shape is a breaking change,
  regardless of size (see `VERSIONING.md`).

## Style

Google-style docstrings with types in `Args:`/`Returns:`/`Raises:`;
dataclasses with `slots=True`; frozen pydantic config with per-constraint
validators; descriptive sentence-style test names; ruff line length 100.

## Keep docs in sync

Behavior changes must sweep `README.md`, the docs site under `docs/`,
`CHANGELOG.md` (`[Unreleased]`), and the consumer-facing skills bundled in
`src/outbox/.agents/skills/` (they quote API names, field lists, and index
definitions). The skills are shipped artifacts for users of the library —
not instructions for working on this repo.

Changes to the public API, `MessageProvider` protocol, schema, or delivery
semantics need a spec in `specs/` first — see `specs/README.md`.
