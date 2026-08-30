# Contributing to outbox-core — guide for AI coding agents (and humans)

This file is the canonical onboarding document for anyone — human or agent —
making changes to this repository. `CLAUDE.md` and
`.github/copilot-instructions.md` both defer to it. The three skills in
`src/outbox/.agents/skills/` are **not** for working on this repo; they're
bundled into the published package and shipped to consumers of the library
(see "Docs that must stay in sync" below).

## What this project is

A broker-agnostic transactional outbox for SQLAlchemy (async) + Postgres.
Callers enqueue an outbox row in the same transaction as their domain write;
a `Relay` later claims rows with `FOR UPDATE SKIP LOCKED` + leases and
delivers them to a pluggable `MessageProvider`.

Three contracts define the library. Never write code, docs, or tests that
weaken them:

1. **At-least-once delivery.** Messages are never lost, occasionally
   duplicated. Consumers dedupe on the row `id` carried by `OutboundMessage`.
2. **No ordering guarantee across different or absent partition keys.**
   Claim *selection* is oldest-first (`available_at, id`) so retries don't
   jump the queue, but nothing promises delivery order between distinct
   `partition_key`s or unkeyed messages — don't add tests or docs that
   assert one there. The carve-out: messages sharing a non-null
   `partition_key` are never claimed concurrently — the claim query only
   claims a keyed row once every earlier same-key row has resolved to
   `delivered` or `dead_letter`, claiming oldest-id-first among committed
   rows, which matches enqueue order for the common case of
   non-overlapping same-key writers (see
   `specs/0001-partition-key-ordering.md` and
   `docs/delivering.md#per-key-ordering`). This is a real, intentional
   narrowing of the contract, not a bug — don't "fix" the claim query to
   ignore `partition_key`, and don't weaken the predicate without updating
   this doc, the spec, and `docs/delivering.md` together.
3. **Broker-agnostic core.** Transport specifics (size limits, partitioning
   semantics, content-type mapping) belong in providers, never in the core.

## Repo map

| Path | What lives there |
|---|---|
| `src/outbox/writer.py` | `OutboxWriter.enqueue()` — inserts into the caller's transaction, never commits |
| `src/outbox/relay/claim.py` | Claim query (raw SQL template) + lease reclaim / poison-message dead-lettering |
| `src/outbox/relay/outcomes.py` | Fenced outcome writes: delivered / retry / dead-letter |
| `src/outbox/relay/dispatcher.py` | `Relay` — poll loop, concurrency, retry orchestration, logging |
| `src/outbox/relay/_sql.py` | `seconds_interval()` — mirrors `make_interval` in the claim SQL template |
| `src/outbox/schemas.py` | SQLAlchemy Core table — must mirror `migrations/sql/0001_initial.sql` exactly |
| `src/outbox/migrations/` | Shipped DDL + `ddl()` helper + `python -m outbox.migrations` |
| `src/outbox/config.py`, `retry.py`, `types.py`, `errors.py` | Validated config, backoff policy, dataclasses, exceptions |
| `src/outbox/providers/` | `MessageProvider` protocol + `InMemoryProvider` reference implementation |
| `src/outbox/__init__.py` | Root exports — this is the versioned public API (`test_public_api.py` enforces it) |
| `tests/` | Unit tests at top level; `tests/integration/` needs a real Postgres |
| `src/outbox/.agents/skills/` | Consumer-facing Claude Code skills, bundled into the published package (shipped artifacts, not contributor docs) |
| `docs/` + `zensical.toml` | The docs site ([Zensical](https://zensical.org)); deployed to GitHub Pages by `.github/workflows/docs.yml` |
| `specs/` | Design specs for non-trivial changes — see `specs/README.md` for when one is required |
| `benchmarks/` | Standalone throughput microbenchmark (`benchmark.py`), not part of CI or the test suite — see `docs/benchmarks.md` |

## Commands

```bash
uv sync                     # install everything (uv is the only supported workflow)
uv run pytest               # unit tests; integration tests skip without a DB
uv run ruff check .
uv run ruff format .
uv run pyright              # strict mode; zero errors is the bar

uv run --group docs zensical serve   # preview the docs site locally
uv run --group docs zensical build   # build it (outputs to site/, gitignored)
```

Integration tests need a real, already-running Postgres (16, 17, or 18 —
CI runs all three); nothing is spun up for you:

```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=outbox postgres:18
export OUTBOX_TEST_DATABASE_URL=postgresql://postgres:outbox@localhost:5432/postgres
uv run pytest -m integration
```

The same database is reused for the throughput microbenchmark, which is
just as destructive to it (see `docs/benchmarks.md`):

```bash
uv run python benchmarks/benchmark.py
```

Run all four checks (ruff check, ruff format, pyright, pytest) before
declaring any change done. CI runs exactly these; doc-only changes (`*.md`
outside `src/`) don't trigger CI. The skills under `src/outbox/.agents/skills/`
are part of the published package, so editing them triggers CI like any
other `src/` change.

## Invariants — do not break these

- **Schema is defined twice, deliberately.** `schemas.py` and
  `migrations/sql/0001_initial.sql` must stay byte-for-byte equivalent in
  meaning. The integration suite builds its schema from the SQL file, so
  drift fails tests — but only if the integration suite runs. If you touch
  either file, change both, and run `-m integration` against a real Postgres.
- **`attempts` increments at claim time only** (in the claim query's
  `RETURNING`). Outcome writers must never touch it — that's the
  poison-message guard: a worker that dies mid-send still consumed an attempt.
- **Outcome writes are fenced** (`status = 'claimed' AND worker_id = :worker_id`)
  and rely on a call-discipline invariant documented in `outcomes.py::_fenced`:
  `poll_once()` awaits all in-flight dispatches before returning, and cycles
  run serially per worker. Don't reuse the outcome helpers anywhere that
  breaks this without adding a real fencing token.
- **No user data in SQL text.** The claim template's `.format()` only ever
  interpolates a fixed literal clause; everything caller-supplied goes
  through bind parameters. Keep it that way.
- **`enqueue()` never manages a transaction.** It inserts into the caller's
  session and returns; commit is the caller's job.
- **`send()` is a single attempt.** The relay owns retry/backoff/dead-letter.
  Providers raise on failure; they never retry internally or swallow
  exceptions.
- **Mirrored SQL helpers.** `_sql.py::seconds_interval` and the textual
  `make_interval(secs => :lease_seconds)` in `claim.py` must stay in sync
  (both files carry a comment saying so).
- **`worker_id` is kept on dead-letter rows** (forensics: which worker held
  the fatal claim) and cleared on every other transition out of `claimed`.
  `claimed_at`/`lease_expires_at` are cleared on all of them.

## Style

- Python ≥3.12, `ruff` (line length 100), `pyright` strict — all must pass.
- Google-style docstrings on public functions/classes, with types repeated in
  `Args:`/`Returns:`/`Raises:` sections (match the existing files).
- Dataclasses with `slots=True` for message types; pydantic
  (`frozen=True`) for config, with a `field_validator` per constraint.
- Tests: descriptive full-sentence names
  (`test_an_outcome_write_failure_does_not_abandon_the_rest_of_the_batch`),
  `pytest-asyncio` in auto mode, DB-touching tests under `tests/integration/`
  marked `integration`.
- Comments explain *why* (invariants, race conditions), not *what*.

## Versioning rules that affect code review

Read `VERSIONING.md` before changing any public surface. The ones that bite:

- **Any change to the `MessageProvider` protocol shape is breaking, no
  exceptions** — every downstream provider implements it.
- The public API is what `src/outbox/__init__.py` exports
  (`test_public_api.py` is the gate). Adding an export is minor; renaming or
  removing one is breaking.
- Schema changes need a migration file plus an "Upgrading from vX" note in
  `CHANGELOG.md` — never edit `0001_initial.sql` in place once released.
- At `0.x`, breaking changes bump **minor** (SemVer major-zero rules).

## Docs that must stay in sync with code

When you change behavior, sweep all of these — they describe the same
surfaces independently and drift silently:

- `README.md` — user-facing behavior, config, schema, operational guidance.
- `docs/` — the published site covers the same ground as the README in more
  depth (`writing.md`, `delivering.md`, `schema.md`, `operations.md`); a
  behavior change that touches one almost always touches both.
- `docs/reference.md` — hand-maintained API reference for the root `outbox`
  package plus `outbox.migrations.ddl()`; a signature, field, default, or
  validation-rule change anywhere in the public surface must be transcribed
  here too.
- `CHANGELOG.md` — add to `[Unreleased]` for anything notable.
- `src/outbox/.agents/skills/outbox-integrate`,
  `src/outbox/.agents/skills/outbox-new-provider`,
  `src/outbox/.agents/skills/outbox-review` — consumer-facing docs that quote
  API names, field lists, and index definitions. If you touch
  `OutboxMessage`, `OutboundMessage`, `MessageProvider`, the schema, or the
  write-path rules, check all three.
- Docstrings that state contracts (ordering, fencing, attempt counting) —
  they're load-bearing documentation, not decoration.

## Specs before code

Non-trivial changes — public API surface, `MessageProvider` protocol, schema,
or delivery semantics — start as a short design doc in `specs/`, reviewed in
its own `docs:` PR before implementation. `specs/README.md` defines exactly
when one is required and the lifecycle; `specs/TEMPLATE.md` is the format.
Bug fixes, docs, tests, and contract-preserving refactors don't need one.

## Commits and PRs

- [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`,
  `fix:`, `docs:`, …, `!` or `BREAKING CHANGE:` for breaks).
- The repo squash-merges; **PR titles** are the enforced conventional-commit
  surface (a CI check validates them).
- Releases are cut with `cz bump --changelog`; don't hand-edit version
  numbers in `pyproject.toml`.
