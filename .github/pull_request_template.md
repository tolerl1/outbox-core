<!-- PR titles must be conventional-commit-shaped (feat:, fix:, docs:, ...);
     the repo squash-merges, so the title becomes the commit message.
     A CI check enforces this. -->

## What & why

<!-- What changes, and what problem it solves. Link the issue/spec if one exists. -->

## Checks run

- [ ] `uv run ruff check .` and `uv run ruff format --check .`
- [ ] `uv run pyright`
- [ ] `uv run pytest` (unit)
- [ ] `uv run pytest -m integration` against a real Postgres — **required** if
      this PR touches `src/outbox/schemas.py`, `src/outbox/migrations/`, or
      `src/outbox/relay/`; write "n/a" otherwise

## Contract & docs sweep

<!-- Check what applies; delete what doesn't. See AGENTS.md for the full rules. -->

- [ ] No public API surface changed, **or** the change is versioned per
      `VERSIONING.md` (`MessageProvider` shape changes are always breaking)
- [ ] `schemas.py` and `0001_initial.sql` still mirror each other (if either changed)
- [ ] `README.md` / docs site updated if behavior changed
- [ ] `CHANGELOG.md` `[Unreleased]` entry added if notable
- [ ] `src/outbox/.agents/skills/` swept if this touches `OutboxMessage`,
      `OutboundMessage`, `MessageProvider`, the schema, or write-path rules
