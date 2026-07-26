# Spec NNNN: <title>

- **Status**: Draft <!-- Draft | Accepted | Implemented | Superseded by NNNN -->
- **Author(s)**: <!-- humans and/or agents; agents note the driving human -->
- **Date**: <!-- YYYY-MM-DD -->
- **Related**: <!-- issues, PRs, prior specs -->

## Problem

<!-- What hurts today, for whom, and why now. One or two paragraphs. A spec
     without a concrete problem statement is a feature in search of a use. -->

## Goals / Non-goals

<!-- Bullet both. Non-goals are load-bearing: they're what reviewers would
     otherwise assume is included. E.g. "Non-goal: ordering guarantees." -->

## Design

<!-- The shape of the change: new/changed functions, config, SQL, control
     flow. Sketch code or SQL where it clarifies. Keep it at the level a
     reviewer needs to say yes/no — not implementation-complete. -->

### Public API impact

<!-- New/changed/removed exports in outbox/__init__.py, config fields,
     dataclass fields. State the version bump per VERSIONING.md.
     MessageProvider shape changes are always breaking — say so explicitly. -->

### Schema impact

<!-- None, or: the migration (NNNN_*.sql), whether schemas.py changes in
     lockstep, index implications for the claim/reclaim paths, and the
     "Upgrading from vX" note CHANGELOG.md will need. -->

### Contracts and invariants

<!-- How the change interacts with: at-least-once delivery, the no-ordering
     contract, claim-time attempts increment, outcome fencing (see
     outcomes.py::_fenced ABA note), lease sizing, transaction ownership
     (enqueue never commits). "Unaffected" is a fine answer — but claim it
     explicitly per invariant you touch, so review can check it. -->

## Failure modes

<!-- What happens when a worker dies, the DB blips, or the transport is down
     mid-change? What does a partially-applied state look like and who
     recovers it? This section is why the spec exists — spend the time here. -->

## Alternatives considered

<!-- The 1-3 designs you rejected and the sentence each died by. -->

## Test plan

<!-- Which behaviors get unit tests vs integration tests (real Postgres).
     Concurrency claims need concurrency tests — name the scenario. -->

## Docs impact

<!-- Which of README.md, docs/, CHANGELOG.md, src/outbox/.agents/skills/*,
     VERSIONING.md, and AGENTS.md need updating when this lands. -->

## Open questions

<!-- Anything unresolved that shouldn't block review of the rest. -->
