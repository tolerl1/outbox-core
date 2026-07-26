# Specs

Short design documents for changes that are expensive to get wrong. A spec
is a page or two of thinking written *before* the implementation PR, so the
design gets reviewed while it's still cheap to change — especially valuable
when the implementer is an AI agent, because the spec becomes the
ground-truth prompt the work is checked against.

## When a spec is required

- Any change to the **public API surface** (`src/outbox/__init__.py` exports)
  beyond adding a purely optional parameter/field.
- Any change to the **`MessageProvider` protocol** — always breaking, per
  `VERSIONING.md`.
- Any **schema change** (a `0002_*.sql` migration).
- Any change to **delivery semantics**: claiming, leases, retry/backoff,
  dead-lettering, fencing, or the at-least-once / no-ordering contracts.
- Any new **long-lived feature surface** (e.g. LISTEN/NOTIFY wakeup, lease
  heartbeating, batched outcome writes).

Not required for: bug fixes that restore documented behavior, docs, tests,
tooling, or internal refactors that change no contract. When in doubt, the
cost of a spec is one page — write it.

## Process

1. Copy `TEMPLATE.md` to `NNNN-short-slug.md` (next free number, e.g.
   `0001-lease-heartbeat.md`). Status: `Draft`.
2. Open a PR containing just the spec (`docs:` title). Review happens there.
3. On merge, flip Status to `Accepted`. Implementation PRs link the spec.
4. When the implementation lands, flip Status to `Implemented` (in the
   implementation PR is fine). Specs that lose to a better idea become
   `Superseded` with a pointer to what replaced them.

Specs are immutable history once `Implemented` — write a new spec instead of
rewriting an old one. The living description of current behavior is the
README and docs site, never a spec.
