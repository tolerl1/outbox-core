# Spec 0001: Per-key delivery ordering via partition_key head-of-line blocking

- **Status**: Accepted
- **Author(s)**: toler1 (driving human); drafted by Claude Code
- **Date**: 2026-07-25
- **Related**: none yet

## Problem

The core "no ordering guarantee" contract (AGENTS.md) is correct as a default
and should stay the default. But `partition_key` already exists end-to-end —
`OutboxMessage`, `OutboundMessage`, `ClaimedMessage`, the schema column, and
the claim query's `RETURNING` clause all carry it, and its docstring already
says "optional key for sharding or ordering." Today that's aspirational: the
claim query treats every pending row identically regardless of key, so two
messages sharing a `partition_key` can be claimed by different concurrent
workers and sent to the provider in either order, and a message that fails
and retries can be delivered after a same-key message enqueued later.

This matters because brokers with native per-key ordering (SQS FIFO message
groups, Kafka partition keys, Service Bus sessions) only preserve whatever
order they *receive* messages in — they cannot fix messages that already
arrived out of order. That half of the problem is producer-side, and the
relay is the only thing common to every provider that's in a position to
enforce it. Callers who need relative ordering between messages for the same
entity/aggregate (the case cited by outbox-pattern guidance, e.g. AWS's
prescriptive guidance) currently have no way to get it short of running
`dispatch_concurrency=1` globally, which serializes everything, not just
same-key messages.

## Goals / Non-goals

Goals:

- Messages sharing a non-null `partition_key` are delivered to the provider
  in the relative order they were enqueued (by `id`).
- Zero behavior/cost change for the default case: `partition_key = NULL`.
- Holds across any number of concurrent `Relay` workers/processes without a
  new coordination primitive — enforced through existing row status under
  ordinary MVCC semantics, not advisory locks or an external lock service.

Non-goals:

- Global ordering across different keys, or between keyed and unkeyed
  messages — still explicitly not guaranteed.
- Retroactively fixing order after a manual dead-letter requeue re-enters an
  old message behind newer same-key messages that already delivered — a
  known limitation (see Failure modes), not solved here.
- Any `MessageProvider` protocol change — providers already receive
  `partition_key` on `OutboundMessage` today.
- Broker-side ordering mechanics (Kafka partitioning, SQS FIFO groups) —
  provider's job, unaffected by this change.

## Design

Add a predicate to the claim query: a pending row is claimable only if no
earlier row (`id` less than its own) sharing its `partition_key` is still
unresolved. "Unresolved" = `status IN ('pending', 'claimed')`, the two
non-terminal `MessageStatus` values; `delivered` and `dead_letter` are
terminal and stop blocking.

```sql
WITH claimed AS (
    SELECT id FROM outbox_message o
    WHERE status = 'pending' AND available_at <= now(){topic_clause}
      AND (
          partition_key IS NULL
          OR NOT EXISTS (
              SELECT 1 FROM outbox_message blocker
              WHERE blocker.partition_key = o.partition_key
                AND blocker.id < o.id
                AND blocker.status IN ('pending', 'claimed')
          )
      )
    ORDER BY available_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT :batch_size
)
-- UPDATE ... RETURNING unchanged
```

Why this needs no new locking:

- **Within one claim statement (one batch)**: if A (`id=1`) and B (`id=2`)
  share a key, B's `NOT EXISTS` check reads A's status as of the statement's
  snapshot — still `'pending'`, since the CTE's row set is fully determined
  before the `UPDATE` touches anything. B is excluded from that batch
  whether or not A itself gets selected in it.
- **Across concurrent `claim_batch()` calls**: B only becomes visible to the
  `NOT EXISTS` check once A's status commits as `delivered` or
  `dead_letter`. While A sits `claimed` (mid-send) or `pending`
  (not yet attempted, or mid-retry-backoff), no worker anywhere can claim B.
  Ordinary read-committed semantics on `status` do the work; no advisory
  lock or application coordination needed.

Intentional consequence: a stuck or slow-retrying message head-of-line-blocks
every later message sharing its key — the same way a slow SQS FIFO consumer
blocks its message group, or a stuck Kafka consumer blocks its partition.
Documented as expected behavior, not a bug (see Failure modes).

### Public API impact

None. `partition_key` is already public on `OutboxMessage`/`OutboundMessage`.
No new exports, no signature changes, `MessageProvider` protocol untouched.

Docstring updates needed: `OutboxMessage.partition_key`'s "optional key for
sharding or ordering" becomes accurate rather than aspirational;
`claim_batch`'s docstring ("no delivery-ordering guarantee is implied") needs
the per-key carve-out spelled out precisely.

### Schema impact

No new column — `partition_key` already exists (`schemas.py`,
`0001_initial.sql`). One new index to support the `NOT EXISTS` lookup:

```sql
CREATE INDEX ix_outbox_partition_key_blocking ON outbox_message (partition_key, id)
    WHERE status IN ('pending', 'claimed') AND partition_key IS NOT NULL;
```

This goes directly into `0001_initial.sql` + `schemas.py` rather than a new
`0002_*.sql`.

### Contracts and invariants

- **At-least-once delivery**: unaffected — this only restricts which pending
  row a claim is willing to pick up, not claim/lease/retry mechanics.
- **No-ordering contract**: narrowed, not broken. AGENTS.md's contract #2
  becomes "no ordering guarantee across different or absent partition keys;
  messages sharing a partition key are delivered in relative order." This is
  a real contract change and should be reviewed as one.
- **`attempts` increments at claim time only**: unaffected — the new
  predicate only gates which rows enter the claiming CTE; the `UPDATE`'s
  column list is untouched.
- **Outcome fencing**: unaffected — outcome writers are unchanged.
- **Poison-message guard / dead-letter worker_id retention**: unaffected —
  same claim/lease/reclaim machinery once a row is claimed.
- **Lease reclaim**: `reclaim_expired_leases` moves an abandoned `claimed`
  row back to `pending` (or dead-letters it if exhausted) without looking at
  `partition_key`; no change needed there — a reclaimed row simply re-enters
  the same blocking predicate on the next claim cycle.

## Failure modes

- **A poison message with a key blocks the rest of its key's queue** for its
  full retry backoff schedule, until it exhausts `max_attempts` and
  dead-letters (terminal, unblocks). Intentional — mirrors SQS FIFO / Kafka
  partition behavior — but it's a new operational failure mode: today one
  bad message only delays itself, not everything sharing its key. Needs
  prominent documentation (README / `docs/operations.md`).
- **Dead-letter requeue does not restore order.** If A (older, keyed)
  dead-letters while B (newer, same key) is already claimed and delivered
  (A's terminal status stopped blocking B), and someone later manually
  resets A to `pending` via the README's documented requeue query, A
  delivers *after* B already went out. Nothing can retroactively fix this —
  document as a known limitation.
- **Lease-expiry reclaim can delay unblocking longer than a retry backoff
  would.** A worker dying mid-send holding a keyed claim blocks that key
  until `lease_duration` elapses and `reclaim_expired_leases` notices —
  potentially longer than an ordinary failure's backoff. Worth flagging as a
  reason to keep lease durations conservative under keyed ordering.
- **`dispatch_concurrency` stops bounding true parallelism for keyed
  workloads.** A batch full of one key's messages now dispatches effectively
  serially regardless of concurrency config, since only one can be `claimed`
  at a time. Not a bug, but a throughput characteristic to document: a key's
  throughput is bounded by its own round-trip latency, not concurrency.

## Alternatives considered

- **Session-level Postgres advisory locks per key**, held for the duration
  of the in-flight send. Rejected: requires holding a dedicated DB
  connection open per in-flight key across an async network call to the
  broker, fighting the short-transaction style the rest of the relay uses
  (claim and outcome writes are separate, quick transactions today) — and it
  doesn't handle the retry-reordering case any better than the status-based
  approach, so it adds real complexity for no extra guarantee.
- **CDC-based single-threaded relay (Debezium-style)**, sidestepping
  concurrency entirely by never having concurrent claimers. Rejected as out
  of scope: a different architecture (WAL tailing via Kafka Connect instead
  of polling), not an incremental change, and it gives up the broker-agnostic
  polling model this library is built around.
- **In-process lock/semaphore per key inside `Relay`.** Rejected: doesn't
  hold across multiple `Relay` instances/processes, which is the normal
  deployment shape this library already supports (concurrent workers with
  distinct `worker_id`s) — would silently stop working past one process.

## Test plan

Unit:

- A `NULL`-keyed message's claim eligibility is unaffected by the new
  predicate (regression coverage for the zero-cost path).

Integration (real Postgres, `tests/integration/`, marked `integration`):

- Two messages, same key, both due: only the older is claimed; the younger
  is excluded from that batch.
- Older message fails and is scheduled for retry (back to `pending`, future
  `available_at`): younger same-key message stays unclaimed even though it's
  independently due *now*.
- Older message succeeds (`delivered`): younger becomes claimable next cycle.
- Older message exhausts retries and dead-letters: younger becomes claimable
  (dead-letter is terminal).
- Different keys never block each other (two distinct keys, both due, both
  claimed in the same batch).
- Lease-expiry path: older message's lease expires and is reclaimed to
  `pending` (not dead-lettered) — younger stays blocked until the reclaimed
  message eventually resolves.
- Concurrency claim: N concurrent `claim_batch()` callers against a batch
  containing several same-key messages — assert at most one same-key message
  is ever in `claimed` status at a time across all callers.

## Docs impact

- `AGENTS.md` — contract #2 needs the per-key carve-out stated precisely;
  highest-risk doc to leave stale since it's what review checks changes
  against.
- `README.md` — the "no ordering guarantee" line, the `partition_key`
  mention, and the batch note ("no ordering is guaranteed either way") all
  need the carve-out.
- `docs/delivering.md` — needs a section on per-key ordering and its
  head-of-line-blocking failure modes.
- `docs/schema.md` — document the new index.
- `docs/reference.md` — `partition_key` field descriptions on
  `OutboxMessage`/`OutboundMessage`, and `claim_batch`'s docstring if quoted.
- `CHANGELOG.md` — `[Unreleased]` entry.
- `skills/outbox-integrate` — check whether it currently describes
  `partition_key` as inert; correct if so.
- `skills/outbox-review` — update if it has a "no ordering" checklist item.

## Open questions

- Should the key be scoped per-topic (same `partition_key` in different
  topics doesn't block) or global as designed above? Leaning global for
  simplicity — the caller chose the string to mean "these belong together,"
  full stop — unless a concrete use case argues for per-topic scoping.
- Worth a metric/log line distinguishing "claim was blocked by an earlier
  unresolved same-key message" from ordinary claim starvation, so operators
  can debug the new failure mode? Not required for a first cut; revisit once
  real usage surfaces whether it's needed.
- Should there be an alerting hook for a key blocked longer than some
  threshold? Deferred as a possible follow-up, not blocking this spec.
