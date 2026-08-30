# Operations

## Retention and purging

Delivered and dead-lettered rows are kept, not deleted immediately - status
transitions to `delivered`/`dead_letter` and the row sits there for whatever
retention window you want (a day to a month is typical; 7 days is a
reasonable default). This costs nothing on the claim path: the only index
the claim query uses is scoped to `WHERE status = 'pending'`.

Purging is intentionally **not** a library API - it's a one-line query
against a public schema, on whatever schedule you already run periodic jobs:

```sql
DELETE FROM outbox_message
WHERE status IN ('delivered', 'dead_letter')
  AND COALESCE(delivered_at, updated_at) < now() - interval '7 days';
```

One caveat on retained rows: `last_error` stores the failed send's exception
(`ExceptionType: message`, truncated) verbatim, for the whole retention
window. Transport SDK errors sometimes embed URLs, tokens, or response
bodies - if yours do, sanitize what your provider raises, and treat the
column as sensitive in dashboards and log shipping.

## Purging at scale: an optional index

The purge query above has no dedicated index behind it: none of the three
shipped indexes (see [Schema](schema.md#what-ships)) cover `delivered`/
`dead_letter` rows - `ix_outbox_pending` is explicitly scoped to
`WHERE status = 'pending'` and is a no-op for anything else. That's
deliberate, not an oversight: this index isn't shipped by default because
its cost is paid on every write, not on the occasional purge run.

If your purge job is slow, or your retained backlog runs into the millions
of rows, add:

```sql
CREATE INDEX ix_outbox_purge ON outbox_message (updated_at)
    WHERE status IN ('delivered', 'dead_letter');
```

This indexes `updated_at` rather than the purge query's
`COALESCE(delivered_at, updated_at)` on purpose: `mark_delivered` sets
`delivered_at` and `updated_at` to the same transaction timestamp, and a
dead-lettered row never sets `delivered_at` at all - so for a terminal row
(one that, per the invariants, is never written to again) `updated_at`
alone already carries the value the purge query needs. A plain index can
satisfy an exact column reference but not a `COALESCE(...)` expression, so
simplify the query to match once you add this index:

```sql
DELETE FROM outbox_message
WHERE status IN ('delivered', 'dead_letter')
  AND updated_at < now() - interval '7 days';
```

Whether this is worth adding at all depends entirely on your purge cadence
and table size, which the library has no visibility into:

- **Purging rarely** (daily/weekly) on a modest retained backlog: an
  unindexed scan is a bounded, predictable, off-peak cost - skip the index
  until that scan is measurably slow or growing.
- **Purging often** (hourly or tighter), or a retained backlog in the
  millions of rows: the index earns back its write-side cost quickly.

The trade-off is the same one every index makes: `delivered`/`dead_letter`
is the single most common transition a row makes in this table, so this
index would be maintained on every successful delivery and every
dead-letter, forever - to speed up a job that runs on your own schedule,
not the hot claim path. Measure your own purge query before adding it.

## Recovering dead-lettered rows

A `dead_letter` row is parked, not gone - the payload is still intact, so
once you've fixed whatever made delivery fail (a down transport, a consumer
bug, a bad payload contract), requeue it with plain SQL. Like purging, this
is deliberately not a library API:

```sql
UPDATE outbox_message
SET status = 'pending',
    attempts = 0,
    available_at = now(),
    worker_id = NULL,
    last_error = NULL,
    updated_at = now()
WHERE status = 'dead_letter'
  AND topic = 'orders.created';  -- always scope it: a topic, id list, or time window
```

Resetting `attempts = 0` matters - a requeued row keeps failing against
`max_attempts` otherwise, so the first error would dead-letter it again
immediately. Clear `worker_id` too (dead-letter rows keep it for forensics;
a pending row shouldn't carry one). And requeue *after* the underlying cause
is fixed, not before: consumers should already tolerate redelivery (delivery
is at-least-once), but re-running a poisoned batch straight back into a
broken transport just burns another round of retries.

## Observability

The relay logs through the stdlib `logging` module, under the
`outbox.relay.dispatcher` logger - claims and deliveries at `DEBUG`, retries
at `WARNING`, dead-letters at `ERROR` (with the triggering exception
attached). Attach a handler, or a logging→OTel/metrics bridge, to wire this
into whatever stack you already use:

```python
import logging

logging.getLogger("outbox.relay.dispatcher").setLevel(logging.DEBUG)
```

## Monitoring

Three queries cover the health of an outbox; run them from whatever metrics
poller you already have (all three are cheap - the first two are satisfied by
the shipped partial indexes):

```sql
-- Backlog depth: how many messages are eligible for delivery right now.
SELECT count(*) FROM outbox_message
WHERE status = 'pending' AND available_at <= now();

-- Oldest-pending age: your actual delivery lag. This is the number to put
-- an SLO on - depth alone can look fine while one row sits stuck for hours.
SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(available_at)), 0) AS lag_seconds
FROM outbox_message
WHERE status = 'pending' AND available_at <= now();

-- Dead letters: should be zero; alert when it isn't. Each one is a message
-- that exhausted max_attempts and is parked until you intervene.
SELECT count(*) FROM outbox_message WHERE status = 'dead_letter';
```

A growing backlog with an idle relay usually means no relay is running (or
it's filtered to the wrong `topics`); a growing backlog with a busy relay
means the transport is slow or down - check the `WARNING`-level retry logs
for the exception.

## Deployment and shutdown

Any number of relay processes can run against the same table - concurrent
claimers partition the pending set via `SKIP LOCKED` rather than
double-claiming or blocking. Each `Relay` needs a distinct `worker_id`; the
auto-generated default guarantees that, so only pass your own (e.g. pod name)
if you want recognizable IDs in logs and dead-letter rows.

To shut down cleanly, cancel the task running `run_forever()` (asyncio
cancellation - the usual SIGTERM handler pattern). Cancellation is safe at
any point: the loop catches `Exception`, not `BaseException`, so it
propagates immediately, and any rows still claimed simply sit until their
lease expires, then get reclaimed and redelivered - by another worker if one
is running, or by the same one after restart. The cost of a mid-flight
shutdown is one burned attempt and up to `lease_duration` of extra latency
for those rows, never a lost message.
