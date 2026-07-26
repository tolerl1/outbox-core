# Operations

## Retention and purging

Delivered and dead-lettered rows are kept, not deleted immediately — status
transitions to `delivered`/`dead_letter` and the row sits there for whatever
retention window you want (a day to a month is typical; 7 days is a
reasonable default). This costs nothing on the claim path: the only index
the claim query uses is scoped to `WHERE status = 'pending'`.

Purging is intentionally **not** a library API — it's a one-line query
against a public schema, on whatever schedule you already run periodic jobs:

```sql
DELETE FROM outbox_message
WHERE status IN ('delivered', 'dead_letter')
  AND COALESCE(delivered_at, updated_at) < now() - interval '7 days';
```

One caveat on retained rows: `last_error` stores the failed send's exception
(`ExceptionType: message`, truncated) verbatim, for the whole retention
window. Transport SDK errors sometimes embed URLs, tokens, or response
bodies — if yours do, sanitize what your provider raises, and treat the
column as sensitive in dashboards and log shipping.

## Recovering dead-lettered rows

A `dead_letter` row is parked, not gone — the payload is still intact, so
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

Resetting `attempts = 0` matters — a requeued row keeps failing against
`max_attempts` otherwise, so the first error would dead-letter it again
immediately. Clear `worker_id` too (dead-letter rows keep it for forensics;
a pending row shouldn't carry one). And requeue *after* the underlying cause
is fixed, not before: consumers should already tolerate redelivery (delivery
is at-least-once), but re-running a poisoned batch straight back into a
broken transport just burns another round of retries.

## Observability

The relay logs through the stdlib `logging` module, under the
`outbox.relay.dispatcher` logger — claims and deliveries at `DEBUG`, retries
at `WARNING`, dead-letters at `ERROR` (with the triggering exception
attached). Attach a handler, or a logging→OTel/metrics bridge, to wire this
into whatever stack you already use:

```python
import logging

logging.getLogger("outbox.relay.dispatcher").setLevel(logging.DEBUG)
```

## Monitoring

Three queries cover the health of an outbox; run them from whatever metrics
poller you already have (all three are cheap — the first two are satisfied by
the shipped partial indexes):

```sql
-- Backlog depth: how many messages are eligible for delivery right now.
SELECT count(*) FROM outbox_message
WHERE status = 'pending' AND available_at <= now();

-- Oldest-pending age: your actual delivery lag. This is the number to put
-- an SLO on — depth alone can look fine while one row sits stuck for hours.
SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(available_at)), 0) AS lag_seconds
FROM outbox_message
WHERE status = 'pending' AND available_at <= now();

-- Dead letters: should be zero; alert when it isn't. Each one is a message
-- that exhausted max_attempts and is parked until you intervene.
SELECT count(*) FROM outbox_message WHERE status = 'dead_letter';
```

A growing backlog with an idle relay usually means no relay is running (or
it's filtered to the wrong `topics`); a growing backlog with a busy relay
means the transport is slow or down — check the `WARNING`-level retry logs
for the exception.

## Deployment and shutdown

Any number of relay processes can run against the same table — concurrent
claimers partition the pending set via `SKIP LOCKED` rather than
double-claiming or blocking. Each `Relay` needs a distinct `worker_id`; the
auto-generated default guarantees that, so only pass your own (e.g. pod name)
if you want recognizable IDs in logs and dead-letter rows.

To shut down cleanly, cancel the task running `run_forever()` (asyncio
cancellation — the usual SIGTERM handler pattern). Cancellation is safe at
any point: the loop catches `Exception`, not `BaseException`, so it
propagates immediately, and any rows still claimed simply sit until their
lease expires, then get reclaimed and redelivered — by another worker if one
is running, or by the same one after restart. The cost of a mid-flight
shutdown is one burned attempt and up to `lease_duration` of extra latency
for those rows, never a lost message.
