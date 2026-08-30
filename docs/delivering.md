# Delivering

## Implementing a provider

Implement the `outbox.MessageProvider` protocol for your transport - it's a
`typing.Protocol`, so there is nothing to subclass, only a shape to match:

```python
from outbox import OutboundMessage


class MyProvider:
    async def send(self, message: OutboundMessage) -> None: ...
```

Raise on failure, don't retry internally - the relay owns retry, backoff,
and dead-lettering. A caught-and-logged exception that returns normally
looks like a successful delivery to the relay: the row gets marked
`delivered` even though nothing was sent. That's the dual-write bug
relocated into the provider.

The `OutboundMessage` you receive carries the row's `id` - stable across
redeliveries, so it's the dedup key to propagate to consumers (put it in a
transport message id/header) - plus `topic`, `payload`, `content_type`
(`application/json` for dict payloads), `headers`, and `partition_key`.
Map `headers` onto the transport's native metadata mechanism, and
`partition_key` (when set) onto its partitioning/grouping concept if one
exists - see "Per-key ordering" below for what a non-null value guarantees
about the order `send()` is called in.

If the transport has a hard message-size limit, check `len(message.payload)`
and raise `outbox.PayloadTooLargeError` rather than silently truncating - the
core enforces no limit because it's transport-agnostic, so the check
belongs in the provider.

`outbox.InMemoryProvider` is a real reference implementation, useful for
local development and tests - sent messages land in `.sent` for assertions.

## Running a relay

```python
from datetime import timedelta
from outbox import Relay, RelayConfig, RetryPolicy

config = RelayConfig(
    retry_policy=RetryPolicy(
        max_attempts=5,
        base_backoff=timedelta(seconds=1),
        max_backoff=timedelta(seconds=30),
        jitter=True,
    ),
)
relay = Relay(session_factory, my_provider, config)

await relay.run_forever()  # long-running process
# or: result = await relay.poll_once()  # one cycle, e.g. from a cron job
```

## Concurrency and lease sizing

By default the relay dispatches one claimed message at a time. If your
provider's `send()` is I/O-bound (a network round-trip per message), raise
`RelayConfig(dispatch_concurrency=N)` to overlap up to `N` sends within a
single poll cycle - no ordering is guaranteed either way across different or
absent partition keys, so this is safe for any such batch. Messages sharing a
partition key are never claimed concurrently regardless of `N` (see "Per-key
ordering" below), so raising `dispatch_concurrency` doesn't parallelize a
single key's throughput - its throughput is bounded by its own round-trip
latency instead. Each in-flight outcome write borrows a connection from your
engine's pool, so size the pool to at least `N`.

**Size `lease_duration` against the whole batch, not one send.** The lease
clock starts at claim time for every row in the batch, and dispatch works
through the batch at `dispatch_concurrency`; budget roughly
`batch_size / dispatch_concurrency × p99 send latency`, with headroom. A
lease that expires mid-batch gets the tail of the batch reclaimed and
redelivered by other workers while it's still in flight - and burns
`attempts` toward dead-letter without a real send failure. With the defaults
(batch 100, sequential dispatch, 30s lease) that means keeping mean send
latency under ~300ms, or shrinking `batch_size` / raising
`dispatch_concurrency` / extending `lease_duration` to match your transport.

## Per-key ordering

Messages sharing a non-null `partition_key` are never claimed concurrently: a
pending row is only claimable once every earlier row (lower `id`) sharing its
`partition_key` has resolved to a terminal status (`delivered` or
`dead_letter`) - while an earlier same-key row sits `pending` (not yet
attempted, or mid-retry-backoff) or `claimed` (in flight), later same-key
rows simply aren't eligible to be claimed by anyone. Rows are claimed
oldest-`id`-first among each key's *committed* pending rows, so for the
common case - one enqueuing transaction in flight per key at a time - this
delivers messages in the order you enqueued them.

That "by id" guarantee is about committed rows, not insertion order across
genuinely concurrent transactions: `id` is assigned when a row is
`INSERT`ed, not when its transaction commits, and the claim query can only
ever see rows that have already committed. If two same-key messages are
enqueued from overlapping transactions, whichever transaction commits first
becomes claimable first, even if it happens to hold the higher `id`. If your
consumers need a strict ordering guarantee under concurrent same-key
writers, make sure same-key enqueues don't overlap - e.g. serialize writes
for a given key so only one enqueuing transaction per key is ever in flight.

This holds across any number of concurrent `Relay` workers/processes without
any extra coordination on your part: it's enforced entirely by the claim
query reading ordinary row status under read-committed semantics, no
advisory locks or external lock service involved. Messages with a `null`
`partition_key` (the default) are unaffected (the check is a no-op for them)
and different keys never block each other.

This mirrors what native per-key ordering on other transports already does:
SQS FIFO message groups and Kafka partitions only preserve the order they
*receive* messages in, so producer-side ordering is a prerequisite either
way. It comes with the same trade-off those transports have:

- **A stuck or slow-retrying keyed message head-of-line-blocks every later
  message sharing its key**, for as long as its retry backoff runs, until it
  either delivers or exhausts `max_attempts` and dead-letters (which is
  terminal and unblocks the key). Today, one bad message only delays itself;
  under per-key ordering, one bad message delays its entire key's backlog.
  Alert on a growing per-key backlog the same way you'd alert on overall
  `dead_letter` count (see "Monitoring" in the README).
- **A lease-expiry reclaim can delay unblocking longer than a normal retry
  would.** A worker dying mid-send while holding a keyed claim leaves that
  key blocked until `lease_expires_at` passes and the next
  `reclaim_expired_leases` pass notices - this can take longer than an
  ordinary failure's backoff. Keep `lease_duration` conservative for keyed
  workloads for this reason.
- **`dispatch_concurrency` stops bounding a key's parallelism.** Since only
  one same-key message can be `claimed` at a time, a batch full of one key's
  messages dispatches effectively serially no matter how high
  `dispatch_concurrency` is set; a key's throughput is bounded by its own
  round-trip latency, not concurrency.
- **A manual dead-letter requeue does not retroactively fix order.** If an
  older keyed message dead-letters while a newer same-key message is already
  claimed and delivers (the older message's terminal status stops blocking
  the newer one), and you later requeue the older message via the README's
  documented requeue query, it delivers *after* the newer one already went
  out. Nothing can retroactively repair this - it's a known limitation, not
  a bug to work around.

Ordering is scoped to the key alone, globally - not per-topic. Two messages
with the same `partition_key` block each other even if they're on different
topics; if you need per-topic scoping, encode the topic into the key itself
(e.g. `f"{topic}:{key}"`).

## Topic filtering

`RelayConfig(topics=[...])` scopes a relay to specific topics. If you shard
relays by topic and a relay's topics are a small slice of the pending
backlog, add a second partial index so its claim query doesn't rescan the
whole hot set every poll:

```sql
CREATE INDEX ix_outbox_pending_topic ON outbox_message (topic, available_at)
    WHERE status = 'pending';
```
