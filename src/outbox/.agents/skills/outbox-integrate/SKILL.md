---
name: outbox-integrate
description: Use when adding outbox-core to an existing transactional write path - wiring OutboxWriter.enqueue() into a commit, or setting up a Relay to deliver claimed rows. Triggers on requests like "make this write reliable", "add an outbox", "publish this without losing it on crash", or any task that follows a DB write with a separate call to a queue/topic/webhook client.
---

# Integrating outbox-core

## What this library exists to prevent

A DB commit followed by a **separate** publish call - a queue client, an HTTP
webhook, an event-grid SDK - invoked *after* `session.commit()` and outside
that transaction. If the process dies, or the publish call exhausts its own
retries, between the commit and the publish, the result is durably persisted
but never delivered, and redelivery logic that checks "is there already a
terminal row for this?" will skip it. The data is silently lost from the
consumer's point of view even though it's sitting right there in Postgres.

**If you find yourself about to write code that commits a transaction and
then separately calls a message/queue/webhook client, stop.** That is the
bug. The fix is always: put the outbox row in the *same* transaction as the
domain write, and let a `Relay` deliver it afterward.

## The write path

```python
from outbox import OutboxMessage, OutboxWriter

writer = OutboxWriter()  # stateless, safe to share/reuse


async def create_order(session: AsyncSession, order: Order) -> None:
    session.add(order)  # the caller's own domain write
    await writer.enqueue(
        session,
        OutboxMessage(
            topic="orders.created",
            payload={"order_id": order.id, "total": str(order.total)},
        ),
    )
    await session.commit()  # domain write + outbox row commit together, or neither does
```

Rules that matter here, in order of how often they get violated:

1. **`enqueue()` goes *before* `commit()`, on the *same* session.** It does not
   manage its own transaction - it just adds a row to whatever transaction the
   caller is already in. Calling it after commit, or on a different session,
   defeats the entire point.
2. **Never call a queue/webhook/SDK client directly from application code that
   also does a DB write in the same operation.** If delivery needs to happen,
   it happens through the outbox, not alongside it.
3. Payload can be a `dict` (serialized to JSON automatically, tagged
   `application/json`) or raw `bytes` (tagged `application/octet-stream`
   unless you pass `content_type=` explicitly on `OutboxMessage`).
4. Pick a `topic` per event kind - the relay and any topic filter you set up
   later key off it.
5. `partition_key` is passed through to the provider (which may map it to a
   transport partition/session key) *and* consulted by the relay's claim
   query: messages sharing a non-null `partition_key` are never claimed
   concurrently and are claimed oldest-`id`-first among that key's committed
   pending rows (matching enqueue order unless same-key writes overlap
   across transactions - see `docs/delivering.md#per-key-ordering`), at the
   cost of a stuck same-key message head-of-line-blocking later ones sharing
   that key. Leave it `None` unless you specifically need that ordering -
   there's still no ordering guarantee across different or absent keys.

## Standing up delivery

Once at process startup (not per-request):

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
relay = Relay(session_factory, my_message_provider, config)

# long-running process:
await relay.run_forever()
# or, if you'd rather drive it from an existing scheduler/cron:
result = await relay.poll_once()
```

`my_message_provider` is anything implementing the `outbox.MessageProvider`
protocol - see the `outbox-new-provider` skill if one doesn't exist yet for
your transport. For local development and tests, `outbox.InMemoryProvider`
is a real reference implementation, not a mock - messages sent through it are
buffered in `.sent` for assertions.

## Schema

The `outbox_message` table must exist before any of this runs - apply
`src/outbox/migrations/sql/0001_initial.sql` directly, or include
`outbox.schemas.metadata` in your own Alembic `target_metadata` and autogenerate.
Do not hand-roll a different schema - the claim query in `outbox.relay.claim`
depends on the exact column set and on the two shipped partial indexes
(`(available_at, id) WHERE status = 'pending'` for claiming,
`(lease_expires_at) WHERE status = 'claimed'` for lease reclaim).

## Checklist for reviewing your own integration before you're done

- [ ] `writer.enqueue(session, ...)` is called before `session.commit()`, on the same session as the domain write it's paired with.
- [ ] There is no direct queue/webhook/SDK call anywhere near that commit - delivery happens only through the `Relay`.
- [ ] The schema migration has actually been applied wherever this runs.
- [ ] A `Relay` is running somewhere (long-lived process or scheduled job) - an outbox row that nothing ever claims is just as lost as the bug this replaces.
