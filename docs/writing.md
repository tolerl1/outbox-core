# Writing

```python
from outbox import OutboxMessage, OutboxWriter

writer = OutboxWriter()  # stateless, reuse it


async def create_order(session: AsyncSession, order: Order) -> None:
    session.add(order)
    await writer.enqueue(
        session,
        OutboxMessage(topic="orders.created", payload={"order_id": order.id}),
    )
    await session.commit()  # domain write + outbox row commit together, or neither does
```

`enqueue()` never manages its own transaction - it just adds a row to
whatever transaction the caller is already in. Call it before `commit()`, on
the same session as the write it's paired with. It returns the new outbox
row's id, in case you want to log or correlate it.

## Rules that matter

1. **`enqueue()` goes *before* `commit()`, on the *same* session.** Calling
   it after commit, or on a different session, defeats the entire point -
   you're back to the dual-write bug.
2. **Never call a queue/webhook/SDK client directly from application code
   that also does a DB write in the same operation.** If delivery needs to
   happen, it happens through the outbox, not alongside it.
3. Payload can be a `dict` (serialized to JSON automatically, tagged
   `application/json` - non-finite floats like `NaN` are rejected at enqueue,
   inside your transaction, rather than persisting invalid JSON) or raw
   `bytes` (tagged `application/octet-stream` unless you pass
   `content_type=` explicitly on `OutboxMessage`).
4. Pick a `topic` per event kind - the relay and any topic filter you set up
   later key off it.
5. `partition_key` is passed through to the provider (which may map it to a
   transport partition/session key) *and* consulted by the relay's claim
   query: messages sharing a non-null `partition_key` are never claimed
   concurrently and are claimed oldest-`id`-first among that key's committed
   pending rows, at the cost of head-of-line blocking within that key (see
   [Delivering](delivering.md#per-key-ordering) for the precise ordering
   guarantee, including the caveat for concurrent same-key writers). Leave
   it `None` unless you specifically need that ordering - it's still no
   ordering guarantee across different or absent keys.
