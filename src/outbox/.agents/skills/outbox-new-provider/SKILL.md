---
name: outbox-new-provider
description: Use when implementing a new MessageProvider for outbox-core to deliver to a transport it doesn't ship a provider for (Azure Storage Queue, Event Grid, SQS, SNS, a webhook, Kafka, etc). Triggers on requests like "send outbox messages to X", "write an outbox provider for Y", or any task implementing outbox.providers.protocol.MessageProvider.
---

# Implementing a MessageProvider

`MessageProvider` is a `typing.Protocol` - there is nothing to subclass, only
a shape to match:

```python
from outbox.types import OutboundMessage


class MyProvider:
    async def send(self, message: OutboundMessage) -> None: ...
```

`OutboundMessage` has `id: int`, `topic: str`, `payload: bytes`,
`content_type: str`, `headers: dict[str, str]`, `partition_key: str | None`.
That's the entire delivery-time contract - no other method is required or
called by the `Relay`.

`id` is the outbox row's id, stable across redeliveries of the same message -
delivery is at-least-once, so propagate it to consumers as their dedup key
(a transport message id, or a header). Use `content_type` to set the
transport's content-type metadata where it has one (`application/json` for
dict payloads).

## The two rules that matter

1. **`send()` is a single attempt. It is not a retry loop.** The `Relay`
   already owns retry count, backoff scheduling, and dead-lettering (see
   `outbox.retry.RetryPolicy`). If `send()` catches an exception from the
   underlying SDK and retries internally before giving up, it's duplicating,
   and likely fighting, logic the relay already provides, and it hides
   failures from the relay's attempt counter. Let the underlying client raise;
   let it propagate.

2. **Raise on failure. Never swallow an exception and return normally.**
   The relay's whole retry/dead-letter state machine is driven by whether
   `send()` raised. A caught-and-logged exception that returns `None` looks
   like a successful delivery to the relay - the row gets marked `delivered`
   and the message is gone even though it was never actually sent. This is
   the same shape of bug as the dual-write problem the library exists to fix,
   just relocated into the provider instead of the caller.

## Payload size limits

If the transport has a hard message-size limit (Azure Storage Queue: ~64KB
base64-encoded; SQS: 256KB; etc.), check `len(message.payload)` and raise
`outbox.errors.PayloadTooLargeError` rather than silently truncating,
compressing without the consumer's knowledge, or dropping the message. There
is no size limit enforced by the core library - it's genuinely
transport-agnostic, so this check belongs in the provider, not upstream.

```python
from outbox.errors import PayloadTooLargeError


class StorageQueueProvider:
    _MAX_BYTES = 48_000  # ~64KB after base64 encoding

    async def send(self, message: OutboundMessage) -> None:
        if len(message.payload) > self._MAX_BYTES:
            raise PayloadTooLargeError(
                f"payload is {len(message.payload)} bytes, "
                f"exceeds Storage Queue's {self._MAX_BYTES}-byte limit"
            )
        await self._client.send_message(message.payload)
```

## Headers and partition_key

Map `message.headers` onto whatever the transport's native metadata mechanism
is (message attributes, custom properties, HTTP headers for a webhook). Don't
silently drop them - a consumer-side dedup key or trace id commonly lives here.

`message.partition_key` may be `None`; when set, map it to the transport's
partitioning/grouping concept if one exists (a Kafka message key, a Service
Bus session id, an SQS message group id). The relay itself consults it: a
non-null key is never claimed concurrently with an earlier same-key row (the
relay only claims a keyed row once every earlier same-key row has resolved),
so `send()` calls for the same key arrive in the order the relay claimed
them - the common case matching enqueue order, see
`docs/delivering.md#per-key-ordering` for the precise guarantee under
concurrent same-key writers. You don't need to re-derive that ordering in
the provider. Whatever *transport-side* semantics the key attaches to beyond
that (partition assignment, consumer group behavior) are still the
provider's to document.

## Testing a new provider

Write it against a fake/mocked transport client for unit tests (no real
credentials or network needed to verify `send()` maps `OutboundMessage`
correctly and raises on failure). Save any real end-to-end verification for
a manually-run integration test gated behind actual credentials - don't wire
real cloud calls into the default test suite.

```python
from unittest.mock import AsyncMock


async def test_send_raises_when_the_client_call_fails():
    client = AsyncMock()
    client.send_message.side_effect = SomeSDKError("boom")
    provider = StorageQueueProvider(client)

    with pytest.raises(SomeSDKError):
        await provider.send(
            OutboundMessage(
                id=1,
                topic="t",
                payload=b"{}",
                content_type="application/json",
                headers={},
                partition_key=None,
            )
        )
```
