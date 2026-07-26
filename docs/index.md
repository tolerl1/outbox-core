# outbox-core

A broker-agnostic transactional outbox for SQLAlchemy (async) + Postgres.

Write a domain row and an outbox row in the same transaction. A `Relay`
claims and delivers outbox rows afterward, to whatever transport you plug in.
No framework dependency beyond SQLAlchemy and Postgres — bring your own
message provider (Storage Queue, SQS, Event Grid, Kafka, a webhook,
whatever).

## Why

A DB commit followed by a *separate* publish call has a gap: if the process
dies, or the publish exhausts its own retries, after the commit but before
the publish lands, the result is durably persisted but never delivered — and
redelivery logic that checks "is there already a terminal row for this?"
will skip it. The event is silently lost. The transactional outbox pattern
closes that gap by making the publish intent part of the same atomic write
as the data it's about.

This library gives you:

- **At-least-once** delivery — never lost, occasionally duplicated;
  consumers should dedupe on the stable row `id` every `OutboundMessage`
  carries.
- **No ordering guarantee across different or absent `partition_key`s** —
  pushed to consumers, since most simple transports don't preserve it
  either. Messages sharing a non-null `partition_key` *are* delivered in
  relative order (see [Delivering](delivering.md#per-key-ordering)).
- **A pluggable delivery side** — implement one `async def send()` and
  you're not tied to any broker.

## Install

```
uv add outbox-core
```

```python
import outbox
```

Requires Python `>=3.12` and Postgres 16 or newer — the test suite runs
against Postgres 16, 17, and 18 in CI. `SQLAlchemy[asyncio]`, `asyncpg`, and
`pydantic` come along as dependencies.

Everything shown in these docs is importable from the root `outbox` package —
that root surface is the public API the [versioning policy](versioning.md)
covers.

## Where to go next

- [Writing](writing.md) — wiring `enqueue()` into your transactional write path.
- [Delivering](delivering.md) — standing up a `Relay` and implementing a provider.
- [Schema](schema.md) — applying the `outbox_message` migration.
- [Operations](operations.md) — retention, dead-letter recovery, monitoring, shutdown.
