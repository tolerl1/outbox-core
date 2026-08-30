# outbox-core

A broker-agnostic transactional outbox for SQLAlchemy (async) + Postgres.

**[📖 Full Documentation](https://tolerl1.github.io/outbox-core/)**

A DB commit followed by a *separate* publish call to a queue/webhook/SDK has a gap: if the process crashes between the commit and the publish, the data is persisted but the event is never delivered. The transactional outbox pattern closes that gap by making the outbox row part of the same atomic write as the domain data.

This library gives you **at-least-once** delivery (never lost, occasionally duplicated), pluggable transports (bring your own `MessageProvider`), and optional per-key ordering guarantees - with no framework dependency beyond SQLAlchemy and Postgres.

## Install

```bash
uv add outbox-core
python -m outbox.migrations | psql "$DATABASE_URL"  # apply schema
```

## Quick Example

```python
from outbox import OutboxMessage, OutboxWriter

writer = OutboxWriter()


async def create_order(session: AsyncSession, order: Order) -> None:
    session.add(order)
    await writer.enqueue(
        session,
        OutboxMessage(topic="orders.created", payload={"order_id": order.id}),
    )
    await session.commit()  # atomic: domain write + outbox row together
```

Then run a `Relay` to claim and deliver messages - see [Delivering](https://tolerl1.github.io/outbox-core/delivering/) for implementing a `MessageProvider` and standing up the relay.

## Agent Skills

Bundled skills for Claude Code / GitHub Copilot - install with `uvx library-skills` ([details](https://library-skills.io/)).

## Versioning

See [VERSIONING.md](https://github.com/tolerl1/outbox-core/blob/main/VERSIONING.md).
Short version: SemVer, `0.x` until the public API is ready to hold stable,
and the DB schema versions separately from the Python API since a schema
change can break you with zero Python code touched.

## Contributing

See [CONTRIBUTING.md](https://github.com/tolerl1/outbox-core/blob/main/CONTRIBUTING.md).
The full contributor guide lives in [AGENTS.md](https://github.com/tolerl1/outbox-core/blob/main/AGENTS.md): repo map, commands, invariants, style, and docs sync requirements.

## License

MIT
