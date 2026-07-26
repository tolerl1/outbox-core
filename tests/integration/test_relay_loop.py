import asyncio
import logging
from dataclasses import dataclass, field
from datetime import timedelta

import pytest
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from outbox.config import RelayConfig
from outbox.providers.in_memory import InMemoryProvider
from outbox.relay.dispatcher import Relay
from outbox.retry import RetryPolicy
from outbox.schemas import outbox_message
from outbox.types import OutboundMessage
from outbox.writer import OutboxWriter

pytestmark = pytest.mark.integration


@dataclass
class FlakyProvider:
    fail_times: int
    sent: list[OutboundMessage] = field(default_factory=list[OutboundMessage])
    calls: int = 0

    async def send(self, message: OutboundMessage) -> None:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("simulated transient failure")
        self.sent.append(message)


class AlwaysFailProvider:
    async def send(self, message: OutboundMessage) -> None:
        raise RuntimeError("simulated permanent failure")


@dataclass
class ConcurrencyTrackingProvider:
    """Records how many `send()` calls were in flight at once, so tests can
    assert that `dispatch_concurrency > 1` actually overlaps sends rather than
    just changing bookkeeping."""

    sent: list[OutboundMessage] = field(default_factory=list[OutboundMessage])
    in_flight: int = 0
    max_in_flight: int = 0

    async def send(self, message: OutboundMessage) -> None:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        await asyncio.sleep(0.05)
        self.in_flight -= 1
        self.sent.append(message)


def make_config(**overrides: object) -> RelayConfig:
    defaults: dict[str, object] = {
        "retry_policy": RetryPolicy(
            max_attempts=3,
            base_backoff=timedelta(milliseconds=10),
            max_backoff=timedelta(milliseconds=10),
        ),
        "poll_interval": timedelta(milliseconds=50),
        "lease_duration": timedelta(seconds=30),
        "batch_size": 10,
    }
    defaults.update(overrides)
    return RelayConfig(**defaults)  # type: ignore[arg-type]


async def _enqueue(session_factory: async_sessionmaker[AsyncSession], **kwargs: object) -> None:
    from outbox.types import OutboxMessage

    async with session_factory() as session:
        async with session.begin():
            await OutboxWriter().enqueue(session, OutboxMessage(**kwargs))  # type: ignore[arg-type]


async def test_poll_once_delivers_a_pending_message(
    session_factory: async_sessionmaker[AsyncSession], engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that poll_once delivers a single pending message."""
    await _enqueue(session_factory, topic="orders.created", payload={"id": 1})
    provider = InMemoryProvider()
    relay = Relay(session_factory, provider, make_config())

    result = await relay.poll_once()

    assert result.claimed == 1
    assert result.delivered == 1
    assert [m.topic for m in provider.sent] == ["orders.created"]

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()
    assert row.status == "delivered"
    assert row.delivered_at is not None

    # The provider sees the row id (the stable dedup key) and the content type.
    sent = provider.sent[0]
    assert sent.id == row.id
    assert sent.content_type == "application/json"


async def test_poll_once_is_a_no_op_when_nothing_is_pending(
    session_factory: async_sessionmaker[AsyncSession], clean_outbox_table: None
) -> None:
    """Verify that poll_once returns zeros when no pending messages exist."""
    relay = Relay(session_factory, InMemoryProvider(), make_config())

    result = await relay.poll_once()

    assert result == type(result)(
        claimed=0, delivered=0, failed=0, dead_lettered=0, duration=result.duration
    )


async def test_failed_send_is_rescheduled_and_eventually_delivered(
    session_factory: async_sessionmaker[AsyncSession], engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that transient failures are retried until eventual delivery succeeds."""
    await _enqueue(session_factory, topic="t", payload=b"{}")
    provider = FlakyProvider(fail_times=2)
    relay = Relay(session_factory, provider, make_config())

    first = await relay.poll_once()
    assert first.claimed == 1
    assert first.failed == 1
    assert first.delivered == 0

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()
    assert row.status == "pending"
    assert row.attempts == 1
    assert row.last_error is not None

    await asyncio.sleep(0.05)
    second = await relay.poll_once()
    assert second.failed == 1

    await asyncio.sleep(0.05)
    third = await relay.poll_once()
    assert third.delivered == 1
    assert len(provider.sent) == 1


async def test_send_failure_at_max_attempts_dead_letters_the_row(
    session_factory: async_sessionmaker[AsyncSession], engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that persistent failures dead-letter the message after max_attempts."""
    await _enqueue(session_factory, topic="t", payload=b"{}")
    relay = Relay(session_factory, AlwaysFailProvider(), make_config())

    for _ in range(3):  # max_attempts=3 in make_config()
        await relay.poll_once()
        await asyncio.sleep(0.05)

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()

    assert row.status == "dead_letter"
    assert row.attempts == 3
    assert row.last_error is not None

    # A dead-lettered row must never be claimed again.
    result = await relay.poll_once()
    assert result.claimed == 0


async def test_delivery_is_logged_at_debug(
    session_factory: async_sessionmaker[AsyncSession],
    clean_outbox_table: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify that successful deliveries are logged at DEBUG level."""
    await _enqueue(session_factory, topic="orders.created", payload=b"{}")
    relay = Relay(session_factory, InMemoryProvider(), make_config())

    with caplog.at_level(logging.DEBUG, logger="outbox.relay.dispatcher"):
        await relay.poll_once()

    delivered_records = [r for r in caplog.records if "delivered" in r.message]
    assert len(delivered_records) == 1
    assert delivered_records[0].levelno == logging.DEBUG


async def test_retry_is_logged_at_warning_with_the_error(
    session_factory: async_sessionmaker[AsyncSession],
    clean_outbox_table: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify that retries are logged at WARNING level with the error."""
    await _enqueue(session_factory, topic="t", payload=b"{}")
    relay = Relay(session_factory, FlakyProvider(fail_times=99), make_config())

    with caplog.at_level(logging.WARNING, logger="outbox.relay.dispatcher"):
        await relay.poll_once()

    retry_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(retry_records) == 1
    assert "simulated transient failure" in retry_records[0].message


async def test_dead_letter_is_logged_at_error_with_the_error(
    session_factory: async_sessionmaker[AsyncSession],
    clean_outbox_table: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify that dead-letters are logged at ERROR level with the error."""
    await _enqueue(session_factory, topic="t", payload=b"{}")
    relay = Relay(session_factory, AlwaysFailProvider(), make_config())

    with caplog.at_level(logging.ERROR, logger="outbox.relay.dispatcher"):
        for _ in range(3):  # max_attempts=3 in make_config()
            await relay.poll_once()
            await asyncio.sleep(0.05)

    dead_letter_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(dead_letter_records) == 1
    assert "simulated permanent failure" in dead_letter_records[0].message


async def test_poll_once_reclaims_and_processes_an_expired_lease_in_the_same_cycle(
    session_factory: async_sessionmaker[AsyncSession], engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that expired leases are reclaimed and delivered in the same cycle."""
    async with engine.begin() as conn:
        await conn.execute(
            insert(outbox_message).values(
                topic="t",
                payload=b"{}",
                status="claimed",
                worker_id="dead-worker",
                claimed_at=text("now() - interval '1 hour'"),
                lease_expires_at=text("now() - interval '1 minute'"),
            )
        )

    provider = InMemoryProvider()
    relay = Relay(session_factory, provider, make_config())

    result = await relay.poll_once()

    assert result.delivered == 1
    assert len(provider.sent) == 1


async def test_dispatch_concurrency_greater_than_one_delivers_the_whole_batch(
    session_factory: async_sessionmaker[AsyncSession], engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that dispatch_concurrency > 1 overlaps sends for faster delivery."""
    for _ in range(5):
        await _enqueue(session_factory, topic="t", payload=b"{}")
    provider = ConcurrencyTrackingProvider()
    relay = Relay(session_factory, provider, make_config(dispatch_concurrency=4, batch_size=10))

    result = await relay.poll_once()

    assert result.claimed == 5
    assert result.delivered == 5
    assert len(provider.sent) == 5
    # With 5 messages, concurrency 4, and a 50ms send, at least two sends must
    # have overlapped — proving this isn't just dispatching sequentially.
    assert provider.max_in_flight > 1

    async with engine.begin() as conn:
        rows = (await conn.execute(select(outbox_message))).all()
    assert all(row.status == "delivered" for row in rows)


async def test_dispatch_concurrency_default_of_one_dispatches_sequentially(
    session_factory: async_sessionmaker[AsyncSession], clean_outbox_table: None
) -> None:
    """Verify that the default dispatch_concurrency of 1 sends messages sequentially."""
    for _ in range(3):
        await _enqueue(session_factory, topic="t", payload=b"{}")
    provider = ConcurrencyTrackingProvider()
    relay = Relay(session_factory, provider, make_config(batch_size=10))

    result = await relay.poll_once()

    assert result.delivered == 3
    assert provider.max_in_flight == 1


async def test_an_outcome_write_failure_does_not_abandon_the_rest_of_the_batch(
    session_factory: async_sessionmaker[AsyncSession],
    engine: AsyncEngine,
    clean_outbox_table: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify that outcome write failures don't cancel sibling sends in flight."""
    import outbox.relay.dispatcher as dispatcher_module

    for _ in range(3):
        await _enqueue(session_factory, topic="t", payload=b"{}")

    real_mark_delivered = dispatcher_module.mark_delivered
    calls = 0

    async def mark_delivered_with_one_blip(
        session: AsyncSession, message_id: int, *, worker_id: str
    ) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("simulated DB blip on outcome write")
        return await real_mark_delivered(session, message_id, worker_id=worker_id)

    monkeypatch.setattr(dispatcher_module, "mark_delivered", mark_delivered_with_one_blip)

    # Overlapping sends (50ms each) so a propagating failure would provably
    # cancel siblings that are already in flight.
    provider = ConcurrencyTrackingProvider()
    relay = Relay(session_factory, provider, make_config(dispatch_concurrency=3))

    with caplog.at_level(logging.ERROR, logger="outbox.relay.dispatcher"):
        result = await relay.poll_once()

    assert result.claimed == 3
    assert result.delivered == 2
    assert result.failed == 1
    assert len(provider.sent) == 3, "no sibling send should have been cancelled"

    async with engine.begin() as conn:
        statuses = [
            row.status
            for row in (await conn.execute(select(outbox_message).order_by(outbox_message.c.id)))
        ]
    # The blipped message stays claimed until its lease expires; the others
    # were marked delivered.
    assert sorted(statuses) == ["claimed", "delivered", "delivered"]

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(error_records) == 1
    assert "outcome write failed" in error_records[0].message


async def test_topics_filter_only_claims_matching_topics(
    session_factory: async_sessionmaker[AsyncSession], clean_outbox_table: None
) -> None:
    """Verify that relay filters and claims only messages from specified topics."""
    await _enqueue(session_factory, topic="orders.created", payload=b"{}")
    await _enqueue(session_factory, topic="orders.shipped", payload=b"{}")
    provider = InMemoryProvider()
    relay = Relay(session_factory, provider, make_config(topics=["orders.shipped"]))

    result = await relay.poll_once()

    assert result.claimed == 1
    assert [m.topic for m in provider.sent] == ["orders.shipped"]
