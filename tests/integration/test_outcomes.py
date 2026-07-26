from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from outbox.relay.claim import claim_batch
from outbox.relay.outcomes import mark_dead_letter, mark_delivered, schedule_retry
from outbox.schemas import outbox_message

pytestmark = pytest.mark.integration


async def _seed_and_claim(engine: AsyncEngine, *, worker_id: str = "w1") -> int:
    async with engine.begin() as conn:
        result = await conn.execute(
            insert(outbox_message).values(topic="t", payload=b"{}").returning(outbox_message.c.id)
        )
        message_id = result.scalar_one()
    async with engine.begin() as conn:
        await claim_batch(
            conn, batch_size=10, lease_duration=timedelta(seconds=30), worker_id=worker_id
        )
    return message_id


async def test_mark_delivered_is_fenced_out_by_a_stale_worker_id(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
    clean_outbox_table: None,
) -> None:
    """Verify that mark_delivered rejects updates from workers not holding the claim."""
    message_id = await _seed_and_claim(engine, worker_id="w1")

    async with session_factory() as session, session.begin():
        applied = await mark_delivered(session, message_id, worker_id="stale-worker")

    assert applied is False

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()
    assert row.status == "claimed"


async def test_mark_delivered_applies_for_the_current_claimant(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
    clean_outbox_table: None,
) -> None:
    """Verify that mark_delivered succeeds for the current claim holder."""
    message_id = await _seed_and_claim(engine, worker_id="w1")

    async with session_factory() as session, session.begin():
        applied = await mark_delivered(session, message_id, worker_id="w1")

    assert applied is True

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()
    assert row.status == "delivered"
    assert row.delivered_at is not None
    assert row.claimed_at is None
    assert row.lease_expires_at is None
    assert row.worker_id is None


async def test_mark_dead_letter_applies_and_keeps_the_worker_id(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
    clean_outbox_table: None,
) -> None:
    """Verify that mark_dead_letter records the error and preserves worker_id."""
    message_id = await _seed_and_claim(engine, worker_id="w1")

    async with session_factory() as session, session.begin():
        applied = await mark_dead_letter(
            session, message_id, error=RuntimeError("boom"), worker_id="w1"
        )

    assert applied is True

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()
    assert row.status == "dead_letter"
    assert row.claimed_at is None
    assert row.lease_expires_at is None
    # worker_id is kept on dead-letter rows: it records whose attempt was fatal.
    assert row.worker_id == "w1"
    # last_error keeps the exception type — the most searchable part.
    assert row.last_error == "RuntimeError: boom"


async def test_schedule_retry_is_fenced_out_by_a_stale_worker_id(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
    clean_outbox_table: None,
) -> None:
    """Verify that schedule_retry rejects updates from workers not holding the claim."""
    message_id = await _seed_and_claim(engine, worker_id="w1")

    async with session_factory() as session, session.begin():
        applied = await schedule_retry(
            session,
            message_id,
            backoff=timedelta(seconds=1),
            error=RuntimeError("boom"),
            worker_id="stale-worker",
        )

    assert applied is False

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()
    assert row.status == "claimed"
    assert row.last_error is None


async def test_mark_dead_letter_is_fenced_out_by_a_stale_worker_id(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
    clean_outbox_table: None,
) -> None:
    """Verify that mark_dead_letter rejects updates from workers not holding the claim."""
    message_id = await _seed_and_claim(engine, worker_id="w1")

    async with session_factory() as session, session.begin():
        applied = await mark_dead_letter(
            session, message_id, error=RuntimeError("boom"), worker_id="stale-worker"
        )

    assert applied is False

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()
    assert row.status == "claimed"
    assert row.last_error is None
