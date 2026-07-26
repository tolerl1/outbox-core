import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from outbox.relay.claim import claim_batch, reclaim_expired_leases
from outbox.relay.outcomes import mark_dead_letter, mark_delivered, schedule_retry
from outbox.schemas import outbox_message

pytestmark = pytest.mark.integration

LEASE = timedelta(seconds=30)


async def _seed(engine: AsyncEngine, rows: list[dict[str, object]]) -> None:
    async with engine.begin() as conn:
        await conn.execute(insert(outbox_message), rows)


async def test_claim_batch_claims_up_to_batch_size_pending_rows(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that claim_batch claims up to the specified batch size."""
    await _seed(engine, [{"topic": "t", "payload": b"{}"} for _ in range(5)])

    async with engine.begin() as conn:
        claimed = await claim_batch(conn, batch_size=3, lease_duration=LEASE, worker_id="w1")

    assert len(claimed) == 3


async def test_claim_batch_sets_status_claimed_and_lease_fields(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that claim_batch sets status, worker_id, and lease expiry."""
    await _seed(engine, [{"topic": "t", "payload": b"{}"}])

    async with engine.begin() as conn:
        await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w1")

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()

    assert row.status == "claimed"
    assert row.worker_id == "w1"
    assert row.claimed_at is not None
    assert row.lease_expires_at is not None


async def test_claim_batch_ignores_rows_with_future_available_at(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that claim_batch skips rows with available_at in the future."""
    async with engine.begin() as conn:
        await conn.execute(
            insert(outbox_message).values(
                topic="t", payload=b"{}", available_at=text("now() + interval '1 hour'")
            )
        )

    async with engine.begin() as conn:
        claimed = await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w1")

    assert claimed == []


async def test_claim_batch_filters_by_topic_when_topics_given(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that claim_batch filters by topic when topics list is provided."""
    await _seed(
        engine,
        [
            {"topic": "orders.created", "payload": b"{}"},
            {"topic": "orders.shipped", "payload": b"{}"},
        ],
    )

    async with engine.begin() as conn:
        claimed = await claim_batch(
            conn, batch_size=10, lease_duration=LEASE, worker_id="w1", topics=["orders.shipped"]
        )

    assert [m.topic for m in claimed] == ["orders.shipped"]


async def test_claim_batch_rejects_an_empty_topics_list(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that an empty topics list is rejected."""
    async with engine.begin() as conn:
        with pytest.raises(ValueError, match="topics"):
            await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w1", topics=[])


async def test_concurrent_claimers_partition_the_pending_set_with_no_double_claim(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that concurrent workers partition rows with no double-claiming."""
    num_rows = 200
    num_workers = 8
    batch_size = 10
    rounds_per_worker = 5  # 8 * 5 * 10 = 400 >= 200, comfortable margin over skip-locked misses

    await _seed(engine, [{"topic": "t", "payload": b"{}"} for _ in range(num_rows)])

    claimed_ids: list[set[int]] = [set() for _ in range(num_workers)]

    async def worker(index: int) -> None:
        worker_id = f"worker-{index}"
        for _ in range(rounds_per_worker):
            async with engine.begin() as conn:
                claimed = await claim_batch(
                    conn, batch_size=batch_size, lease_duration=LEASE, worker_id=worker_id
                )
            claimed_ids[index].update(m.id for m in claimed)

    await asyncio.gather(*(worker(i) for i in range(num_workers)))

    union = set[int]().union(*claimed_ids)
    total_claims = sum(len(s) for s in claimed_ids)

    # Zero overlap: if any row were claimed by two workers, total_claims > len(union).
    assert total_claims == len(union)

    async with engine.begin() as conn:
        remaining_pending = (
            await conn.execute(select(outbox_message).where(outbox_message.c.status == "pending"))
        ).all()

    assert remaining_pending == []
    assert len(union) == num_rows


async def test_reclaim_expired_leases_returns_claimed_rows_to_pending(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that reclaim_expired_leases returns expired claims back to pending."""
    await _seed(engine, [{"topic": "t", "payload": b"{}"}])
    async with engine.begin() as conn:
        await claim_batch(conn, batch_size=10, lease_duration=timedelta(seconds=-1), worker_id="w1")

    async with engine.begin() as conn:
        reclaimed_count = await reclaim_expired_leases(conn, max_attempts=3)

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()

    assert reclaimed_count == 1
    assert row.status == "pending"
    assert row.claimed_at is None
    assert row.lease_expires_at is None
    assert row.worker_id is None


async def test_reclaim_expired_leases_leaves_unexpired_leases_alone(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that reclaim_expired_leases doesn't touch unexpired leases."""
    await _seed(engine, [{"topic": "t", "payload": b"{}"}])
    async with engine.begin() as conn:
        await claim_batch(conn, batch_size=10, lease_duration=timedelta(seconds=30), worker_id="w1")

    async with engine.begin() as conn:
        reclaimed_count = await reclaim_expired_leases(conn, max_attempts=3)

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()

    assert reclaimed_count == 0
    assert row.status == "claimed"


async def test_reclaim_expired_leases_dead_letters_rows_at_max_attempts(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that expired leases at max_attempts are dead-lettered."""
    await _seed(engine, [{"topic": "t", "payload": b"{}", "attempts": 2}])
    async with engine.begin() as conn:
        # claim_batch increments attempts (2 -> 3) as part of claiming.
        await claim_batch(conn, batch_size=10, lease_duration=timedelta(seconds=-1), worker_id="w1")

    async with engine.begin() as conn:
        reclaimed_count = await reclaim_expired_leases(conn, max_attempts=3)

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()

    assert reclaimed_count == 1
    assert row.status == "dead_letter"
    assert row.attempts == 3
    assert row.claimed_at is None
    assert row.lease_expires_at is None
    # Dead-lettered rows keep worker_id: it records whose claim went fatal.
    assert row.worker_id == "w1"
    assert row.last_error is not None
    assert "3" in row.last_error


async def test_claim_batch_increments_attempts(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that claim_batch increments the attempts counter."""
    await _seed(engine, [{"topic": "t", "payload": b"{}"}])

    async with engine.begin() as conn:
        claimed = await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w1")

    assert claimed[0].attempts == 1

    async with engine.begin() as conn:
        row = (await conn.execute(select(outbox_message))).one()

    assert row.attempts == 1


async def test_claim_batch_orders_by_available_at_then_id(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that claim_batch orders by available_at timestamp then by ID."""
    # Both rows are already eligible (available_at in the past); "earlier"
    # should be claimed first despite being inserted second.
    async with engine.begin() as conn:
        await conn.execute(
            insert(outbox_message).values(
                topic="later", payload=b"{}", available_at=text("now() - interval '1 second'")
            )
        )
        await conn.execute(
            insert(outbox_message).values(
                topic="earlier", payload=b"{}", available_at=text("now() - interval '2 seconds'")
            )
        )

    async with engine.begin() as conn:
        claimed = await claim_batch(conn, batch_size=1, lease_duration=LEASE, worker_id="w1")

    assert [m.topic for m in claimed] == ["earlier"]


async def test_claim_batch_breaks_available_at_ties_by_id(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that claim_batch breaks available_at ties by preferring lower IDs."""
    # Both inserts run in one transaction, so now() — and available_at — are
    # identical; the lower id must win the tie. Claimed one at a time
    # (batch_size=1) so the assertion is about *selection*, not the order of
    # UPDATE ... RETURNING, which Postgres doesn't guarantee.
    async with engine.begin() as conn:
        await conn.execute(insert(outbox_message).values(topic="first", payload=b"{}"))
        await conn.execute(insert(outbox_message).values(topic="second", payload=b"{}"))

    async with engine.begin() as conn:
        claimed = await claim_batch(conn, batch_size=1, lease_duration=LEASE, worker_id="w1")

    assert [m.topic for m in claimed] == ["first"]


async def test_null_partition_key_messages_are_unaffected_by_the_blocking_predicate(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify NULL partition_key rows claim exactly as before the blocking predicate."""
    await _seed(engine, [{"topic": "t", "payload": b"{}", "partition_key": None} for _ in range(5)])

    async with engine.begin() as conn:
        claimed = await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w1")

    assert len(claimed) == 5


async def test_only_the_older_of_two_same_key_pending_messages_is_claimed_in_one_batch(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify the younger of two same-key pending messages is excluded from the claiming batch."""
    async with engine.begin() as conn:
        older_id = (
            await conn.execute(
                insert(outbox_message)
                .values(topic="t", payload=b"{}", partition_key="k1")
                .returning(outbox_message.c.id)
            )
        ).scalar_one()
        await conn.execute(
            insert(outbox_message).values(topic="t", payload=b"{}", partition_key="k1")
        )

    async with engine.begin() as conn:
        claimed = await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w1")

    assert [m.id for m in claimed] == [older_id]


async def test_an_older_message_scheduled_for_retry_still_blocks_a_due_younger_same_key_message(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
    clean_outbox_table: None,
) -> None:
    """Verify a same-key message stays blocked while an earlier retry sits pending in the future."""
    async with engine.begin() as conn:
        older_id = (
            await conn.execute(
                insert(outbox_message)
                .values(topic="t", payload=b"{}", partition_key="k1")
                .returning(outbox_message.c.id)
            )
        ).scalar_one()
        await conn.execute(
            insert(outbox_message).values(topic="t", payload=b"{}", partition_key="k1")
        )

    async with engine.begin() as conn:
        # Claims only the older row; the younger same-key row is blocked.
        await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w1")

    async with session_factory() as session, session.begin():
        applied = await schedule_retry(
            session,
            older_id,
            backoff=timedelta(hours=1),
            error=RuntimeError("boom"),
            worker_id="w1",
        )
    assert applied is True

    async with engine.begin() as conn:
        # The younger row is independently due now, but the older row -
        # though not itself due for an hour - is still 'pending' and unresolved,
        # so it keeps blocking.
        claimed = await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w2")

    assert claimed == []


async def test_a_delivered_older_message_unblocks_a_younger_same_key_message(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
    clean_outbox_table: None,
) -> None:
    """Verify a younger same-key message becomes claimable once the older one delivers."""
    async with engine.begin() as conn:
        older_id = (
            await conn.execute(
                insert(outbox_message)
                .values(topic="t", payload=b"{}", partition_key="k1")
                .returning(outbox_message.c.id)
            )
        ).scalar_one()
        younger_id = (
            await conn.execute(
                insert(outbox_message)
                .values(topic="t", payload=b"{}", partition_key="k1")
                .returning(outbox_message.c.id)
            )
        ).scalar_one()

    async with engine.begin() as conn:
        await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w1")

    async with session_factory() as session, session.begin():
        applied = await mark_delivered(session, older_id, worker_id="w1")
    assert applied is True

    async with engine.begin() as conn:
        claimed = await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w2")

    assert [m.id for m in claimed] == [younger_id]


async def test_a_dead_lettered_older_message_unblocks_a_younger_same_key_message(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
    clean_outbox_table: None,
) -> None:
    """Verify a younger same-key message becomes claimable once the older one dead-letters."""
    async with engine.begin() as conn:
        older_id = (
            await conn.execute(
                insert(outbox_message)
                .values(topic="t", payload=b"{}", partition_key="k1")
                .returning(outbox_message.c.id)
            )
        ).scalar_one()
        younger_id = (
            await conn.execute(
                insert(outbox_message)
                .values(topic="t", payload=b"{}", partition_key="k1")
                .returning(outbox_message.c.id)
            )
        ).scalar_one()

    async with engine.begin() as conn:
        await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w1")

    async with session_factory() as session, session.begin():
        applied = await mark_dead_letter(
            session, older_id, error=RuntimeError("boom"), worker_id="w1"
        )
    assert applied is True

    async with engine.begin() as conn:
        claimed = await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w2")

    assert [m.id for m in claimed] == [younger_id]


async def test_different_partition_keys_never_block_each_other(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify two distinct partition keys, both due, are claimed together in one batch."""
    await _seed(
        engine,
        [
            {"topic": "t", "payload": b"{}", "partition_key": "k1"},
            {"topic": "t", "payload": b"{}", "partition_key": "k2"},
        ],
    )

    async with engine.begin() as conn:
        claimed = await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w1")

    assert {m.partition_key for m in claimed} == {"k1", "k2"}


async def test_partition_key_blocking_is_global_not_scoped_per_topic(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify a same partition_key blocks across different topics, not just within one."""
    async with engine.begin() as conn:
        older_id = (
            await conn.execute(
                insert(outbox_message)
                .values(topic="orders.created", payload=b"{}", partition_key="k1")
                .returning(outbox_message.c.id)
            )
        ).scalar_one()
        await conn.execute(
            insert(outbox_message).values(topic="orders.shipped", payload=b"{}", partition_key="k1")
        )

    async with engine.begin() as conn:
        claimed = await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w1")

    # The younger row on a different topic is still blocked by the older
    # same-key row: partition_key blocking is global, not scoped per topic.
    assert [m.id for m in claimed] == [older_id]


async def test_a_lease_expired_older_message_keeps_blocking_until_it_resolves(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify a younger same-key message stays blocked across a lease-expiry reclaim."""
    async with engine.begin() as conn:
        older_id = (
            await conn.execute(
                insert(outbox_message)
                .values(topic="t", payload=b"{}", partition_key="k1")
                .returning(outbox_message.c.id)
            )
        ).scalar_one()
        await conn.execute(
            insert(outbox_message).values(topic="t", payload=b"{}", partition_key="k1")
        )

    async with engine.begin() as conn:
        # Immediately-expired lease: the older row is 'claimed' but abandoned.
        await claim_batch(conn, batch_size=10, lease_duration=timedelta(seconds=-1), worker_id="w1")

    async with engine.begin() as conn:
        # Still blocked: the older row is 'claimed', lease expired or not.
        claimed = await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w2")
    assert claimed == []

    async with engine.begin() as conn:
        reclaimed_count = await reclaim_expired_leases(conn, max_attempts=5)
    assert reclaimed_count == 1

    async with engine.begin() as conn:
        # The reclaimed older row (back to 'pending') is what gets claimed
        # next - the younger row is still blocked behind it.
        claimed = await claim_batch(conn, batch_size=10, lease_duration=LEASE, worker_id="w3")

    assert [m.id for m in claimed] == [older_id]


async def test_concurrent_claimers_never_claim_more_than_one_same_key_message_at_once(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify concurrent claim_batch() callers never put two same-key rows into claimed status."""
    num_rows = 10
    await _seed(
        engine, [{"topic": "t", "payload": b"{}", "partition_key": "k1"} for _ in range(num_rows)]
    )

    async def claim_one(worker_id: str) -> list[int]:
        async with engine.begin() as conn:
            claimed = await claim_batch(
                conn, batch_size=1, lease_duration=LEASE, worker_id=worker_id
            )
        return [m.id for m in claimed]

    results = await asyncio.gather(*(claim_one(f"worker-{i}") for i in range(num_rows)))
    claimed_ids = [message_id for ids in results for message_id in ids]

    # Only the single oldest row is claimable: every other same-key row stays
    # blocked until it resolves, no matter how many callers race for it.
    assert len(claimed_ids) == 1

    async with engine.begin() as conn:
        claimed_rows = (
            await conn.execute(select(outbox_message).where(outbox_message.c.status == "claimed"))
        ).all()
    assert len(claimed_rows) == 1
