import json

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from outbox.schemas import outbox_message
from outbox.types import OutboxMessage
from outbox.writer import OutboxWriter

pytestmark = pytest.mark.integration


async def test_enqueue_does_not_persist_until_the_callers_transaction_commits(
    session_factory: async_sessionmaker[AsyncSession], clean_outbox_table: None
) -> None:
    """Verify that enqueued messages are not visible until the transaction commits."""
    writer = OutboxWriter()

    async with session_factory() as session:
        await writer.enqueue(session, OutboxMessage(topic="orders.created", payload=b"{}"))
        await session.rollback()

    async with session_factory() as session:
        rows = (await session.execute(select(outbox_message))).all()

    assert rows == []


async def test_enqueue_persists_once_the_caller_commits(
    session_factory: async_sessionmaker[AsyncSession], clean_outbox_table: None
) -> None:
    """Verify that enqueued messages are persisted after a commit."""
    writer = OutboxWriter()

    async with session_factory() as session:
        await writer.enqueue(session, OutboxMessage(topic="orders.created", payload=b"{}"))
        await session.commit()

    async with session_factory() as session:
        rows = (await session.execute(select(outbox_message))).all()

    assert len(rows) == 1


async def test_enqueue_returns_the_new_rows_id(
    session_factory: async_sessionmaker[AsyncSession], clean_outbox_table: None
) -> None:
    """Verify that enqueue returns the ID of the newly inserted row."""
    writer = OutboxWriter()

    async with session_factory() as session:
        message_id = await writer.enqueue(session, OutboxMessage(topic="t", payload=b"{}"))
        await session.commit()

    async with session_factory() as session:
        row = (
            await session.execute(select(outbox_message).where(outbox_message.c.id == message_id))
        ).one()

    assert row.id == message_id


async def test_dict_payload_is_serialized_to_json_and_tagged_application_json(
    session_factory: async_sessionmaker[AsyncSession], clean_outbox_table: None
) -> None:
    """Verify that dict payloads are serialized to JSON with correct content-type."""
    writer = OutboxWriter()

    async with session_factory() as session:
        await writer.enqueue(session, OutboxMessage(topic="t", payload={"amount": 42}))
        await session.commit()

    async with session_factory() as session:
        row = (await session.execute(select(outbox_message))).one()

    assert json.loads(row.payload) == {"amount": 42}
    assert row.content_type == "application/json"


async def test_dict_payload_with_non_finite_floats_is_rejected_at_enqueue(
    session_factory: async_sessionmaker[AsyncSession], clean_outbox_table: None
) -> None:
    """Reject non-finite floats (NaN/Infinity) in payloads to prevent invalid JSON."""
    writer = OutboxWriter()

    async with session_factory() as session:
        with pytest.raises(ValueError):
            await writer.enqueue(session, OutboxMessage(topic="t", payload={"x": float("nan")}))
        await session.rollback()

    async with session_factory() as session:
        rows = (await session.execute(select(outbox_message))).all()
    assert rows == []


async def test_bytes_payload_defaults_to_octet_stream_content_type(
    session_factory: async_sessionmaker[AsyncSession], clean_outbox_table: None
) -> None:
    """Verify that bytes payloads default to application/octet-stream content-type."""
    writer = OutboxWriter()

    async with session_factory() as session:
        await writer.enqueue(session, OutboxMessage(topic="t", payload=b"raw-bytes"))
        await session.commit()

    async with session_factory() as session:
        row = (await session.execute(select(outbox_message))).one()

    assert row.payload == b"raw-bytes"
    assert row.content_type == "application/octet-stream"


async def test_bytes_payload_content_type_can_be_overridden(
    session_factory: async_sessionmaker[AsyncSession], clean_outbox_table: None
) -> None:
    """Verify that bytes payload content-type can be explicitly specified."""
    writer = OutboxWriter()

    async with session_factory() as session:
        await writer.enqueue(
            session, OutboxMessage(topic="t", payload=b"<a/>", content_type="application/xml")
        )
        await session.commit()

    async with session_factory() as session:
        row = (await session.execute(select(outbox_message))).one()

    assert row.content_type == "application/xml"


async def test_headers_and_partition_key_are_stored(
    session_factory: async_sessionmaker[AsyncSession], clean_outbox_table: None
) -> None:
    """Verify that custom headers and partition keys are persisted."""
    writer = OutboxWriter()

    async with session_factory() as session:
        await writer.enqueue(
            session,
            OutboxMessage(
                topic="t",
                payload=b"{}",
                headers={"x-request-id": "abc"},
                partition_key="order-123",
            ),
        )
        await session.commit()

    async with session_factory() as session:
        row = (await session.execute(select(outbox_message))).one()

    assert row.headers == {"x-request-id": "abc"}
    assert row.partition_key == "order-123"
