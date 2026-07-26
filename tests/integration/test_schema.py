import pytest
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from outbox.schemas import outbox_message

pytestmark = pytest.mark.integration


async def test_status_check_constraint_rejects_unknown_status(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that the status column check constraint rejects invalid statuses."""
    async with engine.begin() as conn:
        with pytest.raises(IntegrityError, match="ck_outbox_message_status"):
            await conn.execute(
                insert(outbox_message).values(topic="t", payload=b"{}", status="not-a-real-status")
            )


async def test_row_gets_defaults_when_only_required_fields_are_given(
    engine: AsyncEngine, clean_outbox_table: None
) -> None:
    """Verify that omitted columns receive their configured default values."""
    async with engine.begin() as conn:
        result = await conn.execute(
            insert(outbox_message)
            .values(topic="orders.created", payload=b"{}")
            .returning(outbox_message)
        )
        row = result.one()

    assert row.status == "pending"
    assert row.attempts == 0
    assert row.content_type == "application/json"
    assert row.headers == {}
    assert row.delivered_at is None
