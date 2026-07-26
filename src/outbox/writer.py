"""OutboxWriter for enqueuing messages into the transactional outbox."""

from __future__ import annotations

import json

from sqlalchemy import Table, insert
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.schemas import outbox_message
from outbox.types import OutboxMessage


class OutboxWriter:
    """Enqueue messages into the transactional outbox.

    Inserts one outbox row per call into the caller's existing transaction.
    Never manages the transaction itself — the caller's session must already
    be inside one.

    Attributes:
        _table (Table): the outbox_message table to insert into (injectable
            for testing)
    """

    def __init__(self, table: Table = outbox_message) -> None:
        """Initialize the writer with a table reference.

        Args:
            table (Table): the outbox_message table; defaults to the library's
                defined table
        """
        self._table = table

    async def enqueue(self, session: AsyncSession, message: OutboxMessage) -> int:
        """Insert an outbox message into the database.

        Serializes dict payloads to JSON with application/json content-type;
        passes bytes payloads as-is. Rejects dict payloads containing
        non-finite floats (NaN/Infinity) at enqueue time rather than
        persisting invalid JSON.

        Args:
            session (AsyncSession): the async database session inside a
                transaction
            message (OutboxMessage): the message to enqueue

        Returns:
            int: the ID of the inserted outbox row

        Raises:
            ValueError: if a dict payload contains NaN or Infinity
        """
        if isinstance(message.payload, dict):
            # allow_nan=False: NaN/Infinity aren't valid JSON, so fail here,
            # inside the caller's transaction, rather than persisting a payload
            # tagged application/json that breaks at the consumer.
            payload = json.dumps(message.payload, allow_nan=False).encode("utf-8")
            content_type = "application/json"
        else:
            payload = message.payload
            content_type = message.content_type or "application/octet-stream"

        result = await session.execute(
            insert(self._table)
            .values(
                topic=message.topic,
                payload=payload,
                content_type=content_type,
                headers=message.headers,
                partition_key=message.partition_key,
            )
            .returning(self._table.c.id)
        )
        return result.scalar_one()
