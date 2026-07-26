"""SQLAlchemy table definitions and models for the outbox schema."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Identity,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=NAMING_CONVENTION)


def timestamp_columns() -> tuple[Column[datetime], Column[datetime]]:
    """Return database-populated created_at/updated_at timestamp columns.

    Core equivalent of an ORM TimestampMixin; unpack into a Table's column
    list to add created_at and updated_at columns, both server-defaulted
    to now().

    Returns:
        tuple[Column[datetime], Column[datetime]]: The created_at and
            updated_at columns with timezone support and automatic updates.
    """
    return (
        Column(
            "created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")
        ),
        Column(
            "updated_at",
            TIMESTAMP(timezone=True),
            nullable=False,
            server_default=text("now()"),
            onupdate=text("now()"),
        ),
    )


class MessageStatus(StrEnum):
    """Lifecycle states of an outbox message.

    Attributes:
        PENDING (str): message is eligible to be claimed and delivered
        CLAIMED (str): message is currently being delivered by a worker
        DELIVERED (str): message was successfully delivered to the provider
        DEAD_LETTER (str): message exhausted retries and was abandoned
    """

    PENDING = "pending"
    CLAIMED = "claimed"
    DELIVERED = "delivered"
    DEAD_LETTER = "dead_letter"


outbox_message = Table(
    "outbox_message",
    metadata,
    Column("id", BigInteger, Identity(always=True), primary_key=True),
    Column("topic", Text, nullable=False),
    Column("partition_key", Text, nullable=True),
    Column("content_type", Text, nullable=False, server_default="application/json"),
    Column("payload", LargeBinary, nullable=False),
    Column(
        "headers", JSONB(none_as_null=False), nullable=False, server_default=text("'{}'::jsonb")
    ),
    Column(
        "status",
        Text,
        nullable=False,
        server_default=MessageStatus.PENDING.value,
    ),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("available_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("claimed_at", TIMESTAMP(timezone=True), nullable=True),
    Column("lease_expires_at", TIMESTAMP(timezone=True), nullable=True),
    Column("worker_id", Text, nullable=True),
    Column("last_error", Text, nullable=True),
    Column("delivered_at", TIMESTAMP(timezone=True), nullable=True),
    *timestamp_columns(),
    CheckConstraint(
        f"status IN ({', '.join(repr(s.value) for s in MessageStatus)})",
        name="status",
    ),
)

Index(
    "ix_outbox_pending",
    outbox_message.c.available_at,
    outbox_message.c.id,
    postgresql_where=(outbox_message.c.status == MessageStatus.PENDING.value),
)

Index(
    "ix_outbox_claimed_lease",
    outbox_message.c.lease_expires_at,
    postgresql_where=(outbox_message.c.status == MessageStatus.CLAIMED.value),
)

Index(
    "ix_outbox_partition_key_blocking",
    outbox_message.c.partition_key,
    outbox_message.c.id,
    postgresql_where=(
        outbox_message.c.status.in_([MessageStatus.PENDING.value, MessageStatus.CLAIMED.value])
        & outbox_message.c.partition_key.is_not(None)
    ),
)
