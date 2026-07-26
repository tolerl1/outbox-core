"""Claiming logic for polling pending outbox messages."""

from __future__ import annotations

from datetime import timedelta
from typing import cast

from sqlalchemy import Text, bindparam, case, func, text, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from outbox.schemas import MessageStatus, outbox_message
from outbox.types import ClaimedMessage

# The make_interval(secs => :lease_seconds) expression below is mirrored by the
# seconds_interval helper in relay._sql — keep the two in sync when editing.
_CLAIM_SQL_TEMPLATE = """
WITH claimed AS (
    SELECT o.id FROM outbox_message o
    WHERE o.status = 'pending' AND o.available_at <= now(){topic_clause}
      AND (
          o.partition_key IS NULL
          OR NOT EXISTS (
              SELECT 1 FROM outbox_message blocker
              WHERE blocker.partition_key = o.partition_key
                AND blocker.id < o.id
                AND blocker.status IN ('pending', 'claimed')
          )
      )
    ORDER BY o.available_at, o.id
    FOR UPDATE SKIP LOCKED
    LIMIT :batch_size
)
UPDATE outbox_message o
SET status = 'claimed',
    claimed_at = now(),
    lease_expires_at = now() + make_interval(secs => :lease_seconds),
    worker_id = :worker_id,
    attempts = o.attempts + 1,
    updated_at = now()
FROM claimed
WHERE o.id = claimed.id
RETURNING o.id, o.topic, o.payload, o.content_type, o.headers, o.partition_key, o.attempts
"""


def _reclaim_stmt(max_attempts: int):
    """Build an UPDATE statement to reclaim expired leases.

    Rows with expired leases are either returned to pending (if they haven't
    reached max_attempts) or dead-lettered (if they have). Dead-lettered rows
    keep their worker_id to record which worker held the final claim.

    Args:
        max_attempts (int): the maximum number of attempts from the retry policy

    Returns:
        Update: a SQLAlchemy update statement for reclaiming expired leases
    """
    exhausted = outbox_message.c.attempts >= max_attempts
    return (
        update(outbox_message)
        .where(
            outbox_message.c.status == MessageStatus.CLAIMED.value,
            outbox_message.c.lease_expires_at < func.now(),
        )
        .values(
            status=case(
                (exhausted, MessageStatus.DEAD_LETTER.value),
                else_=MessageStatus.PENDING.value,
            ),
            claimed_at=None,
            lease_expires_at=None,
            # A dead-lettered row keeps its worker_id: it identifies the worker
            # that held the final, fatal claim (crashed or lost its lease).
            worker_id=case(
                (exhausted, outbox_message.c.worker_id),
                else_=None,
            ),
            last_error=case(
                (
                    exhausted,
                    "lease expired after "
                    + func.cast(outbox_message.c.attempts, Text)
                    + " attempts",
                ),
                else_=outbox_message.c.last_error,
            ),
            updated_at=func.now(),
        )
    )


async def claim_batch(
    conn: AsyncConnection | AsyncSession,
    *,
    batch_size: int,
    lease_duration: timedelta,
    worker_id: str,
    topics: list[str] | None = None,
) -> list[ClaimedMessage]:
    """Claim a batch of pending messages in a single SKIP LOCKED round-trip.

    Safe under any number of concurrent callers: concurrent claimers partition
    the pending set rather than double-claiming or blocking on each other.
    Selection prefers the oldest available_at (id as tiebreaker), so retries
    re-enter in scheduled order rather than jumping the queue — but the order
    of the returned list is not guaranteed, and no delivery-ordering guarantee
    is implied across different (or absent) partition keys.

    A row with a non-null partition_key is only claimable if no earlier row
    (lower id) sharing that key is still unresolved (status 'pending' or
    'claimed'); 'delivered' and 'dead_letter' are terminal and stop blocking.
    This makes same-key rows claimable in id order among committed pending
    rows and never concurrently claimed — matching enqueue order for the
    common case of non-overlapping same-key writers, though id is assigned
    at insert time rather than commit time, so genuinely concurrent same-key
    transactions can commit out of id order (see docs/delivering.md for the
    precise guarantee). The trade-off: a stuck or slow-retrying keyed message
    head-of-line-blocks every later message sharing its key until it
    resolves. Rows with a null partition_key are unaffected: the predicate
    is a no-op for them.

    Args:
        conn (AsyncConnection | AsyncSession): database connection/session to
            execute through
        batch_size (int): maximum number of messages to claim
        lease_duration (timedelta): duration to hold the claim (lease)
        worker_id (str): unique ID of this worker for claim ownership
        topics (list[str] | None): topics to claim; None claims all; empty
            list raises ValueError

    Returns:
        list[ClaimedMessage]: claimed messages with updated attempt counts

    Raises:
        ValueError: if topics is an empty list
    """
    if topics is not None and not topics:
        raise ValueError("topics must be None (claim all topics) or a non-empty list")
    topic_clause = " AND o.topic IN :topics" if topics else ""
    stmt = text(_CLAIM_SQL_TEMPLATE.format(topic_clause=topic_clause)).columns(
        outbox_message.c.id,
        outbox_message.c.topic,
        outbox_message.c.payload,
        outbox_message.c.content_type,
        outbox_message.c.headers,
        outbox_message.c.partition_key,
        outbox_message.c.attempts,
    )
    params: dict[str, object] = {
        "batch_size": batch_size,
        "lease_seconds": lease_duration.total_seconds(),
        "worker_id": worker_id,
    }
    if topics:
        stmt = stmt.bindparams(bindparam("topics", expanding=True))
        params["topics"] = topics

    result = await conn.execute(stmt, params)
    return [
        ClaimedMessage(
            id=row.id,
            topic=row.topic,
            payload=row.payload,
            content_type=row.content_type,
            headers=row.headers,
            partition_key=row.partition_key,
            attempts=row.attempts,
        )
        for row in result
    ]


async def reclaim_expired_leases(conn: AsyncConnection | AsyncSession, *, max_attempts: int) -> int:
    """Return claimed-but-abandoned rows to pending or dead-letter them.

    A row whose lease expired but which has already reached `max_attempts` is
    dead-lettered instead of recycled — this is the poison-message crash-loop
    guard: since `attempts` is now incremented at claim time (see `claim_batch`),
    a worker process dying mid-send still counts as an attempt, so a row can't
    loop claimed/expired forever without ever being dead-lettered.

    Args:
        conn (AsyncConnection | AsyncSession): database connection/session to
            execute through
        max_attempts (int): the maximum number of attempts from the retry policy

    Returns:
        int: number of rows reclaimed
    """
    result = cast("CursorResult[None]", await conn.execute(_reclaim_stmt(max_attempts)))
    return result.rowcount
