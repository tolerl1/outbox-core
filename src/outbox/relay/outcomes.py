"""Outcome handling for claimed messages."""

from __future__ import annotations

from datetime import timedelta
from typing import cast

from sqlalchemy import func, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.relay._sql import seconds_interval
from outbox.schemas import MessageStatus, outbox_message

_ERROR_MESSAGE_MAX_LENGTH = 2000


def _format_error(error: Exception) -> str:
    """Format an exception as a last_error column value.

    The exception type is kept (it's the most searchable part); the message is
    truncated. Note this column stores whatever the provider's exception says —
    see the README's retention section on keeping secrets out of provider error
    messages.

    Args:
        error (Exception): the exception to format

    Returns:
        str: formatted error message, truncated to _ERROR_MESSAGE_MAX_LENGTH
    """
    return f"{type(error).__name__}: {error}"[:_ERROR_MESSAGE_MAX_LENGTH]


def _fenced(message_id: int, worker_id: str):
    """Build an UPDATE statement that only applies if the row is still claimed.

    Fencing prevents a stale worker whose lease was reclaimed from clobbering
    whatever the new claimant has done to the row since.

    ABA note: the fence is (status = 'claimed' AND worker_id = :worker_id) with
    no per-claim token, so it cannot distinguish "still my original claim" from
    "the same worker re-claimed this row later". That is safe only because of a
    call-discipline invariant Relay upholds: poll_once() awaits every in-flight
    dispatch (TaskGroup) before returning, and cycles run serially, so a given
    worker_id never has outcome writes pending from an older claim generation
    when it claims again. Callers reusing these helpers outside Relay must
    preserve that invariant (or add a fencing token, e.g. compare claimed_at),
    and concurrent Relay instances must use distinct worker_ids — the default
    auto-generated ID guarantees this.

    Args:
        message_id (int): ID of the message to fence
        worker_id (str): expected worker ID that must hold the claim

    Returns:
        Update: a SQLAlchemy update statement with the fence condition
    """
    return (
        update(outbox_message)
        .where(
            outbox_message.c.id == message_id,
            outbox_message.c.status == MessageStatus.CLAIMED.value,
            outbox_message.c.worker_id == worker_id,
        )
        .values(updated_at=func.now())
    )


async def mark_delivered(session: AsyncSession, message_id: int, *, worker_id: str) -> bool:
    """Mark a claimed message as delivered.

    Fenced: only applies if the row is still claimed by worker_id — a stale
    worker whose lease was reclaimed can't clobber whatever the new claimant
    has done to the row since.

    Args:
        session (AsyncSession): database session to execute through
        message_id (int): ID of the message to mark delivered
        worker_id (str): ID of the worker that claimed this message

    Returns:
        bool: True if the update applied, False if fenced out
    """
    stmt = _fenced(message_id, worker_id).values(
        status=MessageStatus.DELIVERED.value,
        delivered_at=func.now(),
        claimed_at=None,
        lease_expires_at=None,
        worker_id=None,
    )
    result = cast("CursorResult[None]", await session.execute(stmt))
    return result.rowcount > 0


async def schedule_retry(
    session: AsyncSession,
    message_id: int,
    *,
    backoff: timedelta,
    error: Exception,
    worker_id: str,
) -> bool:
    """Schedule a claimed message for retry after a backoff delay.

    Fenced like mark_delivered — see its docstring. Does not touch attempts;
    that is incremented at claim time, not at outcome time.

    Args:
        session (AsyncSession): database session to execute through
        message_id (int): ID of the message to retry
        backoff (timedelta): delay before the message becomes claimable again
        error (Exception): the exception that triggered the retry
        worker_id (str): ID of the worker that claimed this message

    Returns:
        bool: True if the update applied, False if fenced out
    """
    stmt = _fenced(message_id, worker_id).values(
        status=MessageStatus.PENDING.value,
        available_at=func.now() + seconds_interval(backoff),
        claimed_at=None,
        lease_expires_at=None,
        worker_id=None,
        last_error=_format_error(error),
    )
    result = cast("CursorResult[None]", await session.execute(stmt))
    return result.rowcount > 0


async def mark_dead_letter(
    session: AsyncSession, message_id: int, *, error: Exception, worker_id: str
) -> bool:
    """Mark a claimed message as dead-lettered.

    Fenced like mark_delivered — see its docstring. Does not touch attempts;
    that is incremented at claim time, not at outcome time.

    Unlike other transitions, worker_id is deliberately kept: on a dead-letter
    row it records which worker made the final, fatal attempt.

    Args:
        session (AsyncSession): database session to execute through
        message_id (int): ID of the message to dead-letter
        error (Exception): the exception that triggered dead-lettering
        worker_id (str): ID of the worker that claimed this message

    Returns:
        bool: True if the update applied, False if fenced out
    """
    stmt = _fenced(message_id, worker_id).values(
        status=MessageStatus.DEAD_LETTER.value,
        claimed_at=None,
        lease_expires_at=None,
        last_error=_format_error(error),
    )
    result = cast("CursorResult[None]", await session.execute(stmt))
    return result.rowcount > 0
