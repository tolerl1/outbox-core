"""Relay for polling, claiming, and dispatching outbox messages."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import timedelta
from typing import Literal

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from outbox.config import RelayConfig
from outbox.providers.protocol import MessageProvider
from outbox.relay.claim import claim_batch, reclaim_expired_leases
from outbox.relay.outcomes import mark_dead_letter, mark_delivered, schedule_retry
from outbox.types import ClaimedMessage, OutboundMessage, RelayCycleResult

logger = logging.getLogger(__name__)

Outcome = Literal["delivered", "failed", "dead_lettered"]


class Relay:
    """Poll, claim, and dispatch outbox rows to a MessageProvider.

    Manages the full delivery lifecycle: polling for pending messages, claiming
    them with leases, dispatching to a provider, and handling outcomes (deliver,
    retry, dead-letter). Owns retry/backoff/dead-lettering logic.

    Purging delivered rows is not this class's responsibility; see the README
    for the recommended purge query.

    Emits stdlib logging records under this module's logger name: claims and
    deliveries at DEBUG, retries at WARNING, dead-letters at ERROR with the
    triggering exception. Wire to a metrics/tracing stack via a handler or
    logging→OTel bridge.

    Attributes:
        _session_factory (async_sessionmaker[AsyncSession]): factory for
            creating database sessions
        _provider (MessageProvider): the transport provider
        _config (RelayConfig): relay configuration
        _worker_id (str): unique ID for this worker (auto-generated if None)
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider: MessageProvider,
        config: RelayConfig,
        worker_id: str | None = None,
    ) -> None:
        """Initialize a relay.

        Args:
            session_factory (async_sessionmaker[AsyncSession]): factory for
                creating database sessions
            provider (MessageProvider): the message provider to deliver to
            config (RelayConfig): relay configuration
            worker_id (str | None): unique worker ID; must be distinct across
                concurrent Relay instances. Auto-generated if None using a random hex suffix.
        """
        self._session_factory = session_factory
        self._provider = provider
        self._config = config
        self._worker_id = worker_id or f"relay-{uuid.uuid4().hex[:12]}"

    async def poll_once(self) -> RelayCycleResult:
        """Run one poll, claim, and dispatch cycle.

        Reclaims expired leases, claims a batch of pending messages, and
        dispatches them concurrently to the provider, handling outcomes
        (deliver, retry, dead-letter) for each one.

        Returns:
            RelayCycleResult: counts and timing for the cycle
        """
        start = time.monotonic()

        async with self._session_factory() as session, session.begin():
            await reclaim_expired_leases(
                session, max_attempts=self._config.retry_policy.max_attempts
            )

        async with self._session_factory() as session, session.begin():
            claimed = await claim_batch(
                session,
                batch_size=self._config.batch_size,
                lease_duration=self._config.lease_duration,
                worker_id=self._worker_id,
                topics=self._config.topics,
            )

        if claimed:
            logger.debug(
                "claimed %d outbox message(s)",
                len(claimed),
                extra={"outbox_claimed": len(claimed), "outbox_worker_id": self._worker_id},
            )

        delivered = failed = dead_lettered = 0
        if claimed:
            counts = {"delivered": 0, "failed": 0, "dead_lettered": 0}
            semaphore = asyncio.Semaphore(self._config.dispatch_concurrency)

            async def _run(message: ClaimedMessage) -> None:
                async with semaphore:
                    # Contain per-message failures: `_dispatch_one` already
                    # handles provider errors, so anything reaching here is an
                    # outcome *write* failing (a DB blip). Letting it propagate
                    # would cancel sibling sends mid-flight via the TaskGroup
                    # and abandon the rest of the batch — instead, count the
                    # message as failed and let its lease expiry redeliver it.
                    try:
                        outcome = await self._dispatch_one(message)
                    except Exception:
                        logger.error(
                            "outcome write failed for outbox message id=%s; the row stays "
                            "claimed and will be redelivered after its lease expires",
                            message.id,
                            exc_info=True,
                            extra={
                                "outbox_message_id": message.id,
                                "outbox_worker_id": self._worker_id,
                            },
                        )
                        outcome = "failed"
                counts[outcome] += 1

            async with asyncio.TaskGroup() as tg:
                for message in claimed:
                    tg.create_task(_run(message))

            delivered = counts["delivered"]
            failed = counts["failed"]
            dead_lettered = counts["dead_lettered"]

        return RelayCycleResult(
            claimed=len(claimed),
            delivered=delivered,
            failed=failed,
            dead_lettered=dead_lettered,
            duration=timedelta(seconds=time.monotonic() - start),
        )

    async def run_forever(self) -> None:
        """Run poll_once() in a loop with automatic resilience and backpressure.

        Catches exceptions from poll_once() and sleeps before retrying. Skips
        sleep when a full batch is claimed (backlog still being drained).
        """
        while True:
            claimed_full_batch = False
            try:
                result = await self.poll_once()
                claimed_full_batch = result.claimed >= self._config.batch_size
            except Exception:
                logger.error(
                    "outbox relay poll cycle failed, will retry after the poll interval",
                    exc_info=True,
                    extra={"outbox_worker_id": self._worker_id},
                )
            if not claimed_full_batch:
                await self._sleep(self._config.poll_interval)

    async def _sleep(self, duration: timedelta) -> None:
        """Sleep for a duration.

        Args:
            duration (timedelta): how long to sleep
        """
        await asyncio.sleep(duration.total_seconds())

    async def _dispatch_one(self, message: ClaimedMessage) -> Outcome:
        """Dispatch a single claimed message to the provider and record the outcome.

        Sends to the provider, then marks the row as delivered, failed, or
        dead-lettered based on the result. Returns the outcome key for result
        counting.

        Args:
            message (ClaimedMessage): the claimed message to dispatch

        Returns:
            Outcome: outcome key: "delivered", "failed", or "dead_lettered"
        """
        outbound = OutboundMessage(
            id=message.id,
            topic=message.topic,
            payload=message.payload,
            content_type=message.content_type,
            headers=message.headers,
            partition_key=message.partition_key,
        )
        # attempts is incremented by the claim query itself, so the RETURNING
        # value is already the count for this attempt — no +1 here.
        attempt = message.attempts

        try:
            await self._provider.send(outbound)
        except Exception as error:
            return await self._handle_failure(message.id, attempt, error)

        async with self._session_factory() as session, session.begin():
            applied = await mark_delivered(session, message.id, worker_id=self._worker_id)
        if not applied:
            self._log_fenced_out(message.id, "delivered")
        logger.debug(
            "delivered outbox message id=%s topic=%s attempt=%d",
            message.id,
            message.topic,
            attempt,
            extra={"outbox_message_id": message.id, "outbox_attempt": attempt},
        )
        return "delivered"

    async def _handle_failure(
        self, message_id: int, attempt: int, error: Exception
    ) -> Literal["failed", "dead_lettered"]:  # subset of Outcome
        """Handle a provider send failure by scheduling retry or dead-lettering.

        Args:
            message_id (int): ID of the failed message
            attempt (int): the attempt number that failed
            error (Exception): the exception from the provider

        Returns:
            Literal["failed", "dead_lettered"]: outcome key
        """
        if self._config.retry_policy.should_dead_letter(attempts=attempt):
            async with self._session_factory() as session, session.begin():
                applied = await mark_dead_letter(
                    session, message_id, error=error, worker_id=self._worker_id
                )
            if not applied:
                self._log_fenced_out(message_id, "dead_lettered")
            logger.error(
                "dead-lettered outbox message id=%s after %d attempt(s): %s",
                message_id,
                attempt,
                error,
                extra={"outbox_message_id": message_id, "outbox_attempt": attempt},
                exc_info=error,
            )
            return "dead_lettered"

        backoff = self._config.retry_policy.next_backoff(attempt)
        async with self._session_factory() as session, session.begin():
            applied = await schedule_retry(
                session, message_id, backoff=backoff, error=error, worker_id=self._worker_id
            )
        if not applied:
            self._log_fenced_out(message_id, "retried")
        logger.warning(
            "outbox message id=%s failed on attempt %d, retrying in %.1fs: %s",
            message_id,
            attempt,
            backoff.total_seconds(),
            error,
            extra={"outbox_message_id": message_id, "outbox_attempt": attempt},
        )
        return "failed"

    def _log_fenced_out(self, message_id: int, outcome: str) -> None:
        """Log that an outcome update was fenced out by lease reclamation.

        The row's outcome UPDATE didn't apply because this worker no longer
        held the claim — another worker reclaimed the expired lease and
        already handled it. Local outcome counters still count this message
        as `outcome` (what this worker observed); the persisted row state
        reflects what the other worker did.

        Args:
            message_id (int): ID of the fenced-out message
            outcome (str): the outcome this worker computed
        """
        logger.warning(
            "outbox message id=%s outcome=%s was fenced out — lease was reclaimed "
            "and handled by another worker",
            message_id,
            outcome,
            extra={"outbox_message_id": message_id, "outbox_worker_id": self._worker_id},
        )
