from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any

import pytest

from outbox.config import RelayConfig
from outbox.providers.in_memory import InMemoryProvider
from outbox.relay.dispatcher import Relay
from outbox.retry import RetryPolicy
from outbox.types import RelayCycleResult


class _RaiseOnceRelay(Relay):
    """A Relay whose poll_once() raises on its first call and then succeeds,
    so run_forever()'s resilience (catch, log, sleep, continue) can be
    exercised without a real database."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.call_count = 0
        self._raised = False

    async def poll_once(self) -> RelayCycleResult:
        self.call_count += 1
        if not self._raised:
            self._raised = True
            raise RuntimeError("simulated transient poll failure")
        return RelayCycleResult(
            claimed=0, delivered=0, failed=0, dead_lettered=0, duration=timedelta()
        )


def _make_config() -> RelayConfig:
    return RelayConfig(
        retry_policy=RetryPolicy(
            max_attempts=3, base_backoff=timedelta(seconds=1), max_backoff=timedelta(seconds=1)
        ),
        poll_interval=timedelta(milliseconds=10),
    )


async def test_run_forever_survives_a_poll_once_exception_and_keeps_looping(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify that run_forever catches poll_once exceptions and continues looping."""
    relay = _RaiseOnceRelay(None, InMemoryProvider(), _make_config())  # type: ignore[arg-type]

    with caplog.at_level(logging.ERROR, logger="outbox.relay.dispatcher"):
        task = asyncio.create_task(relay.run_forever())
        try:
            for _ in range(50):
                if relay.call_count >= 2:
                    break
                await asyncio.sleep(0.01)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    assert relay.call_count >= 2, "run_forever should not have died after the first exception"

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(error_records) == 1
    assert "poll cycle failed" in error_records[0].message
    assert error_records[0].exc_info is not None


class _ScriptedRelay(Relay):
    """A Relay whose poll_once() returns a scripted sequence of claim counts
    and whose _sleep() records calls, so run_forever()'s pacing (skip the
    sleep after a full batch, sleep otherwise) can be asserted without a DB."""

    def __init__(self, *args: Any, claim_counts: list[int], **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._claim_counts = claim_counts
        self.polls = 0
        self.sleeps = 0
        self.first_sleep = asyncio.Event()

    async def poll_once(self) -> RelayCycleResult:
        claimed = self._claim_counts[min(self.polls, len(self._claim_counts) - 1)]
        self.polls += 1
        return RelayCycleResult(
            claimed=claimed, delivered=claimed, failed=0, dead_lettered=0, duration=timedelta()
        )

    async def _sleep(self, duration: timedelta) -> None:
        self.sleeps += 1
        self.first_sleep.set()
        # Park forever; the test cancels the run_forever task once it has
        # observed the first sleep.
        await asyncio.Event().wait()


async def test_run_forever_skips_the_sleep_while_full_batches_are_claimed() -> None:
    """Verify run_forever skips sleep between full batches for faster backlog draining."""
    config = _make_config()  # batch_size defaults to 100
    relay = _ScriptedRelay(
        None,  # type: ignore[arg-type]
        InMemoryProvider(),
        config,
        claim_counts=[config.batch_size, config.batch_size, 3],
    )

    task = asyncio.create_task(relay.run_forever())
    try:
        await asyncio.wait_for(relay.first_sleep.wait(), timeout=1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # Two full-batch cycles ran back-to-back with no sleep in between; only
    # the third (partial) cycle slept.
    assert relay.polls == 3
    assert relay.sleeps == 1
