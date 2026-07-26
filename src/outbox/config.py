"""Configuration for relay polling and delivery."""

from __future__ import annotations

from datetime import timedelta

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from outbox.retry import RetryPolicy


class RelayConfig(BaseModel):
    """Configure relay polling, batching, concurrency, and backoff.

    The validated settings object consumers assemble from environment variables
    or config files. All fields are immutable (frozen).

    Attributes:
        retry_policy (RetryPolicy): exponential backoff and dead-letter rules
        topics (list[str] | None): topics to claim; None claims all topics,
            empty list is rejected as a config plumbing bug
        poll_interval (timedelta): sleep between consecutive poll_once()
            cycles in run_forever(); skipped entirely while full batches are
            being claimed, so it only paces the idle/partial case
            (default 5 seconds)
        batch_size (int): maximum number of messages to claim per cycle
            (default 100)
        lease_duration (timedelta): duration a worker holds a claimed message;
            clock starts at claim time for the whole batch; size against
            batch_size / dispatch_concurrency x p99 send latency with
            headroom; expiry mid-dispatch causes tail messages to be reclaimed
            by other workers and redelivered, burning attempts without real
            failure (default 30 seconds)
        dispatch_concurrency (int): maximum number of claimed messages delivered
            to the provider concurrently per poll cycle; 1 (default) dispatches
            sequentially; raise to overlap provider round-trips within a cycle;
            no ordering is guaranteed either way across different or absent
            partition keys, though same-key messages are never claimed
            concurrently regardless of this setting (see claim_batch), so
            raising it doesn't parallelize a single key's throughput; each
            in-flight outcome write borrows a pool connection
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    retry_policy: RetryPolicy
    topics: list[str] | None = None
    poll_interval: timedelta = timedelta(seconds=5)
    batch_size: int = 100
    lease_duration: timedelta = timedelta(seconds=30)
    dispatch_concurrency: int = 1

    @field_validator("topics")
    @classmethod
    def _topics_must_be_none_or_non_empty(cls, value: list[str] | None) -> list[str] | None:
        """Reject an empty topics list as likely a config plumbing bug.

        Args:
            value (list[str] | None): the topics value to validate

        Returns:
            list[str] | None: value unchanged

        Raises:
            ValueError: if value is an empty list
        """
        if value is not None and not value:
            raise ValueError("topics must be None (claim all topics) or a non-empty list")
        return value

    @field_validator("poll_interval")
    @classmethod
    def _poll_interval_must_be_positive(cls, value: timedelta) -> timedelta:
        """Reject zero or negative poll_interval.

        Args:
            value (timedelta): the poll_interval value to validate

        Returns:
            timedelta: value unchanged

        Raises:
            ValueError: if value <= 0
        """
        if value <= timedelta(0):
            raise ValueError("poll_interval must be positive")
        return value

    @field_validator("batch_size")
    @classmethod
    def _batch_size_must_be_positive(cls, value: int) -> int:
        """Reject zero or negative batch_size.

        Args:
            value (int): the batch_size value to validate

        Returns:
            int: value unchanged

        Raises:
            ValueError: if value <= 0
        """
        if value <= 0:
            raise ValueError("batch_size must be positive")
        return value

    @field_validator("dispatch_concurrency")
    @classmethod
    def _dispatch_concurrency_must_be_positive(cls, value: int) -> int:
        """Reject zero or negative dispatch_concurrency.

        Args:
            value (int): the dispatch_concurrency value to validate

        Returns:
            int: value unchanged

        Raises:
            ValueError: if value < 1
        """
        if value < 1:
            raise ValueError("dispatch_concurrency must be >= 1")
        return value

    @model_validator(mode="after")
    def _lease_duration_must_exceed_poll_interval(self) -> RelayConfig:
        """Enforce that lease_duration > poll_interval.

        Returns:
            RelayConfig: self unchanged

        Raises:
            ValueError: if lease_duration <= poll_interval
        """
        if self.lease_duration <= self.poll_interval:
            raise ValueError("lease_duration must be greater than poll_interval")
        return self
