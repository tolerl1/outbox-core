"""Retry and backoff policies for message delivery."""

from __future__ import annotations

import random as _random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta


@dataclass(slots=True)
class RetryPolicy:
    """Define exponential backoff and dead-lettering for failed deliveries.

    Attributes:
        max_attempts (int): maximum number of times to claim a message; after
            exhaustion, the message is dead-lettered (>= 1)
        base_backoff (timedelta): initial delay before the first retry
        max_backoff (timedelta): cap on backoff duration; must be >=
            base_backoff
        backoff_multiplier (float): exponential growth factor per attempt
            (default 2.0)
        jitter (bool): add randomness to backoff to avoid thundering herd
            (default False)
    """

    max_attempts: int
    base_backoff: timedelta
    max_backoff: timedelta
    backoff_multiplier: float = 2.0
    jitter: bool = False

    def __post_init__(self) -> None:
        """Validate backoff and attempt parameters.

        Raises:
            ValueError: if max_attempts < 1, base_backoff <= 0, max_backoff <
                base_backoff, or backoff_multiplier < 1.0
        """
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_backoff <= timedelta(0):
            raise ValueError("base_backoff must be positive")
        if self.max_backoff < self.base_backoff:
            raise ValueError("max_backoff must be >= base_backoff")
        if self.backoff_multiplier < 1.0:
            raise ValueError("backoff_multiplier must be >= 1.0")

    def next_backoff(
        self, attempt: int, *, rand: Callable[[], float] = _random.random
    ) -> timedelta:
        """Compute the delay before retry number `attempt`.

        Args:
            attempt (int): 1-indexed retry attempt number
            rand (Callable[[], float]): random source for full jitter;
                injectable for testing (default random.random)

        Returns:
            timedelta: exponential backoff capped at max_backoff, scaled by
                rand() when jitter is enabled
        """
        base_secs = self.base_backoff.total_seconds()
        max_secs = self.max_backoff.total_seconds()
        try:
            uncapped_secs = base_secs * (self.backoff_multiplier ** (attempt - 1))
        except OverflowError:
            uncapped_secs = float("inf")
        capped_secs = min(uncapped_secs, max_secs)
        capped = timedelta(seconds=capped_secs)
        if not self.jitter:
            return capped
        return capped * rand()

    def should_dead_letter(self, *, attempts: int) -> bool:
        """Check whether a message should be dead-lettered.

        Args:
            attempts (int): number of times the message has been claimed

        Returns:
            bool: True if attempts >= max_attempts, False otherwise
        """
        return attempts >= self.max_attempts
