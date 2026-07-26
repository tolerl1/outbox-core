from datetime import timedelta

import pytest

from outbox.retry import RetryPolicy


def make_policy(**overrides: object) -> RetryPolicy:
    defaults: dict[str, object] = {
        "max_attempts": 5,
        "base_backoff": timedelta(seconds=1),
        "max_backoff": timedelta(seconds=30),
        "backoff_multiplier": 2.0,
        "jitter": False,
    }
    defaults.update(overrides)
    return RetryPolicy(**defaults)  # type: ignore[arg-type]


def test_first_attempt_backoff_equals_base_backoff() -> None:
    """Verify that the first attempt's backoff equals the base backoff duration."""
    policy = make_policy()

    assert policy.next_backoff(attempt=1) == timedelta(seconds=1)


def test_backoff_grows_exponentially_without_jitter() -> None:
    """Verify that backoff duration grows exponentially based on the multiplier."""
    policy = make_policy()

    assert policy.next_backoff(attempt=1) == timedelta(seconds=1)
    assert policy.next_backoff(attempt=2) == timedelta(seconds=2)
    assert policy.next_backoff(attempt=3) == timedelta(seconds=4)
    assert policy.next_backoff(attempt=4) == timedelta(seconds=8)


def test_backoff_is_capped_at_max_backoff() -> None:
    """Verify that backoff duration never exceeds the configured maximum."""
    policy = make_policy(max_backoff=timedelta(seconds=5))

    assert policy.next_backoff(attempt=10) == timedelta(seconds=5)


def test_backoff_is_monotonic_non_decreasing_until_the_cap() -> None:
    """Verify that backoff duration always increases or stays the same until capped."""
    policy = make_policy()

    backoffs = [policy.next_backoff(attempt=n) for n in range(1, 8)]

    assert backoffs == sorted(backoffs)


def test_jitter_scales_backoff_between_zero_and_the_uncapped_value() -> None:
    """Verify that jitter scales backoff between 0 and the full exponential value."""
    policy = make_policy(jitter=True)

    zero_jitter = policy.next_backoff(attempt=3, rand=lambda: 0.0)
    full_jitter = policy.next_backoff(attempt=3, rand=lambda: 1.0)

    assert zero_jitter == timedelta(seconds=0)
    assert full_jitter == timedelta(seconds=4)


def test_jitter_still_respects_the_cap() -> None:
    """Verify that jitter never causes backoff to exceed the maximum cap."""
    policy = make_policy(max_backoff=timedelta(seconds=5), jitter=True)

    full_jitter = policy.next_backoff(attempt=10, rand=lambda: 1.0)

    assert full_jitter == timedelta(seconds=5)


@pytest.mark.parametrize(
    "attempts,max_attempts,expected", [(1, 5, False), (4, 5, False), (5, 5, True), (6, 5, True)]
)
def test_should_dead_letter_once_attempts_reach_max(
    attempts: int, max_attempts: int, expected: bool
) -> None:
    """Verify that dead-lettering is triggered when attempts reach the maximum."""
    policy = make_policy(max_attempts=max_attempts)

    assert policy.should_dead_letter(attempts=attempts) is expected


def test_rejects_non_positive_max_attempts() -> None:
    """Verify that zero or negative max_attempts values are rejected."""
    with pytest.raises(ValueError, match="max_attempts"):
        make_policy(max_attempts=0)


def test_rejects_max_backoff_smaller_than_base_backoff() -> None:
    """Verify that max_backoff cannot be less than base_backoff."""
    with pytest.raises(ValueError, match="max_backoff"):
        make_policy(base_backoff=timedelta(seconds=10), max_backoff=timedelta(seconds=1))


def test_rejects_backoff_multiplier_below_one() -> None:
    """Verify that backoff_multiplier values below 1.0 are rejected."""
    with pytest.raises(ValueError, match="backoff_multiplier"):
        make_policy(backoff_multiplier=0.5)


def test_huge_attempt_numbers_stay_capped_without_overflow() -> None:
    """Verify that exponential growth past float's range still returns the cap."""
    policy = make_policy(max_attempts=1_000)

    assert policy.next_backoff(attempt=500) == timedelta(seconds=30)
    assert policy.next_backoff(attempt=2000) == timedelta(seconds=30)
