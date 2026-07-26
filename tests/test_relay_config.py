from datetime import timedelta

import pytest
from pydantic import ValidationError

from outbox.config import RelayConfig
from outbox.retry import RetryPolicy


def make_retry_policy() -> RetryPolicy:
    return RetryPolicy(
        max_attempts=5, base_backoff=timedelta(seconds=1), max_backoff=timedelta(seconds=30)
    )


def test_defaults_are_internally_consistent() -> None:
    """Verify that default RelayConfig values are mutually consistent."""
    config = RelayConfig(retry_policy=make_retry_policy())

    assert config.lease_duration > config.poll_interval
    assert config.batch_size > 0
    assert config.topics is None
    assert config.dispatch_concurrency == 1


def test_rejects_non_positive_poll_interval() -> None:
    """Verify that zero or negative poll_interval values are rejected."""
    with pytest.raises(ValidationError, match="poll_interval"):
        RelayConfig(retry_policy=make_retry_policy(), poll_interval=timedelta(0))


def test_rejects_non_positive_batch_size() -> None:
    """Verify that zero or negative batch_size values are rejected."""
    with pytest.raises(ValidationError, match="batch_size"):
        RelayConfig(retry_policy=make_retry_policy(), batch_size=0)


def test_rejects_lease_duration_not_greater_than_poll_interval() -> None:
    """Verify that lease_duration must be greater than poll_interval."""
    with pytest.raises(ValidationError, match="lease_duration"):
        RelayConfig(
            retry_policy=make_retry_policy(),
            poll_interval=timedelta(seconds=10),
            lease_duration=timedelta(seconds=10),
        )


def test_config_is_frozen() -> None:
    """Verify that RelayConfig instances are immutable after creation."""
    config = RelayConfig(retry_policy=make_retry_policy())

    with pytest.raises(ValidationError):
        config.batch_size = 200


def test_rejects_non_positive_dispatch_concurrency() -> None:
    """Verify that zero or negative dispatch_concurrency values are rejected."""
    with pytest.raises(ValidationError, match="dispatch_concurrency"):
        RelayConfig(retry_policy=make_retry_policy(), dispatch_concurrency=0)


def test_rejects_an_empty_topics_list() -> None:
    """Verify that an empty topics list is rejected to prevent config-plumbing bugs."""
    with pytest.raises(ValidationError, match="topics"):
        RelayConfig(retry_policy=make_retry_policy(), topics=[])


def test_accepts_none_and_non_empty_topics() -> None:
    """Verify that topics can be None or a non-empty list."""
    assert RelayConfig(retry_policy=make_retry_policy(), topics=None).topics is None
    assert RelayConfig(retry_policy=make_retry_policy(), topics=["a"]).topics == ["a"]
