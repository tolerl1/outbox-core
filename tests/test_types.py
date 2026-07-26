import pytest

from outbox.types import OutboxMessage


def test_outbox_message_defaults_are_independent_per_instance() -> None:
    """Verify that modifying one OutboxMessage's headers doesn't affect another."""
    a = OutboxMessage(topic="orders.created", payload=b"{}")
    b = OutboxMessage(topic="orders.created", payload=b"{}")

    a.headers["x-request-id"] = "abc"

    assert b.headers == {}
    assert a.partition_key is None


def test_outbox_message_is_slotted() -> None:
    """Verify that OutboxMessage uses slots to prevent arbitrary attribute assignment."""
    message = OutboxMessage(topic="t", payload=b"{}")

    assert not hasattr(message, "__dict__")


def test_outbox_message_rejects_empty_topic() -> None:
    """Verify that an empty topic raises ValueError at construction time."""
    with pytest.raises(ValueError, match="topic"):
        OutboxMessage(topic="", payload=b"{}")


def test_outbox_message_rejects_whitespace_only_topic() -> None:
    """Verify that a whitespace-only topic raises ValueError at construction time."""
    with pytest.raises(ValueError, match="topic"):
        OutboxMessage(topic="   ", payload=b"{}")
