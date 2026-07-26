from __future__ import annotations

import outbox


def test_root_package_exports_the_full_public_api() -> None:
    """Verify that the root outbox package exports all expected public symbols."""
    expected = {
        "OutboxWriter",
        "OutboxMessage",
        "OutboundMessage",
        "ClaimedMessage",
        "RelayCycleResult",
        "Relay",
        "RelayConfig",
        "RetryPolicy",
        "MessageProvider",
        "InMemoryProvider",
        "OutboxError",
        "PayloadTooLargeError",
    }

    assert set(outbox.__all__) == expected
    for name in expected:
        assert hasattr(outbox, name), f"outbox.{name} is not importable from the root package"


def test_scaffold_hello_stub_is_gone() -> None:
    """Verify that scaffold code has been removed from the public API."""
    assert not hasattr(outbox, "hello")
