"""Transactional outbox pattern for SQLAlchemy and Postgres.

Provides at-least-once message delivery with transactional safety: enqueue
outbox rows in the same transaction as domain data, then a Relay claims and
delivers them asynchronously to a pluggable MessageProvider.
"""

from __future__ import annotations

from outbox.config import RelayConfig
from outbox.errors import OutboxError, PayloadTooLargeError
from outbox.providers.in_memory import InMemoryProvider
from outbox.providers.protocol import MessageProvider
from outbox.relay.dispatcher import Relay
from outbox.retry import RetryPolicy
from outbox.types import ClaimedMessage, OutboundMessage, OutboxMessage, RelayCycleResult
from outbox.writer import OutboxWriter

# Grouped by role (writer, message types, relay, providers, errors) rather than
# alphabetically — that's a more useful reading order for this public surface.
__all__ = [  # noqa: RUF022
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
]
