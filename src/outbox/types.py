"""Domain types and data classes for the outbox pattern."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any


@dataclass(slots=True)
class OutboxMessage:
    """Represent a message a caller enqueues for later delivery.

    Attributes:
        topic (str): routing key or subject; identifies the message stream
        payload (bytes | dict[str, Any]): message body; dict is serialized
            to JSON with application/json content-type
        headers (dict[str, str]): optional headers to pass to the provider
        partition_key (str | None): optional key for sharding, or for
            ordering: messages sharing a non-null partition_key are never
            claimed concurrently and are claimed oldest-id-first among that
            key's committed pending rows - matching enqueue order for the
            common case of non-overlapping same-key writes (see
            claim_batch and docs/delivering.md#per-key-ordering for the
            precise guarantee under concurrent same-key writers); a null
            partition_key (the default) is unaffected and carries no
            ordering guarantee, as before
        content_type (str | None): MIME type for bytes payloads; ignored
            for dict payloads (always application/json)
    """

    topic: str
    payload: bytes | dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict[str, str])
    partition_key: str | None = None
    content_type: str | None = None

    def __post_init__(self) -> None:
        """Validate that topic is a non-empty string.

        Raises:
            ValueError: if topic is empty or contains only whitespace
        """
        if not self.topic or not self.topic.strip():
            raise ValueError("topic must be a non-empty string")


@dataclass(slots=True)
class OutboundMessage:
    """Represent a message ready to deliver to a provider.

    The delivery-time view of a claimed outbox row, passed to a
    MessageProvider.send().

    Attributes:
        id (int): outbox row's stable ID; use for deduplication since
            delivery is at-least-once
        topic (str): message's routing key
        payload (bytes): serialized message body
        content_type (str): MIME type of the payload
        headers (dict[str, str]): optional headers from the outbox row
        partition_key (str | None): optional sharding key; if non-null, this
            message was claimed only after all earlier same-key messages
            resolved, so same-key messages reach the provider in order
    """

    id: int
    topic: str
    payload: bytes
    content_type: str
    headers: dict[str, str]
    partition_key: str | None


@dataclass(slots=True)
class ClaimedMessage:
    """Represent a claimed outbox row before translation to OutboundMessage.

    Internal type holding the raw claim query result, including retry attempt
    count for backoff and dead-lettering logic.

    Attributes:
        id (int): outbox row ID
        topic (str): message's routing key
        payload (bytes): serialized message body
        content_type (str): MIME type of the payload
        headers (dict[str, str]): message headers
        partition_key (str | None): optional sharding/ordering key; see
            claim_batch for the per-key ordering guarantee this implies
        attempts (int): number of times this message has been claimed
    """

    id: int
    topic: str
    payload: bytes
    content_type: str
    headers: dict[str, str]
    partition_key: str | None
    attempts: int


@dataclass(slots=True)
class RelayCycleResult:
    """Summarize results from one Relay.poll_once() cycle.

    Attributes:
        claimed (int): number of pending messages claimed in this cycle
        delivered (int): number of messages successfully delivered
        failed (int): number of messages that failed but will be retried
        dead_lettered (int): number of messages abandoned after max attempts
        duration (timedelta): wall-clock time spent in poll_once()
    """

    claimed: int
    delivered: int
    failed: int
    dead_lettered: int
    duration: timedelta
