"""Protocol for message delivery providers."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from outbox.types import OutboundMessage


@runtime_checkable
class MessageProvider(Protocol):
    """Deliver messages to a transport of the consumer's choice.

    Implementations send a single message to their underlying transport.
    The Relay owns retry/backoff/dead-lettering logic, so send() must be a
    single attempt that raises on failure — do not implement retry loops or
    swallow transport exceptions in the provider.
    """

    async def send(self, message: OutboundMessage) -> None:
        """Send a message to the transport.

        Args:
            message (OutboundMessage): the message to deliver

        Raises:
            outbox.errors.PayloadTooLargeError: if the payload exceeds what
                the transport can carry
            Exception: any transport-specific failure; the Relay will retry
                or dead-letter based on its retry policy
        """
        ...
