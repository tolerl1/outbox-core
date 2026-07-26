"""In-memory MessageProvider for testing."""

from __future__ import annotations

from dataclasses import dataclass, field

from outbox.types import OutboundMessage


@dataclass(slots=True)
class InMemoryProvider:
    """Buffer sent messages in memory for test assertions.

    A reference MessageProvider that collects delivered messages for
    inspection during integration testing.

    Attributes:
        sent (list[OutboundMessage]): list of all messages delivered via send()
    """

    sent: list[OutboundMessage] = field(default_factory=list[OutboundMessage])

    async def send(self, message: OutboundMessage) -> None:
        """Append a message to the in-memory buffer.

        Args:
            message (OutboundMessage): the message to buffer
        """
        self.sent.append(message)
