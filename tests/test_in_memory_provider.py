from outbox.providers.in_memory import InMemoryProvider
from outbox.providers.protocol import MessageProvider
from outbox.types import OutboundMessage


def test_in_memory_provider_satisfies_the_message_provider_protocol() -> None:
    """Verify that InMemoryProvider implements the MessageProvider protocol."""
    assert isinstance(InMemoryProvider(), MessageProvider)


def _outbound(message_id: int, topic: str, payload: bytes) -> OutboundMessage:
    return OutboundMessage(
        id=message_id,
        topic=topic,
        payload=payload,
        content_type="application/octet-stream",
        headers={},
        partition_key=None,
    )


async def test_send_buffers_the_message() -> None:
    """Verify that sent messages are buffered for inspection."""
    provider = InMemoryProvider()
    message = _outbound(1, "orders.created", b"{}")

    await provider.send(message)

    assert provider.sent == [message]


async def test_sent_messages_preserve_send_order() -> None:
    """Verify that sent messages maintain FIFO order."""
    provider = InMemoryProvider()
    first = _outbound(1, "a", b"1")
    second = _outbound(2, "b", b"2")

    await provider.send(first)
    await provider.send(second)

    assert provider.sent == [first, second]
