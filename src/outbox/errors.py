"""Exceptions raised by the outbox library."""


class OutboxError(Exception):
    """Base class for all errors raised by this library."""


class PayloadTooLargeError(OutboxError):
    """Raised when a payload exceeds the transport's size limit."""
