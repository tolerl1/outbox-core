#!/usr/bin/env python3
"""Throughput microbenchmark for Relay.poll_once().

Not part of the test suite or CI — a standalone script for measuring your
own batch_size/dispatch_concurrency trade-offs on your own hardware, against
a real Postgres. See docs/benchmarks.md for methodology, what this does and
doesn't measure, and why the numbers it prints are not portable to your
production environment.

Destructive: like the integration suite, this drops and recreates the
outbox_message table on every run. Point OUTBOX_TEST_DATABASE_URL at a
scratch Postgres, never at anything holding real data.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

from sqlalchemy import insert, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from outbox import MessageProvider, OutboundMessage, Relay, RelayConfig, RetryPolicy
from outbox.schemas import outbox_message

_MIGRATION_SQL_PATH = (
    Path(__file__).parent.parent / "src" / "outbox" / "migrations" / "sql" / "0001_initial.sql"
)
_ASYNCPG_SCHEME = re.compile(r"^postgresql(\+\w+)?://")

_MISSING_DATABASE_URL = """\
OUTBOX_TEST_DATABASE_URL is not set. Point this at a scratch Postgres — the
same one the integration suite uses, and just as destructively:

  docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=outbox postgres:18
  export OUTBOX_TEST_DATABASE_URL=postgresql://postgres:outbox@localhost:5432/postgres
"""

_COLUMNS = ("batch_size", "concurrency", "delivered", "seconds", "msgs/sec")
_WIDTHS = (10, 11, 9, 8, 9)


@dataclass(slots=True)
class _LatencyProvider:
    """MessageProvider that sleeps to simulate a transport round-trip.

    Never raises, so every message this benchmark claims is expected to be
    delivered on the first attempt — a failure/dead-letter during a run means
    something is wrong with the benchmark itself, not the transport.

    Attributes:
        latency_seconds (float): simulated per-message send latency
    """

    latency_seconds: float

    async def send(self, message: OutboundMessage) -> None:
        """Simulate a transport send by sleeping for the configured latency.

        Args:
            message (OutboundMessage): unused; present to satisfy the protocol
        """
        del message
        if self.latency_seconds > 0:
            await asyncio.sleep(self.latency_seconds)


@dataclass(slots=True)
class _SqsProvider:
    """MessageProvider that sends to a real SQS queue via boto3.

    boto3 is synchronous, so send() offloads each call to a thread rather
    than blocking the event loop — a real async SQS-backed provider would do
    the same via its own thread pool or an async SDK. Requires `boto3`
    (`uv sync --group bench`); imported lazily so `--provider latency` (the
    default) never needs it installed.

    Attributes:
        queue_url (str): the SQS queue to send benchmark messages to
    """

    queue_url: str
    _client: object = field(init=False, repr=False, default=None)

    def __post_init__(self) -> None:
        """Build the boto3 SQS client, raising a clear error if boto3 is missing.

        Raises:
            RuntimeError: if boto3 isn't installed
        """
        try:
            import boto3
        except ImportError as error:
            raise RuntimeError(
                "boto3 is required for --provider sqs: uv sync --group bench"
            ) from error
        self._client = boto3.client("sqs")

    async def send(self, message: OutboundMessage) -> None:
        """Send a message to SQS, decoding the payload as UTF-8 text.

        The benchmark's own seeded payload is always UTF-8 JSON, so a plain
        text body is sufficient here — a provider for arbitrary binary
        payloads would need to base64-encode instead.

        Args:
            message (OutboundMessage): the message to send

        Raises:
            botocore.exceptions.ClientError: on any SQS-side failure
        """
        await asyncio.to_thread(
            self._client.send_message,
            QueueUrl=self.queue_url,
            MessageBody=message.payload.decode("utf-8"),
        )


def _as_asyncpg_url(url: str) -> str:
    """Convert a connection URL to use the asyncpg driver.

    Args:
        url (str): database connection URL

    Returns:
        str: URL with the postgresql+asyncpg:// scheme
    """
    return _ASYNCPG_SCHEME.sub("postgresql+asyncpg://", url, count=1)


async def _reset_schema(engine: AsyncEngine) -> None:
    """Drop and recreate outbox_message from the shipped migration SQL.

    Args:
        engine (AsyncEngine): engine to run the reset through
    """
    sql_script = _MIGRATION_SQL_PATH.read_text(encoding="utf-8")
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS outbox_message CASCADE"))
        # asyncpg can't run a multi-statement string through SQLAlchemy's
        # text() (it prepares statements one at a time), so drop to the raw
        # asyncpg connection, which runs the whole script as one simple query.
        raw_connection = await conn.get_raw_connection()
        driver_connection = raw_connection.driver_connection
        assert driver_connection is not None
        await driver_connection.execute(sql_script)


async def _seed(engine: AsyncEngine, rows: int) -> None:
    """Bulk-insert pending rows directly, bypassing OutboxWriter's per-row cost.

    This benchmark measures the relay's claim/dispatch/outcome-write path,
    not the writer's single-row insert — so seeding takes the fast path.

    Args:
        engine (AsyncEngine): engine to insert through
        rows (int): number of pending rows to create
    """
    payload = b'{"bench":true}'
    async with engine.begin() as conn:
        await conn.execute(
            insert(outbox_message),
            [
                {"topic": "bench", "payload": payload, "content_type": "application/json"}
                for _ in range(rows)
            ],
        )


async def _drain(
    session_factory: async_sessionmaker[AsyncSession],
    config: RelayConfig,
    provider: MessageProvider,
) -> tuple[float, int]:
    """Run poll_once() until the backlog is empty.

    Args:
        session_factory (async_sessionmaker[AsyncSession]): session factory for the Relay
        config (RelayConfig): relay configuration under test
        provider (MessageProvider): provider under test, shared across the whole matrix

    Returns:
        tuple[float, int]: wall-clock seconds elapsed and messages delivered

    Raises:
        RuntimeError: if any message fails or dead-letters — max_attempts is
            1, so a single real send failure means the provider (or its
            credentials/network reachability) needs checking, not a retry
    """
    relay = Relay(session_factory, provider, config)
    delivered = 0
    start = time.perf_counter()
    while True:
        result = await relay.poll_once()
        delivered += result.delivered
        if result.failed or result.dead_lettered:
            raise RuntimeError(
                f"send failure during benchmark: {result!r} — check the provider's "
                "reachability/credentials (max_attempts=1, so this isn't a transient retry)"
            )
        if result.claimed == 0:
            break
    elapsed = time.perf_counter() - start
    return elapsed, delivered


async def _run_matrix(
    database_url: str,
    rows: int,
    batch_sizes: list[int],
    concurrencies: list[int],
    provider: MessageProvider,
) -> list[tuple[int, int, int, float, float]]:
    """Run the full (batch_size x dispatch_concurrency) matrix.

    Args:
        database_url (str): asyncpg connection URL
        rows (int): pending rows to seed before each run
        batch_sizes (list[int]): batch_size values to test
        concurrencies (list[int]): dispatch_concurrency values to test
        provider (MessageProvider): provider under test, built once and
            reused across every combination (matches how a real Relay uses
            one long-lived provider instance rather than one per cycle)

    Returns:
        list[tuple[int, int, int, float, float]]: rows of
            (batch_size, dispatch_concurrency, delivered, elapsed_seconds, msgs_per_sec)
    """
    # +2 headroom over the highest dispatch_concurrency under test: each
    # in-flight outcome write borrows a pool connection (see RelayConfig's
    # dispatch_concurrency docstring), and a starved pool would silently
    # serialize sends and understate the benchmarked concurrency.
    pool_size = max(concurrencies) + 2
    engine = create_async_engine(database_url, pool_size=pool_size, max_overflow=0)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    results: list[tuple[int, int, int, float, float]] = []
    try:
        for batch_size in batch_sizes:
            for concurrency in concurrencies:
                await _reset_schema(engine)
                await _seed(engine, rows)
                config = RelayConfig(
                    retry_policy=RetryPolicy(
                        max_attempts=1,
                        base_backoff=timedelta(seconds=1),
                        max_backoff=timedelta(seconds=1),
                    ),
                    poll_interval=timedelta(milliseconds=1),
                    batch_size=batch_size,
                    lease_duration=timedelta(seconds=60),
                    dispatch_concurrency=concurrency,
                )
                elapsed, delivered = await _drain(session_factory, config, provider)
                rate = delivered / elapsed if elapsed > 0 else float("inf")
                results.append((batch_size, concurrency, delivered, elapsed, rate))
    finally:
        await engine.dispose()
    return results


def _print_results(results: list[tuple[int, int, int, float, float]]) -> None:
    """Print the results matrix as a plain-text table.

    Args:
        results (list[tuple[int, int, int, float, float]]): rows from _run_matrix
    """
    header = "  ".join(name.rjust(width) for name, width in zip(_COLUMNS, _WIDTHS, strict=True))
    print(header)
    print("-" * len(header))
    for batch_size, concurrency, delivered, elapsed, rate in results:
        values = (
            str(batch_size),
            str(concurrency),
            str(delivered),
            f"{elapsed:.2f}",
            f"{rate:.1f}",
        )
        print("  ".join(value.rjust(width) for value, width in zip(values, _WIDTHS, strict=True)))


def _parse_int_list(value: str) -> list[int]:
    """Parse a comma-separated list of positive integers.

    Args:
        value (str): comma-separated string, e.g. "50,100,200"

    Returns:
        list[int]: parsed integers

    Raises:
        argparse.ArgumentTypeError: if any value isn't a positive integer
    """
    try:
        parsed = [int(v.strip()) for v in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"expected comma-separated integers, got {value!r}"
        ) from error
    if any(v <= 0 for v in parsed):
        raise argparse.ArgumentTypeError("all values must be positive")
    return parsed


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        argparse.Namespace: parsed arguments
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows", type=int, default=5000, help="pending rows seeded per run (default: 5000)"
    )
    parser.add_argument(
        "--batch-sizes",
        type=_parse_int_list,
        default=[50, 100, 200],
        help="comma-separated batch_size values (default: 50,100,200)",
    )
    parser.add_argument(
        "--concurrency",
        type=_parse_int_list,
        default=[1, 5, 20],
        help="comma-separated dispatch_concurrency values (default: 1,5,20)",
    )
    parser.add_argument(
        "--send-latency-ms",
        type=float,
        default=0.0,
        help="simulated provider send latency in milliseconds; ignored for "
        "--provider sqs (default: 0)",
    )
    parser.add_argument(
        "--provider",
        choices=["latency", "sqs"],
        default="latency",
        help="provider to benchmark: 'latency' (default) simulates a transport "
        "with --send-latency-ms; 'sqs' sends real messages to --sqs-queue-url",
    )
    parser.add_argument(
        "--sqs-queue-url",
        default=None,
        help="required when --provider sqs; the queue to send benchmark messages to",
    )
    return parser.parse_args()


def _build_provider(args: argparse.Namespace) -> MessageProvider:
    """Build the MessageProvider selected by --provider.

    Args:
        args (argparse.Namespace): parsed arguments

    Returns:
        MessageProvider: the provider to benchmark

    Raises:
        SystemExit: if --provider sqs is chosen without --sqs-queue-url
    """
    if args.provider == "sqs":
        if not args.sqs_queue_url:
            print("--sqs-queue-url is required when --provider sqs", file=sys.stderr)
            raise SystemExit(1)
        return _SqsProvider(args.sqs_queue_url)
    return _LatencyProvider(args.send_latency_ms / 1000)


def main() -> None:
    """Parse args, run the benchmark matrix against a real Postgres, and print results."""
    args = _parse_args()

    database_url = os.environ.get("OUTBOX_TEST_DATABASE_URL")
    if not database_url:
        print(_MISSING_DATABASE_URL, file=sys.stderr)
        raise SystemExit(1)

    provider = _build_provider(args)
    print(
        f"rows={args.rows} batch_sizes={args.batch_sizes} concurrency={args.concurrency} "
        f"provider={args.provider} send_latency_ms={args.send_latency_ms}\n"
    )
    results = asyncio.run(
        _run_matrix(
            _as_asyncpg_url(database_url),
            args.rows,
            args.batch_sizes,
            args.concurrency,
            provider,
        )
    )
    _print_results(results)


if __name__ == "__main__":
    main()
