# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project follows [Semantic Versioning](https://semver.org/) with
`0.x`-specific rules — see [VERSIONING.md](./VERSIONING.md) for what counts
as a breaking change while the public API and schema are still moving.

## [Unreleased]

### Added

- `benchmarks/benchmark.py` — a standalone throughput microbenchmark
  sweeping `RelayConfig.batch_size` / `dispatch_concurrency` combinations
  against a real Postgres, with methodology and hardware-variance caveats
  in `docs/benchmarks.md`. Not part of the test suite or CI.
- `benchmarks/benchmark.py --provider sqs` — an optional real SQS-backed
  `MessageProvider` (via `boto3`, `uv sync --group bench`) so the benchmark
  can measure a real transport instead of simulated latency. Pairs with the
  new companion [outbox-core-bench-infra](https://github.com/tolerl1/outbox-core-bench-infra)
  repo, which provisions RDS + an EC2 client + the SQS queue.

### Changed

- Bumped the `sqlalchemy[asyncio]` requirement from `>=2.0` to `>=2.0.51`.
- Bumped the `uv_build` requirement from `>=0.8.17,<0.9.0` to
  `>=0.11.32,<0.12.0`.

## [0.1.0] - 2026-07-26

### Added

- Initial release of `outbox-core`.
- `OutboxWriter.enqueue()` — inserts an outbox row into the caller's own
  transaction; never manages the transaction itself.
- `Relay` — polls, claims (`FOR UPDATE SKIP LOCKED` + leases), and dispatches
  outbox rows to a pluggable `MessageProvider`, with retry/backoff and
  dead-lettering via `RetryPolicy`.
- At-least-once delivery, with per-`partition_key` ordering: messages
  sharing a non-null `partition_key` are never claimed concurrently (see
  `specs/0001-partition-key-ordering.md`).
- `MessageProvider` protocol and an `InMemoryProvider` reference
  implementation for tests.
- Initial schema migration (`src/outbox/migrations/sql/0001_initial.sql`)
  and the `python -m outbox.migrations` CLI to print the DDL.

[Unreleased]: https://github.com/tolerl1/outbox-core/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/tolerl1/outbox-core/releases/tag/v0.1.0
