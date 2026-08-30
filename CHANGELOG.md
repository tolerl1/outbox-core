# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project follows [Semantic Versioning](https://semver.org/) with
`0.x`-specific rules - see [VERSIONING.md](./VERSIONING.md) for what counts
as a breaking change while the public API and schema are still moving.

## [Unreleased]

### Added

- Documented an optional `(updated_at) WHERE status IN ('delivered',
  'dead_letter')` index for deployments whose purge job is slow at scale,
  with the write-cost trade-off and cadence guidance for when it's worth
  adding (`docs/operations.md`).

### Changed

- Bumped the `sqlalchemy[asyncio]` requirement from `>=2.0` to `>=2.0.51`.
- Bumped the `uv_build` requirement from `>=0.8.17,<0.9.0` to
  `>=0.11.32,<0.12.0`.

## [0.1.0] - 2026-07-26

### Added

- Initial release of `outbox-core`.
- `OutboxWriter.enqueue()` - inserts an outbox row into the caller's own
  transaction; never manages the transaction itself.
- `Relay` - polls, claims (`FOR UPDATE SKIP LOCKED` + leases), and dispatches
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
