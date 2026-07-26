# API Reference

This page is maintained by hand against `src/outbox/` — Zensical has no
docstring-extraction plugin yet. If anything here disagrees with the
docstrings in the source, the source is canonical.

Scope: every symbol exported from the root `outbox` package (`outbox.__all__`,
enforced by `tests/test_public_api.py`), grouped the way `src/outbox/__init__.py`
groups them, plus the `outbox.migrations` entry points documented in the
README. Internal helpers (`outbox.relay.claim`, `outbox.relay.outcomes`,
`outbox.relay._sql`) are not covered — see [Versioning](versioning.md) for
what "public" means here.

## Writer

### `outbox.OutboxWriter`

```python
class OutboxWriter:
    def __init__(self, table: Table = outbox_message) -> None
```

Enqueue messages into the transactional outbox. Inserts one outbox row per
call into the caller's existing transaction. Never manages the transaction
itself — the caller's session must already be inside one.

**`__init__`**

- `table` (`Table`): the `outbox_message` table to insert into; defaults to
  the library's defined table (injectable for testing).

#### `enqueue`

```python
async def enqueue(self, session: AsyncSession, message: OutboxMessage) -> int
```

Insert an outbox message into the database.

Serializes dict payloads to JSON with `application/json` content-type;
passes bytes payloads as-is. Rejects dict payloads containing non-finite
floats (NaN/Infinity) at enqueue time rather than persisting invalid JSON.

`enqueue()` never manages a transaction — it inserts into the caller's
session and returns; commit is the caller's job.

- **Args**
    - `session` (`AsyncSession`): the async database session inside a transaction.
    - `message` (`OutboxMessage`): the message to enqueue.
- **Returns**: `int` — the ID of the inserted outbox row.
- **Raises**: `ValueError` — if a dict payload contains NaN or Infinity.

## Message types

### `outbox.OutboxMessage`

```python
@dataclass(slots=True)
class OutboxMessage:
    topic: str
    payload: bytes | dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict[str, str])
    partition_key: str | None = None
    content_type: str | None = None
```

Represents a message a caller enqueues for later delivery.

- `topic` (`str`): routing key or subject; identifies the message stream.
- `payload` (`bytes | dict[str, Any]`): message body; dict is serialized to
  JSON with `application/json` content-type.
- `headers` (`dict[str, str]`, default `{}`): optional headers to pass to the
  provider.
- `partition_key` (`str | None`, default `None`): optional key for sharding,
  or for ordering: messages sharing a non-null `partition_key` are never
  claimed concurrently and are claimed oldest-`id`-first among that key's
  committed pending rows — matching enqueue order for the common case of
  non-overlapping same-key writes, see
  [Delivering](delivering.md#per-key-ordering) for the precise guarantee
  under concurrent same-key writers; a `None` `partition_key` carries no
  ordering guarantee, as before.
- `content_type` (`str | None`, default `None`): MIME type for bytes
  payloads; ignored for dict payloads (always `application/json`).

**`__post_init__` validation**: raises `ValueError` if `topic` is empty or
contains only whitespace.

### `outbox.OutboundMessage`

```python
@dataclass(slots=True)
class OutboundMessage:
    id: int
    topic: str
    payload: bytes
    content_type: str
    headers: dict[str, str]
    partition_key: str | None
```

The delivery-time view of a claimed outbox row, passed to a
`MessageProvider.send()`.

- `id` (`int`): outbox row's stable ID; use for deduplication since delivery
  is at-least-once.
- `topic` (`str`): message's routing key.
- `payload` (`bytes`): serialized message body.
- `content_type` (`str`): MIME type of the payload.
- `headers` (`dict[str, str]`): optional headers from the outbox row.
- `partition_key` (`str | None`): optional sharding key; if non-null, this
  message was claimed only after all earlier same-key messages resolved to a
  terminal status.

No fields have defaults; no `__post_init__` validation.

### `outbox.ClaimedMessage`

```python
@dataclass(slots=True)
class ClaimedMessage:
    id: int
    topic: str
    payload: bytes
    content_type: str
    headers: dict[str, str]
    partition_key: str | None
    attempts: int
```

Represents a claimed outbox row before translation to `OutboundMessage`.
Internal type holding the raw claim query result, including retry attempt
count for backoff and dead-lettering logic.

- `id` (`int`): outbox row ID.
- `topic` (`str`): message's routing key.
- `payload` (`bytes`): serialized message body.
- `content_type` (`str`): MIME type of the payload.
- `headers` (`dict[str, str]`): message headers.
- `partition_key` (`str | None`): optional sharding/ordering key; see
  [Delivering](delivering.md#per-key-ordering) for the ordering guarantee
  this implies.
- `attempts` (`int`): number of times this message has been claimed.

No fields have defaults; no `__post_init__` validation.

### `outbox.RelayCycleResult`

```python
@dataclass(slots=True)
class RelayCycleResult:
    claimed: int
    delivered: int
    failed: int
    dead_lettered: int
    duration: timedelta
```

Summarizes results from one `Relay.poll_once()` cycle.

- `claimed` (`int`): number of pending messages claimed in this cycle.
- `delivered` (`int`): number of messages successfully delivered.
- `failed` (`int`): number of messages that failed but will be retried.
- `dead_lettered` (`int`): number of messages abandoned after max attempts.
- `duration` (`timedelta`): wall-clock time spent in `poll_once()`.

No fields have defaults; no `__post_init__` validation.

## Relay

### `outbox.Relay`

```python
class Relay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider: MessageProvider,
        config: RelayConfig,
        worker_id: str | None = None,
    ) -> None
```

Polls, claims, and dispatches outbox rows to a `MessageProvider`. Manages the
full delivery lifecycle: polling for pending messages, claiming them with
leases, dispatching to a provider, and handling outcomes (deliver, retry,
dead-letter). Owns retry/backoff/dead-lettering logic.

Purging delivered rows is not this class's responsibility; see
[Operations](operations.md) for the recommended purge query.

Emits stdlib `logging` records under this module's logger name: claims and
deliveries at `DEBUG`, retries at `WARNING`, dead-letters at `ERROR` with the
triggering exception. Wire to a metrics/tracing stack via a handler or
logging→OTel bridge.

**`__init__` Args**

- `session_factory` (`async_sessionmaker[AsyncSession]`): factory for
  creating database sessions.
- `provider` (`MessageProvider`): the message provider to deliver to.
- `config` (`RelayConfig`): relay configuration.
- `worker_id` (`str | None`, default `None`): unique worker ID; must be
  distinct across concurrent `Relay` instances. Auto-generated if `None`
  using a random hex suffix.

#### `poll_once`

```python
async def poll_once(self) -> RelayCycleResult
```

Runs one poll, claim, and dispatch cycle.

Reclaims expired leases, claims a batch of pending messages, and dispatches
them concurrently to the provider, handling outcomes (deliver, retry,
dead-letter) for each one.

- **Returns**: `RelayCycleResult` — counts and timing for the cycle.

#### `run_forever`

```python
async def run_forever(self) -> None
```

Runs `poll_once()` in a loop with automatic resilience and backpressure.

Catches exceptions from `poll_once()` and sleeps before retrying. Skips sleep
when a full batch is claimed (backlog still being drained).

### `outbox.RelayConfig`

```python
class RelayConfig(BaseModel):
    retry_policy: RetryPolicy
    topics: list[str] | None = None
    poll_interval: timedelta = timedelta(seconds=5)
    batch_size: int = 100
    lease_duration: timedelta = timedelta(seconds=30)
    dispatch_concurrency: int = 1
```

Configures relay polling, batching, concurrency, and backoff. A pydantic
`BaseModel` with `model_config = ConfigDict(frozen=True,
arbitrary_types_allowed=True)` — all fields are immutable after construction.
The validated settings object consumers assemble from environment variables
or config files.

- `retry_policy` (`RetryPolicy`, required): exponential backoff and
  dead-letter rules.
- `topics` (`list[str] | None`, default `None`): topics to claim; `None`
  claims all topics, empty list is rejected as a config plumbing bug.
- `poll_interval` (`timedelta`, default `timedelta(seconds=5)`): sleep
  between consecutive `poll_once()` cycles in `run_forever()`; skipped
  entirely while full batches are being claimed, so it only paces the
  idle/partial case.
- `batch_size` (`int`, default `100`): maximum number of messages to claim
  per cycle.
- `lease_duration` (`timedelta`, default `timedelta(seconds=30)`): duration a
  worker holds a claimed message; clock starts at claim time for the whole
  batch; size against `batch_size / dispatch_concurrency × p99 send latency`
  with headroom — expiry mid-dispatch causes tail messages to be reclaimed by
  other workers and redelivered, burning attempts without real failure.
- `dispatch_concurrency` (`int`, default `1`): maximum number of claimed
  messages delivered to the provider concurrently per poll cycle; `1`
  (default) dispatches sequentially; raise to overlap provider round-trips
  within a cycle; no ordering is guaranteed either way across different or
  absent partition keys, though same-key messages are never claimed
  concurrently regardless of this setting (see
  [Delivering](delivering.md#per-key-ordering)); each in-flight outcome
  write borrows a pool connection.

**Validation rules**:

- `topics` must be `None` (claim all topics) or a non-empty list — an empty
  list raises `ValueError`.
- `poll_interval` must be positive (`> timedelta(0)`) — otherwise raises
  `ValueError`.
- `batch_size` must be positive (`> 0`) — otherwise raises `ValueError`.
- `dispatch_concurrency` must be `>= 1` — otherwise raises `ValueError`.
- `lease_duration` must be strictly greater than `poll_interval` (model-level
  validator, runs after field validation) — otherwise raises `ValueError`.

### `outbox.RetryPolicy`

```python
@dataclass(slots=True)
class RetryPolicy:
    max_attempts: int
    base_backoff: timedelta
    max_backoff: timedelta
    backoff_multiplier: float = 2.0
    jitter: bool = False
```

Defines exponential backoff and dead-lettering for failed deliveries.

- `max_attempts` (`int`): maximum number of times to claim a message; after
  exhaustion, the message is dead-lettered (must be `>= 1`).
- `base_backoff` (`timedelta`): initial delay before the first retry.
- `max_backoff` (`timedelta`): cap on backoff duration; must be `>=
  base_backoff`.
- `backoff_multiplier` (`float`, default `2.0`): exponential growth factor
  per attempt.
- `jitter` (`bool`, default `False`): add randomness to backoff to avoid
  thundering herd.

**`__post_init__` validation**: raises `ValueError` if `max_attempts < 1`,
`base_backoff <= timedelta(0)`, `max_backoff < base_backoff`, or
`backoff_multiplier < 1.0`.

#### `next_backoff`

```python
def next_backoff(
    self, attempt: int, *, rand: Callable[[], float] = random.random
) -> timedelta
```

Computes the delay before retry number `attempt`.

- **Args**
    - `attempt` (`int`): 1-indexed retry attempt number.
    - `rand` (`Callable[[], float]`, default `random.random`): random source
      for full jitter; injectable for testing.
- **Returns**: `timedelta` — exponential backoff capped at `max_backoff`,
  scaled by `rand()` when `jitter` is enabled.

#### `should_dead_letter`

```python
def should_dead_letter(self, *, attempts: int) -> bool
```

Checks whether a message should be dead-lettered.

- **Args**: `attempts` (`int`): number of times the message has been claimed.
- **Returns**: `bool` — `True` if `attempts >= max_attempts`, `False` otherwise.

## Providers

### `outbox.MessageProvider`

```python
@runtime_checkable
class MessageProvider(Protocol):
    async def send(self, message: OutboundMessage) -> None: ...
```

`MessageProvider` is a `typing.Protocol` (decorated `@runtime_checkable`) —
there is nothing to subclass, only a shape to match. Any change to this
protocol's shape is a breaking release, no exceptions, per
[Versioning](versioning.md) — every downstream provider implements it.

Implementations send a single message to their underlying transport. The
`Relay` owns retry/backoff/dead-lettering logic, so `send()` must be a
**single attempt** that raises on failure — do not implement retry loops or
swallow transport exceptions in the provider.

#### `send`

```python
async def send(self, message: OutboundMessage) -> None
```

Sends a message to the transport.

- **Args**: `message` (`OutboundMessage`): the message to deliver.
- **Raises**:
    - `outbox.errors.PayloadTooLargeError` — if the payload exceeds what the
      transport can carry.
    - `Exception` — any transport-specific failure; the Relay will retry or
      dead-letter based on its retry policy.

### `outbox.InMemoryProvider`

```python
@dataclass(slots=True)
class InMemoryProvider:
    sent: list[OutboundMessage] = field(default_factory=list[OutboundMessage])
```

Buffers sent messages in memory for test assertions. A reference
`MessageProvider` that collects delivered messages for inspection during
integration testing.

- `sent` (`list[OutboundMessage]`, default `[]`): list of all messages
  delivered via `send()`.

No `__post_init__` validation.

#### `send`

```python
async def send(self, message: OutboundMessage) -> None
```

Appends a message to the in-memory buffer.

- **Args**: `message` (`OutboundMessage`): the message to buffer.

## Errors

### `outbox.OutboxError`

```python
class OutboxError(Exception): ...
```

Base class for all errors raised by this library.

### `outbox.PayloadTooLargeError`

```python
class PayloadTooLargeError(OutboxError): ...
```

Raised when a payload exceeds the transport's size limit. Providers raise
this from `send()`; the core itself enforces no size limit since it's
transport-agnostic.

## Migrations

Not part of `outbox.__all__`, but public entry points documented in the
README and covered by the same versioning policy as the rest of the schema
surface (see [Versioning](versioning.md) and [Schema](schema.md)).

### `outbox.migrations.ddl`

```python
def ddl() -> str
```

Returns the full schema DDL for the current migration.

Reads the shipped SQL file via `importlib.resources`, so this works from a
pip install too — not just a source checkout where
`src/outbox/migrations/sql/` is on disk relative to the repo root.

- **Returns**: `str` — the contents of the current migration's SQL file.

### `python -m outbox.migrations`

CLI wrapper around `ddl()` (`src/outbox/migrations/__main__.py`): prints the
DDL to stdout with no trailing newline added beyond what the file already
has (`print(ddl(), end="")`), so it composes with `psql`:

```
python -m outbox.migrations | psql "$DATABASE_URL"
```

## Internal, not covered by versioning

`outbox.relay.claim`, `outbox.relay.outcomes`, and `outbox.relay._sql` are
internal implementation modules — the claim query, fenced outcome writers,
and shared SQL helpers. They're not exported from the root package, aren't
covered by [Versioning](versioning.md), and can change shape without notice.
