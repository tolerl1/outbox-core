# Benchmarks

`benchmarks/benchmark.py` measures how `Relay.poll_once()` throughput
changes across `batch_size` and `dispatch_concurrency` combinations, on
whatever Postgres you point it at. It is not part of the test suite or CI,
and it ships no numbers of its own — run it against your own hardware
before trusting any figure it prints.

## Running it

Needs the same real, already-running Postgres as the integration suite —
nothing is spun up for you, and the script drops and recreates
`outbox_message` on every run, exactly like `tests/conftest.py` does:

```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=outbox postgres:18
export OUTBOX_TEST_DATABASE_URL=postgresql://postgres:outbox@localhost:5432/postgres
uv run python benchmarks/benchmark.py
```

Never point `OUTBOX_TEST_DATABASE_URL` at anything holding real data.

Flags (all optional):

```bash
uv run python benchmarks/benchmark.py \
  --rows 5000 \
  --batch-sizes 50,100,200 \
  --concurrency 1,5,20 \
  --send-latency-ms 20
```

- `--rows`: pending rows seeded before each combination is timed.
- `--batch-sizes`, `--concurrency`: comma-separated values swept as
  `RelayConfig.batch_size` / `RelayConfig.dispatch_concurrency` — see their
  docstrings and [Delivering](delivering.md#concurrency-and-lease-sizing)
  for what each controls.
- `--send-latency-ms`: an `asyncio.sleep` inserted into a fake provider's
  `send()`, standing in for a transport round-trip. `0` (the default)
  measures the relay's own DB-bound ceiling with no transport in the way;
  a nonzero value shows how raising `dispatch_concurrency` overlaps that
  latency instead of serializing behind it.

Output is a plain-text table: `batch_size`, `concurrency`, `delivered`,
wall-clock `seconds`, and `msgs/sec` for each combination.

## What it measures — and what it doesn't

The timed portion is exactly one thing: repeated `poll_once()` cycles
(reclaim expired leases, claim a batch, dispatch to the fake provider,
write outcomes) until a pre-seeded backlog is fully drained. Seeding itself
(a bulk `INSERT`, not `OutboxWriter.enqueue()`) happens before the clock
starts, because `OutboxWriter` is a single-row insert dominated by your own
application's transaction — not something this library has a throughput
ceiling to report on.

It does **not** measure:

- **A real transport.** `--send-latency-ms` is a sleep, not a network call,
  a broker's own throttling, or serialization cost. Swap in your actual
  `MessageProvider` if you want a number that includes it.
- **Multiple relay workers.** This script runs one `Relay` in one process.
  Horizontal scaling (more worker processes against the same table via
  `SKIP LOCKED` — see [Operations](operations.md#deployment-and-shutdown))
  is additive on top of whatever ceiling one process hits here, up to
  Postgres's own capacity — this script doesn't model that add.
- **A loaded table.** The table is emptied and reseeded before every run.
  A production table also carries a retained `delivered`/`dead_letter`
  backlog between purges; the shipped indexes keep the claim path
  unaffected by that backlog's size (see [Schema](schema.md#what-ships)),
  but this script doesn't reproduce a large retained backlog to confirm it
  on your Postgres version and settings.

## Example run

These are real output from this script, not illustrative placeholders —
but they're included only to show the table's shape and what the
`dispatch_concurrency` effect looks like, not as a number to compare
yourself against. See the next section for why. Environment: a single-node
Postgres 16 on the machine that wrote this doc, no containerization, no
concurrent load, one relay process.

`uv run python benchmarks/benchmark.py` (defaults: 5000 rows,
`--send-latency-ms 0` — no simulated transport, so this is the relay's own
DB-bound ceiling):

```
batch_size  concurrency  delivered   seconds   msgs/sec
---------------------------------------------------------
        50            1       5000      9.64      518.6
        50            5       5000      5.27      948.2
        50           20       5000      4.83     1035.0
       100            1       5000     10.76      464.6
       100            5       5000      5.10      979.8
       100           20       5000      4.00     1249.4
       200            1       5000     10.58      472.6
       200            5       5000      4.97     1005.5
       200           20       5000      4.15     1206.2
```

`uv run python benchmarks/benchmark.py --rows 2000 --batch-sizes 100 --concurrency 1,5,20 --send-latency-ms 20`
(a simulated 20ms transport round-trip — this is the shape that matters for
tuning `dispatch_concurrency` against a real broker's latency):

```
batch_size  concurrency  delivered   seconds   msgs/sec
---------------------------------------------------------
       100            1       2000     46.38       43.1
       100            5       2000     10.39      192.4
       100           20       2000      4.03      496.5
```

At `dispatch_concurrency=1`, throughput is capped near `1000ms / 20ms ≈ 50`
messages/sec regardless of batch size — every send blocks the next.
Raising it to `20` overlaps up to 20 round-trips at once and gets over 10x
the throughput on the same workload. That qualitative relationship (higher
concurrency helps more, the slower your real transport is) is the useful
takeaway here — the absolute `msgs/sec` figures are not.

## Why the numbers won't match anyone else's

This is the deliberate design of the script, not a caveat to work around:
it is a relative tool for comparing configurations *on one environment*,
not a source of absolute numbers to publish or compare across machines.
Everything below moves the result, often by multiples, not percentages:

- **Hardware and storage.** CPU, memory, and — especially — disk I/O
  latency for WAL fsyncs dominate write-heavy workloads like this one. A
  laptop's NVMe, a cloud block volume, and `tmpfs`-backed CI container all
  give meaningfully different numbers for the identical query.
- **Where Postgres runs relative to the script.** Localhost, a Docker
  bridge network, and a real network hop to a managed database each add
  their own latency to every round trip, and this benchmark makes many.
- **Postgres configuration.** `synchronous_commit`, `fsync`,
  `shared_buffers`, `max_connections`, and autovacuum settings all affect
  this workload; a stock `postgres:18` container image is tuned for
  neither a laptop nor a production instance.
- **Connection pool sizing.** The script sizes its own pool to
  `max(concurrency) + 2`; your application's pool (and how much of it this
  relay competes for against everything else you run) will differ.
- **What else is running.** A shared CI runner, a laptop with a browser
  open, and a dedicated benchmark host are not the same machine even when
  the specs match on paper.

Use this script to answer *"does raising `dispatch_concurrency` help my
workload, and by how much, here?"* — not *"is this library fast?"* in the
abstract. Re-run it after any change to `batch_size`, `dispatch_concurrency`,
or `lease_duration` you're considering, on the environment you're actually
tuning for, and compare the runs to each other rather than to numbers from
a README or a blog post (including this one).
