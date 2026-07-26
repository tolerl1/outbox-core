# Schema

Apply the migration before using this library. From a source checkout:

```
psql "$DATABASE_URL" -f src/outbox/migrations/sql/0001_initial.sql
```

From an installed package (no `src/` tree on disk), print the same DDL
through the package instead:

```
python -m outbox.migrations | psql "$DATABASE_URL"
```

or use `outbox.migrations.ddl() -> str` directly if you're wiring the schema
up from Python.

Or, if you use Alembic, include `outbox.schemas.metadata` in your own
`target_metadata` and run `alembic revision --autogenerate` — the table will
be picked up like any other model in your env.

## What ships

One table, `outbox_message`, and three partial indexes:

- `ix_outbox_pending` on `(available_at, id) WHERE status = 'pending'` —
  matches the claim query's `ORDER BY` exactly, so claiming stops after
  `batch_size` rows without a sort, and stays small no matter how many
  delivered/dead-letter rows accumulate between purges.
- `ix_outbox_claimed_lease` on `(lease_expires_at) WHERE status = 'claimed'`
  — supports the expired-lease reclaim that runs every poll cycle.
- `ix_outbox_partition_key_blocking` on `(partition_key, id) WHERE status IN
  ('pending', 'claimed') AND partition_key IS NOT NULL` — supports the claim
  query's per-key ordering check (see [Delivering](delivering.md#per-key-ordering)):
  a pending row with a non-null `partition_key` is only claimable once every
  earlier row (lower `id`) sharing that key has resolved to a terminal
  status. Scoped to unresolved statuses and non-null keys, so it stays small
  and costs nothing for the common `partition_key IS NULL` case.

Do not hand-roll a different schema — the claim query depends on the exact
column set and on all three indexes.
