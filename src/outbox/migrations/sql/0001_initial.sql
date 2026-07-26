-- outbox-core 0001: initial outbox_message table.
-- Mirrors src/outbox/schemas.py exactly — keep both in sync if you edit this by hand.

CREATE TABLE outbox_message (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic            TEXT NOT NULL,
    partition_key    TEXT,
    content_type     TEXT NOT NULL DEFAULT 'application/json',
    payload          BYTEA NOT NULL,
    headers          JSONB NOT NULL DEFAULT '{}'::jsonb,
    status           TEXT NOT NULL DEFAULT 'pending'
                         CONSTRAINT ck_outbox_message_status
                         CHECK (status IN ('pending', 'claimed', 'delivered', 'dead_letter')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    available_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at       TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    worker_id        TEXT,
    last_error       TEXT,
    delivered_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The only index the claim query needs. (available_at, id) matches the claim
-- query's ORDER BY exactly, so LIMIT stops after batch_size rows without a
-- sort. Scoped to 'pending' so it stays small regardless of how many
-- delivered/dead_letter rows accumulate between purges.
CREATE INDEX ix_outbox_pending ON outbox_message (available_at, id)
    WHERE status = 'pending';

-- Supports reclaim_expired_leases(), which scans for claimed-but-abandoned
-- rows every poll cycle. Scoped to 'claimed' so it stays small for the same
-- reason ix_outbox_pending does.
CREATE INDEX ix_outbox_claimed_lease ON outbox_message (lease_expires_at)
    WHERE status = 'claimed';

-- Supports the claim query's per-partition-key NOT EXISTS blocking check:
-- a pending row is only claimable if no earlier row (lower id) sharing its
-- partition_key is still unresolved. Scoped to unresolved statuses and
-- non-null keys so it stays small and is a no-op for the common
-- partition_key IS NULL case.
CREATE INDEX ix_outbox_partition_key_blocking ON outbox_message (partition_key, id)
    WHERE status IN ('pending', 'claimed') AND partition_key IS NOT NULL;
