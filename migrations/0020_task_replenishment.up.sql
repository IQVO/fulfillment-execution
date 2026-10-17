-- ADR 0050: a REPLENISH task executes one pick-face replenishment move.
-- replenishment_* is the planned move stamped at release time
-- (WorkReleased.data.replenishment); replenished_* is the executed result
-- recorded at completion (the operator-confirmed quantity and the actual
-- bins), published on TaskCompleted together with request_ref. Nullable and
-- additive: every existing row and every non-REPLENISH task has NULL in all
-- of them; replenished_* stays NULL until a REPLENISH task is completed.
ALTER TABLE tasks
    ADD COLUMN replenishment_request_id TEXT,
    ADD COLUMN replenishment_sku        TEXT,
    ADD COLUMN replenishment_from_bin   TEXT,
    ADD COLUMN replenishment_to_bin     TEXT,
    ADD COLUMN replenishment_quantity   INTEGER,
    ADD COLUMN replenishment_priority   TEXT,
    ADD COLUMN replenished_quantity     INTEGER,
    ADD COLUMN replenished_from_bin     TEXT,
    ADD COLUMN replenished_to_bin       TEXT;
