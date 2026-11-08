-- ADR 0063: the WorkBatch aggregate's persistence. One row per batch plus
-- one row per member snapshot (the pickability facts Form validated are
-- frozen at formation; the batch never re-reads the task pool). Additive:
-- with PICK_BATCH_MODE off (the fleet default) no row is ever written.
CREATE TABLE work_batches (
    id           TEXT PRIMARY KEY,
    station_id   TEXT        NOT NULL,
    cpt          TIMESTAMPTZ NOT NULL,
    status       TEXT        NOT NULL,
    version      BIGINT      NOT NULL,
    formed_at    TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    abandoned_at TIMESTAMPTZ
);

-- The one-open-batch-per-station formation guard (model.yaml invariant) is
-- a partial unique index, not an application check: two concurrent
-- formations for one station race in the application layer, and the loser
-- must fail on the write, exactly like the per-task claim compare-and-set.
CREATE UNIQUE INDEX work_batches_one_open_per_station
    ON work_batches (station_id)
    WHERE status = 'OPEN';

CREATE TABLE work_batch_members (
    batch_id     TEXT        NOT NULL REFERENCES work_batches (id) ON DELETE CASCADE,
    task_id      TEXT        NOT NULL,
    cpt          TIMESTAMPTZ NOT NULL,
    from_bin     TEXT        NOT NULL,
    hazard_class INT         NOT NULL,
    hint_rank    INT         NOT NULL,
    stop_seq     INT         NOT NULL,
    compartment  INT         NOT NULL,
    state        TEXT        NOT NULL,
    PRIMARY KEY (batch_id, task_id)
);

-- FindOpenByTaskId/FindByTaskId resolve an OPEN batch by member task in one
-- indexed read instead of a members scan.
CREATE INDEX work_batch_members_task
    ON work_batch_members (task_id);
