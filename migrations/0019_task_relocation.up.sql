-- ADR 0047 (slotting-optimization ADR 0010, decision E1): a RELOCATE task
-- executes one slot-plan move. relocation_* is the planned move stamped at
-- release time (WorkReleased.data.relocation); relocated_* is the executed
-- result recorded at completion (the operator-confirmed quantity and the
-- actual bins), published on TaskCompleted. Nullable and additive: every
-- existing row and every non-RELOCATE task has NULL in all of them;
-- relocated_* stays NULL until a RELOCATE task is completed.
ALTER TABLE tasks
    ADD COLUMN relocation_plan_id  TEXT,
    ADD COLUMN relocation_move_seq INTEGER,
    ADD COLUMN relocation_kind     TEXT,
    ADD COLUMN relocation_sku      TEXT,
    ADD COLUMN relocation_from_bin TEXT,
    ADD COLUMN relocation_to_bin   TEXT,
    ADD COLUMN relocated_quantity  INTEGER,
    ADD COLUMN relocated_from_bin  TEXT,
    ADD COLUMN relocated_to_bin    TEXT;
