-- ADR 0054: a PICK task released with data.pick carries its pick plan (SKU,
-- planned eaches, the EACH/CASE unit the operator scans, the optional source
-- bin) and the eaches confirmed by scans so far; a BREAK_CASE task carries
-- the sealed case to break (lpn, sku, the loose bin, the EACH pick that found
-- only the case). Nullable and additive: every existing row and every other
-- task has NULL in all of them.
ALTER TABLE tasks
    ADD COLUMN pick_sku             TEXT,
    ADD COLUMN pick_quantity        INTEGER,
    ADD COLUMN pick_unit            TEXT,
    ADD COLUMN pick_from_bin        TEXT,
    ADD COLUMN picked_eaches        INTEGER,
    ADD COLUMN break_lpn            TEXT,
    ADD COLUMN break_sku            TEXT,
    ADD COLUMN break_loose_bin      TEXT,
    ADD COLUMN break_pick_task_id   TEXT;
