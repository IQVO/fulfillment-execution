-- ADR 0065: the fulfillment site an ORDER named, carried on the task that
-- works it. Order work is released with the order's site
-- (WorkReleased.site_id, omit-when-unknown); the PICK task stores it and the
-- rebin PACK task inherits it, so PackageManifested can publish site_id.
-- Nullable and additive: every existing row, transfer task and release
-- without a site keeps NULL, which rehydrates as "" — a task that carries no
-- site, published exactly as before. Distinct from site_id (the transfer
-- block's site, migration 0014) and pick_site_id (the pick plan's site,
-- migration 0025).
ALTER TABLE tasks
    ADD COLUMN order_site_id TEXT;

-- The rebin PACK task finds the order's PICK tasks by the bare order id they
-- were released for. order_ref (idx_tasks_order_ref) holds the per-line work
-- unit id, so that lookup needs its own index; partial, because the column is
-- NULL for every transfer, relocation and REST-created task.
CREATE INDEX IF NOT EXISTS idx_tasks_source_order_id
    ON tasks (source_order_id) WHERE source_order_id IS NOT NULL;
