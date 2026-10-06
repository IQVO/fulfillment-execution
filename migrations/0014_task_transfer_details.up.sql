-- Inter-warehouse-transfer correlation (transfer-task-facts ADR): a task
-- released for a network-inventory-planning transfer carries the transfer's
-- correlation block. All columns nullable — every existing task row (and
-- every non-transfer task created after this migration) has NULL
-- transfer_ref, meaning "not transfer work: no transfer fact on completion".
ALTER TABLE tasks ADD COLUMN transfer_ref TEXT;
ALTER TABLE tasks ADD COLUMN demand_id TEXT;
ALTER TABLE tasks ADD COLUMN work_kind TEXT;
ALTER TABLE tasks ADD COLUMN site_id TEXT;
ALTER TABLE tasks ADD COLUMN sku TEXT;
ALTER TABLE tasks ADD COLUMN quantity INTEGER;
