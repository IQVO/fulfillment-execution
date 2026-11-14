DROP INDEX IF EXISTS idx_tasks_source_order_id;
ALTER TABLE tasks
    DROP COLUMN order_site_id;
