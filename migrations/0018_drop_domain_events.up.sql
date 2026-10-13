-- ADR 0045: drop the legacy domain_events table. Created by 0001_init,
-- never read or written by any code; the owner approved the drop on
-- 2026-10-12 (ecosystem gap D23). Destructive by design: the table holds
-- no data any process wrote.
DROP TABLE IF EXISTS domain_events;
