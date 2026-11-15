-- ADR 0064: the per-stop work clock. NULL (the only pre-existing shape)
-- means the work clock is the claim time: only the members of a formed
-- work batch ever get a value (the first member at its claim, each later
-- member when the previous stop completes), so every existing row and
-- every task outside a batch keeps exactly today's duration_seconds.
ALTER TABLE tasks
    ADD COLUMN work_started_at TIMESTAMPTZ;
