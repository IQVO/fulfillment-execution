-- ADR 0041 (decision 18, per-line confirm-pick): the 1-based order LINE a
-- task's work was released for (WorkReleased.line_no on order-originated
-- work), published as TaskCompleted.line_no next to order_ref. Nullable and
-- additive: every existing row, and every task released without a line_no /
-- for a transfer, has NULL, meaning "unknown: line_no is omitted from
-- TaskCompleted".
ALTER TABLE tasks ADD COLUMN source_line_no INTEGER;
