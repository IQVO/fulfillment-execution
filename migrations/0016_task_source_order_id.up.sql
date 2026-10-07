-- ADR 0040 (audit decision 17): the upstream ORDER id a task's work was
-- released for (WorkReleased.ref on order-originated work), published as
-- TaskCompleted.order_ref. Distinct from order_ref above, which holds the
-- per-line work unit id. Nullable and additive: every existing row, and
-- every task released without a ref / for a transfer, has NULL, meaning
-- "no order id: order_ref is omitted from TaskCompleted".
ALTER TABLE tasks ADD COLUMN source_order_id TEXT;
