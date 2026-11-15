-- ADR 0062: a PICK task's plan carries the site it is worked at
-- (WorkReleased.data.pick.site_id, omit-when-unknown), next to the optional
-- pick face (pick_from_bin, migration 0023). Nullable and additive: every
-- existing row and every plan released without a site keeps NULL, which
-- rehydrates as "" — a plan that carries no placement, announced or not
-- exactly as before.
ALTER TABLE tasks
    ADD COLUMN pick_site_id TEXT;
