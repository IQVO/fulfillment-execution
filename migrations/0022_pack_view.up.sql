-- Local PackView (ADR 0054): product-master's pack levels keyed by 14-digit
-- GTIN, fed by the ProductPackLevelDeclared / ProductPackLevelRetired consumer
-- on warehouse.product-master.events and read by the pick scan confirmation.
-- A message is applied only when its version is greater than the stored one
-- (the version is the product aggregate's, so it only grows per GTIN). A
-- retired level keeps its row with effective_to set: its GTIN stays
-- resolvable for packs already on the shelf. Dedupe of CloudEvents ids reuses
-- processed_events (migration 0002).
CREATE TABLE pack_view (
    gtin           TEXT PRIMARY KEY CHECK (gtin ~ '^[0-9]{14}$'),
    sku            TEXT NOT NULL,
    level          TEXT NOT NULL CHECK (level IN ('EACH', 'INNER', 'CASE', 'PALLET')),
    units_per_pack INTEGER NOT NULL CHECK (units_per_pack >= 1),
    child_gtin     TEXT CHECK (child_gtin ~ '^[0-9]{14}$'),
    version        BIGINT NOT NULL CHECK (version >= 1),
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to   TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX pack_view_sku_level ON pack_view (sku, level);
