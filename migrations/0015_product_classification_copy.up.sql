-- Local copy of product-master's classifications (ADR-0039), fed by the
-- ProductClassified consumer on warehouse.product-master.events and read by
-- SealPackage's ProductClassificationLookup. One row per SKU; a message is
-- applied only when its version is greater than the stored one.
-- temperature_class and dot_hazard_class are NULL when the producer omitted
-- them. Dedupe of CloudEvents ids reuses processed_events (migration 0002).
CREATE TABLE product_classification_copy (
    sku               TEXT PRIMARY KEY,
    handling_tags     TEXT[] NOT NULL DEFAULT '{}',
    temperature_class TEXT,
    dot_hazard_class  INTEGER CHECK (dot_hazard_class BETWEEN 1 AND 9),
    version           BIGINT NOT NULL CHECK (version >= 1),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
