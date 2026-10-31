ALTER TABLE packages
    DROP CONSTRAINT packages_container_complete,
    DROP COLUMN container_weight_g,
    DROP COLUMN container_height_mm,
    DROP COLUMN container_width_mm,
    DROP COLUMN container_length_mm,
    DROP COLUMN container_gtin,
    DROP COLUMN container_source;

ALTER TABLE pack_view
    DROP CONSTRAINT pack_view_dims_together,
    DROP COLUMN weight_g,
    DROP COLUMN height_mm,
    DROP COLUMN width_mm,
    DROP COLUMN length_mm;
