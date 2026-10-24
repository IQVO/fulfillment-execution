-- Ship-in-own-container (ADR 0056).
--
-- pack_view keeps the DECLARED dimensions (mm) and weight (g) that
-- ProductPackLevelDeclared carries in its optional `dims`. All four are NULL
-- when product-master declared none (and for every row written before this
-- migration, until the level is re-declared); they are set together or not at
-- all.
ALTER TABLE pack_view
    ADD COLUMN length_mm INTEGER CHECK (length_mm IS NULL OR length_mm >= 1),
    ADD COLUMN width_mm  INTEGER CHECK (width_mm  IS NULL OR width_mm  >= 1),
    ADD COLUMN height_mm INTEGER CHECK (height_mm IS NULL OR height_mm >= 1),
    ADD COLUMN weight_g  INTEGER CHECK (weight_g  IS NULL OR weight_g  >= 1),
    ADD CONSTRAINT pack_view_dims_together CHECK (
        (length_mm IS NULL) = (width_mm IS NULL)
        AND (length_mm IS NULL) = (height_mm IS NULL)
        AND (length_mm IS NULL) = (weight_g IS NULL));

-- packages record the container they ship in. container_source is NULL for an
-- ordinary (cartonised) package, which is every row written before this
-- migration; when set, the case GTIN and its dimensions are copied here at
-- seal time so a later re-declaration of the case never changes a sealed
-- package.
ALTER TABLE packages
    ADD COLUMN container_source    TEXT CHECK (container_source IS NULL OR container_source IN ('CASE')),
    ADD COLUMN container_gtin      TEXT CHECK (container_gtin IS NULL OR container_gtin ~ '^[0-9]{14}$'),
    ADD COLUMN container_length_mm INTEGER CHECK (container_length_mm IS NULL OR container_length_mm >= 1),
    ADD COLUMN container_width_mm  INTEGER CHECK (container_width_mm  IS NULL OR container_width_mm  >= 1),
    ADD COLUMN container_height_mm INTEGER CHECK (container_height_mm IS NULL OR container_height_mm >= 1),
    ADD COLUMN container_weight_g  INTEGER CHECK (container_weight_g  IS NULL OR container_weight_g  >= 1),
    ADD CONSTRAINT packages_container_complete CHECK (
        (container_source IS NULL
            AND container_gtin IS NULL AND container_length_mm IS NULL AND container_width_mm IS NULL
            AND container_height_mm IS NULL AND container_weight_g IS NULL)
        OR (container_source IS NOT NULL
            AND container_gtin IS NOT NULL AND container_length_mm IS NOT NULL AND container_width_mm IS NOT NULL
            AND container_height_mm IS NOT NULL AND container_weight_g IS NOT NULL));
