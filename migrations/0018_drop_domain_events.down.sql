-- Recreates the legacy table exactly as 0001_init created it (empty).
CREATE TABLE IF NOT EXISTS domain_events (
    id          BIGSERIAL PRIMARY KEY,
    event_name  TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload     JSONB NOT NULL
);
