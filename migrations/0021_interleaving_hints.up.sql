-- ADR 0051: the local copy of task-interleaving's advisory hints. One
-- ACTIVE hint per station (station_id is the primary key): a newer
-- InterleavingHintIssued for the same station supersedes the previous row.
-- candidates is the ranked candidate list ([{rank, task_id}, ...], rank
-- starts at 1) exactly as consumed; claimNext reads the unexpired row as a
-- tie-break inside one priority band. claimNext never writes here and the
-- consumer never reads: an absent or expired row means "no hint", which
-- costs nothing (fail-open).
CREATE TABLE interleaving_hints (
    station_id  TEXT PRIMARY KEY,
    hint_id     TEXT NOT NULL,
    site_id     TEXT NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    candidates  JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
