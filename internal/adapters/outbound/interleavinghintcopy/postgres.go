package interleavinghintcopy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/pgtx"
)

// querier is the subset of pgx both *pgxpool.Pool and pgx.Tx satisfy.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PostgresStore is the production hint copy, table interleaving_hints
// (migration 0021). Writes join the transaction a UnitOfWork bound to ctx
// (internal/pgtx, the same mechanism every repo in
// internal/adapters/outbound/postgres uses), so the processed-event claim
// and the hint upsert commit together.
type PostgresStore struct {
	pool *pgxpool.Pool
}

var (
	_ ports.InterleavingHintStore  = (*PostgresStore)(nil)
	_ ports.InterleavingHintLookup = (*PostgresStore)(nil)
)

// NewPostgresStore constructs a PostgresStore over pool.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) q(ctx context.Context) querier {
	if tx, ok := pgtx.TxFrom(ctx); ok {
		return tx
	}
	return s.pool
}

// candidateJSON is the wire shape of one ranked candidate inside the
// candidates JSONB column: only what claimNext needs is stored.
type candidateJSON struct {
	Rank   int    `json:"rank"`
	TaskId string `json:"task_id"`
}

// ReplaceForStation implements ports.InterleavingHintStore. One row per
// station (PRIMARY KEY), replaced wholesale: the newer hint supersedes the
// previous one.
func (s *PostgresStore) ReplaceForStation(ctx context.Context, hint ports.InterleavingHint) error {
	candidates := make([]candidateJSON, len(hint.Candidates))
	for i, c := range hint.Candidates {
		candidates[i] = candidateJSON{Rank: c.Rank, TaskId: c.TaskId}
	}
	raw, err := json.Marshal(candidates)
	if err != nil {
		return fmt.Errorf("interleavinghintcopy: encode candidates of %s: %w", hint.HintId, err)
	}
	receivedAt := hint.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}
	_, err = s.q(ctx).Exec(ctx, `
INSERT INTO interleaving_hints (station_id, hint_id, site_id, expires_at, candidates, received_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (station_id) DO UPDATE SET
    hint_id     = EXCLUDED.hint_id,
    site_id     = EXCLUDED.site_id,
    expires_at  = EXCLUDED.expires_at,
    candidates  = EXCLUDED.candidates,
    received_at = EXCLUDED.received_at`,
		hint.StationId, hint.HintId, hint.SiteId, hint.ExpiresAt, raw, receivedAt)
	if err != nil {
		return fmt.Errorf("interleavinghintcopy: upsert hint %s for station %s: %w", hint.HintId, hint.StationId, err)
	}
	return nil
}

// ActiveForStation implements ports.InterleavingHintLookup: the station's
// stored hint, or nil (not an error) when there is none or it expired at or
// before now (filtered in SQL, so an expired hint never leaves the store).
func (s *PostgresStore) ActiveForStation(ctx context.Context, stationId shared.StationId, now time.Time) (*ports.InterleavingHint, error) {
	var (
		hint  ports.InterleavingHint
		raw   []byte
		found = true
	)
	err := s.q(ctx).QueryRow(ctx, `
SELECT hint_id, station_id, site_id, expires_at, candidates, received_at
FROM interleaving_hints
WHERE station_id = $1 AND expires_at > $2`,
		string(stationId), now,
	).Scan(&hint.HintId, &hint.StationId, &hint.SiteId, &hint.ExpiresAt, &raw, &hint.ReceivedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		found = false
	} else if err != nil {
		return nil, fmt.Errorf("interleavinghintcopy: read hint for station %s: %w", stationId, err)
	}
	if !found {
		return nil, nil
	}
	var candidates []candidateJSON
	if err := json.Unmarshal(raw, &candidates); err != nil {
		return nil, fmt.Errorf("interleavinghintcopy: decode candidates of %s: %w", hint.HintId, err)
	}
	hint.Candidates = make([]ports.InterleavingHintCandidate, len(candidates))
	for i, c := range candidates {
		hint.Candidates[i] = ports.InterleavingHintCandidate{Rank: c.Rank, TaskId: c.TaskId}
	}
	return &hint, nil
}
