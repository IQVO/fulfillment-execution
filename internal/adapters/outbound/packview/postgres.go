package packview

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/pgtx"
)

// querier is the subset of pgx both *pgxpool.Pool and pgx.Tx satisfy.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PostgresStore is the production PackView, table pack_view (migration
// 0022). Writes join the transaction a UnitOfWork bound to ctx (internal/pgtx).
type PostgresStore struct {
	pool *pgxpool.Pool
}

var (
	_ ports.PackViewLookup = (*PostgresStore)(nil)
	_ ports.PackViewCopy   = (*PostgresStore)(nil)
)

// NewPostgresStore constructs a PostgresStore over pool.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore { return &PostgresStore{pool: pool} }

func (s *PostgresStore) q(ctx context.Context) querier {
	if tx, ok := pgtx.TxFrom(ctx); ok {
		return tx
	}
	return s.pool
}

// upsertSQL inserts the level or replaces it only for a strictly greater
// version; a re-declaration clears effective_to.
const upsertSQL = `
INSERT INTO pack_view (gtin, sku, level, units_per_pack, child_gtin, version, effective_from, effective_to, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, now())
ON CONFLICT (gtin) DO UPDATE SET
    sku            = EXCLUDED.sku,
    level          = EXCLUDED.level,
    units_per_pack = EXCLUDED.units_per_pack,
    child_gtin     = EXCLUDED.child_gtin,
    version        = EXCLUDED.version,
    effective_from = EXCLUDED.effective_from,
    effective_to   = NULL,
    updated_at     = now()
WHERE pack_view.version < EXCLUDED.version`

// UpsertIfNewer implements ports.PackViewCopy.
func (s *PostgresStore) UpsertIfNewer(ctx context.Context, rec ports.PackRecord) (bool, error) {
	var child any
	if rec.ChildGTIN != "" {
		child = rec.ChildGTIN
	}
	tag, err := s.q(ctx).Exec(ctx, upsertSQL, rec.GTIN, rec.SKU, string(rec.Level), rec.UnitsPerPack, child, rec.Version, rec.EffectiveFrom)
	if err != nil {
		return false, fmt.Errorf("packview: upsert %s: %w", rec.GTIN, err)
	}
	return tag.RowsAffected() > 0, nil
}

// RetireIfNewer implements ports.PackViewCopy.
func (s *PostgresStore) RetireIfNewer(ctx context.Context, gtin string, effectiveTo time.Time, version int64) (bool, error) {
	tag, err := s.q(ctx).Exec(ctx,
		`UPDATE pack_view SET effective_to = $2, version = $3, updated_at = now() WHERE gtin = $1 AND version < $3`,
		gtin, effectiveTo, version)
	if err != nil {
		return false, fmt.Errorf("packview: retire %s: %w", gtin, err)
	}
	return tag.RowsAffected() > 0, nil
}

// FindByGTIN implements ports.PackViewLookup.
func (s *PostgresStore) FindByGTIN(ctx context.Context, gtin string) (ports.PackRecord, bool, error) {
	var (
		rec   ports.PackRecord
		level string
		child *string
	)
	err := s.q(ctx).QueryRow(ctx,
		`SELECT gtin, sku, level, units_per_pack, child_gtin, version, effective_from, effective_to FROM pack_view WHERE gtin = $1`, gtin,
	).Scan(&rec.GTIN, &rec.SKU, &level, &rec.UnitsPerPack, &child, &rec.Version, &rec.EffectiveFrom, &rec.EffectiveTo)
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.PackRecord{}, false, nil
	}
	if err != nil {
		return ports.PackRecord{}, false, fmt.Errorf("packview: find %s: %w", gtin, err)
	}
	rec.Level = task.PackLevel(level)
	if child != nil {
		rec.ChildGTIN = *child
	}
	return rec, true, nil
}
