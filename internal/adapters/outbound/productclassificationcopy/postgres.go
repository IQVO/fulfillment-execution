package productclassificationcopy

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/pgtx"
)

// querier is the subset of pgx both *pgxpool.Pool and pgx.Tx satisfy.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PostgresStore is the production local copy, table
// product_classification_copy (migration 0015). Writes join the
// transaction a UnitOfWork bound to ctx (internal/pgtx, the same mechanism
// every repo in internal/adapters/outbound/postgres uses), so the
// processed-event claim and the upsert commit together.
type PostgresStore struct {
	pool *pgxpool.Pool
}

var (
	_ ports.ProductClassificationLookup = (*PostgresStore)(nil)
	_ ports.ProductClassificationCopy   = (*PostgresStore)(nil)
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

// upsertSQL inserts the row, or replaces it only when the incoming version
// is strictly greater than the stored one. RowsAffected is 0 for a stale
// or equal version.
const upsertSQL = `
INSERT INTO product_classification_copy (sku, handling_tags, temperature_class, dot_hazard_class, version, updated_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (sku) DO UPDATE SET
    handling_tags     = EXCLUDED.handling_tags,
    temperature_class = EXCLUDED.temperature_class,
    dot_hazard_class  = EXCLUDED.dot_hazard_class,
    version           = EXCLUDED.version,
    updated_at        = now()
WHERE product_classification_copy.version < EXCLUDED.version`

// UpsertIfNewer implements ports.ProductClassificationCopy.
func (s *PostgresStore) UpsertIfNewer(ctx context.Context, rec ports.ProductClassificationRecord) (bool, error) {
	tags := rec.HandlingTags
	if tags == nil {
		tags = []string{}
	}
	var temperature, hazard any
	if rec.TemperatureClass != "" {
		temperature = rec.TemperatureClass
	}
	if rec.DOTHazardClass != 0 {
		hazard = rec.DOTHazardClass
	}
	tag, err := s.q(ctx).Exec(ctx, upsertSQL, rec.SKU, tags, temperature, hazard, rec.Version)
	if err != nil {
		return false, fmt.Errorf("productclassificationcopy: upsert %s: %w", rec.SKU, err)
	}
	return tag.RowsAffected() > 0, nil
}

// GetClassification implements ports.ProductClassificationLookup: Known=false
// with a nil error when the copy has no row for sku, an error when the read
// itself fails.
func (s *PostgresStore) GetClassification(ctx context.Context, sku string) (ports.ClassificationInfo, error) {
	var (
		tags   []string
		hazard *int32
	)
	err := s.q(ctx).QueryRow(ctx,
		`SELECT handling_tags, dot_hazard_class FROM product_classification_copy WHERE sku = $1`, sku,
	).Scan(&tags, &hazard)
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.ClassificationInfo{Known: false}, nil
	}
	if err != nil {
		return ports.ClassificationInfo{}, fmt.Errorf("productclassificationcopy: read %s: %w", sku, err)
	}
	dot := 0
	if hazard != nil {
		dot = int(*hazard)
	}
	return toInfo(tags, dot), nil
}

// Find returns the full stored record of sku (ok=false when absent). It is
// a read helper for tests and diagnostics; the port only needs
// GetClassification.
func (s *PostgresStore) Find(ctx context.Context, sku string) (ports.ProductClassificationRecord, bool, error) {
	var (
		rec         ports.ProductClassificationRecord
		temperature *string
		hazard      *int32
	)
	err := s.q(ctx).QueryRow(ctx,
		`SELECT sku, handling_tags, temperature_class, dot_hazard_class, version FROM product_classification_copy WHERE sku = $1`, sku,
	).Scan(&rec.SKU, &rec.HandlingTags, &temperature, &hazard, &rec.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.ProductClassificationRecord{}, false, nil
	}
	if err != nil {
		return ports.ProductClassificationRecord{}, false, fmt.Errorf("productclassificationcopy: find %s: %w", sku, err)
	}
	if temperature != nil {
		rec.TemperatureClass = *temperature
	}
	if hazard != nil {
		rec.DOTHazardClass = int(*hazard)
	}
	return rec, true, nil
}
