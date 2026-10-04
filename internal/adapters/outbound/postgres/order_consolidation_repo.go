package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/domain/consolidation"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// OrderConsolidationRepo is a pgxpool-backed implementation of
// ports.OrderConsolidationRepo.
type OrderConsolidationRepo struct {
	pool *pgxpool.Pool
}

// NewOrderConsolidationRepo constructs an OrderConsolidationRepo backed by pool.
func NewOrderConsolidationRepo(pool *pgxpool.Pool) *OrderConsolidationRepo {
	return &OrderConsolidationRepo{pool: pool}
}

func (r *OrderConsolidationRepo) Save(ctx context.Context, oc *consolidation.OrderConsolidation) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO order_consolidations (order_ref, required_lines, arrived_lines)
		VALUES ($1, $2, $3)
		ON CONFLICT (order_ref) DO UPDATE SET
			required_lines = EXCLUDED.required_lines,
			arrived_lines = EXCLUDED.arrived_lines
	`, oc.OrderRef(), oc.RequiredLineIds(), oc.ArrivedLineIds())
	return err
}

func (r *OrderConsolidationRepo) FindByOrderRef(ctx context.Context, orderRef shared.OrderRef) (*consolidation.OrderConsolidation, error) {
	return r.find(ctx, orderRef, "")
}

// FindByOrderRefForUpdate serializes concurrent arrivals for one order
// (ADR-0034). Inside a UnitOfWork it takes a transaction-scoped advisory
// lock keyed on the order_ref, then reads the row with FOR UPDATE:
//
//   - the advisory lock covers the FIRST arrival, when there is no row yet
//     for FOR UPDATE to lock and two creators would otherwise both see "no
//     consolidation" and the later Save would overwrite the earlier line;
//   - FOR UPDATE keeps the row locked against any other writer for the rest
//     of the transaction.
//
// Both are released at COMMIT/ROLLBACK, and under READ COMMITTED the SELECT
// that follows the lock sees the previous holder's committed arrival.
// Outside a transaction the lock would be released immediately, so there it
// degrades to a plain read.
func (r *OrderConsolidationRepo) FindByOrderRefForUpdate(ctx context.Context, orderRef shared.OrderRef) (*consolidation.OrderConsolidation, error) {
	if tx, ok := txFrom(ctx); ok {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, string(orderRef)); err != nil {
			return nil, err
		}
		return r.find(ctx, orderRef, " FOR UPDATE")
	}
	return r.find(ctx, orderRef, "")
}

func (r *OrderConsolidationRepo) find(ctx context.Context, orderRef shared.OrderRef, lockClause string) (*consolidation.OrderConsolidation, error) {
	var (
		storedOrderRef string
		requiredLines  []string
		arrivedLines   []string
	)
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT order_ref, required_lines, arrived_lines
		FROM order_consolidations WHERE order_ref = $1`+lockClause, string(orderRef))
	if err := row.Scan(&storedOrderRef, &requiredLines, &arrivedLines); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return consolidation.Rehydrate(storedOrderRef, requiredLines, arrivedLines), nil
}
