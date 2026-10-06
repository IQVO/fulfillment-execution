package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// PackageRepo is a pgxpool-backed implementation of ports.PackageRepo.
type PackageRepo struct {
	pool *pgxpool.Pool
}

// NewPackageRepo constructs a PackageRepo backed by pool.
func NewPackageRepo(pool *pgxpool.Pool) *PackageRepo {
	return &PackageRepo{pool: pool}
}

func (r *PackageRepo) Save(ctx context.Context, p *pack.Package) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO packages (id, order_ref, task_id, status, scanned_contents, fragile_handling, scanned_hazard_classes, gift_wrap_requested)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET order_ref = EXCLUDED.order_ref, task_id = EXCLUDED.task_id, status = EXCLUDED.status, scanned_contents = EXCLUDED.scanned_contents, fragile_handling = EXCLUDED.fragile_handling, scanned_hazard_classes = EXCLUDED.scanned_hazard_classes, gift_wrap_requested = EXCLUDED.gift_wrap_requested
	`, string(p.Id()), string(p.OrderRef()), nullableTaskId(p.TaskId()), string(p.Status()), p.ScannedContents(), p.FragileHandling(), nonNilInts(p.ScannedHazardClasses()), p.GiftWrapRequested())
	return err
}

// nonNilInts guards packages.scanned_hazard_classes' NOT NULL constraint:
// pgx encodes a nil Go slice as SQL NULL (not an empty array), so any
// package with zero hazmat-classified items — the common case, not just
// an edge case — would otherwise fail this INSERT. A non-nil empty slice
// encodes as the column's own '{}' default.
func nonNilInts(s []int) []int {
	if s == nil {
		return []int{}
	}
	return s
}

// packageColumns is the column list every package read selects, in the
// order scanPackage expects.
const packageColumns = `id, order_ref, task_id, status, scanned_contents, fragile_handling, scanned_hazard_classes, gift_wrap_requested`

// scanPackage rehydrates one packages row selected with packageColumns.
func scanPackage(row rowScanner) (*pack.Package, error) {
	var (
		packageId, orderRef, status string
		taskId                      *string
		scannedContents             []string
		fragileHandling             bool
		scannedHazardClasses        []int
		giftWrapRequested           bool
	)
	if err := row.Scan(&packageId, &orderRef, &taskId, &status, &scannedContents, &fragileHandling, &scannedHazardClasses, &giftWrapRequested); err != nil {
		return nil, err
	}
	parsedStatus, err := pack.ParseStatus(status)
	if err != nil {
		return nil, fmt.Errorf("rehydrate package %q: %w", packageId, err)
	}
	return pack.Rehydrate(shared.PackageId(packageId), shared.OrderRef(orderRef), shared.TaskId(deref(taskId)), parsedStatus, scannedContents, fragileHandling, scannedHazardClasses, giftWrapRequested), nil
}

// Package read queries, built only from constants.
const (
	selectPackageById        = `SELECT ` + packageColumns + ` FROM packages WHERE id = $1`
	selectPackageByTaskId    = `SELECT ` + packageColumns + ` FROM packages WHERE task_id = $1`
	selectPackagesByOrderRef = `SELECT ` + packageColumns + ` FROM packages WHERE order_ref = $1 ORDER BY id`
)

// findOne runs a single-row package query, mapping "no row" to (nil, nil).
func (r *PackageRepo) findOne(ctx context.Context, query, arg string) (*pack.Package, error) {
	p, err := scanPackage(querierFrom(ctx, r.pool).QueryRow(ctx, query, arg))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (r *PackageRepo) FindById(ctx context.Context, id shared.PackageId) (*pack.Package, error) {
	return r.findOne(ctx, selectPackageById, string(id))
}

// FindByTaskId returns the Package already sealed for taskId, backing
// SealPackage's idempotency guard (see ports.PackageRepo.FindByTaskId's
// doc comment). Relies on idx_packages_task_id (migration 0011) for both
// lookup speed and — via Save's ON CONFLICT plus that index's partial
// uniqueness constraint — real database-level protection against two
// concurrent retries both slipping past this pre-check and inserting two
// rows for the same taskId.
func (r *PackageRepo) FindByTaskId(ctx context.Context, taskId shared.TaskId) (*pack.Package, error) {
	if taskId == "" {
		return nil, nil
	}
	return r.findOne(ctx, selectPackageByTaskId, string(taskId))
}

// FindByOrderRef returns every package sealed for orderRef, ordered by id,
// backing GET /packages?orderRef= (ADR-0033). Served by
// idx_packages_order_ref (migration 0013). An unknown orderRef returns an
// empty, non-nil slice.
func (r *PackageRepo) FindByOrderRef(ctx context.Context, orderRef shared.OrderRef) ([]*pack.Package, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, selectPackagesByOrderRef, string(orderRef))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*pack.Package, 0)
	for rows.Next() {
		p, err := scanPackage(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

// nullableTaskId converts a domain shared.TaskId into the pointer form
// pgx needs to write SQL NULL for a package that (pre-this-feature,
// should never happen for a package sealed after migration 0011, but
// handled defensively) carries no taskId, rather than writing the
// literal empty string into the nullable task_id column.
func nullableTaskId(id shared.TaskId) *string {
	if id == "" {
		return nil
	}
	s := string(id)
	return &s
}

// deref returns "" for a nil pointer (a NULL task_id column), or the
// pointed-to string otherwise.
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
