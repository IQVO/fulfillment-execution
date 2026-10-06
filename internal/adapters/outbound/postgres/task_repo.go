// Package postgres provides pgxpool-backed implementations of every
// outbound port, plus golang-migrate wiring for schema migrations.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TaskRepo is a pgxpool-backed implementation of ports.TaskRepo.
type TaskRepo struct {
	pool *pgxpool.Pool
}

// NewTaskRepo constructs a TaskRepo backed by pool.
func NewTaskRepo(pool *pgxpool.Pool) *TaskRepo {
	return &TaskRepo{pool: pool}
}

func (r *TaskRepo) Save(ctx context.Context, t *task.Task) error {
	var leaseStationId *string
	var leaseExpiry *time.Time
	if lease := t.Lease(); lease != nil {
		id := string(lease.StationId)
		leaseStationId = &id
		leaseExpiry = &lease.Expiry
	}

	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO tasks (id, task_type, status, cpt, order_ref, required_capabilities, lease_station_id, lease_expiry, fragile, gift_wrap, claimed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (id) DO UPDATE SET
			task_type = EXCLUDED.task_type,
			status = EXCLUDED.status,
			cpt = EXCLUDED.cpt,
			order_ref = EXCLUDED.order_ref,
			required_capabilities = EXCLUDED.required_capabilities,
			lease_station_id = EXCLUDED.lease_station_id,
			lease_expiry = EXCLUDED.lease_expiry,
			fragile = EXCLUDED.fragile,
			gift_wrap = EXCLUDED.gift_wrap,
			claimed_at = EXCLUDED.claimed_at
	`, string(t.Id()), string(t.Type()), string(t.Status()), t.CPT().Time(), string(t.OrderRef()),
		capabilitiesToSlice(t.RequiredCapabilities()), leaseStationId, leaseExpiry, t.Fragile(), t.GiftWrap(), t.ClaimedAt())
	return err
}

func (r *TaskRepo) FindById(ctx context.Context, id shared.TaskId) (*task.Task, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT id, task_type, status, cpt, order_ref, required_capabilities, lease_station_id, lease_expiry, fragile, gift_wrap, claimed_at
		FROM tasks WHERE id = $1
	`, string(id))
	t, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

func (r *TaskRepo) FindClaimableByType(ctx context.Context, taskType task.Type, now time.Time) ([]*task.Task, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT id, task_type, status, cpt, order_ref, required_capabilities, lease_station_id, lease_expiry, fragile, gift_wrap, claimed_at
		FROM tasks
		WHERE task_type = $1
		  AND (status = 'PENDING' OR (status = 'CLAIMED' AND lease_expiry <= $2))
		ORDER BY cpt ASC
	`, string(taskType), now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}

// SaveClaim is the compare-and-set behind at-most-once claiming: the
// UPDATE only matches while the row is still claimable at now, so of N
// stations racing for one task exactly one gets RowsAffected == 1.
func (r *TaskRepo) SaveClaim(ctx context.Context, t *task.Task, now time.Time) (bool, error) {
	lease := t.Lease()
	if lease == nil {
		return false, fmt.Errorf("postgres: SaveClaim on task %s with no lease", t.Id())
	}
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		UPDATE tasks
		SET status = $2, lease_station_id = $3, lease_expiry = $4, claimed_at = $5
		WHERE id = $1
		  AND (status = 'PENDING' OR (status = 'CLAIMED' AND lease_expiry <= $6))
	`, string(t.Id()), string(t.Status()), string(lease.StationId), lease.Expiry, t.ClaimedAt(), now)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (r *TaskRepo) FindAllClaimed(ctx context.Context) ([]*task.Task, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT id, task_type, status, cpt, order_ref, required_capabilities, lease_station_id, lease_expiry, fragile, gift_wrap, claimed_at
		FROM tasks WHERE status = 'CLAIMED'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}

// FindOpenPastCPT returns every Pending or Claimed task whose cpt is at or
// before now, for the CPT-missed sweep (ADR-0025). A Completed task is
// never returned regardless of its CPT — mirrors task.Task.IsCPTMissed's
// own status guard.
func (r *TaskRepo) FindOpenPastCPT(ctx context.Context, now time.Time) ([]*task.Task, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT id, task_type, status, cpt, order_ref, required_capabilities, lease_station_id, lease_expiry, fragile, gift_wrap, claimed_at
		FROM tasks
		WHERE status IN ('PENDING', 'CLAIMED') AND cpt <= $1
	`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}

func (r *TaskRepo) CountByTypeAndStatus(ctx context.Context, taskType task.Type, status task.Status) (int, error) {
	var count int
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT COUNT(*) FROM tasks WHERE task_type = $1 AND status = $2
	`, string(taskType), string(status)).Scan(&count)
	return count, err
}

func (r *TaskRepo) FindByOrderRef(ctx context.Context, orderRef shared.OrderRef) ([]*task.Task, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT id, task_type, status, cpt, order_ref, required_capabilities, lease_station_id, lease_expiry, fragile, gift_wrap, claimed_at
		FROM tasks WHERE order_ref = $1
	`, string(orderRef))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTask(row rowScanner) (*task.Task, error) {
	var (
		id, taskType, status, orderRef string
		cpt                            time.Time
		requiredCapabilities           []string
		leaseStationId                 *string
		leaseExpiry                    *time.Time
		fragile                        bool
		giftWrap                       bool
		claimedAt                      *time.Time
	)
	if err := row.Scan(&id, &taskType, &status, &cpt, &orderRef, &requiredCapabilities, &leaseStationId, &leaseExpiry, &fragile, &giftWrap, &claimedAt); err != nil {
		return nil, err
	}

	taskTypeParsed, err := task.ParseType(taskType)
	if err != nil {
		return nil, fmt.Errorf("rehydrate task %q: %w", id, err)
	}
	statusParsed, err := task.ParseStatus(status)
	if err != nil {
		return nil, fmt.Errorf("rehydrate task %q: %w", id, err)
	}

	var lease *task.Lease
	if leaseStationId != nil && leaseExpiry != nil {
		lease = &task.Lease{StationId: shared.StationId(*leaseStationId), Expiry: *leaseExpiry}
	}

	return task.Rehydrate(
		shared.TaskId(id),
		taskTypeParsed,
		statusParsed,
		shared.NewCPT(cpt),
		shared.OrderRef(orderRef),
		sliceToCapabilities(requiredCapabilities),
		lease,
		fragile,
		giftWrap,
		claimedAt,
	), nil
}

func scanTasks(rows pgx.Rows) ([]*task.Task, error) {
	var result []*task.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

func capabilitiesToSlice(set shared.CapabilitySet) []string {
	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, string(c))
	}
	return out
}

func sliceToCapabilities(caps []string) shared.CapabilitySet {
	typed := make([]shared.Capability, len(caps))
	for i, c := range caps {
		typed[i] = shared.Capability(c)
	}
	return shared.NewCapabilitySet(typed...)
}
