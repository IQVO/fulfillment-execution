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

	var transferRef, demandId, workKind, siteId, sku *string
	var quantity *int
	if tr := t.Transfer(); tr != nil {
		transferRef, demandId, workKind = nullableString(tr.TransferRef), nullableString(tr.DemandId), nullableString(string(tr.WorkKind))
		siteId, sku = nullableString(tr.SiteId), nullableString(tr.SKU)
		q := tr.Quantity
		quantity = &q
	}

	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO tasks (`+taskColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
		        $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37,
		        $38, $39, $40, $41, $42, $43, $44, $45, $46)
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
			claimed_at = EXCLUDED.claimed_at,
			transfer_ref = EXCLUDED.transfer_ref,
			demand_id = EXCLUDED.demand_id,
			work_kind = EXCLUDED.work_kind,
			site_id = EXCLUDED.site_id,
			sku = EXCLUDED.sku,
			quantity = EXCLUDED.quantity,
			source_order_id = EXCLUDED.source_order_id,
			source_line_no = EXCLUDED.source_line_no,
			relocation_plan_id = EXCLUDED.relocation_plan_id,
			relocation_move_seq = EXCLUDED.relocation_move_seq,
			relocation_kind = EXCLUDED.relocation_kind,
			relocation_sku = EXCLUDED.relocation_sku,
			relocation_from_bin = EXCLUDED.relocation_from_bin,
			relocation_to_bin = EXCLUDED.relocation_to_bin,
			relocated_quantity = EXCLUDED.relocated_quantity,
			relocated_from_bin = EXCLUDED.relocated_from_bin,
			relocated_to_bin = EXCLUDED.relocated_to_bin,
			replenishment_request_id = EXCLUDED.replenishment_request_id,
			replenishment_sku = EXCLUDED.replenishment_sku,
			replenishment_from_bin = EXCLUDED.replenishment_from_bin,
			replenishment_to_bin = EXCLUDED.replenishment_to_bin,
			replenishment_quantity = EXCLUDED.replenishment_quantity,
			replenishment_priority = EXCLUDED.replenishment_priority,
			replenished_quantity = EXCLUDED.replenished_quantity,
			replenished_from_bin = EXCLUDED.replenished_from_bin,
			replenished_to_bin = EXCLUDED.replenished_to_bin,
			pick_sku = EXCLUDED.pick_sku,
			pick_quantity = EXCLUDED.pick_quantity,
			pick_unit = EXCLUDED.pick_unit,
			pick_from_bin = EXCLUDED.pick_from_bin,
			picked_eaches = EXCLUDED.picked_eaches,
			break_lpn = EXCLUDED.break_lpn,
			break_sku = EXCLUDED.break_sku,
			break_loose_bin = EXCLUDED.break_loose_bin,
			break_pick_task_id = EXCLUDED.break_pick_task_id
	`, append(append(append([]any{string(t.Id()), string(t.Type()), string(t.Status()), t.CPT().Time(), string(t.OrderRef()),
		capabilitiesToSlice(t.RequiredCapabilities()), leaseStationId, leaseExpiry, t.Fragile(), t.GiftWrap(), t.ClaimedAt(),
		transferRef, demandId, workKind, siteId, sku, quantity, nullableString(t.SourceOrderId()), nullableLineNo(t.SourceLineNo())},
		relocationColumnsOf(t).values()...), replenishmentColumnsOf(t).values()...), pickColumnsOf(t).values()...)...)
	return err
}

// taskColumns is the column list every INSERT and SELECT of a task uses, in
// scanTask's order. The relocation_* / relocated_* columns (migration 0019,
// ADR 0047) and the replenishment_* / replenished_* columns (migration
// 0020, ADR 0050) and the pick_* / break_* columns (migration 0023, ADR
// 0054) come last.
const taskColumns = `id, task_type, status, cpt, order_ref, required_capabilities, lease_station_id, lease_expiry, fragile, gift_wrap, claimed_at, ` +
	`transfer_ref, demand_id, work_kind, site_id, sku, quantity, source_order_id, source_line_no, ` +
	`relocation_plan_id, relocation_move_seq, relocation_kind, relocation_sku, relocation_from_bin, relocation_to_bin, ` +
	`relocated_quantity, relocated_from_bin, relocated_to_bin, ` +
	`replenishment_request_id, replenishment_sku, replenishment_from_bin, replenishment_to_bin, replenishment_quantity, replenishment_priority, ` +
	`replenished_quantity, replenished_from_bin, replenished_to_bin, ` +
	`pick_sku, pick_quantity, pick_unit, pick_from_bin, picked_eaches, break_lpn, break_sku, break_loose_bin, break_pick_task_id`

func (r *TaskRepo) FindById(ctx context.Context, id shared.TaskId) (*task.Task, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT `+taskColumns+`
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
		SELECT `+taskColumns+`
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
		SELECT `+taskColumns+`
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
		SELECT `+taskColumns+`
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
		SELECT `+taskColumns+`
		FROM tasks WHERE order_ref = $1
	`, string(orderRef))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}
