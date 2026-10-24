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

// relocationColumns is a task's nullable relocation block as stored
// (migration 0019): the planned move and, once completed, the executed
// result. Every field is nil for a non-RELOCATE task.
type relocationColumns struct {
	planId, kind, sku, fromBin, toBin *string
	moveSeq                           *int
	quantity                          *int
	resultFromBin, resultToBin        *string
}

// relocationColumnsOf maps t's move and result onto their columns; an empty
// optional bin is stored as NULL.
func relocationColumnsOf(t *task.Task) relocationColumns {
	var c relocationColumns
	if m := t.Relocation(); m != nil {
		seq := m.MoveSeq
		c.planId, c.moveSeq, c.kind, c.sku = &m.PlanId, &seq, nullableString(string(m.Kind)), &m.SKU
		c.fromBin, c.toBin = nullableString(m.FromBin), nullableString(m.ToBin)
	}
	if r := t.RelocationResult(); r != nil {
		q := r.Quantity
		c.quantity, c.resultFromBin, c.resultToBin = &q, nullableString(r.FromBin), nullableString(r.ToBin)
	}
	return c
}

// values is the INSERT argument order of taskColumns' relocation tail.
func (c relocationColumns) values() []any {
	return []any{c.planId, c.moveSeq, c.kind, c.sku, c.fromBin, c.toBin, c.quantity, c.resultFromBin, c.resultToBin}
}

// dest is the Scan destination order of taskColumns' relocation tail.
func (c *relocationColumns) dest() []any {
	return []any{&c.planId, &c.moveSeq, &c.kind, &c.sku, &c.fromBin, &c.toBin, &c.quantity, &c.resultFromBin, &c.resultToBin}
}

// rehydrate rebuilds the move and result. A NULL plan id is "not RELOCATE
// work" (nil, nil), exactly like a row written before migration 0019. A
// stored move that no longer validates fails the rehydrate (same discipline
// as the status/type validation): a RELOCATE task whose completion could
// not publish a usable move must not silently load.
func (c relocationColumns) rehydrate(id string) (*task.RelocationMove, *task.RelocationResult, error) {
	if c.planId == nil {
		return nil, nil, nil
	}
	move, err := task.NewRelocationMove(*c.planId, derefInt(c.moveSeq), derefString(c.kind), derefString(c.sku), derefString(c.fromBin), derefString(c.toBin))
	if err != nil {
		return nil, nil, fmt.Errorf("rehydrate task %q: %w", id, err)
	}
	if c.quantity == nil {
		return &move, nil, nil
	}
	return &move, &task.RelocationResult{Quantity: *c.quantity, FromBin: derefString(c.resultFromBin), ToBin: derefString(c.resultToBin)}, nil
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func derefInt(v *int) int {
	if v == nil {
		return -1 // fails NewRelocationMove's move_seq >= 0 rule
	}
	return *v
}

// nullableString maps "" to SQL NULL for the transfer-correlation block's
// optional text columns: an absent value is stored as NULL, never as an
// empty string, so a NULL transfer_ref keeps meaning "not transfer work"
// and a present block with an absent demand_id stays distinguishable from
// one carrying an empty-string demand id.
func nullableString(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

// nullableLineNo maps the unknown line (0) to SQL NULL: an absent source
// line is stored as NULL, never as 0 (ADR 0041).
func nullableLineNo(n int) *int {
	if n < 1 {
		return nil
	}
	return &n
}

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
		transferRef                    *string
		demandId                       *string
		workKind                       *string
		siteId                         *string
		sku                            *string
		quantity                       *int
		sourceOrderId                  *string
		sourceLineNo                   *int
	)
	var relocation relocationColumns
	var replenishment replenishmentColumns
	var pick pickColumns
	dest := append(append(append([]any{&id, &taskType, &status, &cpt, &orderRef, &requiredCapabilities, &leaseStationId, &leaseExpiry, &fragile, &giftWrap, &claimedAt, &transferRef, &demandId, &workKind, &siteId, &sku, &quantity, &sourceOrderId, &sourceLineNo},
		relocation.dest()...), replenishment.dest()...), pick.dest()...)
	if err := row.Scan(dest...); err != nil {
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

	transfer, err := scanTransferDetails(id, transferRef, demandId, workKind, siteId, sku, quantity)
	if err != nil {
		return nil, err
	}
	move, result, err := relocation.rehydrate(id)
	if err != nil {
		return nil, err
	}
	replMove, replResult, err := replenishment.rehydrate(id)
	if err != nil {
		return nil, err
	}
	pickPlan, pickedEaches, caseBreak, err := pick.rehydrate(id)
	if err != nil {
		return nil, err
	}
	t := task.RehydrateTransfer(
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
		transfer,
	)
	if sourceOrderId != nil {
		t.WithSourceOrderId(*sourceOrderId)
	}
	if sourceLineNo != nil {
		t.WithSourceLineNo(*sourceLineNo)
	}
	return t.WithRelocation(move, result).WithReplenishment(replMove, replResult).WithPickPlan(pickPlan, pickedEaches).WithCaseBreak(caseBreak), nil
}

// scanTransferDetails rebuilds the nullable transfer-correlation block
// from its columns. A NULL transfer_ref is "not transfer work" — nil,
// exactly like a task created before this column existed. A present
// transfer_ref with an unknown work_kind fails the rehydrate (same
// discipline as the status/type validation), because a task whose
// completion cannot select a transfer fact must not silently load.
func scanTransferDetails(id string, transferRef, demandId, workKind, siteId, sku *string, quantity *int) (*task.TransferDetails, error) {
	if transferRef == nil {
		return nil, nil
	}
	transfer := &task.TransferDetails{TransferRef: *transferRef}
	if demandId != nil {
		transfer.DemandId = *demandId
	}
	if workKind != nil {
		parsed, err := task.ParseWorkKind(*workKind)
		if err != nil {
			return nil, fmt.Errorf("rehydrate task %q: %w", id, err)
		}
		transfer.WorkKind = parsed
	}
	if siteId != nil {
		transfer.SiteId = *siteId
	}
	if sku != nil {
		transfer.SKU = *sku
	}
	if quantity != nil {
		transfer.Quantity = *quantity
	}
	return transfer, nil
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
