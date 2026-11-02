package kafka

import (
	"fmt"

	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// requestOf maps one decoded WorkReleased onto the use case request,
// validating its optional transfer and relocation blocks. An error is a
// deterministic payload-contract violation.
func requestOf(env workReleased) (usecases.WorkReleasedRequest, error) {
	transfer, err := transferDetailsOf(env.Data)
	if err != nil {
		return usecases.WorkReleasedRequest{}, err
	}
	relocation, err := relocationMoveOf(env.Data)
	if err != nil {
		return usecases.WorkReleasedRequest{}, err
	}
	replenishment, err := replenishmentMoveOf(env.Data)
	if err != nil {
		return usecases.WorkReleasedRequest{}, err
	}
	pick, err := pickPlanOf(env.Data)
	if err != nil {
		return usecases.WorkReleasedRequest{}, err
	}
	caseBreak, err := caseBreakOf(env.Data)
	if err != nil {
		return usecases.WorkReleasedRequest{}, err
	}
	return usecases.WorkReleasedRequest{
		EventId:       env.EventId,
		PathId:        env.Data.PathId,
		WorkUnitId:    env.Data.WorkUnitId,
		CPT:           env.Data.CPT,
		Ref:           env.Data.Ref,
		LineNo:        env.Data.LineNo.Int(),
		Fragile:       env.Data.Fragile,
		GiftWrap:      env.Data.GiftWrap,
		Transfer:      transfer,
		Relocation:    relocation,
		Replenishment: replenishment,
		Pick:          pick,
		CaseBreak:     caseBreak,
	}, nil
}

// pickPlanOf maps the optional WorkReleased pick object onto a validated
// task.PickPlan (ADR 0054): nil when absent. A present object that is not a
// valid plan (blank sku, missing or non-positive quantity, a unit other than
// EACH or CASE) is a hard error, never a silent default.
func pickPlanOf(d WorkReleasedData) (*task.PickPlan, error) {
	p := d.Pick
	if p == nil {
		return nil, nil
	}
	if p.Quantity == nil {
		return nil, fmt.Errorf("kafka: pick of work unit %q: %w: quantity is required", d.WorkUnitId, task.ErrInvalidPickPlan)
	}
	plan, err := task.NewPickPlan(p.SKU, *p.Quantity, p.Unit, p.FromBin)
	if err != nil {
		return nil, fmt.Errorf("kafka: pick of work unit %q: %w", d.WorkUnitId, err)
	}
	return &plan, nil
}

// caseBreakOf maps the optional WorkReleased case_break object onto a
// validated task.CaseBreak (ADR 0054): nil when absent.
func caseBreakOf(d WorkReleasedData) (*task.CaseBreak, error) {
	b := d.CaseBreak
	if b == nil {
		return nil, nil
	}
	brk, err := task.NewCaseBreak(b.LPN, b.SKU, b.LooseBin, b.PickTaskId)
	if err != nil {
		return nil, fmt.Errorf("kafka: case break of work unit %q: %w", d.WorkUnitId, err)
	}
	return &brk, nil
}

// replenishmentMoveOf maps the optional WorkReleased replenishment object
// onto a validated task.ReplenishmentMove (ADR 0050): nil when the object is
// absent. A present object that is not a valid move (a blank request_id,
// sku or bin, a missing or non-positive quantity, equal bins, a priority
// other than NORMAL or URGENT) is a hard error wrapping
// task.ErrInvalidReplenishmentMove or task.ErrUnknownReplenishmentPriority
// — never a silent default.
func replenishmentMoveOf(d WorkReleasedData) (*task.ReplenishmentMove, error) {
	r := d.Replenishment
	if r == nil {
		return nil, nil
	}
	if r.Quantity == nil {
		return nil, fmt.Errorf("kafka: replenishment of work unit %q: %w: quantity is required", d.WorkUnitId, task.ErrInvalidReplenishmentMove)
	}
	move, err := task.NewReplenishmentMove(r.RequestId, r.SKU, r.FromBin, r.ToBin, *r.Quantity, r.Priority)
	if err != nil {
		return nil, fmt.Errorf("kafka: replenishment of work unit %q: %w", d.WorkUnitId, err)
	}
	return &move, nil
}

// relocationMoveOf maps the optional WorkReleased relocation object onto a
// validated task.RelocationMove (ADR 0047): nil when the object is absent.
// A present object that is not a valid move (missing move_seq, unknown
// kind, a bin its kind requires missing, blank plan_id or sku) is a hard
// error wrapping task.ErrInvalidRelocationMove or
// task.ErrUnknownRelocationKind — never a silent default.
func relocationMoveOf(d WorkReleasedData) (*task.RelocationMove, error) {
	r := d.Relocation
	if r == nil {
		return nil, nil
	}
	if r.MoveSeq == nil {
		return nil, fmt.Errorf("kafka: relocation of work unit %q: %w: move_seq is required", d.WorkUnitId, task.ErrInvalidRelocationMove)
	}
	move, err := task.NewRelocationMove(r.PlanId, *r.MoveSeq, r.Kind, r.SKU, r.FromBin, r.ToBin)
	if err != nil {
		return nil, fmt.Errorf("kafka: relocation of work unit %q: %w", d.WorkUnitId, err)
	}
	return &move, nil
}

// transferDetailsOf maps WorkReleasedData's optional transfer-correlation
// fields onto the domain's task.TransferDetails. A payload with no
// transfer_ref is not transfer work: nil, and the created task publishes
// no transfer fact on completion. A payload WITH a transfer_ref but an
// unknown work_kind is a hard error — never a silent default, mirroring
// the path_id discipline (ADR-0017).
func transferDetailsOf(d WorkReleasedData) (*task.TransferDetails, error) {
	if d.TransferRef == "" {
		return nil, nil
	}
	kind, err := task.ParseWorkKind(d.WorkKind)
	if err != nil {
		return nil, fmt.Errorf("kafka: transfer_ref %q carries unknown work_kind %q: %w", d.TransferRef, d.WorkKind, err)
	}
	return &task.TransferDetails{
		TransferRef: d.TransferRef,
		DemandId:    d.Ref,
		WorkKind:    kind,
		SiteId:      d.SiteId,
		SKU:         d.SKU,
		Quantity:    d.Quantity,
	}, nil
}
