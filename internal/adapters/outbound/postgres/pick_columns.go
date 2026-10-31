package postgres

import (
	"fmt"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// pickColumns is a task's nullable pick-plan and case-break blocks as stored
// (migration 0023, ADR 0054). Every field is nil for a task that carries
// neither (every non-PICK / non-BREAK_CASE task and a PICK released without
// data.pick).
type pickColumns struct {
	pickSKU, pickUnit, pickFromBin    *string
	pickQuantity, pickedEaches        *int
	breakLPN, breakSKU, breakLooseBin *string
	breakPickTaskId                   *string
}

// pickColumnsOf maps t's pick plan and case break onto their columns.
func pickColumnsOf(t *task.Task) pickColumns {
	var c pickColumns
	if p := t.PickPlan(); p != nil {
		q, picked, unit := p.Quantity, t.PickedEaches(), string(p.Unit)
		c.pickSKU, c.pickQuantity, c.pickUnit, c.pickFromBin = &p.SKU, &q, &unit, nullableString(p.FromBin)
		c.pickedEaches = &picked
	}
	if b := t.CaseBreak(); b != nil {
		c.breakLPN, c.breakSKU, c.breakLooseBin, c.breakPickTaskId = &b.LPN, &b.SKU, &b.LooseBin, &b.PickTaskId
	}
	return c
}

// values is the INSERT argument order of taskColumns' pick tail.
func (c pickColumns) values() []any {
	return []any{c.pickSKU, c.pickQuantity, c.pickUnit, c.pickFromBin, c.pickedEaches,
		c.breakLPN, c.breakSKU, c.breakLooseBin, c.breakPickTaskId}
}

// dest is the Scan destination order of taskColumns' pick tail.
func (c *pickColumns) dest() []any {
	return []any{&c.pickSKU, &c.pickQuantity, &c.pickUnit, &c.pickFromBin, &c.pickedEaches,
		&c.breakLPN, &c.breakSKU, &c.breakLooseBin, &c.breakPickTaskId}
}

// rehydrate rebuilds the plan, the confirmed eaches and the break. A NULL
// pick_sku / break_lpn is "not that work" (nil), exactly like a row written
// before migration 0023. A stored block that no longer validates fails the
// rehydrate, like the relocation and replenishment blocks.
func (c pickColumns) rehydrate(id string) (*task.PickPlan, int, *task.CaseBreak, error) {
	var plan *task.PickPlan
	picked := 0
	if c.pickSKU != nil {
		p, err := task.NewPickPlan(*c.pickSKU, derefInt(c.pickQuantity), derefString(c.pickUnit), derefString(c.pickFromBin))
		if err != nil {
			return nil, 0, nil, fmt.Errorf("rehydrate task %q: %w", id, err)
		}
		plan, picked = &p, derefInt(c.pickedEaches)
	}
	var brk *task.CaseBreak
	if c.breakLPN != nil {
		b, err := task.NewCaseBreak(*c.breakLPN, derefString(c.breakSKU), derefString(c.breakLooseBin), derefString(c.breakPickTaskId))
		if err != nil {
			return nil, 0, nil, fmt.Errorf("rehydrate task %q: %w", id, err)
		}
		brk = &b
	}
	return plan, picked, brk, nil
}
