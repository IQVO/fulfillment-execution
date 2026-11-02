package postgres

import (
	"fmt"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

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
