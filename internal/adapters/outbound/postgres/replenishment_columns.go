package postgres

import (
	"fmt"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// replenishmentColumns is a task's nullable replenishment block as stored
// (migration 0020, ADR 0050): the planned move and, once completed, the
// executed result. Every field is nil for a non-REPLENISH task.
type replenishmentColumns struct {
	requestId, sku, fromBin, toBin, priority *string
	plannedQuantity                          *int
	quantity                                 *int
	resultFromBin, resultToBin               *string
}

// replenishmentColumnsOf maps t's move and result onto their columns.
func replenishmentColumnsOf(t *task.Task) replenishmentColumns {
	var c replenishmentColumns
	if m := t.Replenishment(); m != nil {
		q, priority := m.Quantity, string(m.Priority)
		c.requestId, c.sku, c.fromBin, c.toBin = &m.RequestId, &m.SKU, &m.FromBin, &m.ToBin
		c.plannedQuantity, c.priority = &q, &priority
	}
	if r := t.ReplenishmentResult(); r != nil {
		q := r.Quantity
		c.quantity, c.resultFromBin, c.resultToBin = &q, nullableString(r.FromBin), nullableString(r.ToBin)
	}
	return c
}

// values is the INSERT argument order of taskColumns' replenishment tail.
func (c replenishmentColumns) values() []any {
	return []any{c.requestId, c.sku, c.fromBin, c.toBin, c.plannedQuantity, c.priority, c.quantity, c.resultFromBin, c.resultToBin}
}

// dest is the Scan destination order of taskColumns' replenishment tail.
func (c *replenishmentColumns) dest() []any {
	return []any{&c.requestId, &c.sku, &c.fromBin, &c.toBin, &c.plannedQuantity, &c.priority, &c.quantity, &c.resultFromBin, &c.resultToBin}
}

// rehydrate rebuilds the move and result. A NULL request id is "not
// REPLENISH work" (nil, nil), exactly like a row written before migration
// 0020. A stored move that no longer validates fails the rehydrate (same
// discipline as the relocation block): a REPLENISH task whose completion
// could not publish a usable move must not silently load.
func (c replenishmentColumns) rehydrate(id string) (*task.ReplenishmentMove, *task.ReplenishmentResult, error) {
	if c.requestId == nil {
		return nil, nil, nil
	}
	move, err := task.NewReplenishmentMove(*c.requestId, derefString(c.sku), derefString(c.fromBin), derefString(c.toBin), derefInt(c.plannedQuantity), derefString(c.priority))
	if err != nil {
		return nil, nil, fmt.Errorf("rehydrate task %q: %w", id, err)
	}
	if c.quantity == nil {
		return &move, nil, nil
	}
	return &move, &task.ReplenishmentResult{Quantity: *c.quantity, FromBin: derefString(c.resultFromBin), ToBin: derefString(c.resultToBin)}, nil
}
