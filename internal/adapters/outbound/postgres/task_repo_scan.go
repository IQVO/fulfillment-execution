package postgres

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

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
