package usecases

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/consolidation"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// ArriveAtRebin records that one required line of an order has reached
// the Rebin path, and — once every required line has arrived — creates
// the order's PACK task and marks the consolidation complete.
//
// This is the trigger the Rebin path was introduced to provide: a
// multi-line order picked from different pods produces independent PICK
// completions, and Pack cannot start until they have all converged. See
// consolidation.OrderConsolidation for the fan-in tracking itself.
type ArriveAtRebin struct {
	Consolidations ports.OrderConsolidationRepo
	CreateTask     *CreateTask
	Publisher      ports.EventPublisher
	Clock          ports.Clock
	// UnitOfWork brackets the consolidation Save, the nested CreateTask
	// (whose own scope joins this one) and every Publish in ONE atomic
	// scope (ADR 0020); nil runs them back to back.
	UnitOfWork ports.UnitOfWork
}

// Execute records lineId's arrival at Rebin for orderRef. If this is the
// first arrival seen for orderRef, a new OrderConsolidation is created
// with requiredLineIds as its full required set — every subsequent call
// for the same orderRef must pass the SAME requiredLineIds (this is the
// caller's responsibility; the aggregate does not itself resolve
// disagreement between calls, since the required set is meant to be
// fixed once known). Once every required line has arrived, this method
// creates a PACK task for the order (via the existing CreateTask use
// case — same reuse principle as CreateTask's own doc comment: this IS
// its intended use, not a workaround) and publishes OrderConsolidated.
//
// packCPT, packRequired, packFragile and packGiftWrap are the values the
// resulting PACK task should carry — this use case does not invent them;
// the caller (the Rebin-arrival adapter) supplies them from whatever
// context it already has about the order (e.g. carried on the Rebin
// task itself, mirroring how CreateTask's own callers already source
// fragile/giftWrap hints).
func (uc *ArriveAtRebin) Execute(
	ctx context.Context,
	orderRef shared.OrderRef,
	lineId string,
	requiredLineIds []string,
	packCPT shared.CPT,
	packRequired shared.CapabilitySet,
	packFragile bool,
	packGiftWrap bool,
) error {
	// The read-modify-write of the consolidation runs INSIDE the unit of
	// work, behind FindByOrderRefForUpdate's per-order lock (ADR-0034):
	// two lines of one order arriving at the same instant are serialized,
	// so the second reads the first's committed arrival instead of
	// overwriting it (a lost line would leave the order never consolidating).
	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		oc, err := uc.Consolidations.FindByOrderRefForUpdate(ctx, orderRef)
		if err != nil {
			return err
		}
		if oc == nil {
			oc = consolidation.New(string(orderRef), requiredLineIds)
		}
		wasAlreadyComplete := oc.IsComplete()

		if err := oc.RecordArrival(lineId); err != nil {
			return err
		}

		now := uc.Clock.Now()

		// Once consolidation has completed, every further call for this
		// orderRef (a redelivered arrival, or an arrival for a line that
		// somehow arrives twice) is a pure idempotent no-op with respect to
		// PACK task creation — the PACK task was already created exactly
		// once, on the arrival that first completed consolidation. Without
		// this guard, IsComplete() staying true forever would re-trigger
		// CreateTask on every subsequent call.
		if wasAlreadyComplete {
			return nil
		}

		// The consolidation state is saved FIRST and the arrival event
		// published after it, inside the same scope: with a transactional
		// outbox the publish is itself a write bound to this transaction, so
		// the order only matters for the nil-UnitOfWork fallback, where
		// "state, then event" is the convention every other use case follows.
		if err := uc.Consolidations.Save(ctx, oc); err != nil {
			return err
		}
		if err := uc.Publisher.Publish(ctx, shared.NewItemArrivedAtRebin(orderRef, lineId, now)); err != nil {
			return err
		}
		if !oc.IsComplete() {
			return nil
		}
		if _, err := uc.CreateTask.Execute(ctx, task.Pack, packCPT, orderRef, packRequired, packFragile, packGiftWrap); err != nil {
			return err
		}
		return uc.Publisher.Publish(ctx, shared.NewOrderConsolidated(orderRef, now))
	})
}
