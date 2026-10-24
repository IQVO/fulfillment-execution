package usecases

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared/gs1"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// ErrUnknownGTIN is returned by ConfirmPickScan when the scanned GTIN is not
// in the local PackView (never declared by product-master).
var ErrUnknownGTIN = errors.New("usecases: scanned GTIN is not a known pack level")

// ErrInvalidScannedGTIN is returned when the scanned text is not a valid GTIN
// (length, digits, mod-10 check digit, variable measure). It wraps the gs1
// error so callers can still inspect the cause.
var ErrInvalidScannedGTIN = errors.New("usecases: scanned GTIN is invalid")

// PickScanResult is what one accepted scan did: the eaches it added and the
// eaches confirmed so far against the planned quantity.
type PickScanResult struct {
	ScannedGTIN  string
	SKU          string
	Level        task.PackLevel
	AddedEaches  int
	PickedEaches int
	Planned      int
}

// ConfirmPickScan confirms one scan of a PICK task (ADR 0054): the GTIN is
// resolved against the LOCAL PackView (no call to product-master), must
// identify the task's SKU at the task's pick unit, and adds count eaches (an
// EACH pick) or count x unitsPerPack eaches (a CASE pick). Quantities stay in
// eaches; the confirm-pick completion (POST /tasks/{id}/complete) and its
// TaskCompleted are unchanged. A rejected scan changes nothing.
type ConfirmPickScan struct {
	Tasks      ports.TaskRepo
	Packs      ports.PackViewLookup
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

// Execute applies one scan of gtin x count by stationId on taskId.
func (uc *ConfirmPickScan) Execute(ctx context.Context, taskId shared.TaskId, stationId shared.StationId, gtin string, count int) (PickScanResult, error) {
	parsed, err := gs1.ParseGTIN(strings.TrimSpace(gtin))
	if err == nil {
		err = gs1.RejectVariableMeasure(parsed)
	}
	if err != nil {
		return PickScanResult{}, fmt.Errorf("%w: %w", ErrInvalidScannedGTIN, err)
	}
	var res PickScanResult
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		t, err := uc.Tasks.FindById(ctx, taskId)
		if err != nil {
			return err
		}
		if t == nil {
			return ErrTaskNotFound
		}
		rec, ok, err := uc.Packs.FindByGTIN(ctx, parsed.String())
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: %s", ErrUnknownGTIN, parsed.String())
		}
		added, err := t.ConfirmPickScan(stationId, uc.Clock.Now(), task.ScannedPack{
			GTIN: rec.GTIN, SKU: rec.SKU, Level: rec.Level, UnitsPerPack: rec.UnitsPerPack,
		}, count)
		if err != nil {
			return err
		}
		if err := uc.Tasks.Save(ctx, t); err != nil {
			return err
		}
		res = PickScanResult{ScannedGTIN: rec.GTIN, SKU: rec.SKU, Level: rec.Level, AddedEaches: added,
			PickedEaches: t.PickedEaches(), Planned: t.PickPlan().Quantity}
		return nil
	})
	return res, err
}
