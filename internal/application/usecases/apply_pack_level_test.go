package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/packview"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

var t0 = time.Date(2026, 10, 6, 20, 0, 0, 0, time.UTC)

func declared(eventId, gtin string, level task.PackLevel, upp int, version int64) usecases.PackLevelRequest {
	return usecases.PackLevelRequest{EventId: eventId, Record: ports.PackRecord{
		GTIN: gtin, SKU: "SKU-1", Level: level, UnitsPerPack: upp, Version: version, EffectiveFrom: t0,
	}}
}

func retired(eventId, gtin string, version int64, at time.Time) usecases.PackLevelRequest {
	return usecases.PackLevelRequest{EventId: eventId, Retire: true, RetiredAt: at, Record: ports.PackRecord{GTIN: gtin, Version: version}}
}

func TestApplyPackLevel_DeclareThenRetireKeepsTheGTINResolvable(t *testing.T) {
	view := packview.NewMemoryStore()
	apply := &usecases.ApplyPackLevel{Processed: memory.NewProcessedEventsRepo(), View: view}
	ctx := context.Background()

	if err := apply.Execute(ctx, declared("e1", "24006381333935", task.PackLevelCase, 12, 3)); err != nil {
		t.Fatalf("declare: %v", err)
	}
	got, ok, _ := view.FindByGTIN(ctx, "24006381333935")
	if !ok || got.SKU != "SKU-1" || got.Level != task.PackLevelCase || got.UnitsPerPack != 12 || got.EffectiveTo != nil {
		t.Fatalf("declared row = %+v ok=%v", got, ok)
	}

	end := t0.Add(time.Hour)
	if err := apply.Execute(ctx, retired("e2", "24006381333935", 4, end)); err != nil {
		t.Fatalf("retire: %v", err)
	}
	got, ok, _ = view.FindByGTIN(ctx, "24006381333935")
	if !ok || got.EffectiveTo == nil || !got.EffectiveTo.Equal(end) || got.UnitsPerPack != 12 {
		t.Fatalf("retired row = %+v ok=%v; the GTIN must stay resolvable, end-dated", got, ok)
	}
}

func TestApplyPackLevel_StaleAndDuplicateAreNoOps(t *testing.T) {
	view := packview.NewMemoryStore()
	apply := &usecases.ApplyPackLevel{Processed: memory.NewProcessedEventsRepo(), View: view}
	ctx := context.Background()
	_ = apply.Execute(ctx, declared("e1", "24006381333935", task.PackLevelCase, 12, 5))

	// older declaration, then older retirement, then a replay of e1: nothing changes.
	for _, r := range []usecases.PackLevelRequest{
		declared("e2", "24006381333935", task.PackLevelCase, 24, 4),
		retired("e3", "24006381333935", 5, t0.Add(time.Hour)),
		declared("e1", "24006381333935", task.PackLevelCase, 99, 9),
	} {
		if err := apply.Execute(ctx, r); err != nil {
			t.Fatalf("apply %s: %v", r.EventId, err)
		}
	}
	got, _, _ := view.FindByGTIN(ctx, "24006381333935")
	if got.UnitsPerPack != 12 || got.Version != 5 || got.EffectiveTo != nil {
		t.Fatalf("row = %+v, want untouched v5/12", got)
	}
}

func TestApplyPackLevel_RetireOfUnknownGTINCreatesNothing(t *testing.T) {
	view := packview.NewMemoryStore()
	apply := &usecases.ApplyPackLevel{Processed: memory.NewProcessedEventsRepo(), View: view}
	if err := apply.Execute(context.Background(), retired("e1", "24006381333935", 4, t0)); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if _, ok, _ := view.FindByGTIN(context.Background(), "24006381333935"); ok {
		t.Fatal("a retirement must not create a level")
	}
}

type failingView struct{ ports.PackViewCopy }

func (failingView) UpsertIfNewer(context.Context, ports.PackRecord) (bool, error) {
	return false, errors.New("boom")
}

func TestApplyPackLevel_FailedWriteReleasesTheClaimWithoutUnitOfWork(t *testing.T) {
	processed := memory.NewProcessedEventsRepo()
	apply := &usecases.ApplyPackLevel{Processed: processed, View: failingView{}}
	if err := apply.Execute(context.Background(), declared("e1", "24006381333935", task.PackLevelCase, 12, 1)); err == nil {
		t.Fatal("want the transient error")
	}
	isNew, _ := processed.MarkProcessed(context.Background(), "e1")
	if !isNew {
		t.Fatal("the claim must be released so the redelivery re-applies")
	}
}

func TestApplyPackLevel_RunsInOneUnitOfWork(t *testing.T) {
	uow := &recordingUoW{}
	apply := &usecases.ApplyPackLevel{Processed: memory.NewProcessedEventsRepo(), View: packview.NewMemoryStore(), UnitOfWork: uow}
	if err := apply.Execute(context.Background(), declared("e1", "24006381333935", task.PackLevelCase, 12, 1)); err != nil {
		t.Fatal(err)
	}
	if uow.calls != 1 {
		t.Fatalf("unit of work calls = %d, want 1", uow.calls)
	}
}
