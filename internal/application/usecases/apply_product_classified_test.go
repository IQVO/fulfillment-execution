package usecases_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// fakeCopy is a ports.ProductClassificationCopy with the production version
// guard, a call counter and an injectable failure for the first failN calls.
type fakeCopy struct {
	rows  map[string]ports.ProductClassificationRecord
	calls int
	failN int
}

func newFakeCopy() *fakeCopy {
	return &fakeCopy{rows: map[string]ports.ProductClassificationRecord{}}
}

func (f *fakeCopy) UpsertIfNewer(_ context.Context, rec ports.ProductClassificationRecord) (bool, error) {
	f.calls++
	if f.failN > 0 {
		f.failN--
		return false, errors.New("fake: transient upsert failure")
	}
	if cur, ok := f.rows[rec.SKU]; ok && rec.Version <= cur.Version {
		return false, nil
	}
	f.rows[rec.SKU] = rec
	return true, nil
}

// failingProcessed fails MarkProcessed with err.
type failingProcessed struct{ err error }

func (f failingProcessed) MarkProcessed(context.Context, string) (bool, error) { return false, f.err }

// recordingUoW is a UnitOfWork that runs fn directly and counts calls.
type recordingUoW struct{ calls int }

func (u *recordingUoW) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	u.calls++
	return fn(ctx)
}

func classified(eventId, sku string, version int64, dot int, tags ...string) usecases.ProductClassifiedRequest {
	return usecases.ProductClassifiedRequest{
		EventId: eventId,
		Record:  ports.ProductClassificationRecord{SKU: sku, HandlingTags: tags, DOTHazardClass: dot, Version: version},
	}
}

func TestApplyProductClassified_InsertsThenNewerVersionReplaces(t *testing.T) {
	cp := newFakeCopy()
	apply := &usecases.ApplyProductClassified{Processed: memory.NewProcessedEventsRepo(), Copy: cp}
	ctx := context.Background()

	if err := apply.Execute(ctx, classified("e1", "SKU-1", 1, 3, "Hazmat")); err != nil {
		t.Fatalf("apply v1: %v", err)
	}
	if err := apply.Execute(ctx, classified("e2", "SKU-1", 2, 0, "Fragile")); err != nil {
		t.Fatalf("apply v2: %v", err)
	}
	got := cp.rows["SKU-1"]
	if got.Version != 2 || got.DOTHazardClass != 0 || len(got.HandlingTags) != 1 || got.HandlingTags[0] != "Fragile" {
		t.Fatalf("row = %+v, want the full v2 state", got)
	}
}

func TestApplyProductClassified_StaleVersionIsIgnoredWithoutError(t *testing.T) {
	cp := newFakeCopy()
	apply := &usecases.ApplyProductClassified{Processed: memory.NewProcessedEventsRepo(), Copy: cp}
	ctx := context.Background()

	if err := apply.Execute(ctx, classified("e3", "SKU-2", 5, 3, "Hazmat")); err != nil {
		t.Fatalf("apply v5: %v", err)
	}
	if err := apply.Execute(ctx, classified("e4", "SKU-2", 4, 8, "Hazmat")); err != nil {
		t.Fatalf("stale v4 must not be an error: %v", err)
	}
	if got := cp.rows["SKU-2"]; got.Version != 5 || got.DOTHazardClass != 3 {
		t.Fatalf("row = %+v, a stale version overwrote the copy", got)
	}
}

func TestApplyProductClassified_DuplicateEventIdAppliesOnce(t *testing.T) {
	cp := newFakeCopy()
	apply := &usecases.ApplyProductClassified{Processed: memory.NewProcessedEventsRepo(), Copy: cp}
	ctx := context.Background()
	req := classified("e5", "SKU-3", 1, 9, "Hazmat")

	for i := 0; i < 3; i++ {
		if err := apply.Execute(ctx, req); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	if cp.calls != 1 {
		t.Fatalf("upsert called %d times, want exactly 1 (dedupe on the CloudEvents id)", cp.calls)
	}
}

// Without a UnitOfWork a failed upsert must release the claim, or the retry
// of the same event would be skipped as already processed and the
// classification lost.
func TestApplyProductClassified_FailedUpsertReleasesClaimSoRetryApplies(t *testing.T) {
	cp := newFakeCopy()
	cp.failN = 1
	processed := memory.NewProcessedEventsRepo()
	apply := &usecases.ApplyProductClassified{Processed: processed, Copy: cp}
	ctx := context.Background()
	req := classified("e6", "SKU-4", 1, 3, "Hazmat")

	if err := apply.Execute(ctx, req); err == nil {
		t.Fatal("expected the transient upsert failure to surface")
	}
	if err := apply.Execute(ctx, req); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got, ok := cp.rows["SKU-4"]; !ok || got.Version != 1 {
		t.Fatalf("the retry did not apply the classification: %+v", cp.rows)
	}
}

// With a UnitOfWork the claim rolls back with the transaction, so the use
// case must NOT also release it explicitly.
func TestApplyProductClassified_WithUnitOfWorkRunsInsideItAndDoesNotRelease(t *testing.T) {
	cp := newFakeCopy()
	cp.failN = 1
	processed := memory.NewProcessedEventsRepo()
	uow := &recordingUoW{}
	apply := &usecases.ApplyProductClassified{Processed: processed, Copy: cp, UnitOfWork: uow}
	ctx := context.Background()

	if err := apply.Execute(ctx, classified("e7", "SKU-5", 1, 3, "Hazmat")); err == nil {
		t.Fatal("expected the upsert failure to surface")
	}
	if uow.calls != 1 {
		t.Fatalf("UnitOfWork.Execute called %d times, want 1", uow.calls)
	}
	// The recording UoW does not roll back, so the claim is still there:
	// proof that the use case left the release to the transaction.
	isNew, err := processed.MarkProcessed(ctx, "e7")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if isNew {
		t.Fatal("the use case released the claim although a UnitOfWork owns the rollback")
	}
}

func TestApplyProductClassified_MarkProcessedErrorIsReturned(t *testing.T) {
	cp := newFakeCopy()
	boom := errors.New("db down")
	apply := &usecases.ApplyProductClassified{Processed: failingProcessed{err: boom}, Copy: cp}

	err := apply.Execute(context.Background(), classified("e8", "SKU-6", 1, 0, "Fragile"))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the MarkProcessed failure", err)
	}
	if cp.calls != 0 {
		t.Fatal("upsert must not run when the claim failed")
	}
}

// releaseFailingProcessed claims like the memory repo but fails to release.
type releaseFailingProcessed struct {
	*memory.ProcessedEventsRepo
}

func (releaseFailingProcessed) ReleaseProcessed(context.Context, string) error {
	return errors.New("fake: release failed")
}

func TestApplyProductClassified_ReleaseFailureIsJoined(t *testing.T) {
	cp := newFakeCopy()
	cp.failN = 1
	apply := &usecases.ApplyProductClassified{Processed: releaseFailingProcessed{memory.NewProcessedEventsRepo()}, Copy: cp}

	err := apply.Execute(context.Background(), classified("e9", "SKU-7", 1, 3, "Hazmat"))
	if err == nil || !containsAll(err.Error(), "transient upsert failure", "release failed") {
		t.Fatalf("err = %v, want both the upsert and the release failures", err)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
