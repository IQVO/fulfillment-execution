package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// atomicEpoch is the fixed clock for the atomicity tests.
var atomicEpoch = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// failingTasksRepo fails Save with errFake for the first failSaves calls,
// then delegates — models the transient create failure that used to
// permanently suppress WorkReleased redelivery.
type failingTasksRepo struct {
	*memory.TaskRepo
	failSaves int32
}

func (r *failingTasksRepo) Save(ctx context.Context, t *task.Task) error {
	if r.failSaves > 0 {
		r.failSaves--
		return errors.New("fake: transient task save failure")
	}
	return r.TaskRepo.Save(ctx, t)
}

func atomicCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}},
	})
}

// TestApplyWorkReleased_FailedCreateLeavesNoProcessedMarkerAndRetryCreatesExactlyOneTask
// is the regression test for the lost-WorkReleased bug: under the old
// claim-then-create shape, a task save that failed AFTER the processed-id
// insert committed left an orphaned marker, and the Kafka redelivery was
// acked as already-processed with NO task ever existing. Under
// ApplyWorkReleased the marker and the create are one atomic unit: the
// failed create leaves no marker (released explicitly here — the
// in-memory, no-UnitOfWork configuration), and the retry creates exactly
// one task.
func TestApplyWorkReleased_FailedCreateLeavesNoProcessedMarkerAndRetryCreatesExactlyOneTask(t *testing.T) {
	tasks := &failingTasksRepo{TaskRepo: memory.NewTaskRepo(), failSaves: 1}
	processed := memory.NewProcessedEventsRepo()
	create := &usecases.CreateTask{
		Tasks:     tasks,
		Publisher: events.NewBufferedPublisher(),
		Clock:     memory.NewFixedClock(atomicEpoch),
		NewId:     fixedTaskId("a"),
	}
	apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: processed, Catalogue: atomicCatalogue()}

	req := usecases.WorkReleasedRequest{
		EventId:    "evt-atomic-1",
		PathId:     "PICK",
		WorkUnitId: "wu-atomic",
		CPT:        atomicEpoch.Add(time.Hour),
		Ref:        "release-1",
	}

	// First delivery: the task save fails once.
	if err := apply.Execute(context.Background(), req); err == nil {
		t.Fatal("expected the first delivery to fail while the task save is failing")
	}

	// THE REGRESSION ASSERTION: no processed marker may survive the
	// failed create. Under the old shape this is exactly where the bug
	// lived — the marker was already committed.
	isNew, err := processed.MarkProcessed(context.Background(), "evt-atomic-1")
	if err != nil {
		t.Fatalf("probe mark: %v", err)
	}
	if !isNew {
		t.Fatal("a processed marker survived the failed create — redelivery would be suppressed forever (the lost-WorkReleased bug)")
	}
	// Undo the probe so the retry below is a faithful redelivery.
	if err := processed.ReleaseProcessed(context.Background(), "evt-atomic-1"); err != nil {
		t.Fatalf("release probe: %v", err)
	}

	// Redelivery (Kafka at-least-once): the create now succeeds and
	// creates exactly ONE task.
	if err := apply.Execute(context.Background(), req); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if err := apply.Execute(context.Background(), req); err != nil {
		t.Fatalf("second redelivery: %v", err)
	}
	n, err := tasks.CountByTypeAndStatus(context.Background(), task.Pick, task.Pending)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 task after the redelivery, got %d", n)
	}
}

// TestApplyWorkReleased_FailedCatalogueLookupLeavesNoProcessedMarker is
// the same invariant one step earlier in the atomic scope: an unknown
// path_id (a hard error, ADR-0017) must not leave a marker either — the
// dead-lettered message can be replayed after the catalogue is fixed
// without clearing processed_events by hand (the manual step the old
// INTEGRATION.md documented).
func TestApplyWorkReleased_FailedCatalogueLookupLeavesNoProcessedMarker(t *testing.T) {
	tasks := memory.NewTaskRepo()
	processed := memory.NewProcessedEventsRepo()
	create := &usecases.CreateTask{
		Tasks:     tasks,
		Publisher: events.NewBufferedPublisher(),
		Clock:     memory.NewFixedClock(atomicEpoch),
		NewId:     fixedTaskId("c"),
	}
	apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: processed, Catalogue: atomicCatalogue()}

	req := usecases.WorkReleasedRequest{EventId: "evt-atomic-2", PathId: "NOT-A-PATH", WorkUnitId: "wu-x", CPT: atomicEpoch.Add(time.Hour)}
	if err := apply.Execute(context.Background(), req); err == nil {
		t.Fatal("expected an error for the unknown path_id")
	}

	isNew, err := processed.MarkProcessed(context.Background(), "evt-atomic-2")
	if err != nil {
		t.Fatalf("probe mark: %v", err)
	}
	if !isNew {
		t.Fatal("a processed marker survived the failed catalogue lookup")
	}
}

// TestApplyWorkReleased_TransferRequestCreatesTransferTask checks the
// use-case level wiring of the transfer path (the consumer's
// transferDetailsOf mapping is covered in the kafka package's tests).
func TestApplyWorkReleased_TransferRequestCreatesTransferTask(t *testing.T) {
	tasks := memory.NewTaskRepo()
	processed := memory.NewProcessedEventsRepo()
	create := &usecases.CreateTask{
		Tasks:     tasks,
		Publisher: events.NewBufferedPublisher(),
		Clock:     memory.NewFixedClock(atomicEpoch),
		NewId:     fixedTaskId("d"),
	}
	apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: processed, Catalogue: atomicCatalogue()}

	req := usecases.WorkReleasedRequest{
		EventId:    "evt-atomic-3",
		PathId:     "PICK",
		WorkUnitId: "wu-tr-atomic",
		CPT:        atomicEpoch.Add(time.Hour),
		Transfer: &task.TransferDetails{
			TransferRef: "tr-1",
			DemandId:    "demand-1",
			WorkKind:    task.WorkKindTransferPick,
			SiteId:      "site-a",
			SKU:         "SKU-1",
			Quantity:    3,
		},
	}
	if err := apply.Execute(context.Background(), req); err != nil {
		t.Fatalf("apply: %v", err)
	}
	list, err := tasks.FindClaimableByType(context.Background(), task.Pick, atomicEpoch)
	if err != nil || len(list) != 1 {
		t.Fatalf("expected 1 claimable pick task, got %d err=%v", len(list), err)
	}
	tr := list[0].Transfer()
	if tr == nil || tr.TransferRef != "tr-1" || tr.WorkKind != task.WorkKindTransferPick {
		t.Fatalf("transfer block = %+v", tr)
	}
	if list[0].OrderRef() != shared.OrderRef("wu-tr-atomic") {
		t.Fatalf("orderRef = %q, want the work_unit_id", list[0].OrderRef())
	}
}
