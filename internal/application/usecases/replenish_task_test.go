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

// replenishCatalogue is the process-path catalogue with the RELOCATE and
// REPLENISH paths process-path-management publishes next to PICK.
func replenishCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}},
		{Id: "RELOCATE", MatchPrefix: "relocate", Direct: true, RequiredCapabilities: []string{"relocate"}},
		{Id: "REPLENISH", MatchPrefix: "replenish", Direct: true, RequiredCapabilities: []string{"replenish"}},
	})
}

type replenishFixture struct {
	tasks     *memory.TaskRepo
	publisher *events.BufferedPublisher
	processed *memory.ProcessedEventsRepo
	apply     *usecases.ApplyWorkReleased
	complete  *usecases.CompleteTask
}

func newReplenishFixture() replenishFixture {
	tasks := memory.NewTaskRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(atomicEpoch)
	processed := memory.NewProcessedEventsRepo()
	create := &usecases.CreateTask{Tasks: tasks, Publisher: publisher, Clock: clock, NewId: fixedTaskId("rep")}
	return replenishFixture{
		tasks:     tasks,
		publisher: publisher,
		processed: processed,
		apply:     &usecases.ApplyWorkReleased{CreateTask: create, Processed: processed, Catalogue: replenishCatalogue()},
		complete:  &usecases.CompleteTask{Tasks: tasks, Publisher: publisher, Clock: clock},
	}
}

func replenishmentMove(t *testing.T, priority string) *task.ReplenishmentMove {
	t.Helper()
	m, err := task.NewReplenishmentMove("req-9", "SKU-1", "WH1-RSV-AMB-B02-03-02-C", "WH1-FWD-AMB-A01-01-01-A", 24, priority)
	if err != nil {
		t.Fatalf("NewReplenishmentMove: %v", err)
	}
	return &m
}

func replenishRequest(eventId string, move *task.ReplenishmentMove) usecases.WorkReleasedRequest {
	return usecases.WorkReleasedRequest{EventId: eventId, PathId: "REPLENISH", WorkUnitId: "replenish-req-9", Ref: "req-9",
		CPT: atomicEpoch.Add(time.Hour), Replenishment: move}
}

// releaseAndClaim applies a REPLENISH WorkReleased and claims the created
// task for station s1, returning it.
func (f replenishFixture) releaseAndClaim(t *testing.T, move *task.ReplenishmentMove) *task.Task {
	t.Helper()
	if err := f.apply.Execute(context.Background(), replenishRequest("evt-rep-1", move)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	list, err := f.tasks.FindClaimableByType(context.Background(), task.Replenish, atomicEpoch)
	if err != nil || len(list) != 1 {
		t.Fatalf("expected 1 REPLENISH task, got %d err=%v", len(list), err)
	}
	tk := list[0]
	if err := tk.Claim("s1", shared.NewCapabilitySet("replenish"), atomicEpoch, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := f.tasks.Save(context.Background(), tk); err != nil {
		t.Fatalf("save claim: %v", err)
	}
	return tk
}

func TestApplyWorkReleased_ReplenishPathCreatesReplenishTaskWithTheMove(t *testing.T) {
	f := newReplenishFixture()
	move := replenishmentMove(t, "URGENT")
	tk := f.releaseAndClaim(t, move)

	if tk.Type() != task.Replenish {
		t.Fatalf("type = %s, want REPLENISH", tk.Type())
	}
	if tk.Replenishment() == nil || *tk.Replenishment() != *move {
		t.Fatalf("move = %+v, want %+v", tk.Replenishment(), move)
	}
	if tk.Relocation() != nil {
		t.Fatalf("a REPLENISH task carries no relocation, got %+v", tk.Relocation())
	}
	if !tk.RequiredCapabilities().HasAll(shared.NewCapabilitySet("replenish")) {
		t.Fatalf("required capabilities = %v, want replenish", tk.RequiredCapabilities())
	}
	if tk.SourceOrderId() != "" || tk.SourceLineNo() != 0 || tk.Transfer() != nil {
		t.Fatalf("replenishment work is neither order nor transfer work: %q %d %+v", tk.SourceOrderId(), tk.SourceLineNo(), tk.Transfer())
	}
	if string(tk.OrderRef()) != "replenish-req-9" {
		t.Fatalf("orderRef = %q, want the work unit id", tk.OrderRef())
	}
}

func TestApplyWorkReleased_ReplenishPairingRule(t *testing.T) {
	relocation := relocationMove(t, "Relocate", "A", "B")
	cases := []struct {
		name string
		req  usecases.WorkReleasedRequest
		want error
	}{
		{"REPLENISH without a move", usecases.WorkReleasedRequest{EventId: "e1", PathId: "REPLENISH", WorkUnitId: "wu", CPT: atomicEpoch}, usecases.ErrReplenishmentMoveMissing},
		{"move on PICK", usecases.WorkReleasedRequest{EventId: "e2", PathId: "PICK", WorkUnitId: "wu", CPT: atomicEpoch, Replenishment: replenishmentMove(t, "NORMAL")}, usecases.ErrReplenishmentMoveOnOtherPath},
		{"move on RELOCATE", usecases.WorkReleasedRequest{EventId: "e3", PathId: "RELOCATE", WorkUnitId: "wu", CPT: atomicEpoch, Relocation: relocation, Replenishment: replenishmentMove(t, "NORMAL")}, usecases.ErrReplenishmentMoveOnOtherPath},
		{"relocation on REPLENISH", usecases.WorkReleasedRequest{EventId: "e4", PathId: "REPLENISH", WorkUnitId: "wu", CPT: atomicEpoch, Relocation: relocation, Replenishment: replenishmentMove(t, "NORMAL")}, usecases.ErrRelocationMoveOnOtherPath},
		{"relocation only on REPLENISH", usecases.WorkReleasedRequest{EventId: "e5", PathId: "REPLENISH", WorkUnitId: "wu", CPT: atomicEpoch, Relocation: relocation}, usecases.ErrRelocationMoveOnOtherPath},
		{"RELOCATE without relocation but with a replenishment", usecases.WorkReleasedRequest{EventId: "e6", PathId: "RELOCATE", WorkUnitId: "wu", CPT: atomicEpoch, Replenishment: replenishmentMove(t, "NORMAL")}, usecases.ErrRelocationMoveMissing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplenishFixture()
			if err := f.apply.Execute(context.Background(), tc.req); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			for _, typ := range []task.Type{task.Pick, task.Relocate, task.Replenish} {
				if n, _ := f.tasks.CountByTypeAndStatus(context.Background(), typ, task.Pending); n != 0 {
					t.Fatalf("no %s task may be created, got %d", typ, n)
				}
			}
			isNew, err := f.processed.MarkProcessed(context.Background(), tc.req.EventId)
			if err != nil || !isNew {
				t.Fatalf("a rejected release must leave no processed marker (isNew=%v err=%v)", isNew, err)
			}
			if len(f.publisher.Events()) != 0 {
				t.Fatalf("a rejected release publishes nothing, got %v", f.publisher.Events())
			}
		})
	}
}

func TestApplyWorkReleased_ReplenishReplayCreatesOneTask(t *testing.T) {
	f := newReplenishFixture()
	req := replenishRequest("evt-replay", replenishmentMove(t, "NORMAL"))
	for i := 0; i < 3; i++ {
		if err := f.apply.Execute(context.Background(), req); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}
	if n, _ := f.tasks.CountByTypeAndStatus(context.Background(), task.Replenish, task.Pending); n != 1 {
		t.Fatalf("replayed release created %d tasks, want 1", n)
	}
	got := 0
	for _, e := range f.publisher.Events() {
		if e.EventName() == "TaskAvailable" {
			got++
		}
	}
	if got != 1 {
		t.Fatalf("TaskAvailable raised %d times, want once", got)
	}
}

func TestCompleteTask_ReplenishWithConfirmationPublishesTheExecutedMove(t *testing.T) {
	const from, to = "WH1-RSV-AMB-B02-03-02-C", "WH1-FWD-AMB-A01-01-01-A"
	for _, tc := range []struct {
		name    string
		confirm task.ReplenishmentConfirmation
	}{
		{"quantity only", task.ReplenishmentConfirmation{Quantity: 20}},
		{"with matching scans", task.ReplenishmentConfirmation{Quantity: 24, FromBin: from, ToBin: to}},
		{"more than planned", task.ReplenishmentConfirmation{Quantity: 30}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplenishFixture()
			tk := f.releaseAndClaim(t, replenishmentMove(t, "NORMAL"))

			if err := f.complete.ExecuteWith(context.Background(), tk.Id(), "s1", tc.confirm); err != nil {
				t.Fatalf("complete: %v", err)
			}

			got := lastTaskCompleted(t, f.publisher)
			want := shared.TaskReplenishment{SKU: "SKU-1", FromBin: from, ToBin: to, Quantity: tc.confirm.Quantity, RequestRef: "req-9"}
			if got.Replenishment == nil || *got.Replenishment != want {
				t.Fatalf("TaskCompleted.Replenishment = %+v, want %+v", got.Replenishment, want)
			}
			if got.Relocation != nil {
				t.Fatalf("a REPLENISH TaskCompleted carries no relocation, got %+v", got.Relocation)
			}
			saved, _ := f.tasks.FindById(context.Background(), tk.Id())
			if saved.Status() != task.Completed || saved.ReplenishmentResult() == nil || saved.ReplenishmentResult().Quantity != tc.confirm.Quantity {
				t.Fatalf("saved task = %s / %+v", saved.Status(), saved.ReplenishmentResult())
			}
		})
	}
}

func TestCompleteTask_ReplenishRejectionsPublishNothingAndChangeNothing(t *testing.T) {
	cases := []struct {
		name    string
		confirm task.ReplenishmentConfirmation
		want    error
	}{
		{"no confirmation", task.ReplenishmentConfirmation{}, task.ErrReplenishmentConfirmationRequired},
		{"negative quantity", task.ReplenishmentConfirmation{Quantity: -2}, task.ErrInvalidReplenishmentConfirmation},
		{"bins without quantity", task.ReplenishmentConfirmation{FromBin: "WH1-RSV-AMB-B02-03-02-C"}, task.ErrInvalidReplenishmentConfirmation},
		{"other source bin", task.ReplenishmentConfirmation{Quantity: 5, FromBin: "WH1-RSV-AMB-Z99-01-01-A"}, task.ErrInvalidReplenishmentConfirmation},
		{"other destination bin", task.ReplenishmentConfirmation{Quantity: 5, ToBin: "WH1-FWD-AMB-Z99-01-01-A"}, task.ErrInvalidReplenishmentConfirmation},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplenishFixture()
			tk := f.releaseAndClaim(t, replenishmentMove(t, "NORMAL"))
			before := len(f.publisher.Events())

			if err := f.complete.ExecuteWith(context.Background(), tk.Id(), "s1", tc.confirm); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if len(f.publisher.Events()) != before {
				t.Fatal("a rejected completion must publish nothing")
			}
			saved, _ := f.tasks.FindById(context.Background(), tk.Id())
			if saved.Status() != task.Claimed || saved.ReplenishmentResult() != nil {
				t.Fatalf("a rejected completion changes nothing, got %s / %+v", saved.Status(), saved.ReplenishmentResult())
			}
		})
	}
}

func TestCompleteTask_ReplenishClaimRulesApplyFirst(t *testing.T) {
	f := newReplenishFixture()
	tk := f.releaseAndClaim(t, replenishmentMove(t, "NORMAL"))
	good := task.ReplenishmentConfirmation{Quantity: 20}

	if err := f.complete.ExecuteWith(context.Background(), tk.Id(), "s2", good); !errors.Is(err, task.ErrNotOwner) {
		t.Fatalf("another station: err = %v, want ErrNotOwner", err)
	}
	if err := f.complete.ExecuteWith(context.Background(), "ghost", "s1", good); !errors.Is(err, usecases.ErrTaskNotFound) {
		t.Fatalf("unknown task: err = %v, want ErrTaskNotFound", err)
	}
}

// A replayed completion (the second POST after a lost response) is a
// conflict and publishes no second TaskCompleted.
func TestCompleteTask_ReplenishReplayPublishesOneTaskCompleted(t *testing.T) {
	f := newReplenishFixture()
	tk := f.releaseAndClaim(t, replenishmentMove(t, "NORMAL"))
	good := task.ReplenishmentConfirmation{Quantity: 20}

	if err := f.complete.ExecuteWith(context.Background(), tk.Id(), "s1", good); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if err := f.complete.ExecuteWith(context.Background(), tk.Id(), "s1", good); !errors.Is(err, task.ErrAlreadyCompleted) {
		t.Fatalf("replay: err = %v, want ErrAlreadyCompleted", err)
	}
	completed := 0
	for _, e := range f.publisher.Events() {
		if e.EventName() == "TaskCompleted" {
			completed++
		}
	}
	if completed != 1 {
		t.Fatalf("TaskCompleted published %d times, want once", completed)
	}
}

func TestCompleteTask_ReplenishConfirmationOnAnotherTypeIsStillRejected(t *testing.T) {
	f := newReplenishFixture()
	if err := f.apply.Execute(context.Background(), usecases.WorkReleasedRequest{EventId: "evt-pick", PathId: "PICK", WorkUnitId: "ORD-1-line-1", Ref: "ORD-1", CPT: atomicEpoch.Add(time.Hour)}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	list, _ := f.tasks.FindClaimableByType(context.Background(), task.Pick, atomicEpoch)
	tk := list[0]
	if err := tk.Claim("s1", shared.NewCapabilitySet("pick"), atomicEpoch, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	_ = f.tasks.Save(context.Background(), tk)

	if err := f.complete.ExecuteWith(context.Background(), tk.Id(), "s1", task.ReplenishmentConfirmation{Quantity: 3}); !errors.Is(err, task.ErrNotRelocationTask) {
		t.Fatalf("err = %v, want ErrNotRelocationTask", err)
	}
	if err := f.complete.ExecuteWith(context.Background(), tk.Id(), "s1", task.ReplenishmentConfirmation{}); err != nil {
		t.Fatalf("a PICK completes without inputs: %v", err)
	}
	if got := lastTaskCompleted(t, f.publisher); got.Replenishment != nil || got.Relocation != nil {
		t.Fatalf("a PICK TaskCompleted carries no move, got %+v / %+v", got.Replenishment, got.Relocation)
	}
}

// The REPLENISH creation path raises TaskCreated then one TaskAvailable.
func TestCreateTask_ExecuteReplenishmentRaisesTheTwoCreationEvents(t *testing.T) {
	h := newHarness()
	tk, err := newPoolCreate(h).ExecuteReplenishment(context.Background(), shared.NewCPT(epoch.Add(time.Hour)), "o", shared.NewCapabilitySet("replenish"), *replenishmentMove(t, "URGENT"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable")
	if a := lastAvailable(t, h); a.TaskId != tk.Id() {
		t.Fatalf("TaskAvailable names %s, want %s", a.TaskId, tk.Id())
	}
}

// Atomicity: the REPLENISH completion's save and TaskCompleted publish run in
// ONE unit of work, and a publish failure rolls it back.
func replenishClaimedInRepo(t *testing.T) (*scopedTaskRepo, shared.TaskId, *memory.FixedClock) {
	t.Helper()
	tasks := &scopedTaskRepo{TaskRepo: memory.NewTaskRepo()}
	clock := memory.NewFixedClock(epoch)
	tk := task.NewReplenishmentTask("rep-1", shared.NewCPT(epoch.Add(time.Hour)), "wu", shared.NewCapabilitySet("replenish"), *replenishmentMove(t, "NORMAL"))
	if err := tk.Claim("s1", shared.NewCapabilitySet("replenish"), epoch, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := tasks.TaskRepo.Save(context.Background(), tk); err != nil {
		t.Fatalf("save: %v", err)
	}
	return tasks, tk.Id(), clock
}

func TestCompleteTask_ReplenishSaveAndPublishRunInsideOneUnitOfWork(t *testing.T) {
	tasks, id, clock := replenishClaimedInRepo(t)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := &usecases.CompleteTask{Tasks: tasks, Publisher: pub, Clock: clock, UnitOfWork: uow}

	if err := uc.ExecuteWith(context.Background(), id, "s1", task.ReplenishmentConfirmation{Quantity: 20}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, tasks.savesInScope)
}

func TestCompleteTask_ReplenishPublishFailureRollsBack(t *testing.T) {
	tasks, id, clock := replenishClaimedInRepo(t)
	uow := &recordingUnitOfWork{}
	uc := &usecases.CompleteTask{Tasks: tasks, Publisher: &scopedPublisher{err: errors.New("boom")}, Clock: clock, UnitOfWork: uow}

	if err := uc.ExecuteWith(context.Background(), id, "s1", task.ReplenishmentConfirmation{Quantity: 20}); err == nil {
		t.Fatal("expected the publish error to propagate")
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected a rollback, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}
