package usecases_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// Pool events (ADR 0049): TaskAvailable when a task enters or re-enters the
// pool (creation, lease-expiry sweep), TaskClaimed when it leaves. The tests
// pin, per path, exactly which events are raised, in order, with the exact
// payload, and that replays and rejected commands raise none.

func eventNames(h *harness) []string {
	evs := h.publisher.Events()
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.EventName())
	}
	return out
}

func wantNames(t *testing.T, h *harness, want ...string) {
	t.Helper()
	if got := eventNames(h); !slices.Equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func lastAvailable(t *testing.T, h *harness) shared.TaskAvailable {
	t.Helper()
	evs := h.publisher.Events()
	for i := len(evs) - 1; i >= 0; i-- {
		if a, ok := evs[i].(shared.TaskAvailable); ok {
			return a
		}
	}
	t.Fatalf("no TaskAvailable among %v", eventNames(h))
	return shared.TaskAvailable{}
}

// poolStack is a harness with a CreateTask, a registered "pick" station and a
// claimed-then-expirable PICK task.
func newPoolCreate(h *harness) *usecases.CreateTask {
	return &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("p")}
}

func createPick(t *testing.T, h *harness) *task.Task {
	t.Helper()
	tk, err := newPoolCreate(h).Execute(context.Background(), task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return tk
}

// Every way a task is created raises exactly one TaskAvailable, right after
// TaskCreated, at the creation instant, naming the new task.
func TestPoolEvents_EveryCreationPathRaisesOneTaskAvailable(t *testing.T) {
	ctx := context.Background()
	cpt := shared.NewCPT(epoch.Add(time.Hour))
	caps := shared.NewCapabilitySet("pick")
	paths := map[string]func(uc *usecases.CreateTask) (*task.Task, error){
		"Execute": func(uc *usecases.CreateTask) (*task.Task, error) {
			return uc.Execute(ctx, task.Pick, cpt, "o", caps, false, false)
		},
		"ExecuteTransfer": func(uc *usecases.CreateTask) (*task.Task, error) {
			return uc.ExecuteTransfer(ctx, task.Pick, cpt, "o", caps, false, false, transferDetails(task.WorkKindTransferPick))
		},
		"ExecuteRelease": func(uc *usecases.CreateTask) (*task.Task, error) {
			return uc.ExecuteRelease(ctx, task.Pick, cpt, "o", caps, false, false, "order-9", 2, nil)
		},
		"ExecuteRelocation": func(uc *usecases.CreateTask) (*task.Task, error) {
			return uc.ExecuteRelocation(ctx, cpt, "o", shared.NewCapabilitySet("relocate"), *relocationMove(t, "Relocate", "WH1-A-B-C-D-E-F", "WH1-A-B-C-D-E-G"))
		},
		"ExecuteReplenishment": func(uc *usecases.CreateTask) (*task.Task, error) {
			return uc.ExecuteReplenishment(ctx, cpt, "o", shared.NewCapabilitySet("replenish"), *replenishmentMove(t, "NORMAL"))
		},
	}
	for name, run := range paths {
		t.Run(name, func(t *testing.T) {
			h := newHarness()
			tk, err := run(newPoolCreate(h))
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			wantNames(t, h, "TaskCreated", "TaskAvailable")
			a := lastAvailable(t, h)
			if a.TaskId != tk.Id() || !a.OccurredAt().Equal(epoch) {
				t.Fatalf("TaskAvailable = %+v, want task %s at %v", a, tk.Id(), epoch)
			}
		})
	}
}

func TestPoolEvents_FailedCreationRaisesNothing(t *testing.T) {
	h := newHarness()
	tasks := newErrTaskRepo()
	tasks.failSave = true
	uc := &usecases.CreateTask{Tasks: tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("p")}
	if _, err := uc.Execute(context.Background(), task.Pick, shared.NewCPT(epoch), "o", shared.NewCapabilitySet(), false, false); err == nil {
		t.Fatal("expected the save error")
	}
	wantNames(t, h)
}

// A redelivered WorkReleased is acknowledged without a second task and
// without a second TaskAvailable; a rejected one (unknown path) raises none.
func TestPoolEvents_WorkReleasedReplayAndRejectionRaiseNoSecondEvent(t *testing.T) {
	h := newHarness()
	create := newPoolCreate(h)
	apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: memory.NewProcessedEventsRepo(), Catalogue: atomicCatalogue()}
	req := usecases.WorkReleasedRequest{EventId: "evt-1", PathId: "PICK", WorkUnitId: "wu-1", CPT: epoch.Add(time.Hour), Ref: "order-1"}

	for i := 0; i < 3; i++ {
		if err := apply.Execute(context.Background(), req); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable")

	bad := req
	bad.EventId, bad.PathId = "evt-2", "NOPE"
	if err := apply.Execute(context.Background(), bad); err == nil {
		t.Fatal("expected the unknown path to be rejected")
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable")
}

// A claim raises exactly one TaskClaimed (no TaskAvailable: the task LEFT the
// pool), naming the claiming station at the claim instant.
func TestPoolEvents_ClaimRaisesExactlyOneTaskClaimed(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	tk := createPick(t, h)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	h.clock.Advance(time.Minute)

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	if _, err := claim.Execute(ctx, "s1", task.Pick); err != nil {
		t.Fatalf("claim: %v", err)
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable", "TaskClaimed")
	c, ok := h.publisher.Events()[2].(shared.TaskClaimed)
	if !ok || c.TaskId != tk.Id() || c.StationId != "s1" || !c.OccurredAt().Equal(epoch.Add(time.Minute)) {
		t.Fatalf("TaskClaimed = %+v", h.publisher.Events()[2])
	}
}

// Nothing claimed means nothing raised: an empty pool and a station lacking
// the task's capability.
func TestPoolEvents_RejectedClaimsRaiseNothing(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	if _, err := claim.Execute(ctx, "s1", task.Pick); err == nil {
		t.Fatal("expected no claimable task")
	}
	wantNames(t, h)

	_, _ = newPoolCreate(h).Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "o", shared.NewCapabilitySet("pick", "hazmat"), false, false)
	if _, err := claim.Execute(ctx, "s1", task.Pick); err == nil {
		t.Fatal("expected a capability mismatch to find nothing")
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable")
}

// A claim that loses the compare-and-set race raises nothing for the loser.
type losingClaimRepo struct{ *memory.TaskRepo }

func (losingClaimRepo) SaveClaim(context.Context, *task.Task, time.Time) (bool, error) {
	return false, nil
}

func TestPoolEvents_LostClaimRaceRaisesNothing(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	createPick(t, h)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	claim := &usecases.ClaimNext{Tasks: losingClaimRepo{h.tasks}, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	if _, err := claim.Execute(ctx, "s1", task.Pick); err == nil {
		t.Fatal("expected the lost race to leave nothing to claim")
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable")
}

// The lease-expiry sweep returns a claimed task to the pool: LeaseExpired
// (analytics) plus ONE TaskAvailable at the sweep instant (a NEW available_at,
// not the creation time). A second sweep, and a sweep before expiry, raise
// nothing: the task is no longer (or not yet) expired.
func TestPoolEvents_LeaseExpiryReEntersThePoolOnce(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	tk := createPick(t, h)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock, LeaseDuration: time.Minute}
	if _, err := claim.Execute(ctx, "s1", task.Pick); err != nil {
		t.Fatalf("claim: %v", err)
	}
	sweep := &usecases.ExpireLeases{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock}

	if n, err := sweep.Execute(ctx); err != nil || n != 0 {
		t.Fatalf("sweep before expiry: n=%d err=%v", n, err)
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable", "TaskClaimed")

	h.clock.Advance(2 * time.Minute)
	if n, err := sweep.Execute(ctx); err != nil || n != 1 {
		t.Fatalf("sweep: n=%d err=%v", n, err)
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable", "TaskClaimed", "LeaseExpired", "TaskAvailable")
	a := lastAvailable(t, h)
	if a.TaskId != tk.Id() || !a.OccurredAt().Equal(epoch.Add(2*time.Minute)) {
		t.Fatalf("re-entry TaskAvailable = %+v, want task %s at %v", a, tk.Id(), epoch.Add(2*time.Minute))
	}

	if n, err := sweep.Execute(ctx); err != nil || n != 0 {
		t.Fatalf("second sweep: n=%d err=%v", n, err)
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable", "TaskClaimed", "LeaseExpired", "TaskAvailable")
}

// Re-claiming a task whose lease expired before the sweep ran is one claim,
// one TaskClaimed: the task never re-entered the pool through the sweep, so no
// TaskAvailable is raised for it.
func TestPoolEvents_ReclaimOfExpiredLeaseRaisesOnlyTaskClaimed(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	createPick(t, h)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	_ = h.stations.Save(ctx, station.New("s2", shared.NewCapabilitySet("pick")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock, LeaseDuration: time.Minute}
	_, _ = claim.Execute(ctx, "s1", task.Pick)
	h.clock.Advance(2 * time.Minute)
	if _, err := claim.Execute(ctx, "s2", task.Pick); err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable", "TaskClaimed", "TaskClaimed")
}

// Renewing, rejected completions and a successful completion never raise a
// pool event: the task stays out of the pool.
func TestPoolEvents_RenewAndCompleteRaiseNoPoolEvent(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	tk := createPick(t, h)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	_, _ = claim.Execute(ctx, "s1", task.Pick)

	renew := &usecases.RenewLease{Tasks: h.tasks, Clock: h.clock}
	if err := renew.Execute(ctx, tk.Id(), "s1"); err != nil {
		t.Fatalf("renew: %v", err)
	}
	complete := &usecases.CompleteTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock}
	if err := complete.Execute(ctx, tk.Id(), "other"); err == nil {
		t.Fatal("expected a non-owner completion to be rejected")
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable", "TaskClaimed")

	if err := complete.Execute(ctx, tk.Id(), "s1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	wantNames(t, h, "TaskCreated", "TaskAvailable", "TaskClaimed", "TaskCompleted")
}
