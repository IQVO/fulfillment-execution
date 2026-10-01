package usecases_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// Regression for at-most-once claiming (ADR-0003). Observed live in the
// warehouse-day simulation: one PICK task was handed to all 8 stations
// racing claim-next for it, each got 200 + a lease, and all but one then
// failed complete with 409 task-not-owner / task-already-completed.
// FindClaimableByType is a plain read, so the claim must be a
// compare-and-set (TaskRepo.SaveClaim), not a blind Save.
func TestClaimNext_ConcurrentStations_ExactlyOneWinsEachTask(t *testing.T) {
	const stations, tasks = 8, 3
	ctx := context.Background()
	now := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	taskRepo := memory.NewTaskRepo()
	stationRepo := memory.NewStationRepo()
	for i := 0; i < tasks; i++ {
		tk := task.New(shared.TaskId(fmt.Sprintf("t-%d", i)), task.Pick, shared.NewCPT(now.Add(time.Hour)), shared.OrderRef(fmt.Sprintf("o-%d", i)), shared.NewCapabilitySet("pick"), false, false)
		if err := taskRepo.Save(ctx, tk); err != nil {
			t.Fatalf("seed task: %v", err)
		}
	}
	for i := 0; i < stations; i++ {
		if err := stationRepo.Save(ctx, station.New(shared.StationId(fmt.Sprintf("s-%d", i)), shared.NewCapabilitySet("pick"))); err != nil {
			t.Fatalf("seed station: %v", err)
		}
	}
	uc := &usecases.ClaimNext{Tasks: taskRepo, Stations: stationRepo, Publisher: events.NewLogPublisher(nil), Clock: memory.NewFixedClock(now)}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners = map[shared.TaskId][]shared.StationId{}
		empty   int
	)
	start := make(chan struct{})
	for i := 0; i < stations; i++ {
		wg.Add(1)
		go func(id shared.StationId) {
			defer wg.Done()
			<-start
			tk, err := uc.Execute(ctx, id, task.Pick)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				empty++
				return
			}
			winners[tk.Id()] = append(winners[tk.Id()], id)
		}(shared.StationId(fmt.Sprintf("s-%d", i)))
	}
	close(start)
	wg.Wait()

	if len(winners) != tasks {
		t.Fatalf("claimed %d distinct tasks, want all %d: %v", len(winners), tasks, winners)
	}
	for id, ws := range winners {
		if len(ws) != 1 {
			t.Fatalf("task %s handed to %d stations %v, want exactly 1", id, len(ws), ws)
		}
	}
	if empty != stations-tasks {
		t.Fatalf("%d stations got no task, want %d", empty, stations-tasks)
	}
}

// A lost compare-and-set must fall through to the next candidate rather
// than report "nothing claimable" while work is still pending.
func TestClaimNext_LostRace_FallsThroughToNextCandidate(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	taskRepo := memory.NewTaskRepo()
	stationRepo := memory.NewStationRepo()
	first := task.New("t-first", task.Pick, shared.NewCPT(now.Add(time.Hour)), "o-1", shared.NewCapabilitySet("pick"), false, false)
	second := task.New("t-second", task.Pick, shared.NewCPT(now.Add(2*time.Hour)), "o-2", shared.NewCapabilitySet("pick"), false, false)
	_ = taskRepo.Save(ctx, first)
	_ = taskRepo.Save(ctx, second)
	_ = stationRepo.Save(ctx, station.New("s-a", shared.NewCapabilitySet("pick")))
	_ = stationRepo.Save(ctx, station.New("s-b", shared.NewCapabilitySet("pick")))
	racer := &raceOnFirstClaim{TaskRepo: taskRepo, stations: stationRepo, now: now}
	uc := &usecases.ClaimNext{Tasks: racer, Stations: stationRepo, Publisher: events.NewLogPublisher(nil), Clock: memory.NewFixedClock(now)}

	got, err := uc.Execute(ctx, "s-a", task.Pick)
	if err != nil {
		t.Fatalf("expected the second candidate, got error %v", err)
	}
	if got.Id() != "t-second" {
		t.Fatalf("claimed %s, want t-second (t-first was taken by s-b mid-flight)", got.Id())
	}
}

// raceOnFirstClaim lets station s-b claim the earliest-CPT task between
// s-a's read of the candidates and s-a's compare-and-set.
type raceOnFirstClaim struct {
	*memory.TaskRepo
	stations *memory.StationRepo
	now      time.Time
	once     sync.Once
}

func (r *raceOnFirstClaim) SaveClaim(ctx context.Context, t *task.Task, now time.Time) (bool, error) {
	r.once.Do(func() {
		rival, _ := r.FindById(ctx, t.Id())
		fresh := task.New(rival.Id(), rival.Type(), rival.CPT(), rival.OrderRef(), rival.RequiredCapabilities(), false, false)
		if err := fresh.Claim("s-b", shared.NewCapabilitySet("pick"), r.now, time.Minute); err == nil {
			_, _ = r.TaskRepo.SaveClaim(ctx, fresh, r.now)
		}
	})
	return r.TaskRepo.SaveClaim(ctx, t, now)
}
