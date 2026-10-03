//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TestTaskRepo_SaveClaim_ConcurrentClaimsExactlyOneWins races N stations'
// claims for ONE Pending task over separate pool connections: exactly one
// SaveClaim may report true, and the stored lease must belong to it.
func TestTaskRepo_SaveClaim_ConcurrentClaimsExactlyOneWins(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	id := shared.TaskId(fmt.Sprintf("integration-claim-cas-%d", now.UnixNano()))
	if err := repo.Save(ctx, task.New(id, task.Pick, shared.NewCPT(now.Add(time.Hour)), "order-cas", shared.NewCapabilitySet("pick"), false, false)); err != nil {
		t.Fatalf("Save: %v", err)
	}

	const stations = 12
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners []shared.StationId
	)
	start := make(chan struct{})
	// loaded is a barrier: every station must have read the still-Pending
	// row and claimed it in memory BEFORE any SaveClaim runs. A fixed sleep
	// was not enough under CI load -- a late goroutine loaded the row after
	// the winner's save and failed its in-memory Claim with "already
	// claimed", which is not the race this test is about.
	var allLoaded sync.WaitGroup
	allLoaded.Add(stations)
	for i := 0; i < stations; i++ {
		wg.Add(1)
		go func(st shared.StationId) {
			defer wg.Done()
			ready := false
			defer func() {
				if !ready {
					allLoaded.Done()
				}
			}()
			loaded, err := repo.FindById(ctx, id)
			if err != nil || loaded == nil {
				t.Errorf("FindById: %v", err)
				return
			}
			if err := loaded.Claim(st, shared.NewCapabilitySet("pick"), now, time.Minute); err != nil {
				t.Errorf("Claim: %v", err)
				return
			}
			ready = true
			allLoaded.Done()
			<-start
			won, err := repo.SaveClaim(ctx, loaded, now)
			if err != nil {
				t.Errorf("SaveClaim: %v", err)
				return
			}
			if won {
				mu.Lock()
				winners = append(winners, st)
				mu.Unlock()
			}
		}(shared.StationId(fmt.Sprintf("cas-station-%d", i)))
	}
	allLoaded.Wait() // every goroutine has loaded and claimed the Pending row
	close(start)
	wg.Wait()

	if len(winners) != 1 {
		t.Fatalf("%d stations won the claim %v, want exactly 1", len(winners), winners)
	}
	stored, err := repo.FindById(ctx, id)
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if stored.Lease() == nil || stored.Lease().StationId != winners[0] {
		t.Fatalf("stored lease %+v, want station %s", stored.Lease(), winners[0])
	}
}

// An expired lease is claimable again: the compare-and-set must accept it.
func TestTaskRepo_SaveClaim_ExpiredLeaseCanBeReclaimed(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	id := shared.TaskId(fmt.Sprintf("integration-claim-expired-%d", now.UnixNano()))
	tk := task.New(id, task.Pick, shared.NewCPT(now.Add(time.Hour)), "order-exp", shared.NewCapabilitySet("pick"), false, false)
	_ = repo.Save(ctx, tk)
	first, _ := repo.FindById(ctx, id)
	_ = first.Claim("st-old", shared.NewCapabilitySet("pick"), now, time.Minute)
	if won, err := repo.SaveClaim(ctx, first, now); err != nil || !won {
		t.Fatalf("first claim: won=%v err=%v", won, err)
	}
	later := now.Add(2 * time.Minute)
	second, _ := repo.FindById(ctx, id)
	if err := second.Claim("st-new", shared.NewCapabilitySet("pick"), later, time.Minute); err != nil {
		t.Fatalf("Claim after expiry: %v", err)
	}
	if won, err := repo.SaveClaim(ctx, second, later); err != nil || !won {
		t.Fatalf("reclaim after lease expiry: won=%v err=%v", won, err)
	}
	// ...but a live lease is not.
	third, _ := repo.FindById(ctx, id)
	third2 := task.Rehydrate(third.Id(), third.Type(), task.Pending, third.CPT(), third.OrderRef(), third.RequiredCapabilities(), nil, false, false, nil)
	_ = third2.Claim("st-thief", shared.NewCapabilitySet("pick"), later, time.Minute)
	if won, err := repo.SaveClaim(ctx, third2, later); err != nil || won {
		t.Fatalf("claim over a live lease: won=%v err=%v, want false", won, err)
	}
}
