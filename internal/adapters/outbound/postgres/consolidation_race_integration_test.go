//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// ADR-0034: N lines of ONE order arrive at Rebin at the same instant, each in
// its own transaction. Before the per-order lock every goroutine read the same
// (empty) consolidation and the last upsert won, losing lines — the order
// then never completed, or the PACK task was created more than once. With the
// lock the arrivals are serialized: every line is recorded and the PACK task
// is created exactly once.
func TestOrderConsolidation_ConcurrentArrivals_NoLostLineAndSinglePackTask(t *testing.T) {
	const lines = 8

	pool := outboxDB(t)
	tasks := postgres.NewTaskRepo(pool)
	stations := postgres.NewStationRepo(pool)
	consolidations := postgres.NewOrderConsolidationRepo(pool)
	uow := postgres.NewUnitOfWork(pool)

	var evt, tid atomic.Int64
	nextEvent := func() string { return fmt.Sprintf("race-evt-%d", evt.Add(1)) }
	pub := postgres.NewOutboxPublisher(pool,
		outboundkafka.NewPublisherWithWriter(nil, tasks, stations, nextEvent),
		outboundkafka.NewAnalyticsPublisherWithWriter(nil, tasks, nextEvent))
	clock := fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)}

	createTask := &usecases.CreateTask{
		Tasks: tasks, Publisher: pub, Clock: clock, UnitOfWork: uow,
		NewId: func() shared.TaskId { return shared.TaskId(fmt.Sprintf("race-pack-%d", tid.Add(1))) },
	}
	uc := &usecases.ArriveAtRebin{Consolidations: consolidations, CreateTask: createTask, Publisher: pub, Clock: clock, UnitOfWork: uow}

	required := make([]string, lines)
	for i := range required {
		required[i] = fmt.Sprintf("line-%d", i+1)
	}

	ctx := context.Background()
	start := make(chan struct{})
	errs := make(chan error, lines)
	var wg sync.WaitGroup
	for _, line := range required {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- uc.Execute(ctx, "order-race", line, required,
				shared.NewCPT(clock.t.Add(time.Hour)), shared.NewCapabilitySet("pack"), false, false)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent arrival failed: %v", err)
		}
	}

	oc, err := consolidations.FindByOrderRef(ctx, "order-race")
	if err != nil || oc == nil {
		t.Fatalf("consolidation not stored: %v err=%v", oc, err)
	}
	if got := len(oc.ArrivedLineIds()); got != lines {
		t.Errorf("arrived lines = %d (%v), want %d: a concurrent arrival was lost", got, oc.ArrivedLineIds(), lines)
	}
	if !oc.IsComplete() {
		t.Error("consolidation must be complete once every line has arrived")
	}

	packs, err := tasks.FindByOrderRef(ctx, "order-race")
	if err != nil {
		t.Fatalf("find tasks: %v", err)
	}
	if len(packs) != 1 || packs[0].Type() != task.Pack {
		t.Errorf("PACK tasks for the order = %d, want exactly 1", len(packs))
	}
}
