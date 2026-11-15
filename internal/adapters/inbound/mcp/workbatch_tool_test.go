package mcp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// TestGetWorkBatch pins the read tool against a batch formed through the
// domain aggregate: the stops come back in visit order with their cart
// compartments, and an unknown id is the use case's ErrBatchNotFound.
func TestGetWorkBatch(t *testing.T) {
	batches := memory.NewWorkBatchRepo()
	cpt := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	b, err := workbatch.Form(
		shared.BatchId("wb-1"), shared.StationId("stn-1"), 4, cpt,
		[]workbatch.Member{
			{TaskId: "t-anchor", Cpt: cpt, FromBin: "A-01"},
			{TaskId: "t-second", Cpt: cpt, FromBin: "A-02", Rank: 2},
			{TaskId: "t-first", Cpt: cpt, FromBin: "A-03", Rank: 1},
		},
		cpt.Add(5*time.Minute),
	)
	if err != nil {
		t.Fatalf("Form: %v", err)
	}
	if err := batches.Save(context.Background(), b); err != nil {
		t.Fatalf("Save: %v", err)
	}
	deps := Deps{GetWorkBatch: &usecases.GetWorkBatch{Batches: batches}}

	out, err := deps.getWorkBatch(context.Background(), getWorkBatchInput{BatchId: "wb-1"})
	if err != nil {
		t.Fatalf("getWorkBatch: %v", err)
	}
	if out.BatchId != "wb-1" || out.StationId != "stn-1" || out.Status != "OPEN" {
		t.Fatalf("out = %+v", out)
	}
	// Form orders stops by hint rank (D6): the anchor is unhinted (0), so
	// it goes first, then rank 1, then rank 2.
	wantStops := []string{"t-anchor", "t-first", "t-second"}
	if len(out.Stops) != len(wantStops) {
		t.Fatalf("stops = %+v", out.Stops)
	}
	for i, s := range out.Stops {
		if s.TaskId != wantStops[i] || s.StopSeq != i+1 || s.Compartment != i+1 || s.State != "OPEN" {
			t.Fatalf("stop[%d] = %+v, want task %s seq %d", i, s, wantStops[i], i+1)
		}
	}

	if _, err := deps.getWorkBatch(context.Background(), getWorkBatchInput{BatchId: "nope"}); !errors.Is(err, usecases.ErrBatchNotFound) {
		t.Fatalf("err = %v, want ErrBatchNotFound", err)
	}
	if _, err := deps.getWorkBatch(context.Background(), getWorkBatchInput{}); err == nil {
		t.Fatal("empty batchId must be an error")
	}
}
