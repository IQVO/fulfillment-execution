package usecases_test

import (
	"bytes"
	"context"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TestApplyWorkReleased_StampsSourceLineNoFromLineNo pins decision 18
// (per-line confirm-pick, ADR 0041): wes-work-planning's WorkReleased
// carries an optional explicit line_no for order-originated work, and the
// task keeps it as source_line_no (published as TaskCompleted.line_no).
// Transfer work never carries one (it is not order work), an absent
// line_no stays unknown, and the "<order>-line-<n>" work unit id is never
// parsed to guess it.
func TestApplyWorkReleased_StampsSourceLineNoFromLineNo(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  usecases.WorkReleasedRequest
		want int
	}{
		{"order line release with line_no", usecases.WorkReleasedRequest{EventId: "evt-ln-1", PathId: "PICK", WorkUnitId: "ORD-9-line-2", Ref: "ORD-9", LineNo: 2, CPT: atomicEpoch.Add(time.Hour)}, 2},
		{"line_no is not derived from the work unit id", usecases.WorkReleasedRequest{EventId: "evt-ln-2", PathId: "PICK", WorkUnitId: "ORD-9-line-3", Ref: "ORD-9", CPT: atomicEpoch.Add(time.Hour)}, 0},
		{"line_no differs from the id suffix: the explicit field wins", usecases.WorkReleasedRequest{EventId: "evt-ln-3", PathId: "PICK", WorkUnitId: "ORD-9-line-3", Ref: "ORD-9", LineNo: 1, CPT: atomicEpoch.Add(time.Hour)}, 1},
		{"non-positive line_no is unknown", usecases.WorkReleasedRequest{EventId: "evt-ln-4", PathId: "PICK", WorkUnitId: "ORD-9-line-4", Ref: "ORD-9", LineNo: -4, CPT: atomicEpoch.Add(time.Hour)}, 0},
		{"line_no 0 is unknown", usecases.WorkReleasedRequest{EventId: "evt-ln-6", PathId: "PICK", WorkUnitId: "ORD-9-line-5", Ref: "ORD-9", LineNo: 0, CPT: atomicEpoch.Add(time.Hour)}, 0},
		{"line_no 1 is the lowest valid line", usecases.WorkReleasedRequest{EventId: "evt-ln-7", PathId: "PICK", WorkUnitId: "ORD-9-line-1", Ref: "ORD-9", LineNo: 1, CPT: atomicEpoch.Add(time.Hour)}, 1},
		{"line_no MaxInt32 is the highest valid line", usecases.WorkReleasedRequest{EventId: "evt-ln-8", PathId: "PICK", WorkUnitId: "ORD-9-line-max", Ref: "ORD-9", LineNo: math.MaxInt32, CPT: atomicEpoch.Add(time.Hour)}, math.MaxInt32},
		{"line_no above MaxInt32 is unknown but the task is still created", usecases.WorkReleasedRequest{EventId: "evt-ln-9", PathId: "PICK", WorkUnitId: "ORD-9-line-big", Ref: "ORD-9", LineNo: math.MaxInt32 + 1, CPT: atomicEpoch.Add(time.Hour)}, 0},
		{"line_no MaxInt64 is unknown but the task is still created", usecases.WorkReleasedRequest{EventId: "evt-ln-10", PathId: "PICK", WorkUnitId: "ORD-9-line-huge", Ref: "ORD-9", LineNo: math.MaxInt64, CPT: atomicEpoch.Add(time.Hour)}, 0},
		{"transfer work never carries a line", usecases.WorkReleasedRequest{EventId: "evt-ln-5", PathId: "PICK", WorkUnitId: "demand-1", Ref: "demand-1", LineNo: 2, CPT: atomicEpoch.Add(time.Hour),
			Transfer: &task.TransferDetails{TransferRef: "tr-1", DemandId: "demand-1", WorkKind: task.WorkKindTransferPick}}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tasks := memory.NewTaskRepo()
			create := &usecases.CreateTask{Tasks: tasks, Publisher: events.NewBufferedPublisher(), Clock: memory.NewFixedClock(atomicEpoch), NewId: fixedTaskId("l")}
			apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: memory.NewProcessedEventsRepo(), Catalogue: atomicCatalogue()}
			if err := apply.Execute(context.Background(), tc.req); err != nil {
				t.Fatalf("apply: %v", err)
			}
			list, err := tasks.FindClaimableByType(context.Background(), task.Pick, atomicEpoch)
			if err != nil || len(list) != 1 {
				t.Fatalf("expected 1 task, got %d err=%v", len(list), err)
			}
			if got := list[0].SourceLineNo(); got != tc.want {
				t.Errorf("SourceLineNo = %d, want %d", got, tc.want)
			}
		})
	}
}

// An out-of-range line_no is ignored with a WARN naming the event id; a
// valid one logs nothing.
func TestApplyWorkReleased_OutOfRangeLineNoWarnsWithEventId(t *testing.T) {
	for _, tc := range []struct {
		name     string
		lineNo   int
		wantWarn bool
	}{
		{"MaxInt32 is valid: no warning", math.MaxInt32, false},
		{"MaxInt32+1 warns", math.MaxInt32 + 1, true},
		{"MaxInt64 warns", math.MaxInt64, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			tasks := memory.NewTaskRepo()
			create := &usecases.CreateTask{Tasks: tasks, Publisher: events.NewBufferedPublisher(), Clock: memory.NewFixedClock(atomicEpoch), NewId: fixedTaskId("w")}
			apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: memory.NewProcessedEventsRepo(), Catalogue: atomicCatalogue(), Logger: logger}
			req := usecases.WorkReleasedRequest{EventId: "evt-warn-1", PathId: "PICK", WorkUnitId: "ORD-9-line-w", Ref: "ORD-9", LineNo: tc.lineNo, CPT: atomicEpoch.Add(time.Hour)}
			if err := apply.Execute(context.Background(), req); err != nil {
				t.Fatalf("apply: %v", err)
			}
			out := buf.String()
			if tc.wantWarn && (!strings.Contains(out, "level=WARN") || !strings.Contains(out, "evt-warn-1")) {
				t.Errorf("want a WARN naming event evt-warn-1, got %q", out)
			}
			if !tc.wantWarn && out != "" {
				t.Errorf("want no log output, got %q", out)
			}
		})
	}
}
