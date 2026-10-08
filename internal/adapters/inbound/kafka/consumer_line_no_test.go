package kafka_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

func workReleasedJSONWithLine(eventId, pathId, workUnitId string, extra map[string]any) []byte {
	data := map[string]any{
		"path_id":      pathId,
		"work_unit_id": workUnitId,
		"cpt":          "2026-08-21T23:00:00Z",
		"ref":          "ORD-7",
	}
	for k, v := range extra {
		data[k] = v
	}
	return workReleasedCE(eventId, kafka.TypeWorkReleased, workUnitId, data)
}

func pickTask(t *testing.T, tasks *memory.TaskRepo) *task.Task {
	t.Helper()
	list, err := tasks.FindClaimableByType(context.Background(), task.Pick, epoch)
	if err != nil || len(list) != 1 {
		t.Fatalf("expected exactly 1 Pick task, got %d err=%v", len(list), err)
	}
	return list[0]
}

// WorkReleased.line_no is optional and additive (decision 18, ADR 0041):
// present, it is decoded and stamped on the created Task as source_line_no.
func TestHandleMessage_ReadsOptionalLineNo(t *testing.T) {
	c, tasks := newConsumer(t)

	if err := c.HandleMessage(context.Background(), workReleasedJSONWithLine("evt-line", "PICK", "ORD-7-line-2", map[string]any{"line_no": 2})); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pickTask(t, tasks).SourceLineNo(); got != 2 {
		t.Fatalf("SourceLineNo = %d, want 2 (from data.line_no)", got)
	}
}

// A producer that predates line_no (every WorkReleased today) must keep
// working: the field decodes as absent and the task's line stays unknown,
// even though its work unit id ends in "-line-3" (never parsed).
func TestHandleMessage_MissingLineNoIsUnknown(t *testing.T) {
	c, tasks := newConsumer(t)

	if err := c.HandleMessage(context.Background(), workReleasedJSONWithLine("evt-no-line", "PICK", "ORD-7-line-3", nil)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pickTask(t, tasks).SourceLineNo(); got != 0 {
		t.Fatalf("SourceLineNo = %d, want 0 (unknown) when data.line_no is absent", got)
	}
}

// Transfer work never carries a line even if a producer sent one.
func TestHandleMessage_TransferWorkIgnoresLineNo(t *testing.T) {
	c, tasks := newConsumer(t)

	raw := workReleasedJSONWithLine("evt-tr-line", "PICK", "demand-1", map[string]any{
		"line_no":      4,
		"ref":          "demand-1",
		"transfer_ref": "tr-1",
		"work_kind":    "TRANSFER_PICK",
		"site_id":      "WH-A",
		"sku":          "SKU-1",
		"quantity":     3,
	})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pickTask(t, tasks).SourceLineNo(); got != 0 {
		t.Fatalf("SourceLineNo = %d, want 0 for transfer work", got)
	}
}

// line_no is a 32-bit order-line number (decision 18 follow-up): the highest
// valid value is stamped; anything above it — including values that do not
// even fit an int64 — is ignored (line unknown), the task is still created,
// and the message neither errors nor reaches the dead-letter topic.
func TestHandleMessage_LineNoBoundary(t *testing.T) {
	for _, tc := range []struct {
		name string
		line json.Number
		want int
	}{
		{"1 is stamped", "1", 1},
		{"MaxInt32 is stamped", "2147483647", 2147483647},
		{"MaxInt32+1 is unknown", "2147483648", 0},
		{"MaxInt64 is unknown", "9223372036854775807", 0},
		{"above int64 is unknown", "9223372036854775808", 0},
		{"huge exponent form is unknown", "1e30", 0},
		{"negative is unknown", "-5", 0},
		{"zero is unknown", "0", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, tasks := newConsumer(t)
			raw := workReleasedJSONWithLine("evt-bound", "PICK", "ORD-7-line-9", map[string]any{"line_no": tc.line})
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("an out-of-range line_no must not fail the message: %v", err)
			}
			if got := pickTask(t, tasks).SourceLineNo(); got != tc.want {
				t.Fatalf("SourceLineNo = %d, want %d", got, tc.want)
			}
		})
	}
}
