package kafka_test

import (
	"context"
	"errors"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// workReleasedRelocationJSON builds a WorkReleased CloudEvent for work
// released on the RELOCATE path, carrying the ADR 0047 `relocation` object
// (nil relocation = the object is absent).
func workReleasedRelocationJSON(eventId, pathId string, relocation map[string]any) []byte {
	data := map[string]any{
		"path_id":      pathId,
		"work_unit_id": "slot-plan-7-move-3",
		"cpt":          "2026-08-21T23:00:00Z",
		"ref":          "plan-7",
	}
	if relocation != nil {
		data["relocation"] = relocation
	}
	return workReleasedCE(eventId, kafka.TypeWorkReleased, "slot-plan-7-move-3", data)
}

func relocateTask(t *testing.T, tasks *memory.TaskRepo) *task.Task {
	t.Helper()
	list, err := tasks.FindClaimableByType(context.Background(), task.Relocate, epoch)
	if err != nil || len(list) != 1 {
		t.Fatalf("expected exactly 1 RELOCATE task, got %d (err %v)", len(list), err)
	}
	return list[0]
}

// TestHandleMessage_RelocationMoveDecodesOntoTheTask pins the wire names of
// the WorkReleased relocation object (ADR 0047): plan_id, move_seq, kind,
// sku, from_bin, to_bin, for each move kind (the open side omitted).
func TestHandleMessage_RelocationMoveDecodesOntoTheTask(t *testing.T) {
	cases := []struct {
		name string
		wire map[string]any
		want task.RelocationMove
	}{
		{"relocate", map[string]any{"plan_id": "plan-7", "move_seq": 3, "kind": "Relocate", "sku": "SKU-1", "from_bin": "FWD-A", "to_bin": "FWD-B"},
			task.RelocationMove{PlanId: "plan-7", MoveSeq: 3, Kind: task.RelocationKindRelocate, SKU: "SKU-1", FromBin: "FWD-A", ToBin: "FWD-B"}},
		{"assign omits from_bin", map[string]any{"plan_id": "plan-7", "move_seq": 0, "kind": "Assign", "sku": "SKU-2", "to_bin": "FWD-C"},
			task.RelocationMove{PlanId: "plan-7", MoveSeq: 0, Kind: task.RelocationKindAssign, SKU: "SKU-2", ToBin: "FWD-C"}},
		{"vacate omits to_bin", map[string]any{"plan_id": "plan-7", "move_seq": 9, "kind": "Vacate", "sku": "SKU-3", "from_bin": "FWD-D"},
			task.RelocationMove{PlanId: "plan-7", MoveSeq: 9, Kind: task.RelocationKindVacate, SKU: "SKU-3", FromBin: "FWD-D"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, tasks := newConsumer(t)
			if err := c.HandleMessage(context.Background(), workReleasedRelocationJSON("evt-"+tc.name, "RELOCATE", tc.wire)); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			got := relocateTask(t, tasks)
			if got.Relocation() == nil || *got.Relocation() != tc.want {
				t.Fatalf("move = %+v, want %+v", got.Relocation(), tc.want)
			}
			if string(got.OrderRef()) != "slot-plan-7-move-3" {
				t.Fatalf("orderRef = %q, want the work_unit_id", got.OrderRef())
			}
			if got.SourceOrderId() != "" || got.Transfer() != nil {
				t.Fatalf("relocation work is neither order nor transfer work: source order %q transfer %+v", got.SourceOrderId(), got.Transfer())
			}
		})
	}
}

// TestHandle_RelocationRetryPathDecodesTheMoveToo drives Handle (the
// production retry path), not only the single-attempt HandleMessage.
func TestHandle_RelocationRetryPathDecodesTheMoveToo(t *testing.T) {
	c, tasks := newConsumer(t)
	raw := workReleasedRelocationJSON("evt-retry", "RELOCATE",
		map[string]any{"plan_id": "plan-7", "move_seq": 4, "kind": "Relocate", "sku": "SKU-1", "from_bin": "A", "to_bin": "B"})
	if err := c.Handle(context.Background(), kafkago.Message{Topic: "warehouse.work-planning.events", Value: raw}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got := relocateTask(t, tasks).Relocation(); got == nil || got.Ref() != "plan-7:4" {
		t.Fatalf("move = %+v, want ref plan-7:4", got)
	}
}

// TestHandleMessage_InvalidRelocationMoveIsAPermanentError: a relocation
// object that can never become a valid move (missing move_seq, unknown kind,
// a kind's required bin missing, blank plan/sku) is rejected before any task
// exists, wrapping task.ErrInvalidRelocationMove / ErrUnknownRelocationKind.
func TestHandleMessage_InvalidRelocationMoveIsAPermanentError(t *testing.T) {
	cases := []struct {
		name string
		wire map[string]any
		want error
	}{
		{"missing move_seq", map[string]any{"plan_id": "p", "kind": "Relocate", "sku": "S", "from_bin": "A", "to_bin": "B"}, task.ErrInvalidRelocationMove},
		{"unknown kind", map[string]any{"plan_id": "p", "move_seq": 1, "kind": "Swap", "sku": "S", "from_bin": "A", "to_bin": "B"}, task.ErrUnknownRelocationKind},
		{"relocate without to_bin", map[string]any{"plan_id": "p", "move_seq": 1, "kind": "Relocate", "sku": "S", "from_bin": "A"}, task.ErrInvalidRelocationMove},
		{"blank plan_id", map[string]any{"plan_id": " ", "move_seq": 1, "kind": "Assign", "sku": "S", "to_bin": "B"}, task.ErrInvalidRelocationMove},
		{"blank sku", map[string]any{"plan_id": "p", "move_seq": 1, "kind": "Vacate", "from_bin": "A"}, task.ErrInvalidRelocationMove},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, tasks := newConsumer(t)
			err := c.HandleMessage(context.Background(), workReleasedRelocationJSON("evt-bad", "RELOCATE", tc.wire))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if n, _ := tasks.CountByTypeAndStatus(context.Background(), task.Relocate, task.Pending); n != 0 {
				t.Fatalf("no task may be created, got %d", n)
			}
		})
	}
}

// TestHandleMessage_RelocatePathWithoutRelocationObjectIsRejected and its
// mirror: the pairing rule of ApplyWorkReleased reaches the wire unchanged.
func TestHandleMessage_RelocationPairingRule(t *testing.T) {
	c, _ := newConsumer(t)
	err := c.HandleMessage(context.Background(), workReleasedRelocationJSON("evt-nomove", "RELOCATE", nil))
	if !errors.Is(err, usecases.ErrRelocationMoveMissing) {
		t.Fatalf("RELOCATE without relocation: err = %v, want ErrRelocationMoveMissing", err)
	}
	err = c.HandleMessage(context.Background(), workReleasedRelocationJSON("evt-onpick", "PICK",
		map[string]any{"plan_id": "p", "move_seq": 1, "kind": "Relocate", "sku": "S", "from_bin": "A", "to_bin": "B"}))
	if !errors.Is(err, usecases.ErrRelocationMoveOnOtherPath) {
		t.Fatalf("relocation on PICK: err = %v, want ErrRelocationMoveOnOtherPath", err)
	}
}

// TestHandleMessage_NonRelocationPayloadIsUnchanged: a release without the
// relocation object creates a task with no move (byte-compatible decode).
func TestHandleMessage_NonRelocationPayloadIsUnchanged(t *testing.T) {
	c, tasks := newConsumer(t)
	if err := c.HandleMessage(context.Background(), workReleasedJSON("evt-pick", "PICK", "wu-pick")); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if got := findTheTask(t, tasks); got.Relocation() != nil {
		t.Fatalf("a PICK release carries no move, got %+v", got.Relocation())
	}
}
