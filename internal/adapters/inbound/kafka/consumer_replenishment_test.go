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

// replenishmentWire is a valid WorkReleased.data.replenishment object.
func replenishmentWire() map[string]any {
	return map[string]any{
		"request_id": "req-9", "sku": "SKU-1", "from_bin": "WH1-RSV-AMB-B02-03-02-C",
		"to_bin": "WH1-FWD-AMB-A01-01-01-A", "quantity": 24, "priority": "URGENT",
	}
}

// withoutKey returns a copy of m without key.
func withoutKey(m map[string]any, key string) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		if k != key {
			out[k] = v
		}
	}
	return out
}

// withKey returns a copy of m with key set to v.
func withKey(m map[string]any, key string, v any) map[string]any {
	out := withoutKey(m, key)
	out[key] = v
	return out
}

// workReleasedReplenishmentJSON builds a WorkReleased CloudEvent for work
// released on pathId, carrying the ADR 0050 `replenishment` object and,
// when relocation is not nil, a `relocation` object too (nil = absent).
func workReleasedReplenishmentJSON(eventId, pathId string, replenishment, relocation map[string]any) []byte {
	data := map[string]any{
		"path_id":      pathId,
		"work_unit_id": "replenish-req-9",
		"cpt":          "2026-08-21T23:00:00Z",
		"ref":          "req-9",
	}
	if replenishment != nil {
		data["replenishment"] = replenishment
	}
	if relocation != nil {
		data["relocation"] = relocation
	}
	return workReleasedCE(eventId, kafka.TypeWorkReleased, "replenish-req-9", data)
}

func replenishTaskOf(t *testing.T, tasks *memory.TaskRepo) *task.Task {
	t.Helper()
	list, err := tasks.FindClaimableByType(context.Background(), task.Replenish, epoch)
	if err != nil || len(list) != 1 {
		t.Fatalf("expected exactly 1 REPLENISH task, got %d (err %v)", len(list), err)
	}
	return list[0]
}

// TestHandleMessage_ReplenishmentMoveDecodesOntoTheTask pins the wire names
// of the WorkReleased replenishment object (ADR 0050): request_id, sku,
// from_bin, to_bin, quantity, priority.
func TestHandleMessage_ReplenishmentMoveDecodesOntoTheTask(t *testing.T) {
	for _, priority := range []string{"NORMAL", "URGENT"} {
		t.Run(priority, func(t *testing.T) {
			c, tasks := newConsumer(t)
			wire := withKey(replenishmentWire(), "priority", priority)
			if err := c.HandleMessage(context.Background(), workReleasedReplenishmentJSON("evt-"+priority, "REPLENISH", wire, nil)); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			got := replenishTaskOf(t, tasks)
			want := task.ReplenishmentMove{RequestId: "req-9", SKU: "SKU-1", FromBin: "WH1-RSV-AMB-B02-03-02-C", ToBin: "WH1-FWD-AMB-A01-01-01-A", Quantity: 24, Priority: task.ReplenishmentPriority(priority)}
			if got.Replenishment() == nil || *got.Replenishment() != want {
				t.Fatalf("move = %+v, want %+v", got.Replenishment(), want)
			}
			if string(got.OrderRef()) != "replenish-req-9" {
				t.Fatalf("orderRef = %q, want the work_unit_id", got.OrderRef())
			}
			if got.SourceOrderId() != "" || got.Transfer() != nil || got.Relocation() != nil {
				t.Fatalf("replenishment work is neither order, transfer nor relocation work: %q %+v %+v", got.SourceOrderId(), got.Transfer(), got.Relocation())
			}
		})
	}
}

// TestHandle_ReplenishmentRetryPathDecodesTheMoveToo drives Handle (the
// production retry path), not only the single-attempt HandleMessage.
func TestHandle_ReplenishmentRetryPathDecodesTheMoveToo(t *testing.T) {
	c, tasks := newConsumer(t)
	raw := workReleasedReplenishmentJSON("evt-retry", "REPLENISH", replenishmentWire(), nil)
	if err := c.Handle(context.Background(), kafkago.Message{Topic: "warehouse.work-planning.events", Value: raw}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got := replenishTaskOf(t, tasks).Replenishment(); got == nil || got.Ref() != "req-9" {
		t.Fatalf("move = %+v, want request req-9", got)
	}
}

// TestHandleMessage_InvalidReplenishmentMoveIsAPermanentError: an object
// that can never become a valid move is rejected before any task exists.
func TestHandleMessage_InvalidReplenishmentMoveIsAPermanentError(t *testing.T) {
	cases := []struct {
		name string
		wire map[string]any
		want error
	}{
		{"missing quantity", withoutKey(replenishmentWire(), "quantity"), task.ErrInvalidReplenishmentMove},
		{"zero quantity", withKey(replenishmentWire(), "quantity", 0), task.ErrInvalidReplenishmentMove},
		{"missing request_id", withoutKey(replenishmentWire(), "request_id"), task.ErrInvalidReplenishmentMove},
		{"blank sku", withKey(replenishmentWire(), "sku", " "), task.ErrInvalidReplenishmentMove},
		{"missing from_bin", withoutKey(replenishmentWire(), "from_bin"), task.ErrInvalidReplenishmentMove},
		{"missing to_bin", withoutKey(replenishmentWire(), "to_bin"), task.ErrInvalidReplenishmentMove},
		{"same bins", withKey(replenishmentWire(), "to_bin", "WH1-RSV-AMB-B02-03-02-C"), task.ErrInvalidReplenishmentMove},
		{"missing priority", withoutKey(replenishmentWire(), "priority"), task.ErrUnknownReplenishmentPriority},
		{"unknown priority", withKey(replenishmentWire(), "priority", "HIGH"), task.ErrUnknownReplenishmentPriority},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, tasks := newConsumer(t)
			err := c.HandleMessage(context.Background(), workReleasedReplenishmentJSON("evt-bad", "REPLENISH", tc.wire, nil))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if n, _ := tasks.CountByTypeAndStatus(context.Background(), task.Replenish, task.Pending); n != 0 {
				t.Fatalf("no task may be created, got %d", n)
			}
		})
	}
}

// TestHandleMessage_ReplenishmentPairingRule: the pairing rule of
// ApplyWorkReleased reaches the wire unchanged, in both directions and
// against the relocation object.
func TestHandleMessage_ReplenishmentPairingRule(t *testing.T) {
	relocation := map[string]any{"plan_id": "p", "move_seq": 1, "kind": "Relocate", "sku": "S", "from_bin": "A", "to_bin": "B"}
	cases := []struct {
		name          string
		path          string
		replenishment map[string]any
		relocation    map[string]any
		want          error
	}{
		{"REPLENISH without the object", "REPLENISH", nil, nil, usecases.ErrReplenishmentMoveMissing},
		{"object on PICK", "PICK", replenishmentWire(), nil, usecases.ErrReplenishmentMoveOnOtherPath},
		{"replenishment on RELOCATE", "RELOCATE", replenishmentWire(), relocation, usecases.ErrReplenishmentMoveOnOtherPath},
		{"relocation on REPLENISH", "REPLENISH", replenishmentWire(), relocation, usecases.ErrRelocationMoveOnOtherPath},
		{"relocation only on REPLENISH", "REPLENISH", nil, relocation, usecases.ErrRelocationMoveOnOtherPath},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, tasks := newConsumer(t)
			err := c.HandleMessage(context.Background(), workReleasedReplenishmentJSON("evt-pair", tc.path, tc.replenishment, tc.relocation))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if n, _ := tasks.CountByTypeAndStatus(context.Background(), task.Replenish, task.Pending); n != 0 {
				t.Fatalf("no REPLENISH task may be created, got %d", n)
			}
		})
	}
}

// TestHandleMessage_ReplenishmentIsDeduplicatedOnTheEventId: a redelivery of
// the same CloudEvent creates no second task.
func TestHandleMessage_ReplenishmentIsDeduplicatedOnTheEventId(t *testing.T) {
	c, tasks := newConsumer(t)
	raw := workReleasedReplenishmentJSON("evt-dup", "REPLENISH", replenishmentWire(), nil)
	for i := 0; i < 2; i++ {
		if err := c.HandleMessage(context.Background(), raw); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}
	replenishTaskOf(t, tasks) // fails unless there is exactly one
}

// TestHandleMessage_NonReplenishmentPayloadIsUnchanged: a release without
// the replenishment object creates a task with no replenishment move.
func TestHandleMessage_NonReplenishmentPayloadIsUnchanged(t *testing.T) {
	c, tasks := newConsumer(t)
	if err := c.HandleMessage(context.Background(), workReleasedJSON("evt-pick", "PICK", "wu-pick")); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if got := findTheTask(t, tasks); got.Replenishment() != nil {
		t.Fatalf("a PICK release carries no replenishment move, got %+v", got.Replenishment())
	}
}
