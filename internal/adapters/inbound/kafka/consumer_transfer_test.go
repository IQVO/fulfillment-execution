package kafka_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// workReleasedTransferJSON builds a WorkReleased CloudEvent carrying the
// optional transfer-correlation block.
func workReleasedTransferJSON(eventId, pathId, workUnitId, transferRef, workKind, siteId, sku string, quantity int) []byte {
	return workReleasedCE(eventId, kafka.TypeWorkReleased, workUnitId, map[string]any{
		"path_id":      pathId,
		"work_unit_id": workUnitId,
		"cpt":          "2026-08-21T23:00:00Z",
		"ref":          "demand-77",
		"transfer_ref": transferRef,
		"work_kind":    workKind,
		"site_id":      siteId,
		"sku":          sku,
		"quantity":     quantity,
	})
}

// findTheTask returns the single task in an otherwise-empty repo.
func findTheTask(t *testing.T, tasks *memory.TaskRepo) *task.Task {
	t.Helper()
	all := 0
	for _, tt := range []task.Type{task.Pick, task.Pack, task.Rebin, task.Slam, task.Dispatch, task.Arrival} {
		n, err := tasks.CountByTypeAndStatus(context.Background(), tt, task.Pending)
		if err != nil {
			t.Fatalf("CountByTypeAndStatus: %v", err)
		}
		all += n
	}
	if all != 1 {
		t.Fatalf("expected exactly 1 task, got %d", all)
	}
	// fetch via FindClaimableByType across every type
	for _, tt := range []task.Type{task.Pick, task.Pack, task.Rebin, task.Slam, task.Dispatch, task.Arrival} {
		list, err := tasks.FindClaimableByType(context.Background(), tt, epoch)
		if err != nil {
			t.Fatalf("FindClaimableByType: %v", err)
		}
		if len(list) == 1 {
			return list[0]
		}
	}
	t.Fatal("task not found under any type")
	return nil
}

// TestHandleMessage_TransferFieldsSurviveDecodeToTask proves the strictly
// additive transfer-correlation block decodes and lands on the created
// Task: WorkUnitId stays the OrderRef, and every transfer field survives
// verbatim.
func TestHandleMessage_TransferFieldsSurviveDecodeToTask(t *testing.T) {
	c, tasks := newConsumer(t)

	if err := c.HandleMessage(context.Background(), workReleasedTransferJSON("evt-tr-1", "dispatch-dock-1", "wu-tr-1", "tr-9f2a", "TRANSFER_DISPATCH", "site-north", "SKU-0042", 12)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := findTheTask(t, tasks)
	if got.Type() != task.Dispatch {
		t.Fatalf("task type = %q, want DISPATCH (derived from the catalogue path id)", got.Type())
	}
	if string(got.OrderRef()) != "wu-tr-1" {
		t.Fatalf("orderRef = %q, want the work_unit_id (unchanged contract)", got.OrderRef())
	}
	tr := got.Transfer()
	if tr == nil {
		t.Fatal("expected a transfer correlation block on the task")
	}
	if tr.TransferRef != "tr-9f2a" || tr.WorkKind != task.WorkKindTransferDispatch || tr.SiteId != "site-north" || tr.SKU != "SKU-0042" || tr.Quantity != 12 {
		t.Fatalf("transfer block = %+v", tr)
	}
	if tr.DemandId != "demand-77" {
		t.Fatalf("demandId = %q, want the release ref", tr.DemandId)
	}
}

// TestHandleMessage_LegacyPayloadCreatesNonTransferTask proves a legacy
// payload (no transfer fields at all) decodes exactly as before: no
// transfer block, same task type derivation, no error.
func TestHandleMessage_LegacyPayloadCreatesNonTransferTask(t *testing.T) {
	c, tasks := newConsumer(t)

	if err := c.HandleMessage(context.Background(), workReleasedJSON("evt-legacy", "PICK", "wu-legacy")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := findTheTask(t, tasks)
	if got.Transfer() != nil {
		t.Fatalf("legacy payload must create a non-transfer task, got %+v", got.Transfer())
	}
}

// TestHandleMessage_TransferRefWithUnknownWorkKindIsRejected pins the
// hard-error discipline: a transfer_ref whose work_kind is not one of the
// three declared kinds is a payload-contract violation, never a silent
// default (mirroring the path_id rule, ADR-0017).
func TestHandleMessage_TransferRefWithUnknownWorkKindIsRejected(t *testing.T) {
	c, tasks := newConsumer(t)

	err := c.HandleMessage(context.Background(), workReleasedTransferJSON("evt-bad-kind", "PICK", "wu-bad", "tr-1", "NOT_A_KIND", "s", "SKU", 1))
	if err == nil {
		t.Fatal("expected an error for transfer_ref with an unknown work_kind")
	}
	if !errors.Is(err, task.ErrUnknownWorkKind) {
		t.Fatalf("err = %v, want task.ErrUnknownWorkKind", err)
	}
	if got := countAllPending(t, tasks); got != 0 {
		t.Fatalf("no task may be created for a rejected payload, got %d", got)
	}
}

func countAllPending(t *testing.T, tasks *memory.TaskRepo) int {
	t.Helper()
	total := 0
	for _, tt := range []task.Type{task.Pick, task.Pack, task.Rebin, task.Slam, task.Dispatch, task.Arrival} {
		n, err := tasks.CountByTypeAndStatus(context.Background(), tt, task.Pending)
		if err != nil {
			t.Fatalf("CountByTypeAndStatus: %v", err)
		}
		total += n
	}
	return total
}
