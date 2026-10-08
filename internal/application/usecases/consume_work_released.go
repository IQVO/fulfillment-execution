package usecases

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// WorkReleasedRequest is one decoded WorkReleased occurrence, already
// validated at the wire boundary, that ApplyWorkReleased applies exactly
// once. Transfer carries the inter-warehouse-transfer correlation block
// when the release is transfer work (nil otherwise).
type WorkReleasedRequest struct {
	EventId    string
	PathId     string
	WorkUnitId string
	CPT        time.Time
	Ref        string
	// LineNo is the optional 1-based order line of the release
	// (WorkReleased.line_no; 0 = absent/unknown).
	LineNo   int
	Fragile  bool
	GiftWrap bool
	Transfer *task.TransferDetails
}

// ApplyWorkReleased applies one WorkReleased occurrence exactly once
// despite Kafka's at-least-once delivery: the processed-event claim, the
// process-path catalogue validation, the Task save and the TaskCreated
// outbox publish all commit inside ONE UnitOfWork, so a failure anywhere
// rolls the claim back with the effect and a redelivery re-applies
// instead of being mistaken for an already-handled event.
//
// This is the fix for the consumer's pre-existing lost-event bug (see the
// atomic-workreleased-consumer ADR): the previous shape committed the
// processed-id insert BEFORE the catalogue lookup and the task creation
// and OUTSIDE their UnitOfWork, so a failed create permanently suppressed
// redelivery — the WorkReleased was acknowledged with no Task ever
// existing. It ports wes-work-planning's onceAtomically (its ADR-0028),
// the fleet's reference fix for the same bug shape.
type ApplyWorkReleased struct {
	CreateTask *CreateTask
	Processed  ports.ProcessedEvents
	Catalogue  ports.PathCatalogue
	// UnitOfWork brackets the processed-event claim and the whole of
	// CreateTask's work (ADR 0020). CreateTask's own UnitOfWork must be
	// the SAME instance: its nested Execute then joins this scope rather
	// than opening a second one, which is what makes the claim, the task
	// row and the outbox rows commit or roll back together. Nil means no
	// transactional backing (the in-memory configuration) — a failure
	// then undoes the claim explicitly via ports.ProcessedEventReleaser
	// so a retry still re-applies.
	UnitOfWork ports.UnitOfWork
	// Logger receives the WARN for an ignored out-of-range line_no. Nil
	// means slog.Default().
	Logger *slog.Logger
}

func (uc *ApplyWorkReleased) logger() *slog.Logger {
	if uc.Logger != nil {
		return uc.Logger
	}
	return slog.Default()
}

// boundedLineNo returns the line number to stamp: req.LineNo when it is in
// 1..task.MaxSourceLineNo, else 0 (unknown). A value above the maximum is a
// corrupt producer value; it is ignored like an absent one (with a WARN
// naming the event) so the task is still created and the value can never
// fail the tasks insert (32-bit column) or poison the partition. Non-positive
// values are unknown silently, as before.
func (uc *ApplyWorkReleased) boundedLineNo(ctx context.Context, req WorkReleasedRequest) int {
	if req.LineNo > task.MaxSourceLineNo && req.Transfer == nil {
		uc.logger().WarnContext(ctx, "WorkReleased line_no above the 32-bit maximum ignored; source line left unknown",
			"event_id", req.EventId, "work_unit_id", req.WorkUnitId, "line_no", req.LineNo, "max", task.MaxSourceLineNo)
	}
	if req.LineNo < 1 || req.LineNo > task.MaxSourceLineNo {
		return 0
	}
	return req.LineNo
}

// Execute applies req. A nil return means the occurrence is fully applied
// — including the "already applied by a prior delivery" case, which is
// the success it exists to make cheap.
func (uc *ApplyWorkReleased) Execute(ctx context.Context, req WorkReleasedRequest) error {
	marked := false
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		isNew, err := uc.Processed.MarkProcessed(ctx, req.EventId)
		if err != nil {
			return err
		}
		if !isNew {
			// Already applied by a prior delivery of this CloudEvents
			// id; ack without creating a duplicate Task.
			return nil
		}
		marked = true

		// A path_id this catalogue does not recognize is a hard error —
		// NOT a silent default to task.Pick (ADR-0017). Inside the
		// atomic scope, the failed lookup rolls the claim back with it.
		pathDef, err := uc.Catalogue.Lookup(req.PathId)
		if err != nil {
			return fmt.Errorf("path_id %q not found in the process-path catalogue: %w", req.PathId, err)
		}

		required := shared.NewCapabilitySet(capabilitiesOf(pathDef)...)
		orderRef := shared.OrderRef(req.WorkUnitId)
		// The release ref is the ORDER id only for order-originated work;
		// for transfer work it is a demand id, so nothing is stamped.
		sourceOrderId := req.Ref
		// line_no is likewise order-only and explicit: never parsed from
		// the "<order>-line-<n>" work unit id (decision 18, ADR 0041).
		sourceLineNo := uc.boundedLineNo(ctx, req)
		if req.Transfer != nil {
			sourceOrderId = ""
			sourceLineNo = 0
		}
		_, err = uc.CreateTask.ExecuteRelease(ctx, task.Type(pathDef.Id), shared.NewCPT(req.CPT), orderRef, required, req.Fragile, req.GiftWrap, sourceOrderId, sourceLineNo, req.Transfer)
		return err
	})
	if err == nil {
		return nil
	}
	// Without a UnitOfWork the claim cannot roll back with the failed
	// effect, so it is undone explicitly — otherwise the retry of this
	// same event would see the orphaned claim, report success, and the
	// WorkReleased would be lost exactly like the bug this use case fixes.
	if uc.UnitOfWork == nil && marked {
		if releaser, ok := uc.Processed.(ports.ProcessedEventReleaser); ok {
			if relErr := releaser.ReleaseProcessed(ctx, req.EventId); relErr != nil {
				return errors.Join(err, fmt.Errorf("release processed-event mark %s: %w", req.EventId, relErr))
			}
		}
	}
	return err
}

// capabilitiesOf converts a catalogue path definition's declared
// capability strings into the domain's shared.Capability type.
func capabilitiesOf(def pathcatalog.PathDefinition) []shared.Capability {
	out := make([]shared.Capability, len(def.RequiredCapabilities))
	for i, c := range def.RequiredCapabilities {
		out[i] = shared.Capability(c)
	}
	return out
}
