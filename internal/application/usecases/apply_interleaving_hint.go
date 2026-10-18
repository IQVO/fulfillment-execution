package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// InterleavingHintRequest is one decoded task-interleaving
// InterleavingHintIssued occurrence, already validated at the wire
// boundary: the CloudEvents id (the idempotency key) plus the hint it
// carries.
type InterleavingHintRequest struct {
	EventId string
	Hint    ports.InterleavingHint
}

// ApplyInterleavingHint stores one interleaving hint in the local copy
// (ADR 0051), exactly once per CloudEvents id despite Kafka's at-least-once
// delivery: the processed-event claim and the per-station superseding
// upsert run inside ONE UnitOfWork, so a failure rolls the claim back with
// the effect and the redelivery re-applies it — the same atomic shape as
// ApplyProductClassified and ApplyWorkReleased.
//
// Storing a hint changes nothing claimNext decides on its own: the copy is
// read through ports.InterleavingHintLookup as an advisory tie-break only.
type ApplyInterleavingHint struct {
	Processed ports.ProcessedEvents
	Hints     ports.InterleavingHintStore
	// UnitOfWork brackets the claim and the upsert. Nil means no
	// transactional backing (the in-memory configuration): a failed upsert
	// then undoes the claim through ports.ProcessedEventReleaser so the
	// retry re-applies.
	UnitOfWork ports.UnitOfWork
}

// Execute applies req. A nil return means the occurrence is fully handled:
// newly stored, or already applied by a prior delivery of the same
// CloudEvents id. A non-nil error is a transient failure; the caller
// retries the same message.
func (uc *ApplyInterleavingHint) Execute(ctx context.Context, req InterleavingHintRequest) error {
	marked := false
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		isNew, err := uc.Processed.MarkProcessed(ctx, req.EventId)
		if err != nil {
			return fmt.Errorf("mark processed %s: %w", req.EventId, err)
		}
		if !isNew {
			return nil
		}
		marked = true
		if err := uc.Hints.ReplaceForStation(ctx, req.Hint); err != nil {
			return fmt.Errorf("store interleaving hint %s for station %s: %w", req.Hint.HintId, req.Hint.StationId, err)
		}
		return nil
	})
	if err == nil {
		return nil
	}
	if uc.UnitOfWork == nil && marked {
		if releaser, ok := uc.Processed.(ports.ProcessedEventReleaser); ok {
			if relErr := releaser.ReleaseProcessed(ctx, req.EventId); relErr != nil {
				return errors.Join(err, fmt.Errorf("release processed-event mark %s: %w", req.EventId, relErr))
			}
		}
	}
	return err
}
