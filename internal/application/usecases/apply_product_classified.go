package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// ProductClassifiedRequest is one decoded product-master ProductClassified
// occurrence, already validated at the wire boundary: the CloudEvents id
// (the idempotency key) plus the full-state classification it carries.
type ProductClassifiedRequest struct {
	EventId string
	Record  ports.ProductClassificationRecord
}

// ApplyProductClassified keeps this context's local copy of product-master
// classifications current (ADR-0039). The processed-event claim and the
// version-guarded upsert run inside ONE UnitOfWork, so a failure rolls the
// claim back with the effect and the redelivery re-applies it, the same
// atomic shape as ApplyWorkReleased.
//
// It changes nothing SealPackage decides: the copy is read through the
// unchanged ports.ProductClassificationLookup, with the same Known=false
// (not classified) and error (fail-open per item) semantics as the HTTP
// client it replaces.
type ApplyProductClassified struct {
	Processed ports.ProcessedEvents
	Copy      ports.ProductClassificationCopy
	// UnitOfWork brackets the claim and the upsert. Nil means no
	// transactional backing (in-memory configuration): a failed upsert
	// then undoes the claim through ports.ProcessedEventReleaser so the
	// retry re-applies.
	UnitOfWork ports.UnitOfWork
}

// Execute applies req. A nil return means the occurrence is fully handled:
// newly applied, ignored as stale (an equal or older version is already
// stored) or already applied by a prior delivery of the same id. A non-nil
// error is a transient failure; the caller retries the same message.
func (uc *ApplyProductClassified) Execute(ctx context.Context, req ProductClassifiedRequest) error {
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
		if _, err := uc.Copy.UpsertIfNewer(ctx, req.Record); err != nil {
			return fmt.Errorf("upsert classification copy for %s: %w", req.Record.SKU, err)
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
