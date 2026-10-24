package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// PackLevelRequest is one decoded product-master pack-level occurrence,
// already validated at the wire boundary: the CloudEvents id (the idempotency
// key) plus either the declared level (Retire false, Record complete) or the
// retirement of Record.GTIN (Retire true, Record.GTIN, Record.Version and
// RetiredAt set).
type PackLevelRequest struct {
	EventId   string
	Retire    bool
	Record    ports.PackRecord
	RetiredAt time.Time
}

// ApplyPackLevel keeps the local PackView current from product-master's
// ProductPackLevelDeclared and ProductPackLevelRetired events (ADR 0054). The
// processed-event claim and the version-guarded write run inside ONE
// UnitOfWork, the same atomic shape as ApplyProductClassified.
type ApplyPackLevel struct {
	Processed  ports.ProcessedEvents
	View       ports.PackViewCopy
	UnitOfWork ports.UnitOfWork
}

// Execute applies req. A nil return means the occurrence is fully handled:
// applied, stale, or already applied by a prior delivery of the same id. A
// non-nil error is transient; the caller retries the same message.
func (uc *ApplyPackLevel) Execute(ctx context.Context, req PackLevelRequest) error {
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
		if req.Retire {
			if _, err := uc.View.RetireIfNewer(ctx, req.Record.GTIN, req.RetiredAt, req.Record.Version); err != nil {
				return fmt.Errorf("retire pack level %s: %w", req.Record.GTIN, err)
			}
			return nil
		}
		if _, err := uc.View.UpsertIfNewer(ctx, req.Record); err != nil {
			return fmt.Errorf("upsert pack level %s: %w", req.Record.GTIN, err)
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
