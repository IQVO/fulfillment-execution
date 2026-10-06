package memory

import (
	"context"
	"sync"
)

// ProcessedEventsRepo is a thread-safe in-memory implementation of
// ports.ProcessedEvents.
type ProcessedEventsRepo struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewProcessedEventsRepo constructs an empty ProcessedEventsRepo.
func NewProcessedEventsRepo() *ProcessedEventsRepo {
	return &ProcessedEventsRepo{seen: make(map[string]struct{})}
}

func (r *ProcessedEventsRepo) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.seen[eventId]; ok {
		return false, nil
	}
	r.seen[eventId] = struct{}{}
	return true, nil
}

// ReleaseProcessed forgets eventId, implementing
// ports.ProcessedEventReleaser: the in-memory stand-in for a rolled-back
// processed-event insert, used when the task creation the claim guarded
// failed and there is no UnitOfWork to roll the claim back with (see
// ApplyWorkReleased).
func (r *ProcessedEventsRepo) ReleaseProcessed(_ context.Context, eventId string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.seen, eventId)
	return nil
}
