package interleavinghintcopy

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// MemoryStore is the in-memory hint copy (no DATABASE_URL, unit tests).
// Hints are advisory and short-lived, so losing them at restart is
// harmless: claimNext simply behaves as without a hint until the next
// InterleavingHintIssued arrives.
type MemoryStore struct {
	mu    sync.RWMutex
	hints map[string]ports.InterleavingHint
}

var (
	_ ports.InterleavingHintStore  = (*MemoryStore)(nil)
	_ ports.InterleavingHintLookup = (*MemoryStore)(nil)
)

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{hints: make(map[string]ports.InterleavingHint)}
}

// ReplaceForStation implements ports.InterleavingHintStore: the newer hint
// supersedes whatever the station had.
func (s *MemoryStore) ReplaceForStation(_ context.Context, hint ports.InterleavingHint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hints[hint.StationId] = hint
	return nil
}

// ActiveForStation implements ports.InterleavingHintLookup: nil (not an
// error) when the station has no hint or its hint expired at or before now.
func (s *MemoryStore) ActiveForStation(_ context.Context, stationId shared.StationId, now time.Time) (*ports.InterleavingHint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hint, ok := s.hints[string(stationId)]
	if !ok || !hint.ExpiresAt.After(now) {
		return nil, nil
	}
	return &hint, nil
}
