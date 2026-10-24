// Package packview holds the local PackView (ADR 0054): a copy of
// product-master's pack levels keyed by GTIN, fed by the pack-level consumer
// and read by the pick scan confirmation. MemoryStore is a test double; the
// runtime binary uses PostgresStore.
package packview

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// MemoryStore is a thread-safe in-memory PackView with the same version
// guards as PostgresStore. It has no transactional backing.
type MemoryStore struct {
	mu   sync.RWMutex
	rows map[string]ports.PackRecord
}

var (
	_ ports.PackViewLookup = (*MemoryStore)(nil)
	_ ports.PackViewCopy   = (*MemoryStore)(nil)
)

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{rows: make(map[string]ports.PackRecord)}
}

// UpsertIfNewer implements ports.PackViewCopy.
func (s *MemoryStore) UpsertIfNewer(_ context.Context, rec ports.PackRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.rows[rec.GTIN]; ok && rec.Version <= cur.Version {
		return false, nil
	}
	rec.EffectiveTo = nil
	s.rows[rec.GTIN] = rec
	return true, nil
}

// RetireIfNewer implements ports.PackViewCopy.
func (s *MemoryStore) RetireIfNewer(_ context.Context, gtin string, effectiveTo time.Time, version int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.rows[gtin]
	if !ok || version <= cur.Version {
		return false, nil
	}
	cur.EffectiveTo, cur.Version = &effectiveTo, version
	s.rows[gtin] = cur
	return true, nil
}

// FindByGTIN implements ports.PackViewLookup.
func (s *MemoryStore) FindByGTIN(_ context.Context, gtin string) (ports.PackRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.rows[gtin]
	return rec, ok, nil
}
