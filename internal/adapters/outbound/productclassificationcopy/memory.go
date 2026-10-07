package productclassificationcopy

import (
	"context"
	"sync"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// MemoryStore is a thread-safe in-memory local copy, used when the service
// runs without Postgres (DATABASE_URL unset) and by unit tests. It applies
// the same version guard as PostgresStore. It has no transactional
// backing: ApplyProductClassified then releases the processed-event claim
// itself when the upsert fails.
type MemoryStore struct {
	mu   sync.RWMutex
	rows map[string]ports.ProductClassificationRecord
}

var (
	_ ports.ProductClassificationLookup = (*MemoryStore)(nil)
	_ ports.ProductClassificationCopy   = (*MemoryStore)(nil)
)

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{rows: make(map[string]ports.ProductClassificationRecord)}
}

// UpsertIfNewer stores rec when its SKU is absent or rec.Version is greater
// than the stored version.
func (s *MemoryStore) UpsertIfNewer(_ context.Context, rec ports.ProductClassificationRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.rows[rec.SKU]; ok && rec.Version <= cur.Version {
		return false, nil
	}
	rec.HandlingTags = append([]string(nil), rec.HandlingTags...)
	s.rows[rec.SKU] = rec
	return true, nil
}

// GetClassification returns the stored classification of sku, or
// Known=false when the copy has no row for it.
func (s *MemoryStore) GetClassification(_ context.Context, sku string) (ports.ClassificationInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.rows[sku]
	if !ok {
		return ports.ClassificationInfo{Known: false}, nil
	}
	return toInfo(rec.HandlingTags, rec.DOTHazardClass), nil
}
