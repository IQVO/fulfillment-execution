package memory

import (
	"context"
	"sort"
	"sync"

	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// PackageRepo is a thread-safe in-memory implementation of ports.PackageRepo.
type PackageRepo struct {
	mu       sync.RWMutex
	packages map[shared.PackageId]*pack.Package
}

// NewPackageRepo constructs an empty PackageRepo.
func NewPackageRepo() *PackageRepo {
	return &PackageRepo{packages: make(map[shared.PackageId]*pack.Package)}
}

func (r *PackageRepo) Save(_ context.Context, p *pack.Package) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.packages[p.Id()] = p
	return nil
}

func (r *PackageRepo) FindById(_ context.Context, id shared.PackageId) (*pack.Package, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.packages[id]
	if !ok {
		return nil, nil
	}
	return p, nil
}

// FindByTaskId scans every stored package for a matching TaskId(). This
// repo backs unit tests and local/no-DB runs only (small package counts),
// so a linear scan is appropriate — the postgres adapter uses an indexed
// column lookup instead. An empty taskId never matches anything, even a
// package whose own TaskId() is also empty (pre-this-feature data) — see
// pack.Rehydrate's doc comment.
func (r *PackageRepo) FindByTaskId(_ context.Context, taskId shared.TaskId) (*pack.Package, error) {
	if taskId == "" {
		return nil, nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.packages {
		if p.TaskId() == taskId {
			return p, nil
		}
	}
	return nil, nil
}

// FindByOrderRef returns every stored package whose OrderRef() matches,
// sorted by id so the result is deterministic (the postgres adapter
// orders by id too). Linear scan, like FindByTaskId — this repo only backs
// tests and local/no-DB runs. An unknown orderRef yields an empty, non-nil
// slice.
func (r *PackageRepo) FindByOrderRef(_ context.Context, orderRef shared.OrderRef) ([]*pack.Package, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*pack.Package, 0)
	for _, p := range r.packages {
		if p.OrderRef() == orderRef {
			result = append(result, p)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Id() < result[j].Id()
	})
	return result, nil
}
