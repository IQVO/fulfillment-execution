package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// WorkBatchRepo is a thread-safe in-memory implementation of
// ports.WorkBatchRepo. Like every adapter in this package it is a TEST
// DOUBLE: no cmd/** binary imports it.
type WorkBatchRepo struct {
	mu      sync.RWMutex
	batches map[shared.BatchId]*workbatch.WorkBatch
}

// NewWorkBatchRepo constructs an empty WorkBatchRepo.
func NewWorkBatchRepo() *WorkBatchRepo {
	return &WorkBatchRepo{batches: make(map[shared.BatchId]*workbatch.WorkBatch)}
}

func (r *WorkBatchRepo) Save(_ context.Context, b *workbatch.WorkBatch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches[b.Id()] = cloneBatch(b)
	return nil
}

func (r *WorkBatchRepo) FindById(_ context.Context, id shared.BatchId) (*workbatch.WorkBatch, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if b, ok := r.batches[id]; ok {
		return cloneBatch(b), nil
	}
	return nil, nil
}

func (r *WorkBatchRepo) FindOpenByStation(_ context.Context, stationId shared.StationId) (*workbatch.WorkBatch, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, b := range r.batches {
		if b.StationId() == stationId && b.Status() == workbatch.StatusOpen {
			return cloneBatch(b), nil
		}
	}
	return nil, nil
}

func (r *WorkBatchRepo) FindOpenByTaskId(_ context.Context, taskId shared.TaskId) (*workbatch.WorkBatch, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, b := range r.batches {
		if b.Status() != workbatch.StatusOpen {
			continue
		}
		for _, m := range b.Members() {
			if m.TaskId == taskId && m.State == workbatch.MemberOpen {
				return cloneBatch(b), nil
			}
		}
	}
	return nil, nil
}

func (r *WorkBatchRepo) FindByTaskId(_ context.Context, taskId shared.TaskId) (*workbatch.WorkBatch, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// Most recently formed first, so a task that detached and re-entered a
	// later batch resolves to the batch its CURRENT completion belongs to.
	ids := make([]shared.BatchId, 0, len(r.batches))
	for id := range r.batches {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return r.batches[ids[i]].FormedAt().After(r.batches[ids[j]].FormedAt())
	})
	for _, id := range ids {
		b := r.batches[id]
		if b.Status() == workbatch.StatusAbandoned {
			continue
		}
		for _, m := range b.Members() {
			if m.TaskId == taskId && m.State != workbatch.MemberDetached {
				return cloneBatch(b), nil
			}
		}
	}
	return nil, nil
}

// cloneBatch deep-copies b so callers can never mutate stored state, the
// same contract memory.TaskRepo gives.
func cloneBatch(b *workbatch.WorkBatch) *workbatch.WorkBatch {
	members := make([]workbatch.Member, len(b.Members()))
	copy(members, b.Members())
	return workbatch.Rehydrate(b.Id(), b.StationId(), b.CPT(), members, b.Status(), b.Version(), b.FormedAt(), cloneTimePtr(b.CompletedAt()), cloneTimePtr(b.AbandonedAt()))
}

func cloneTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	cp := *t
	return &cp
}
