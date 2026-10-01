// Package memory provides thread-safe in-memory implementations of every
// outbound port, for tests and local development.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TaskRepo is a thread-safe in-memory implementation of ports.TaskRepo.
type TaskRepo struct {
	mu    sync.RWMutex
	tasks map[shared.TaskId]*task.Task
	// persisted records each task's claim state as of its last Save /
	// SaveClaim. tasks holds live pointers that a use case mutates before
	// saving, so SaveClaim's compare-and-set must compare against this
	// snapshot, exactly like the Postgres row it stands in for.
	persisted map[shared.TaskId]claimSnapshot
}

type claimSnapshot struct {
	status      task.Status
	leaseExpiry *time.Time
}

func snapshotOf(t *task.Task) claimSnapshot {
	snap := claimSnapshot{status: t.Status()}
	if l := t.Lease(); l != nil {
		e := l.Expiry
		snap.leaseExpiry = &e
	}
	return snap
}

func (s claimSnapshot) claimable(now time.Time) bool {
	if s.status == task.Pending {
		return true
	}
	return s.status == task.Claimed && s.leaseExpiry != nil && !now.Before(*s.leaseExpiry)
}

// NewTaskRepo constructs an empty TaskRepo.
func NewTaskRepo() *TaskRepo {
	return &TaskRepo{tasks: make(map[shared.TaskId]*task.Task), persisted: make(map[shared.TaskId]claimSnapshot)}
}

func (r *TaskRepo) Save(_ context.Context, t *task.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks[t.Id()] = cloneTask(t)
	r.persisted[t.Id()] = snapshotOf(t)
	return nil
}

// SaveClaim implements ports.TaskRepo's compare-and-set claim.
func (r *TaskRepo) SaveClaim(_ context.Context, t *task.Task, now time.Time) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if snap, ok := r.persisted[t.Id()]; ok && !snap.claimable(now) {
		return false, nil
	}
	r.tasks[t.Id()] = cloneTask(t)
	r.persisted[t.Id()] = snapshotOf(t)
	return true, nil
}

func (r *TaskRepo) FindById(_ context.Context, id shared.TaskId) (*task.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tasks[id]
	if !ok {
		return nil, nil
	}
	return cloneTask(t), nil
}

func (r *TaskRepo) FindClaimableByType(_ context.Context, taskType task.Type, now time.Time) ([]*task.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*task.Task
	for _, t := range r.tasks {
		if t.Type() != taskType {
			continue
		}
		if t.IsAvailable(now) {
			result = append(result, cloneTask(t))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CPT().Before(result[j].CPT())
	})
	return result, nil
}

func (r *TaskRepo) FindAllClaimed(_ context.Context) ([]*task.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*task.Task
	for _, t := range r.tasks {
		if t.Status() == task.Claimed {
			result = append(result, cloneTask(t))
		}
	}
	return result, nil
}

// FindOpenPastCPT returns every Pending or Claimed task whose CPT is at or
// before now, for the CPT-missed sweep (ADR-0025).
func (r *TaskRepo) FindOpenPastCPT(_ context.Context, now time.Time) ([]*task.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*task.Task
	for _, t := range r.tasks {
		if t.IsCPTMissed(now) {
			result = append(result, cloneTask(t))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Id() < result[j].Id()
	})
	return result, nil
}

func (r *TaskRepo) CountByTypeAndStatus(_ context.Context, taskType task.Type, status task.Status) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, t := range r.tasks {
		if t.Type() == taskType && t.Status() == status {
			count++
		}
	}
	return count, nil
}

func (r *TaskRepo) FindByOrderRef(_ context.Context, orderRef shared.OrderRef) ([]*task.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*task.Task
	for _, t := range r.tasks {
		if t.OrderRef() == orderRef {
			result = append(result, cloneTask(t))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Id() < result[j].Id()
	})
	return result, nil
}

// cloneTask returns an independent copy of t, the way every Postgres read
// rehydrates a fresh aggregate. Handing callers the stored pointer let two
// concurrent ClaimNext calls mutate ONE task (a data race, and a claim
// race no real database would exhibit).
func cloneTask(t *task.Task) *task.Task {
	var lease *task.Lease
	if l := t.Lease(); l != nil {
		cp := *l
		lease = &cp
	}
	var claimedAt *time.Time
	if c := t.ClaimedAt(); c != nil {
		cp := *c
		claimedAt = &cp
	}
	return task.Rehydrate(t.Id(), t.Type(), t.Status(), t.CPT(), t.OrderRef(), t.RequiredCapabilities(), lease, t.Fragile(), t.GiftWrap(), claimedAt)
}
