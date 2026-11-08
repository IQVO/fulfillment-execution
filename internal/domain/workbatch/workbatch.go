// Package workbatch implements the WorkBatch aggregate (ADR 0063): the PICK
// tasks one station holds for one walk, formed at assignment time inside one
// priority band. Not a Wave — nothing waits for a batch to fill, and a batch
// of one (the anchor alone) is valid. Disposable after completion.
//
// The aggregate enforces the model's formation invariants (size, pickability,
// band, cart segregation) and owns the member lifecycle OPEN -> COMPLETED
// (CompleteMember) and OPEN -> ABANDONED (DetachMember, when the last open
// member's lease expired and it returned to the pool). It knows nothing
// about claiming, leases or events: those belong to the application layer,
// which claims each member through the existing per-task compare-and-set.
package workbatch

import (
	"errors"
	"sort"
	"time"

	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// Status is the lifecycle state of a work batch.
type Status string

const (
	// StatusOpen: formed, at least one member still open (not completed,
	// not detached).
	StatusOpen Status = "OPEN"
	// StatusCompleted: the last open member completed.
	StatusCompleted Status = "COMPLETED"
	// StatusAbandoned: every member left without completing (detached after
	// a lease expiry); the tasks themselves completed or returned to the
	// pool through the existing sweep.
	StatusAbandoned Status = "ABANDONED"
)

// ParseStatus validates the persisted string form of a Status. Matching is
// exact (case-sensitive): every writer persists the canonical constant.
func ParseStatus(value string) (Status, error) {
	switch Status(value) {
	case StatusOpen, StatusCompleted, StatusAbandoned:
		return Status(value), nil
	default:
		return "", ErrUnknownStatus
	}
}

// ErrUnknownStatus is returned by ParseStatus for a string that is not one of
// the declared work batch statuses.
var ErrUnknownStatus = errors.New("workbatch: unknown status")

// MemberState is the lifecycle state of one member inside its batch.
type MemberState string

const (
	// MemberOpen: claimed into the batch, not yet completed or detached.
	MemberOpen MemberState = "OPEN"
	// MemberCompleted: the task completed as part of this batch.
	MemberCompleted MemberState = "COMPLETED"
	// MemberDetached: the task left the batch (lease expired, returned to
	// the pool through the existing sweep).
	MemberDetached MemberState = "DETACHED"
)

// ParseMemberState validates the persisted string form of a MemberState.
func ParseMemberState(value string) (MemberState, error) {
	switch MemberState(value) {
	case MemberOpen, MemberCompleted, MemberDetached:
		return MemberState(value), nil
	default:
		return "", ErrUnknownMemberState
	}
}

// ErrUnknownMemberState is returned by ParseMemberState for a string that is
// not one of the declared member states.
var ErrUnknownMemberState = errors.New("workbatch: unknown member state")

// Formation invariants (model.yaml, enforced by Form).
var (
	// ErrBatchSize: a batch has at least one member (the anchor) and at most
	// the configured maximum (D4).
	ErrBatchSize = errors.New("workbatch: batch size outside 1..maximum")
	// ErrMemberNotPickable: every member must be a PICK task with a planned
	// from_bin — a pick face of "" cannot be a stop.
	ErrMemberNotPickable = errors.New("workbatch: member is not a pickable task with a planned from_bin")
	// ErrCrossesPriorityBand: every member must share the anchor's CPT
	// instant (D3, same band only) so a batch can never delay an earlier
	// cutoff.
	ErrCrossesPriorityBand = errors.New("workbatch: member crosses the anchor's priority band")
	// ErrSegregationConflict: hazard classes on one cart must not conflict
	// under the segregation matrix.
	ErrSegregationConflict = errors.New("workbatch: member hazard classes conflict on one cart")
	// ErrStationHasOpenBatch: a station holds at most one OPEN batch at a
	// time (model.yaml invariant). Enforced by the forming use case over
	// the repository, because whether the station already holds one is a
	// fact about other batches, not about the batch being formed.
	ErrStationHasOpenBatch = errors.New("workbatch: station already holds an open batch")
)

// Member is one task of the walk, snapshotted at formation. The pickability
// facts the invariants need (the pick face, the band instant, the SKU's DOT
// hazard class) are carried on the member so Form can enforce them without
// reaching back into the task pool; the application layer fills them from
// the candidate tasks and the classification local copy.
type Member struct {
	TaskId shared.TaskId
	// Cpt is the member task's priority-band instant at formation; Form
	// requires it to equal the batch's (the anchor's) instant.
	Cpt time.Time
	// FromBin is the planned pick face the walk stops at.
	FromBin string
	// HazardClass is the member SKU's DOT hazard class (1..9); 0 means
	// unclassified, which never triggers or blocks segregation (fail-open).
	HazardClass int
	// Rank is the interleaving hint's rank for this task (1 = the
	// producer's preferred candidate); 0 for an unhinted anchor.
	Rank int
	// StopSeq is the 1-based visit order, assigned by Form in rank order
	// (D6: v1 visits stops in hint rank order).
	StopSeq int
	// Compartment is the cart position holding this task's items; v1
	// assigns it by stop order.
	Compartment int
	// State is the member's lifecycle state inside the batch.
	State MemberState
}

// WorkBatch is the aggregate root: the PICK tasks one station holds for one
// walk.
type WorkBatch struct {
	id        shared.BatchId
	stationId shared.StationId
	cpt       time.Time
	members   []Member
	status    Status
	version   int64
	formedAt  time.Time
	// completedAt is set when the last open member completed.
	completedAt *time.Time
	// abandonedAt is set when the last open member detached.
	abandonedAt *time.Time
}

// Form builds an OPEN batch of members around the anchor's priority band
// (cpt). It enforces every formation invariant of the model: size, member
// pickability, one band, and cart segregation under the existing DOT matrix.
// Members are stored in rank order (D6) with stop_seq and compartment
// assigned by that order. The optional formedAt is the formation instant
// walk_seconds is later measured from; when omitted (the pure-domain fixture
// shape) the band's CPT instant stands in, so the aggregate is fully
// deterministic without a clock.
func Form(id shared.BatchId, stationId shared.StationId, maximum int, cpt time.Time, members []Member, formedAt ...time.Time) (*WorkBatch, error) {
	if len(members) < 1 || len(members) > maximum {
		return nil, ErrBatchSize
	}
	walkStart := cpt
	if len(formedAt) > 0 {
		walkStart = formedAt[0]
	}
	sorted := make([]Member, len(members))
	copy(sorted, members)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Rank < sorted[j].Rank })
	classes := make([]int, 0, len(sorted))
	for i := range sorted {
		m := &sorted[i]
		if m.FromBin == "" {
			return nil, ErrMemberNotPickable
		}
		if !m.Cpt.Equal(cpt) {
			return nil, ErrCrossesPriorityBand
		}
		for _, other := range classes {
			if pack.IsSegregationIncompatible(other, m.HazardClass) {
				return nil, ErrSegregationConflict
			}
		}
		classes = append(classes, m.HazardClass)
		m.StopSeq = i + 1
		m.Compartment = i + 1
		m.State = MemberOpen
	}
	return &WorkBatch{
		id:        id,
		stationId: stationId,
		cpt:       cpt,
		members:   sorted,
		status:    StatusOpen,
		version:   1,
		formedAt:  walkStart,
	}, nil
}

// Rehydrate reconstructs a WorkBatch from persisted state without
// re-validating the formation invariants (used by repository adapters).
func Rehydrate(id shared.BatchId, stationId shared.StationId, cpt time.Time, members []Member, status Status, version int64, formedAt time.Time, completedAt, abandonedAt *time.Time) *WorkBatch {
	return &WorkBatch{
		id:          id,
		stationId:   stationId,
		cpt:         cpt,
		members:     members,
		status:      status,
		version:     version,
		formedAt:    formedAt,
		completedAt: completedAt,
		abandonedAt: abandonedAt,
	}
}

func (b *WorkBatch) Id() shared.BatchId          { return b.id }
func (b *WorkBatch) StationId() shared.StationId { return b.stationId }
func (b *WorkBatch) CPT() time.Time              { return b.cpt }
func (b *WorkBatch) Status() Status              { return b.status }
func (b *WorkBatch) Version() int64              { return b.version }

// Members returns the member snapshot in stop order. The returned slice is
// the aggregate's own: callers must not mutate it.
func (b *WorkBatch) Members() []Member { return b.members }

// OpenMembers returns exactly the still-open members, in stop order — the
// set lease renewal covers.
func (b *WorkBatch) OpenMembers() []Member {
	out := make([]Member, 0, len(b.members))
	for _, m := range b.members {
		if m.State == MemberOpen {
			out = append(out, m)
		}
	}
	return out
}

// NextOpenMember returns the lowest stop_seq member still open — the stop
// whose work clock starts when the previous stop completes (the NextStopClock
// policy, ADR 0064) — or nil when none is open.
func (b *WorkBatch) NextOpenMember() *Member {
	for i := range b.members {
		if b.members[i].State == MemberOpen {
			return &b.members[i]
		}
	}
	return nil
}

// Lifecycle errors.
var (
	// ErrBatchNotOpen: the batch is COMPLETED or ABANDONED; no member
	// command applies.
	ErrBatchNotOpen = errors.New("workbatch: batch is not open")
	// ErrMemberNotInBatch: the task is not a member of this batch.
	ErrMemberNotInBatch = errors.New("workbatch: task is not a member of this batch")
	// ErrMemberAlreadyCompleted: the member already completed into this
	// batch; a replayed completion never double-counts.
	ErrMemberAlreadyCompleted = errors.New("workbatch: member already completed")
	// ErrMemberDetached: the member left the batch (lease expired); a late
	// completion cannot re-enter it.
	ErrMemberDetached = errors.New("workbatch: member left the batch")
)

func (b *WorkBatch) member(id shared.TaskId) (*Member, error) {
	if b.status != StatusOpen {
		return nil, ErrBatchNotOpen
	}
	for i := range b.members {
		if b.members[i].TaskId == id {
			return &b.members[i], nil
		}
	}
	return nil, ErrMemberNotInBatch
}

// CompleteMember records that taskId's stop completed at at. Completing the
// last open member completes the batch (status COMPLETED, walk_seconds
// measurable from formation). The caller publishes WorkBatchCompleted when
// the status flipped; the aggregate stays event-free.
func (b *WorkBatch) CompleteMember(id shared.TaskId, at time.Time) error {
	m, err := b.member(id)
	if err != nil {
		return err
	}
	switch m.State {
	case MemberCompleted:
		return ErrMemberAlreadyCompleted
	case MemberDetached:
		return ErrMemberDetached
	}
	m.State = MemberCompleted
	b.version++
	if b.NextOpenMember() == nil {
		b.status = StatusCompleted
		cp := at
		b.completedAt = &cp
	}
	return nil
}

// DetachMember records that taskId left the batch at at — its lease expired
// and the task returned to the pool through the existing sweep. When no open
// member remains the batch is ABANDONED. Detaching an already-detached
// member is an idempotent no-op (the sweep may re-fire), never an error.
func (b *WorkBatch) DetachMember(id shared.TaskId, at time.Time) error {
	m, err := b.member(id)
	if err != nil {
		if errors.Is(err, ErrBatchNotOpen) {
			return err
		}
		// Outside the open-lifecycle gate, a repeated detach must still be
		// idempotent: find the member without the status check.
		for i := range b.members {
			if b.members[i].TaskId == id {
				return nil
			}
		}
		return err
	}
	if m.State == MemberDetached {
		return nil
	}
	m.State = MemberDetached
	b.version++
	if b.NextOpenMember() == nil {
		b.status = StatusAbandoned
		cp := at
		b.abandonedAt = &cp
	}
	return nil
}

// CompletedAt returns when the batch completed, or nil when it has not.
func (b *WorkBatch) CompletedAt() *time.Time { return b.completedAt }

// AbandonedAt returns when the batch was abandoned, or nil when it was not.
func (b *WorkBatch) AbandonedAt() *time.Time { return b.abandonedAt }

// FormedAt returns the formation instant — the start of the walk the
// completed batch reports as walk_seconds.
func (b *WorkBatch) FormedAt() time.Time { return b.formedAt }

// WalkSeconds is the completed batch's walk length: completion minus
// formation, in whole seconds. Zero for a batch that has not completed.
func (b *WorkBatch) WalkSeconds() int64 {
	if b.completedAt == nil {
		return 0
	}
	return int64(b.completedAt.Sub(b.formedAt).Seconds())
}
