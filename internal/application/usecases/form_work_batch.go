package usecases

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// DefaultBatchMaximum is the D4 placeholder: a setting, default 4 in shadow
// mode, until measured against real carts (wave 4's report).
const DefaultBatchMaximum = 4

// BatchMode is the PICK_BATCH_MODE rollout gate (D5): off (the fleet
// default — ClaimNext and POST /stations/{id}/task-claims behave exactly as
// today, no batch forms, no new event is published), shadow (wave 4: each
// normal claim computes the batch it would have formed and publishes
// analytics only), and on (batch formation serves POST
// /stations/{id}/work-batches). Turning the mode on is a separate decision
// taken after the shadow measurements are reviewed.
type BatchMode string

const (
	BatchModeOff    BatchMode = "off"
	BatchModeShadow BatchMode = "shadow"
	BatchModeOn     BatchMode = "on"
)

// ParseBatchMode validates a PICK_BATCH_MODE value. Matching is exact and
// case-sensitive, like every mode/enum parse in this service.
func ParseBatchMode(value string) (BatchMode, error) {
	switch BatchMode(value) {
	case BatchModeOff, BatchModeShadow, BatchModeOn:
		return BatchMode(value), nil
	default:
		return "", errors.New("usecases: PICK_BATCH_MODE must be off, shadow or on")
	}
}

// ErrBatchModeOff is returned by the work-batch use cases when
// PICK_BATCH_MODE is not `on`: batch formation is explicitly off, which is
// a configured state of the surface, not a fault (the fleet default).
var ErrBatchModeOff = errors.New("usecases: pick batching is off (PICK_BATCH_MODE)")

// ErrBatchNotFound is returned when a referenced work batch does not exist.
var ErrBatchNotFound = errors.New("usecases: work batch not found")

// FormWorkBatch implements ADR 0063's FormBatch command: one station's
// walk, formed at assignment time. The anchor is selected through the same
// claimer ClaimNext uses, so it is exactly the task ClaimNext would return
// — batching never changes who goes first. Companions are the station's
// hinted candidates inside the anchor's priority band, in rank order,
// filtered to what the station can do and what may share the cart, each
// claimed through the existing per-task compare-and-set (losing one is not
// an error). A batch of one is valid; nothing waits for a batch to fill.
type FormWorkBatch struct {
	Tasks     ports.TaskRepo
	Stations  ports.StationRepo
	Batches   ports.WorkBatchRepo
	Publisher ports.EventPublisher
	Clock     ports.Clock
	// NewBatchId mints the WorkBatch id, exactly once per formed batch.
	NewBatchId func() shared.BatchId
	// Hints is the station's interleaving-hint read (ADR 0051); nil means
	// no hints at all, so every batch is the anchor alone.
	Hints ports.InterleavingHintLookup
	// Classification resolves a member SKU's DOT hazard class for the cart
	// segregation filter. Nil, or an unknown SKU, means unclassified (0):
	// fail-open, the same convention SealPackage uses (ADR 0010).
	Classification ports.ProductClassificationLookup
	// Maximum is D4's per-batch maximum; <= 0 means DefaultBatchMaximum.
	Maximum int
	// Mode is the PICK_BATCH_MODE gate (D5). Only BatchModeOn forms; off
	// and shadow answer ErrBatchModeOff (shadow's compute-only path is
	// wave 4).
	Mode BatchMode
	// Metrics, UnitOfWork, Logger, LeaseDuration mirror ClaimNext's
	// same-named fields; the claimer they feed is shared with it.
	Metrics       ports.Metrics
	UnitOfWork    ports.UnitOfWork
	LeaseDuration time.Duration
	Logger        *slog.Logger
}

// FormWorkBatchOutcome is the result of forming: the batch, and the anchor
// task id. Batch is nil when the anchor was claimed as an ordinary single
// task because it has no pick face (an unannounced pick can only ever be an
// anchor, never a companion — ADR 0062): the claim stands, exactly as
// ClaimNext would have left it, and no WorkBatch is recorded.
type FormWorkBatchOutcome struct {
	Batch        *workbatch.WorkBatch
	AnchorTaskId shared.TaskId
}

func (uc *FormWorkBatch) core() *claimer {
	return &claimer{
		Tasks:         uc.Tasks,
		Stations:      uc.Stations,
		Publisher:     uc.Publisher,
		Metrics:       uc.Metrics,
		UnitOfWork:    uc.UnitOfWork,
		Hints:         uc.Hints,
		Logger:        uc.Logger,
		LeaseDuration: uc.LeaseDuration,
	}
}

func (uc *FormWorkBatch) maximum() int {
	if uc.Maximum <= 0 {
		return DefaultBatchMaximum
	}
	return uc.Maximum
}

// Execute forms the station's batch. Every claimed member keeps its
// ordinary task lifecycle: leases, scans, completion and TaskCompleted are
// unchanged; the WorkBatch records the walk and the member bookkeeping.
func (uc *FormWorkBatch) Execute(ctx context.Context, stationId shared.StationId) (*FormWorkBatchOutcome, error) {
	if uc.Mode != BatchModeOn {
		return nil, ErrBatchModeOff
	}
	if open, err := uc.Batches.FindOpenByStation(ctx, stationId); err != nil {
		return nil, err
	} else if open != nil {
		return nil, workbatch.ErrStationHasOpenBatch
	}

	now := uc.Clock.Now()
	anchor, err := uc.core().claimFirstClaimable(ctx, stationId, task.Pick, now)
	if err != nil {
		return nil, err
	}

	// An anchor with no pick face cannot be a stop, but it is still the
	// work ClaimNext would have handed the station: the claim stands and
	// no batch is formed around it (ADR 0062).
	if pickFaceOf(anchor) == "" {
		return &FormWorkBatchOutcome{AnchorTaskId: anchor.Id()}, nil
	}

	members := []workbatch.Member{uc.memberOf(ctx, anchor, 0)}
	classes := []int{members[0].HazardClass}
	if len(members) < uc.maximum() {
		st, err := uc.core().station(ctx, stationId)
		if err != nil {
			return nil, err
		}
		for _, companion := range uc.companions(ctx, stationId, anchor, now) {
			if len(members) >= uc.maximum() {
				break
			}
			// The station's capability gate is the same predicate the
			// anchor passed (task.Claim's own HasAll): a companion the
			// station cannot do is skipped, never an error.
			if !st.capabilities.HasAll(companion.RequiredCapabilities()) {
				continue
			}
			// The cart segregation filter (model invariant): a hazard
			// class that may not share the cart with an already-chosen
			// member skips the companion, never fails the batch.
			m := uc.memberOf(ctx, companion, uc.rankOf(ctx, stationId, companion, now))
			conflict := false
			for _, class := range classes {
				if pack.IsSegregationIncompatible(class, m.HazardClass) {
					conflict = true
					break
				}
			}
			if conflict {
				continue
			}
			if err := companion.Claim(stationId, st.capabilities, now, uc.core().leaseDuration()); err != nil {
				return nil, err
			}
			won, err := uc.core().persistClaim(ctx, companion, stationId, now)
			if err != nil {
				return nil, err
			}
			if !won {
				// Another station won this companion; the batch forms
				// without it (per-task compare-and-set, ADR-0003).
				continue
			}
			if uc.Metrics != nil {
				uc.Metrics.TaskClaimed(ctx, companion.Type())
			}
			members = append(members, m)
			classes = append(classes, m.HazardClass)
		}
	}

	b, err := workbatch.Form(uc.NewBatchId(), stationId, uc.maximum(), anchor.CPT().Time(), members, now)
	if err != nil {
		return nil, err
	}
	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Batches.Save(ctx, b); err != nil {
			return err
		}
		return uc.Publisher.Publish(ctx, workBatchFormedEvent(b, now))
	}); err != nil {
		return nil, err
	}
	return &FormWorkBatchOutcome{Batch: b, AnchorTaskId: anchor.Id()}, nil
}

// companions returns the station's hinted candidates that could still join
// the anchor's walk, in hint rank order: still claimable, inside the
// anchor's priority band (the same CPT instant, D3), and carrying a pick
// face. Capability and segregation are applied by the caller (a skip, not
// an error, in both cases).
func (uc *FormWorkBatch) companions(ctx context.Context, stationId shared.StationId, anchor *task.Task, now time.Time) []*task.Task {
	hint := uc.activeHint(ctx, stationId, now)
	if hint == nil {
		return nil
	}
	claimable := make(map[shared.TaskId]*task.Task)
	candidates, err := uc.Tasks.FindClaimableByType(ctx, task.Pick, now)
	if err != nil {
		return nil // fail-open: no companions, the anchor alone
	}
	for _, t := range candidates {
		claimable[t.Id()] = t
	}
	rank := make(map[shared.TaskId]int, len(hint.Candidates))
	for _, cand := range hint.Candidates {
		id := shared.TaskId(cand.TaskId)
		if r, seen := rank[id]; !seen || cand.Rank < r {
			rank[id] = cand.Rank
		}
	}
	ordered := make([]shared.TaskId, 0, len(rank))
	for id := range rank {
		ordered = append(ordered, id)
	}
	sort.SliceStable(ordered, func(i, j int) bool { return rank[ordered[i]] < rank[ordered[j]] })
	out := make([]*task.Task, 0, len(ordered))
	for _, id := range ordered {
		if id == anchor.Id() {
			continue
		}
		t, ok := claimable[id]
		if !ok || !t.CPT().Time().Equal(anchor.CPT().Time()) || pickFaceOf(t) == "" {
			continue
		}
		out = append(out, t)
	}
	return out
}

// activeHint is the station's stored hint or nil, fail-open on every
// failure path (ADR 0051's convention: an ignored hint costs nothing).
func (uc *FormWorkBatch) activeHint(ctx context.Context, stationId shared.StationId, now time.Time) *ports.InterleavingHint {
	if uc.Hints == nil {
		return nil
	}
	hint, err := uc.Hints.ActiveForStation(ctx, stationId, now)
	if err != nil || hint == nil || !hint.ExpiresAt.After(now) || len(hint.Candidates) == 0 {
		return nil
	}
	return hint
}

// rankOf is the hint's rank for taskId, or 0 when unhinted; Form orders
// stops by it (D6) and the anchor's 0 keeps it first.
func (uc *FormWorkBatch) rankOf(ctx context.Context, stationId shared.StationId, t *task.Task, now time.Time) int {
	if hint := uc.activeHint(ctx, stationId, now); hint != nil {
		for _, cand := range hint.Candidates {
			if shared.TaskId(cand.TaskId) == t.Id() {
				return cand.Rank
			}
		}
	}
	return 0
}

// memberOf snapshots t as a batch member candidate: the pickability facts
// Form's invariants need (the band instant, the pick face) plus the SKU's
// DOT hazard class, fail-open (0) when the classification is unknown or the
// lookup fails.
func (uc *FormWorkBatch) memberOf(ctx context.Context, t *task.Task, rank int) workbatch.Member {
	m := workbatch.Member{
		TaskId: t.Id(),
		Cpt:    t.CPT().Time(),
		Rank:   rank,
	}
	if plan := t.PickPlan(); plan != nil {
		m.FromBin = plan.FromBin
		m.HazardClass = uc.hazardClassOf(ctx, plan.SKU)
	}
	return m
}

func (uc *FormWorkBatch) hazardClassOf(ctx context.Context, sku string) int {
	if uc.Classification == nil {
		return 0
	}
	info, err := uc.Classification.GetClassification(ctx, sku)
	if err != nil || !info.Known {
		return 0
	}
	return info.DOTHazardClass
}

// pickFaceOf is the task's planned pick face, "" when it has no pick plan.
func pickFaceOf(t *task.Task) string {
	if plan := t.PickPlan(); plan != nil {
		return plan.FromBin
	}
	return ""
}

// workBatchFormedEvent builds the formation event from the formed batch.
func workBatchFormedEvent(b *workbatch.WorkBatch, at time.Time) shared.WorkBatchFormed {
	facts := make([]shared.WorkBatchMemberFact, len(b.Members()))
	for i, m := range b.Members() {
		facts[i] = shared.WorkBatchMemberFact{TaskId: m.TaskId, StopSeq: m.StopSeq, Compartment: m.Compartment}
	}
	return shared.NewWorkBatchFormed(b.Id(), b.StationId(), b.CPT(), facts, b.Version(), at)
}
