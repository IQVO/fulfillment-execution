package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// recordingPublisher records every event handed to it.
type recordingPublisher struct {
	mu     sync.Mutex
	events []shared.DomainEvent
}

func (p *recordingPublisher) Publish(_ context.Context, evts ...shared.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, evts...)
	return nil
}

func (p *recordingPublisher) of(name string) []shared.DomainEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []shared.DomainEvent
	for _, e := range p.events {
		if e.EventName() == name {
			out = append(out, e)
		}
	}
	return out
}

// pickSeed describes one seeded PICK task.
type pickSeed struct {
	id      string
	cpt     time.Time
	sku     string
	fromBin string
	caps    []string // required capabilities; nil = none
}

// batchFixture is a fully wired FormWorkBatch over in-memory doubles.
type batchFixture struct {
	tasks          *memory.TaskRepo
	stations       *memory.StationRepo
	batches        *memory.WorkBatchRepo
	publisher      *recordingPublisher
	classification *fakeClassificationLookup
	form           *usecases.FormWorkBatch
	claim          *usecases.ClaimNext
	ctx            context.Context
	clock          *memory.FixedClock
	// nextBatchId is atomic: the concurrency test builds one use case per
	// station from two goroutines, and both mint through it.
	nextBatchId atomic.Int64
}

// newBatchFixture seeds one station (caps "pick") and the given tasks, and
// wires FormWorkBatch (mode on) and ClaimNext over the SAME repos so the
// anchor-equivalence tests compare the two use cases over identical pools.
func newBatchFixture(t *testing.T, now time.Time, seeds []pickSeed) *batchFixture {
	t.Helper()
	fx := &batchFixture{
		tasks:          memory.NewTaskRepo(),
		stations:       memory.NewStationRepo(),
		batches:        memory.NewWorkBatchRepo(),
		publisher:      &recordingPublisher{},
		classification: newFakeClassificationLookup(),
		ctx:            context.Background(),
		clock:          memory.NewFixedClock(now),
	}
	for _, s := range seeds {
		converted := make([]shared.Capability, len(s.caps))
		for i, c := range s.caps {
			converted[i] = shared.Capability(c)
		}
		tk := task.New(shared.TaskId(s.id), task.Pick, shared.NewCPT(s.cpt), shared.OrderRef("o-"+s.id), shared.NewCapabilitySet(converted...), false, false)
		if s.sku != "" || s.fromBin != "" {
			plan, err := task.NewPickPlan(s.sku, 1, "EACH", "WH1", s.fromBin)
			if err != nil {
				t.Fatalf("pick plan for %s: %v", s.id, err)
			}
			tk = tk.WithPickPlan(&plan, 0)
		}
		if err := fx.tasks.Save(fx.ctx, tk); err != nil {
			t.Fatalf("seed %s: %v", s.id, err)
		}
	}
	if err := fx.stations.Save(fx.ctx, station.New(shared.StationId("s-1"), shared.NewCapabilitySet(shared.Capability("pick")))); err != nil {
		t.Fatalf("seed station: %v", err)
	}
	fx.form = fx.wired(nil)
	fx.claim = &usecases.ClaimNext{
		Tasks: fx.tasks, Stations: fx.stations, Publisher: fx.publisher,
		Clock: fx.clock, Hints: nil,
	}
	return fx
}

// wired (re)builds the FormWorkBatch use case, so a test can attach a hint
// lookup or classification overrides after seeding.
func (fx *batchFixture) wired(hints ports.InterleavingHintLookup) *usecases.FormWorkBatch {
	return &usecases.FormWorkBatch{
		Tasks:          fx.tasks,
		Stations:       fx.stations,
		Batches:        fx.batches,
		Publisher:      fx.publisher,
		Clock:          fx.clock,
		NewBatchId:     func() shared.BatchId { return shared.BatchId(fmt.Sprintf("b-%d", fx.nextBatchId.Add(1))) },
		Hints:          hints,
		Classification: fx.classification,
		Mode:           usecases.BatchModeOn,
	}
}

// withHint attaches the hint lookup and rebuilds the use case.
func (fx *batchFixture) withHint(hint *ports.InterleavingHint) *usecases.FormWorkBatch {
	return fx.wired(fakeHintLookup{hint: hint})
}

// memberIds lists the batch's member task ids in stop order.
func memberIds(b *workbatch.WorkBatch) []shared.TaskId {
	out := make([]shared.TaskId, len(b.Members()))
	for i, m := range b.Members() {
		out[i] = m.TaskId
	}
	return out
}

func at(clock string) time.Time {
	parsed, err := time.Parse("15:04", clock)
	if err != nil {
		panic("fixture clock must be HH:MM: " + clock)
	}
	return time.Date(2026, 11, 6, parsed.Hour(), parsed.Minute(), 0, 0, time.UTC)
}

// completeStop scans a member's planned quantity (the domain scan, so the
// fixture needs no PackView) and completes it through CompleteTask with the
// batch hook attached.
func (fx *batchFixture) completeStop(t *testing.T, taskId string, sku string, batches *memory.WorkBatchRepo) {
	t.Helper()
	tk, err := fx.tasks.FindById(fx.ctx, shared.TaskId(taskId))
	if err != nil || tk == nil {
		t.Fatalf("find %s: %v", taskId, err)
	}
	if _, err := tk.ConfirmPickScan("s-1", fx.clock.Now(), task.ScannedPack{GTIN: "gtin-" + sku, SKU: sku, Level: task.PackLevel(task.PickUnitEach)}, 1); err != nil {
		t.Fatalf("scan %s: %v", taskId, err)
	}
	if err := fx.tasks.Save(fx.ctx, tk); err != nil {
		t.Fatalf("save scan: %v", err)
	}
	complete := &usecases.CompleteTask{Tasks: fx.tasks, Publisher: fx.publisher, Clock: fx.clock, Batches: batches}
	if err := complete.Execute(fx.ctx, shared.TaskId(taskId), "s-1"); err != nil {
		t.Fatalf("complete %s: %v", taskId, err)
	}
}

// --- The anchor is exactly what ClaimNext would return -----------------------

// TestFormWorkBatch_AnchorEqualsClaimNext_NoHint: over identical pools and
// with no hint, the batch's first member is the task ClaimNext returns.
func TestFormWorkBatch_AnchorEqualsClaimNext_NoHint(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	seeds := []pickSeed{
		{"t-later", band.Add(time.Minute), "SKU-1", "A-04", nil},
		{"t-anchor", band, "SKU-2", "A-01", nil},
		{"t-peer", band, "SKU-3", "A-02", nil},
	}
	// ClaimNext's pool.
	cfx := newBatchFixture(t, now, seeds)
	claimed, err := cfx.claim.Execute(cfx.ctx, "s-1", task.Pick)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// FormWorkBatch's identical pool.
	ffx := newBatchFixture(t, now, seeds)
	out, err := ffx.form.Execute(ffx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	if out.Batch == nil {
		t.Fatal("batch = nil, want a batch of one")
	}
	if got := memberIds(out.Batch); len(got) != 1 || got[0] != claimed.Id() {
		t.Fatalf("batch members = %v, want exactly the ClaimNext anchor %s", got, claimed.Id())
	}
	if out.AnchorTaskId != claimed.Id() {
		t.Fatalf("anchor = %s, want %s", out.AnchorTaskId, claimed.Id())
	}
}

// TestFormWorkBatch_AnchorEqualsClaimNext_WithHint: the hint re-orders the
// band for ClaimNext (ADR 0051) — FormWorkBatch's anchor must follow it,
// because both run the same ordering.
func TestFormWorkBatch_AnchorEqualsClaimNext_WithHint(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	seeds := []pickSeed{
		{"t-0", band, "SKU-1", "A-01", nil},
		{"t-1", band, "SKU-2", "A-02", nil},
		{"t-2", band, "SKU-3", "A-03", nil},
	}
	hint := hintFor("s-1", now, "t-2", "t-1")
	cfx := newBatchFixture(t, now, seeds)
	cfx.claim.Hints = fakeHintLookup{hint: hint}
	claimed, err := cfx.claim.Execute(cfx.ctx, "s-1", task.Pick)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Id() != "t-2" {
		t.Fatalf("claim returned %s, want the hinted t-2", claimed.Id())
	}
	ffx := newBatchFixture(t, now, seeds)
	out, err := ffx.withHint(hint).Execute(ffx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	if got := memberIds(out.Batch); got[0] != claimed.Id() {
		t.Fatalf("anchor = %s, want %s (the hinted ClaimNext result)", got[0], claimed.Id())
	}
}

// TestFormWorkBatch_AnchorEqualsClaimNext_CapabilityLimited: when the
// earliest-CPT task needs a capability the station lacks, both use cases
// move to the next candidate.
func TestFormWorkBatch_AnchorEqualsClaimNext_CapabilityLimited(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	seeds := []pickSeed{
		{"t-needs-cold", band, "SKU-1", "A-01", []string{"cold"}},
		{"t-plain", band.Add(time.Minute), "SKU-2", "A-02", nil},
	}
	cfx := newBatchFixture(t, now, seeds)
	claimed, err := cfx.claim.Execute(cfx.ctx, "s-1", task.Pick)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	ffx := newBatchFixture(t, now, seeds)
	out, err := ffx.form.Execute(ffx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	if out.AnchorTaskId != claimed.Id() || claimed.Id() != "t-plain" {
		t.Fatalf("anchor = %s, claim = %s, want t-plain for both", out.AnchorTaskId, claimed.Id())
	}
}

// --- Companions ----------------------------------------------------------------

// TestFormWorkBatch_HintRankCompanionsInBand: companions join in hint rank
// order, the anchor stays stop 1, and stops/compartment follow rank (D6).
func TestFormWorkBatch_HintRankCompanionsInBand(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-anchor", band, "SKU-1", "A-01", nil},
		{"t-c2", band, "SKU-2", "A-03", nil},
		{"t-c1", band, "SKU-3", "A-02", nil},
	})
	out, err := fx.withHint(hintFor("s-1", now, "t-anchor", "t-c1", "t-c2")).Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	want := []shared.TaskId{"t-anchor", "t-c1", "t-c2"}
	got := memberIds(out.Batch)
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("members = %v, want %v in rank order", got, want)
	}
	for i, m := range out.Batch.Members() {
		if m.StopSeq != i+1 || m.Compartment != i+1 {
			t.Fatalf("member %s stop_seq=%d compartment=%d, want %d/%d", m.TaskId, m.StopSeq, m.Compartment, i+1, i+1)
		}
	}
	if evs := fx.publisher.of("WorkBatchFormed"); len(evs) != 1 {
		t.Fatalf("WorkBatchFormed published %d times, want 1", len(evs))
	}
}

// TestFormWorkBatch_LaterCPTCompanionNotIncluded: a hint naming a task in a
// later band never joins — the batch cannot delay an earlier cutoff by
// favoring later work (D3, the hard boundary).
func TestFormWorkBatch_LaterCPTCompanionNotIncluded(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-anchor", band, "SKU-1", "A-01", nil},
		{"t-later", band.Add(time.Hour), "SKU-2", "A-02", nil},
	})
	out, err := fx.withHint(hintFor("s-1", now, "t-anchor", "t-later")).Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	if got := memberIds(out.Batch); len(got) != 1 || got[0] != "t-anchor" {
		t.Fatalf("members = %v, want the anchor alone", got)
	}
}

// TestFormWorkBatch_CapabilityFilteredCompanionSkipped: a hinted companion
// the station cannot do is skipped, never an error.
func TestFormWorkBatch_CapabilityFilteredCompanionSkipped(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-anchor", band, "SKU-1", "A-01", nil},
		{"t-cold", band, "SKU-2", "A-02", []string{"cold"}},
	})
	out, err := fx.withHint(hintFor("s-1", now, "t-anchor", "t-cold")).Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	if got := memberIds(out.Batch); len(got) != 1 || got[0] != "t-anchor" {
		t.Fatalf("members = %v, want the anchor alone (companion needs cold)", got)
	}
}

// TestFormWorkBatch_SegregationFilteredCompanionSkipped: a hinted companion
// whose hazard class may not share the cart with the anchor's is skipped
// (class 2 vs class 3 under the DOT matrix).
func TestFormWorkBatch_SegregationFilteredCompanionSkipped(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-anchor", band, "SKU-A", "A-01", nil},
		{"t-safe", band, "SKU-B", "A-02", nil},
		{"t-conflict", band, "SKU-C", "A-03", nil},
	})
	fx.classification.bySKU["SKU-A"] = ports.ClassificationInfo{Known: true, DOTHazardClass: 2}
	fx.classification.bySKU["SKU-C"] = ports.ClassificationInfo{Known: true, DOTHazardClass: 3}
	out, err := fx.withHint(hintFor("s-1", now, "t-anchor", "t-conflict", "t-safe")).Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	got := memberIds(out.Batch)
	if len(got) != 2 || got[0] != "t-anchor" || got[1] != "t-safe" {
		t.Fatalf("members = %v, want [t-anchor t-safe] (class 3 companion skipped)", got)
	}
}

// TestFormWorkBatch_LostCompanionStillForms: when another station already
// claimed a companion (the compare-and-set loses), the batch forms without
// it — losing one is not an error.
func TestFormWorkBatch_LostCompanionStillForms(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-anchor", band, "SKU-1", "A-01", nil},
		{"t-taken", band, "SKU-2", "A-02", nil},
	})
	// A different station claims t-taken first, through the same repos
	// (its hint names t-taken, so it takes exactly that one).
	if err := fx.stations.Save(fx.ctx, station.New(shared.StationId("s-2"), shared.NewCapabilitySet(shared.Capability("pick")))); err != nil {
		t.Fatalf("seed s-2: %v", err)
	}
	other := &usecases.ClaimNext{Tasks: fx.tasks, Stations: fx.stations, Publisher: fx.publisher, Clock: fx.clock, Hints: fakeHintLookup{hint: hintFor("s-2", now, "t-taken")}}
	if _, err := other.Execute(fx.ctx, "s-2", task.Pick); err != nil {
		t.Fatalf("other station claim: %v", err)
	}
	out, err := fx.withHint(hintFor("s-1", now, "t-anchor", "t-taken")).Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	if got := memberIds(out.Batch); len(got) != 1 || got[0] != "t-anchor" {
		t.Fatalf("members = %v, want the anchor alone (companion was won by s-2)", got)
	}
}

// TestFormWorkBatch_MaximumRespected: companions stop at the configured
// maximum (D4).
func TestFormWorkBatch_MaximumRespected(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-anchor", band, "SKU-1", "A-01", nil},
		{"t-c1", band, "SKU-2", "A-02", nil},
		{"t-c2", band, "SKU-3", "A-03", nil},
		{"t-c3", band, "SKU-4", "A-04", nil},
	})
	uc := fx.withHint(hintFor("s-1", now, "t-anchor", "t-c1", "t-c2", "t-c3"))
	uc.Maximum = 2
	out, err := uc.Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	if got := len(out.Batch.Members()); got != 2 {
		t.Fatalf("batch size = %d, want the maximum 2", got)
	}
}

// TestFormWorkBatch_UnbatchableAnchorClaimedWithoutBatch: a pick with no
// pick face (never announced, ADR 0062) still gets claimed exactly as
// ClaimNext would — the claim stands, no batch is formed around it.
func TestFormWorkBatch_UnbatchableAnchorClaimedWithoutBatch(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-nobin", band, "", "", nil},
	})
	out, err := fx.form.Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	if out.Batch != nil || out.AnchorTaskId != "t-nobin" {
		t.Fatalf("out = batch %v anchor %s, want nil batch with t-nobin claimed", out.Batch, out.AnchorTaskId)
	}
	if tk, _ := fx.tasks.FindById(fx.ctx, "t-nobin"); tk.Status() != task.Claimed {
		t.Fatalf("anchor status = %s, want Claimed", tk.Status())
	}
}

// --- The guards -----------------------------------------------------------------

// TestFormWorkBatch_StationWithOpenBatchRejected: a station holding an OPEN
// batch gets ErrStationHasOpenBatch, not a second batch.
func TestFormWorkBatch_StationWithOpenBatchRejected(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-1", band, "SKU-1", "A-01", nil},
		{"t-2", band.Add(time.Minute), "SKU-2", "A-02", nil},
	})
	if _, err := fx.form.Execute(fx.ctx, "s-1"); err != nil {
		t.Fatalf("first form: %v", err)
	}
	_, err := fx.form.Execute(fx.ctx, "s-1")
	if !errors.Is(err, workbatch.ErrStationHasOpenBatch) {
		t.Fatalf("err = %v, want ErrStationHasOpenBatch", err)
	}
}

// TestFormWorkBatch_ModeGate: off and shadow both answer ErrBatchModeOff
// and claim nothing; only on forms.
func TestFormWorkBatch_ModeGate(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	for _, mode := range []usecases.BatchMode{usecases.BatchModeOff, usecases.BatchModeShadow} {
		fx := newBatchFixture(t, now, []pickSeed{{"t-1", band, "SKU-1", "A-01", nil}})
		fx.form.Mode = mode
		out, err := fx.form.Execute(fx.ctx, "s-1")
		if !errors.Is(err, usecases.ErrBatchModeOff) {
			t.Fatalf("mode %s: err = %v, want ErrBatchModeOff", mode, err)
		}
		if out != nil {
			t.Fatalf("mode %s: outcome = %+v, want nil", mode, out)
		}
		if tk, _ := fx.tasks.FindById(fx.ctx, "t-1"); tk.Status() != task.Pending {
			t.Fatalf("mode %s: task claimed although the mode is off", mode)
		}
		if evs := fx.publisher.of("WorkBatchFormed"); len(evs) != 0 {
			t.Fatalf("mode %s: WorkBatchFormed published", mode)
		}
	}
}

// TestFormWorkBatch_NoClaimableTask: an empty pool answers ClaimNext's own
// ErrNoClaimableTask.
func TestFormWorkBatch_NoClaimableTask(t *testing.T) {
	fx := newBatchFixture(t, at("10:00"), nil)
	_, err := fx.form.Execute(fx.ctx, "s-1")
	if !errors.Is(err, usecases.ErrNoClaimableTask) {
		t.Fatalf("err = %v, want ErrNoClaimableTask", err)
	}
}

// --- Two stations, one pool ------------------------------------------------------

// formResult is one station's concurrent formation outcome.
type formResult struct {
	station shared.StationId
	out     *usecases.FormWorkBatchOutcome
	err     error
}

// TestFormWorkBatch_TwoStationsOnePoolNoTaskInTwoBatches: two stations form
// concurrently from one shared pool; no task may end up claimed by — or a
// member of — both.
func TestFormWorkBatch_TwoStationsOnePoolNoTaskInTwoBatches(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	seeds := make([]pickSeed, 12)
	for i := range seeds {
		seeds[i] = pickSeed{id: fmt.Sprintf("t-%02d", i), cpt: band, sku: fmt.Sprintf("SKU-%d", i), fromBin: fmt.Sprintf("A-%02d", i)}
	}
	fx := newBatchFixture(t, now, seeds)
	if err := fx.stations.Save(fx.ctx, station.New(shared.StationId("s-2"), shared.NewCapabilitySet(shared.Capability("pick")))); err != nil {
		t.Fatalf("seed s-2: %v", err)
	}
	var wg sync.WaitGroup
	results := make(chan formResult, 2)
	form := func(id shared.StationId) {
		defer wg.Done()
		uc := fx.wired(nil)
		out, err := uc.Execute(fx.ctx, id)
		results <- formResult{station: id, out: out, err: err}
	}
	wg.Add(2)
	go form("s-1")
	go form("s-2")
	wg.Wait()
	close(results)

	owner, batchCount := collectBatchOwners(t, results)
	if batchCount == 0 {
		t.Fatal("no batch formed at all")
	}
	// Every claimed task belongs to exactly one batch or was claimed
	// singly; none is Pending and claimed at once.
	for _, s := range seeds {
		tk, err := fx.tasks.FindById(fx.ctx, shared.TaskId(s.id))
		if err != nil || tk == nil {
			t.Fatalf("find %s: %v", s.id, err)
		}
		if st, ok := owner[tk.Id()]; ok && st != tk.Lease().StationId {
			t.Fatalf("task %s member of %s's batch but leased to %v", tk.Id(), st, tk.Lease().StationId)
		}
	}
}

// collectBatchOwners folds the two stations' outcomes into the
// task -> owning-station map, asserting along the way that each batch
// belongs to its forming station and no task is a member of two batches.
func collectBatchOwners(t *testing.T, results <-chan formResult) (owner map[shared.TaskId]shared.StationId, batchCount int) {
	t.Helper()
	owner = make(map[shared.TaskId]shared.StationId)
	for res := range results {
		if res.err != nil {
			t.Fatalf("station %s: %v", res.station, res.err)
		}
		if res.out.Batch == nil {
			continue
		}
		batchCount++
		if res.out.Batch.StationId() != res.station {
			t.Fatalf("batch %s belongs to %s, formed by %s", res.out.Batch.Id(), res.out.Batch.StationId(), res.station)
		}
		for _, id := range memberIds(res.out.Batch) {
			if prev, dup := owner[id]; dup {
				t.Fatalf("task %s claimed into two batches (%s and %s)", id, prev, res.station)
			}
			owner[id] = res.station
		}
	}
	return owner, batchCount
}

// --- LeaseTogether / renew -------------------------------------------------------

// TestRenewWorkBatchLease_RenewsEveryOpenMember: renewal extends every open
// member's lease, completed members are left alone.
func TestRenewWorkBatchLease_RenewsEveryOpenMember(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-1", band, "SKU-1", "A-01", nil},
		{"t-2", band, "SKU-2", "A-02", nil},
	})
	out, err := fx.form.Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	expiry := fx.clock.Now().Add(5 * time.Minute)
	for _, id := range memberIds(out.Batch) {
		tk, _ := fx.tasks.FindById(fx.ctx, id)
		if !tk.Lease().Expiry.Equal(expiry) {
			t.Fatalf("%s lease = %v, want %v", id, tk.Lease().Expiry, expiry)
		}
	}
	fx.clock.Advance(4 * time.Minute)
	renew := &usecases.RenewWorkBatchLease{Tasks: fx.tasks, Batches: fx.batches, Clock: fx.clock}
	if err := renew.Execute(fx.ctx, out.Batch.Id()); err != nil {
		t.Fatalf("renew: %v", err)
	}
	newExpiry := fx.clock.Now().Add(5 * time.Minute)
	for _, id := range memberIds(out.Batch) {
		tk, _ := fx.tasks.FindById(fx.ctx, id)
		if !tk.Lease().Expiry.Equal(newExpiry) {
			t.Fatalf("%s lease = %v after renewal, want %v", id, tk.Lease().Expiry, newExpiry)
		}
	}
}

// TestRenewWorkBatchLease_UnknownBatch: 404's own error.
func TestRenewWorkBatchLease_UnknownBatch(t *testing.T) {
	fx := newBatchFixture(t, at("10:00"), nil)
	renew := &usecases.RenewWorkBatchLease{Tasks: fx.tasks, Batches: fx.batches, Clock: fx.clock}
	if err := renew.Execute(fx.ctx, "nope"); !errors.Is(err, usecases.ErrBatchNotFound) {
		t.Fatalf("err = %v, want ErrBatchNotFound", err)
	}
}

// --- Expired member returns; last completion completes ---------------------------

// TestCompleteTask_InBatch_LastCompletionCompletesBatch: each completion
// records the member; the last one completes the batch and raises
// WorkBatchCompleted with the walk measured from formation.
func TestCompleteTask_InBatch_LastCompletionCompletesBatch(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-1", band, "SKU-1", "A-01", nil},
		{"t-2", band, "SKU-2", "A-02", nil},
	})
	out, err := fx.withHint(hintFor("s-1", now, "t-1", "t-2")).Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	fx.clock.Advance(2 * time.Minute)
	fx.completeStop(t, "t-1", "SKU-1", fx.batches)
	if evs := fx.publisher.of("WorkBatchCompleted"); len(evs) != 0 {
		t.Fatal("WorkBatchCompleted after the FIRST member — want it only on the last")
	}
	fx.clock.Advance(2 * time.Minute)
	fx.completeStop(t, "t-2", "SKU-2", fx.batches)
	evs := fx.publisher.of("WorkBatchCompleted")
	if len(evs) != 1 {
		t.Fatalf("WorkBatchCompleted published %d times, want 1", len(evs))
	}
	stored, err := fx.batches.FindById(fx.ctx, out.Batch.Id())
	if err != nil || stored == nil {
		t.Fatalf("stored batch: %v", err)
	}
	if stored.Status() != workbatch.StatusCompleted {
		t.Fatalf("status = %s, want COMPLETED", stored.Status())
	}
	if got := stored.WalkSeconds(); got != 240 {
		t.Fatalf("walk_seconds = %d, want 240", got)
	}
}

// TestCompleteTask_InBatch_NextStopClock: completing stop 1 starts stop
// 2's work clock at that instant (ADR 0064) — duration_seconds stays per
// stop, so labor-performance never sees a batched picker as slow.
func TestCompleteTask_InBatch_NextStopClock(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-1", band, "SKU-1", "A-01", nil},
		{"t-2", band, "SKU-2", "A-02", nil},
	})
	if _, err := fx.withHint(hintFor("s-1", now, "t-1", "t-2")).Execute(fx.ctx, "s-1"); err != nil {
		t.Fatalf("form: %v", err)
	}
	stop1Done := now.Add(2 * time.Minute)
	fx.clock.Advance(2 * time.Minute)
	fx.completeStop(t, "t-1", "SKU-1", fx.batches)
	t2, _ := fx.tasks.FindById(fx.ctx, "t-2")
	if base := t2.DurationBase(); base == nil || !base.Equal(stop1Done) {
		t.Fatalf("t-2 duration base = %v, want stop-1 completion %v", base, stop1Done)
	}
}

// TestExpireLeases_DetachesExpiredMember: when a member's lease expires the
// sweep returns the task to the pool AND removes it from the batch; the
// last open member expiring abandons the batch and raises
// WorkBatchAbandoned.
func TestExpireLeases_DetachesExpiredMember(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-1", band, "SKU-1", "A-01", nil},
		{"t-2", band, "SKU-2", "A-02", nil},
	})
	out, err := fx.withHint(hintFor("s-1", now, "t-1", "t-2")).Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	sweep := &usecases.ExpireLeases{Tasks: fx.tasks, Publisher: fx.publisher, Clock: fx.clock, Batches: fx.batches}
	fx.clock.Advance(6 * time.Minute) // both leases expired
	freed, err := sweep.Execute(fx.ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if freed != 2 {
		t.Fatalf("freed = %d, want 2", freed)
	}
	stored, _ := fx.batches.FindById(fx.ctx, out.Batch.Id())
	if stored.Status() != workbatch.StatusAbandoned {
		t.Fatalf("status = %s, want ABANDONED", stored.Status())
	}
	if evs := fx.publisher.of("WorkBatchAbandoned"); len(evs) != 1 {
		t.Fatalf("WorkBatchAbandoned published %d times, want 1", len(evs))
	}
	// The freed tasks are claimable again — returned to the pool.
	for _, id := range memberIds(out.Batch) {
		tk, _ := fx.tasks.FindById(fx.ctx, id)
		if tk.Status() != task.Pending {
			t.Fatalf("%s status = %s, want Pending (back in the pool)", id, tk.Status())
		}
	}
}

// TestExpireLeases_PartialExpiryKeepsBatchOpen: one expired member leaves;
// the batch stays open around the survivor.
func TestExpireLeases_PartialExpiryKeepsBatchOpen(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{
		{"t-1", band, "SKU-1", "A-01", nil},
		{"t-2", band, "SKU-2", "A-02", nil},
	})
	out, err := fx.withHint(hintFor("s-1", now, "t-1", "t-2")).Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	// Keep stop 1's lease alive with the SINGLE-task renewal (the batch
	// renewal would keep the whole batch alive — LeaseTogether); stop 2's
	// lease is left to expire.
	fx.clock.Advance(4 * time.Minute)
	renew := &usecases.RenewLease{Tasks: fx.tasks, Clock: fx.clock}
	if err := renew.Execute(fx.ctx, "t-1", "s-1"); err != nil {
		t.Fatalf("renew t-1: %v", err)
	}
	fx.clock.Advance(2 * time.Minute) // 6 minutes since formation
	sweep := &usecases.ExpireLeases{Tasks: fx.tasks, Publisher: fx.publisher, Clock: fx.clock, Batches: fx.batches}
	freed, err := sweep.Execute(fx.ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if freed != 1 {
		t.Fatalf("freed = %d, want 1 (only the unrenewed member)", freed)
	}
	stored, _ := fx.batches.FindById(fx.ctx, out.Batch.Id())
	if stored.Status() != workbatch.StatusOpen {
		t.Fatalf("status = %s, want OPEN (the renewed member still holds it)", stored.Status())
	}
	if open := stored.OpenMembers(); len(open) != 1 || open[0].TaskId != "t-1" {
		t.Fatalf("open members = %+v, want [t-1]", open)
	}
	if evs := fx.publisher.of("WorkBatchAbandoned"); len(evs) != 0 {
		t.Fatal("WorkBatchAbandoned raised although a member is still open")
	}
}

// TestGetWorkBatch_RoundTrip: the read side returns the stored batch.
func TestGetWorkBatch_RoundTrip(t *testing.T) {
	now := at("10:00")
	band := now.Add(time.Hour)
	fx := newBatchFixture(t, now, []pickSeed{{"t-1", band, "SKU-1", "A-01", nil}})
	out, err := fx.form.Execute(fx.ctx, "s-1")
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	get := &usecases.GetWorkBatch{Batches: fx.batches}
	b, err := get.Execute(fx.ctx, out.Batch.Id())
	if err != nil || b == nil || b.Id() != out.Batch.Id() {
		t.Fatalf("get = %v, %v", b, err)
	}
	if _, err := get.Execute(fx.ctx, "nope"); !errors.Is(err, usecases.ErrBatchNotFound) {
		t.Fatalf("err = %v, want ErrBatchNotFound", err)
	}
}
