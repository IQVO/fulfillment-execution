package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/consolidation"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// These tests pin the ADR 0020 contract at the application layer: every
// publishing use case runs ALL its writes (repo Saves and Publish) inside
// exactly one UnitOfWork scope, a publish failure rolls that scope back,
// and a nil UnitOfWork keeps the historical back-to-back behaviour.

// scopeKey marks a context as "inside the unit of work" so the fakes can
// assert every Save/Publish happened within the scope, never outside it.
type scopeKey struct{}

// recordingUnitOfWork is a ports.UnitOfWork fake that (a) tags the ctx it
// hands to fn, (b) counts how many scopes were opened (a nested Execute
// on an already-scoped ctx joins rather than counting, like the Postgres
// implementation), and (c) reports whether each scope committed or rolled
// back.
type recordingUnitOfWork struct {
	opened     int
	committed  int
	rolledBack int
	beginErr   error
}

func (u *recordingUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if inScope(ctx) {
		return fn(ctx)
	}
	if u.beginErr != nil {
		return u.beginErr
	}
	u.opened++
	err := fn(context.WithValue(ctx, scopeKey{}, true))
	if err != nil {
		u.rolledBack++
		return err
	}
	u.committed++
	return nil
}

func inScope(ctx context.Context) bool {
	v, _ := ctx.Value(scopeKey{}).(bool)
	return v
}

// scopedPublisher records, per Publish call, whether it ran inside a
// scope, and can be made to fail.
type scopedPublisher struct {
	inScope []bool
	names   []string
	err     error
}

func (p *scopedPublisher) Publish(ctx context.Context, evts ...shared.DomainEvent) error {
	p.inScope = append(p.inScope, inScope(ctx))
	for _, e := range evts {
		p.names = append(p.names, e.EventName())
	}
	return p.err
}

func (p *scopedPublisher) allInScope() bool {
	if len(p.inScope) == 0 {
		return false
	}
	for _, s := range p.inScope {
		if !s {
			return false
		}
	}
	return true
}

// scopedTaskRepo / scopedPackageRepo / scopedConsolidationRepo record
// whether each Save ran inside the scope.
type scopedTaskRepo struct {
	*memory.TaskRepo
	savesInScope []bool
}

func (r *scopedTaskRepo) Save(ctx context.Context, t *task.Task) error {
	r.savesInScope = append(r.savesInScope, inScope(ctx))
	return r.TaskRepo.Save(ctx, t)
}

func (r *scopedTaskRepo) SaveClaim(ctx context.Context, t *task.Task, now time.Time) (bool, error) {
	r.savesInScope = append(r.savesInScope, inScope(ctx))
	return r.TaskRepo.SaveClaim(ctx, t, now)
}

type scopedPackageRepo struct {
	*memory.PackageRepo
	savesInScope []bool
}

func (r *scopedPackageRepo) Save(ctx context.Context, p *pack.Package) error {
	r.savesInScope = append(r.savesInScope, inScope(ctx))
	return r.PackageRepo.Save(ctx, p)
}

type scopedConsolidationRepo struct {
	*memory.OrderConsolidationRepo
	savesInScope []bool
}

func (r *scopedConsolidationRepo) Save(ctx context.Context, oc *consolidation.OrderConsolidation) error {
	r.savesInScope = append(r.savesInScope, inScope(ctx))
	return r.OrderConsolidationRepo.Save(ctx, oc)
}

func allTrue(bs []bool) bool {
	if len(bs) == 0 {
		return false
	}
	for _, b := range bs {
		if !b {
			return false
		}
	}
	return true
}

func assertOneCommittedScope(t *testing.T, uow *recordingUnitOfWork, pub *scopedPublisher, saves []bool) {
	t.Helper()
	if uow.opened != 1 || uow.committed != 1 || uow.rolledBack != 0 {
		t.Fatalf("expected exactly one committed scope, got opened=%d committed=%d rolledBack=%d", uow.opened, uow.committed, uow.rolledBack)
	}
	if !pub.allInScope() {
		t.Fatalf("expected every Publish inside the unit of work, got %v", pub.inScope)
	}
	if !allTrue(saves) {
		t.Fatalf("expected every Save inside the unit of work, got %v", saves)
	}
}

// --- CreateTask ---

func TestCreateTask_SaveAndPublishRunInsideOneUnitOfWork(t *testing.T) {
	tasks := &scopedTaskRepo{TaskRepo: memory.NewTaskRepo()}
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := &usecases.CreateTask{Tasks: tasks, Publisher: pub, Clock: memory.NewFixedClock(epoch), NewId: idSeq("t"), UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, tasks.savesInScope)
}

func TestCreateTask_PublishFailure_RollsBackTheUnitOfWork(t *testing.T) {
	tasks := &scopedTaskRepo{TaskRepo: memory.NewTaskRepo()}
	pub := &scopedPublisher{err: errors.New("outbox insert failed")}
	uow := &recordingUnitOfWork{}
	uc := &usecases.CreateTask{Tasks: tasks, Publisher: pub, Clock: memory.NewFixedClock(epoch), NewId: idSeq("t"), UnitOfWork: uow}

	_, err := uc.Execute(context.Background(), task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	if err == nil || err.Error() != "outbox insert failed" {
		t.Fatalf("expected the publish error to propagate, got %v", err)
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected the scope to roll back, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}

func TestCreateTask_UnitOfWorkBeginFailure_Propagates(t *testing.T) {
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{beginErr: errors.New("begin failed")}
	uc := &usecases.CreateTask{Tasks: memory.NewTaskRepo(), Publisher: pub, Clock: memory.NewFixedClock(epoch), NewId: idSeq("t"), UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false); err == nil || err.Error() != "begin failed" {
		t.Fatalf("expected begin error, got %v", err)
	}
	if len(pub.inScope) != 0 {
		t.Fatal("expected nothing published when the unit of work cannot begin")
	}
}

func TestCreateTask_NilUnitOfWork_StillSavesAndPublishes(t *testing.T) {
	tasks := &scopedTaskRepo{TaskRepo: memory.NewTaskRepo()}
	pub := &scopedPublisher{}
	uc := &usecases.CreateTask{Tasks: tasks, Publisher: pub, Clock: memory.NewFixedClock(epoch), NewId: idSeq("t")}

	tk, err := uc.Execute(context.Background(), task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.inScope) != 1 || pub.inScope[0] || len(tasks.savesInScope) != 1 || tasks.savesInScope[0] {
		t.Fatalf("expected one Save and one Publish outside any scope, got saves=%v publishes=%v", tasks.savesInScope, pub.inScope)
	}
	if found, _ := tasks.FindById(context.Background(), tk.Id()); found == nil {
		t.Fatal("task should have been persisted without a unit of work")
	}
}

// --- ClaimNext ---

func claimHarness(t *testing.T) (*scopedTaskRepo, *memory.StationRepo, *memory.FixedClock) {
	t.Helper()
	ctx := context.Background()
	tasks := &scopedTaskRepo{TaskRepo: memory.NewTaskRepo()}
	stations := memory.NewStationRepo()
	clock := memory.NewFixedClock(epoch)
	create := &usecases.CreateTask{Tasks: tasks.TaskRepo, Publisher: &scopedPublisher{}, Clock: clock, NewId: idSeq("t")}
	if _, err := create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false); err != nil {
		t.Fatalf("setup: %v", err)
	}
	_ = stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	return tasks, stations, clock
}

func TestClaimNext_SaveAndPublishRunInsideOneUnitOfWork(t *testing.T) {
	tasks, stations, clock := claimHarness(t)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := &usecases.ClaimNext{Tasks: tasks, Stations: stations, Publisher: pub, Clock: clock, UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), "s1", task.Pick); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, tasks.savesInScope)
}

func TestClaimNext_PublishFailure_RollsBack(t *testing.T) {
	tasks, stations, clock := claimHarness(t)
	uow := &recordingUnitOfWork{}
	uc := &usecases.ClaimNext{Tasks: tasks, Stations: stations, Publisher: &scopedPublisher{err: errors.New("boom")}, Clock: clock, UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), "s1", task.Pick); err == nil {
		t.Fatal("expected the publish error to propagate")
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected a rollback, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}

func TestClaimNext_NothingClaimable_OpensNoUnitOfWork(t *testing.T) {
	stations := memory.NewStationRepo()
	_ = stations.Save(context.Background(), station.New("s1", shared.NewCapabilitySet("pick")))
	uow := &recordingUnitOfWork{}
	uc := &usecases.ClaimNext{Tasks: memory.NewTaskRepo(), Stations: stations, Publisher: &scopedPublisher{}, Clock: memory.NewFixedClock(epoch), UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), "s1", task.Pick); !errors.Is(err, usecases.ErrNoClaimableTask) {
		t.Fatalf("expected ErrNoClaimableTask, got %v", err)
	}
	if uow.opened != 0 {
		t.Fatalf("a no-op claim must not open a unit of work, got opened=%d", uow.opened)
	}
}

// --- CompleteTask ---

func completeHarness(t *testing.T) (*scopedTaskRepo, shared.TaskId, *memory.FixedClock) {
	t.Helper()
	tasks, stations, clock := claimHarness(t)
	claim := &usecases.ClaimNext{Tasks: tasks.TaskRepo, Stations: stations, Publisher: &scopedPublisher{}, Clock: clock}
	claimed, err := claim.Execute(context.Background(), "s1", task.Pick)
	if err != nil {
		t.Fatalf("setup claim: %v", err)
	}
	tasks.savesInScope = nil
	return tasks, claimed.Id(), clock
}

func TestCompleteTask_SaveAndPublishRunInsideOneUnitOfWork(t *testing.T) {
	tasks, id, clock := completeHarness(t)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := &usecases.CompleteTask{Tasks: tasks, Publisher: pub, Clock: clock, UnitOfWork: uow}

	if err := uc.Execute(context.Background(), id, "s1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, tasks.savesInScope)
}

func TestCompleteTask_PublishFailure_RollsBack(t *testing.T) {
	tasks, id, clock := completeHarness(t)
	uow := &recordingUnitOfWork{}
	uc := &usecases.CompleteTask{Tasks: tasks, Publisher: &scopedPublisher{err: errors.New("boom")}, Clock: clock, UnitOfWork: uow}

	if err := uc.Execute(context.Background(), id, "s1"); err == nil {
		t.Fatal("expected the publish error to propagate")
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected a rollback, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}

// --- ExpireLeases ---

func TestExpireLeases_EachFreedTaskGetsItsOwnCommittedScope(t *testing.T) {
	ctx := context.Background()
	tasks := &scopedTaskRepo{TaskRepo: memory.NewTaskRepo()}
	stations := memory.NewStationRepo()
	clock := memory.NewFixedClock(epoch)
	create := &usecases.CreateTask{Tasks: tasks.TaskRepo, Publisher: &scopedPublisher{}, Clock: clock, NewId: idSeq("t")}
	for i := 0; i < 2; i++ {
		_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
		_ = stations.Save(ctx, station.New(shared.StationId("s"+string(rune('1'+i))), shared.NewCapabilitySet("pick")))
		claim := &usecases.ClaimNext{Tasks: tasks.TaskRepo, Stations: stations, Publisher: &scopedPublisher{}, Clock: clock, LeaseDuration: time.Minute}
		if _, err := claim.Execute(ctx, shared.StationId("s"+string(rune('1'+i))), task.Pick); err != nil {
			t.Fatalf("setup claim %d: %v", i, err)
		}
	}
	clock.Advance(2 * time.Minute)

	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	sweep := &usecases.ExpireLeases{Tasks: tasks, Publisher: pub, Clock: clock, UnitOfWork: uow}
	freed, err := sweep.Execute(ctx)
	if err != nil || freed != 2 {
		t.Fatalf("expected 2 freed, got %d err=%v", freed, err)
	}
	if uow.opened != 2 || uow.committed != 2 {
		t.Fatalf("expected one committed scope per freed task, got opened=%d committed=%d", uow.opened, uow.committed)
	}
	if !pub.allInScope() || !allTrue(tasks.savesInScope) || len(tasks.savesInScope) != 2 {
		t.Fatalf("expected every Save/Publish inside a scope, saves=%v publishes=%v", tasks.savesInScope, pub.inScope)
	}
}

func TestExpireLeases_PublishFailure_RollsBackThatTask(t *testing.T) {
	tasks, _, clock := claimHarness(t)
	claim := &usecases.ClaimNext{Tasks: tasks.TaskRepo, Stations: mustStations(t, "s1"), Publisher: &scopedPublisher{}, Clock: clock, LeaseDuration: time.Minute}
	if _, err := claim.Execute(context.Background(), "s1", task.Pick); err != nil {
		t.Fatalf("setup: %v", err)
	}
	clock.Advance(2 * time.Minute)

	uow := &recordingUnitOfWork{}
	sweep := &usecases.ExpireLeases{Tasks: tasks, Publisher: &scopedPublisher{err: errors.New("boom")}, Clock: clock, UnitOfWork: uow}
	if _, err := sweep.Execute(context.Background()); err == nil {
		t.Fatal("expected the publish error to propagate")
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected a rollback, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}

func mustStations(t *testing.T, ids ...string) *memory.StationRepo {
	t.Helper()
	stations := memory.NewStationRepo()
	for _, id := range ids {
		_ = stations.Save(context.Background(), station.New(shared.StationId(id), shared.NewCapabilitySet("pick", "pack")))
	}
	return stations
}

// --- SweepCPTMisses ---

func TestSweepCPTMisses_EachReportedTaskGetsItsOwnCommittedScope(t *testing.T) {
	ctx := context.Background()
	tasks := memory.NewTaskRepo()
	clock := memory.NewFixedClock(epoch)
	create := &usecases.CreateTask{Tasks: tasks, Publisher: &scopedPublisher{}, Clock: clock, NewId: idSeq("t")}
	for i := 0; i < 2; i++ {
		_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Minute)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	}
	clock.Advance(2 * time.Minute)

	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	sweep := &usecases.SweepCPTMisses{Tasks: tasks, Publisher: pub, Clock: clock, UnitOfWork: uow}
	reported, err := sweep.Execute(ctx)
	if err != nil || reported != 2 {
		t.Fatalf("expected 2 reported, got %d err=%v", reported, err)
	}
	if uow.opened != 2 || uow.committed != 2 {
		t.Fatalf("expected one committed scope per reported task, got opened=%d committed=%d", uow.opened, uow.committed)
	}
	if !pub.allInScope() {
		t.Fatalf("expected every Publish inside a scope, publishes=%v", pub.inScope)
	}
}

func TestSweepCPTMisses_PublishFailure_RollsBackThatTask(t *testing.T) {
	ctx := context.Background()
	tasks := memory.NewTaskRepo()
	clock := memory.NewFixedClock(epoch)
	create := &usecases.CreateTask{Tasks: tasks, Publisher: &scopedPublisher{}, Clock: clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Minute)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	clock.Advance(2 * time.Minute)

	uow := &recordingUnitOfWork{}
	sweep := &usecases.SweepCPTMisses{Tasks: tasks, Publisher: &scopedPublisher{err: errors.New("boom")}, Clock: clock, UnitOfWork: uow}
	if _, err := sweep.Execute(ctx); err == nil {
		t.Fatal("expected the publish error to propagate")
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected a rollback, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}

// --- SealPackage ---

func sealHarness(t *testing.T) (*scopedTaskRepo, *scopedPackageRepo, shared.TaskId, *memory.FixedClock) {
	t.Helper()
	ctx := context.Background()
	tasks := &scopedTaskRepo{TaskRepo: memory.NewTaskRepo()}
	packages := &scopedPackageRepo{PackageRepo: memory.NewPackageRepo()}
	clock := memory.NewFixedClock(epoch)
	create := &usecases.CreateTask{Tasks: tasks.TaskRepo, Publisher: &scopedPublisher{}, Clock: clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	claim := &usecases.ClaimNext{Tasks: tasks.TaskRepo, Stations: mustStations(t, "s1"), Publisher: &scopedPublisher{}, Clock: clock}
	claimed, err := claim.Execute(ctx, "s1", task.Pack)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	return tasks, packages, claimed.Id(), clock
}

func TestSealPackage_SaveAndPublishRunInsideOneUnitOfWork(t *testing.T) {
	_, packages, id, clock := sealHarness(t)
	tasks := packagesTasks(t, id)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := &usecases.SealPackage{Tasks: tasks, Packages: packages, Publisher: pub, Clock: clock, NewId: func() shared.PackageId { return "p1" }, UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), id, "s1", []string{"sku-1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, packages.savesInScope)
}

// packagesTasks rebuilds a task repo holding a Pack task claimed by s1
// under id, for SealPackage tests that only care about the package side.
func packagesTasks(t *testing.T, id shared.TaskId) *memory.TaskRepo {
	t.Helper()
	tasks := memory.NewTaskRepo()
	tk := task.New(id, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	if err := tk.Claim("s1", shared.NewCapabilitySet("pack"), epoch, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	_ = tasks.Save(context.Background(), tk)
	return tasks
}

func TestSealPackage_PublishFailure_RollsBack(t *testing.T) {
	_, packages, id, clock := sealHarness(t)
	uow := &recordingUnitOfWork{}
	uc := &usecases.SealPackage{Tasks: packagesTasks(t, id), Packages: packages, Publisher: &scopedPublisher{err: errors.New("boom")}, Clock: clock, NewId: func() shared.PackageId { return "p1" }, UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), id, "s1", []string{"sku-1"}); err == nil {
		t.Fatal("expected the publish error to propagate")
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected a rollback, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}

// --- RunSlam ---

func slamHarness(t *testing.T) (*scopedPackageRepo, shared.PackageId) {
	t.Helper()
	packages := &scopedPackageRepo{PackageRepo: memory.NewPackageRepo()}
	p := pack.New("p1", "order-1", "task-1", false, false)
	if err := p.ScanItem("sku-1"); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if err := p.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	_ = packages.PackageRepo.Save(context.Background(), p)
	return packages, p.Id()
}

func TestRunSlam_LabelApplied_SaveAndPublishRunInsideOneUnitOfWork(t *testing.T) {
	packages, id := slamHarness(t)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := &usecases.RunSlam{Packages: packages, Publisher: pub, Clock: memory.NewFixedClock(epoch), UnitOfWork: uow}

	if err := uc.Execute(context.Background(), id, 2.0, 2.0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, packages.savesInScope)
	if len(pub.names) != 2 || pub.names[0] != "LabelApplied" || pub.names[1] != "PackageManifested" {
		t.Fatalf("expected LabelApplied+PackageManifested, got %v", pub.names)
	}
}

func TestRunSlam_Diverted_BothEventsAndSaveRunInsideOneUnitOfWork(t *testing.T) {
	packages, id := slamHarness(t)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := &usecases.RunSlam{Packages: packages, Publisher: pub, Clock: memory.NewFixedClock(epoch), UnitOfWork: uow}

	if err := uc.Execute(context.Background(), id, 100.0, 2.0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, packages.savesInScope)
	if len(pub.names) != 2 || pub.names[0] != "WeightDiscrepancyDetected" || pub.names[1] != "PackageDiverted" {
		t.Fatalf("expected WeightDiscrepancyDetected+PackageDiverted, got %v", pub.names)
	}
}

func TestRunSlam_PublishFailure_RollsBack(t *testing.T) {
	packages, id := slamHarness(t)
	uow := &recordingUnitOfWork{}
	uc := &usecases.RunSlam{Packages: packages, Publisher: &scopedPublisher{err: errors.New("boom")}, Clock: memory.NewFixedClock(epoch), UnitOfWork: uow}

	if err := uc.Execute(context.Background(), id, 2.0, 2.0); err == nil {
		t.Fatal("expected the publish error to propagate")
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected a rollback, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}

// --- ArriveAtRebin (nested CreateTask joins the outer scope) ---

func rebinHarness(uow *recordingUnitOfWork, pub *scopedPublisher) (*usecases.ArriveAtRebin, *scopedConsolidationRepo, *scopedTaskRepo) {
	tasks := &scopedTaskRepo{TaskRepo: memory.NewTaskRepo()}
	consolidations := &scopedConsolidationRepo{OrderConsolidationRepo: memory.NewOrderConsolidationRepo()}
	clock := memory.NewFixedClock(epoch)
	createTask := &usecases.CreateTask{Tasks: tasks, Publisher: pub, Clock: clock, NewId: idSeq("pack-t"), UnitOfWork: uow}
	uc := &usecases.ArriveAtRebin{Consolidations: consolidations, CreateTask: createTask, Publisher: pub, Clock: clock, UnitOfWork: uow}
	return uc, consolidations, tasks
}

func TestArriveAtRebin_PartialArrival_SaveThenPublishInsideOneUnitOfWork(t *testing.T) {
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc, consolidations, tasks := rebinHarness(uow, pub)

	if err := uc.Execute(context.Background(), "order-1", "line-1", []string{"line-1", "line-2"}, shared.NewCPT(epoch.Add(time.Hour)), shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, consolidations.savesInScope)
	if len(pub.names) != 1 || pub.names[0] != "ItemArrivedAtRebin" {
		t.Fatalf("expected only ItemArrivedAtRebin, got %v", pub.names)
	}
	if len(tasks.savesInScope) != 0 {
		t.Fatal("no PACK task should be created before consolidation completes")
	}
}

func TestArriveAtRebin_Completion_NestedCreateTaskJoinsTheOuterScope(t *testing.T) {
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc, consolidations, tasks := rebinHarness(uow, pub)
	cpt := shared.NewCPT(epoch.Add(time.Hour))

	if err := uc.Execute(context.Background(), "order-1", "line-1", []string{"line-1"}, cpt, shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// One scope in total: the nested CreateTask must have joined it, not
	// opened a second.
	if uow.opened != 1 || uow.committed != 1 {
		t.Fatalf("expected exactly one scope shared with the nested CreateTask, got opened=%d committed=%d", uow.opened, uow.committed)
	}
	if !allTrue(consolidations.savesInScope) || !allTrue(tasks.savesInScope) || !pub.allInScope() {
		t.Fatalf("every write must be inside the scope: consolidation=%v task=%v publish=%v", consolidations.savesInScope, tasks.savesInScope, pub.inScope)
	}
	if len(pub.names) != 3 || pub.names[0] != "ItemArrivedAtRebin" || pub.names[1] != "TaskCreated" || pub.names[2] != "OrderConsolidated" {
		t.Fatalf("expected ItemArrivedAtRebin, TaskCreated, OrderConsolidated in order, got %v", pub.names)
	}
}

func TestArriveAtRebin_PublishFailure_RollsBackEverything(t *testing.T) {
	pub := &scopedPublisher{err: errors.New("boom")}
	uow := &recordingUnitOfWork{}
	uc, _, _ := rebinHarness(uow, pub)

	if err := uc.Execute(context.Background(), "order-1", "line-1", []string{"line-1"}, shared.NewCPT(epoch.Add(time.Hour)), shared.NewCapabilitySet("pack"), false, false); err == nil {
		t.Fatal("expected the publish error to propagate")
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected a rollback, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}

func TestArriveAtRebin_AlreadyComplete_OpensNoUnitOfWork(t *testing.T) {
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc, _, _ := rebinHarness(uow, pub)
	cpt := shared.NewCPT(epoch.Add(time.Hour))
	if err := uc.Execute(context.Background(), "order-1", "line-1", []string{"line-1"}, cpt, shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := uc.Execute(context.Background(), "order-1", "line-1", []string{"line-1"}, cpt, shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("redelivered arrival: %v", err)
	}
	if uow.opened != 1 {
		t.Fatalf("an idempotent repeat must not open a second unit of work, got opened=%d", uow.opened)
	}
}
