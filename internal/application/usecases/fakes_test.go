package usecases_test

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/consolidation"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// errFake is the sentinel error every fake below returns when configured to fail.
var errFake = errors.New("fake: forced failure")

// errTaskRepo wraps a memory.TaskRepo, letting individual methods be forced
// to fail so use-case error-propagation branches can be exercised.
type errTaskRepo struct {
	*memory.TaskRepo
	failSave                bool
	failFindById            bool
	failFindClaimableByType bool
	failFindAllClaimed      bool
	failFindOpenPastCPT     bool
}

func newErrTaskRepo() *errTaskRepo {
	return &errTaskRepo{TaskRepo: memory.NewTaskRepo()}
}

func (r *errTaskRepo) Save(ctx context.Context, t *task.Task) error {
	if r.failSave {
		return errFake
	}
	return r.TaskRepo.Save(ctx, t)
}

// SaveClaim honours failSave too: claiming is a save, and the existing
// ClaimNext error-propagation tests force it through failSave.
func (r *errTaskRepo) SaveClaim(ctx context.Context, t *task.Task, now time.Time) (bool, error) {
	if r.failSave {
		return false, errFake
	}
	return r.TaskRepo.SaveClaim(ctx, t, now)
}

func (r *errTaskRepo) FindById(ctx context.Context, id shared.TaskId) (*task.Task, error) {
	if r.failFindById {
		return nil, errFake
	}
	return r.TaskRepo.FindById(ctx, id)
}

func (r *errTaskRepo) FindClaimableByType(ctx context.Context, taskType task.Type, now time.Time) ([]*task.Task, error) {
	if r.failFindClaimableByType {
		return nil, errFake
	}
	return r.TaskRepo.FindClaimableByType(ctx, taskType, now)
}

func (r *errTaskRepo) FindAllClaimed(ctx context.Context) ([]*task.Task, error) {
	if r.failFindAllClaimed {
		return nil, errFake
	}
	return r.TaskRepo.FindAllClaimed(ctx)
}

func (r *errTaskRepo) FindOpenPastCPT(ctx context.Context, now time.Time) ([]*task.Task, error) {
	if r.failFindOpenPastCPT {
		return nil, errFake
	}
	return r.TaskRepo.FindOpenPastCPT(ctx, now)
}

// errStationRepo wraps a memory.StationRepo, letting individual methods be
// forced to fail.
type errStationRepo struct {
	*memory.StationRepo
	failSave     bool
	failFindById bool
}

func newErrStationRepo() *errStationRepo {
	return &errStationRepo{StationRepo: memory.NewStationRepo()}
}

func (r *errStationRepo) Save(ctx context.Context, s *station.Station) error {
	if r.failSave {
		return errFake
	}
	return r.StationRepo.Save(ctx, s)
}

func (r *errStationRepo) FindById(ctx context.Context, id shared.StationId) (*station.Station, error) {
	if r.failFindById {
		return nil, errFake
	}
	return r.StationRepo.FindById(ctx, id)
}

// errPackageRepo wraps a memory.PackageRepo, letting individual methods be
// forced to fail.
type errPackageRepo struct {
	*memory.PackageRepo
	failSave     bool
	failFindById bool
}

func newErrPackageRepo() *errPackageRepo {
	return &errPackageRepo{PackageRepo: memory.NewPackageRepo()}
}

func (r *errPackageRepo) Save(ctx context.Context, p *pack.Package) error {
	if r.failSave {
		return errFake
	}
	return r.PackageRepo.Save(ctx, p)
}

func (r *errPackageRepo) FindById(ctx context.Context, id shared.PackageId) (*pack.Package, error) {
	if r.failFindById {
		return nil, errFake
	}
	return r.PackageRepo.FindById(ctx, id)
}

// errConsolidationRepo wraps a memory.OrderConsolidationRepo, letting
// individual methods be forced to fail so ArriveAtRebin's
// error-propagation branches can be exercised.
type errConsolidationRepo struct {
	*memory.OrderConsolidationRepo
	failSave           bool
	failFindByOrderRef bool
}

func newErrConsolidationRepo() *errConsolidationRepo {
	return &errConsolidationRepo{OrderConsolidationRepo: memory.NewOrderConsolidationRepo()}
}

func (r *errConsolidationRepo) Save(ctx context.Context, oc *consolidation.OrderConsolidation) error {
	if r.failSave {
		return errFake
	}
	return r.OrderConsolidationRepo.Save(ctx, oc)
}

func (r *errConsolidationRepo) FindByOrderRef(ctx context.Context, orderRef shared.OrderRef) (*consolidation.OrderConsolidation, error) {
	if r.failFindByOrderRef {
		return nil, errFake
	}
	return r.OrderConsolidationRepo.FindByOrderRef(ctx, orderRef)
}

// errPublisher forces Publish to fail, so use cases' event-publish error
// branches can be exercised.
type errPublisher struct {
	fail bool
}

func (p *errPublisher) Publish(_ context.Context, _ ...shared.DomainEvent) error {
	if p.fail {
		return errFake
	}
	return nil
}

// recordingMetrics is a ports.Metrics that just remembers what it was told,
// so the use cases' business-metric instrumentation can be asserted without
// an OTel pipeline.
type recordingMetrics struct {
	claimed   []task.Type
	completed []task.Type
}

func (m *recordingMetrics) TaskClaimed(_ context.Context, taskType task.Type) {
	m.claimed = append(m.claimed, taskType)
}

func (m *recordingMetrics) TaskCompleted(_ context.Context, taskType task.Type) {
	m.completed = append(m.completed, taskType)
}

// fakeClassificationLookup is a ports.ProductClassificationLookup test
// double: pre-programmed per-SKU responses, with an optional forced error
// for one SKU, so SealPackage's fail-open-per-item behaviour can be
// exercised without a real HTTP call.
type fakeClassificationLookup struct {
	bySKU   map[string]ports.ClassificationInfo
	failFor map[string]bool
	calls   []string
}

func newFakeClassificationLookup() *fakeClassificationLookup {
	return &fakeClassificationLookup{bySKU: map[string]ports.ClassificationInfo{}, failFor: map[string]bool{}}
}

func (f *fakeClassificationLookup) GetClassification(_ context.Context, sku string) (ports.ClassificationInfo, error) {
	f.calls = append(f.calls, sku)
	if f.failFor[sku] {
		return ports.ClassificationInfo{}, errFake
	}
	info, ok := f.bySKU[sku]
	if !ok {
		return ports.ClassificationInfo{Known: false}, nil
	}
	return info, nil
}
