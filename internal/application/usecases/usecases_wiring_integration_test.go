//go:build integration

// Package usecases_test proves the task-lifecycle write use cases against a
// REAL Postgres (testcontainers): the real postgres repos, the real
// UnitOfWork, and a buffering publisher, wired exactly like the composition
// root in cmd/execution. These are integration tests in the fleet's sense:
// they execute the real cross-component contracts (claim's compare-and-set
// at-most-once assignment, the ownership-guarded completion, the lease
// expiry sweep, the atomic Publish-inside-UoW bracket) against real
// infrastructure, with no in-memory repo fakes anywhere in the path.
//
// The package boots its own throwaway Postgres via testcontainers in
// TestMain: one container for the whole package, migrated once into a
// template database, one private database per test (CREATE DATABASE ...
// WITH TEMPLATE, a file-level copy: milliseconds). Never an external
// DATABASE_URL, never t.Skip.
package usecases_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// One Postgres container serves the whole package (containers are slow to
// boot). TestMain starts it, applies every migration ONCE into a template
// database, and each test then gets its own database cloned from that
// template (CREATE DATABASE ... TEMPLATE, a file-level copy: milliseconds).
// Isolation is therefore total — no TRUNCATE bookkeeping and no dependence
// on test order. Never an external DATABASE_URL, never t.Skip.
const templateDB = "usecases_migrated_template"

var (
	sharedBaseURL string // connection URL of the container's default database
	dbSeq         atomic.Uint64
)

// TestMain owns the package-wide container lifecycle.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("fulfillment_execution"),
		tcpostgres.WithUsername("fulfillment"),
		tcpostgres.WithPassword("fulfillment"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	var err2 error
	sharedBaseURL, err2 = container.ConnectionString(ctx, "sslmode=disable")
	if err2 != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err2)
		return 1
	}

	// Migrate a template database once; every test clones it.
	if err := createDatabase(ctx, sharedBaseURL, templateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	migrations, err := filepath.Abs(filepath.Join("..", "..", "..", "migrations"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve migrations dir: %v\n", err)
		return 1
	}
	if err := postgres.Migrate(withDB(sharedBaseURL, templateDB), migrations); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}

	return m.Run()
}

// withDB rewrites the path of a connection URL to the named database.
func withDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// createDatabase creates an empty database inside the shared container.
func createDatabase(ctx context.Context, baseURL, name string) error {
	conn, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// migratedDB hands the test a connection URL to its own private database,
// cloned from the migrated template. Cloning is a file-level copy, so it
// costs milliseconds and the test's writes never leak into another test.
func migratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("usecases_%d", dbSeq.Add(1))
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, sharedBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, templateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return withDB(sharedBaseURL, name)
}

// wiringEpoch is the fixed instant every wired clock starts at, so lease
// expiry and event timestamps stay deterministic.
var wiringEpoch = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

// wiredStack is the real adapter stack over a private migrated database:
// postgres repos, the real UnitOfWork, and a buffering publisher the tests
// assert on — wired exactly like cmd/execution's composition root for the
// task lifecycle. No in-memory repo fakes.
type wiredStack struct {
	pool      *pgxpool.Pool
	tasks     *postgres.TaskRepo
	stations  *postgres.StationRepo
	publisher *events.BufferedPublisher
	clock     *memory.FixedClock
	idSeq     int
	create    *usecases.CreateTask
	register  *usecases.RegisterStation
	claim     *usecases.ClaimNext
	complete  *usecases.CompleteTask
	expire    *usecases.ExpireLeases
	depth     *usecases.GetQueueDepth
}

// newWiredUsecases builds the stack over a fresh private database.
func newWiredUsecases(t *testing.T) *wiredStack {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, migratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	tasks := postgres.NewTaskRepo(pool)
	stations := postgres.NewStationRepo(pool)
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(wiringEpoch)
	uow := postgres.NewUnitOfWork(pool)

	w := &wiredStack{
		pool:      pool,
		tasks:     tasks,
		stations:  stations,
		publisher: publisher,
		clock:     clock,
	}
	w.create = &usecases.CreateTask{
		Tasks: tasks, Publisher: publisher, Clock: clock,
		NewId: w.nextId, UnitOfWork: uow,
	}
	w.register = &usecases.RegisterStation{Stations: stations, Publisher: publisher}
	w.claim = &usecases.ClaimNext{
		Tasks: tasks, Stations: stations, Publisher: publisher,
		Clock: clock, LeaseDuration: 5 * time.Minute, UnitOfWork: uow,
	}
	w.complete = &usecases.CompleteTask{
		Tasks: tasks, Publisher: publisher, Clock: clock, UnitOfWork: uow,
	}
	w.expire = &usecases.ExpireLeases{
		Tasks: tasks, Publisher: publisher, Clock: clock, UnitOfWork: uow,
	}
	w.depth = &usecases.GetQueueDepth{Tasks: tasks}
	return w
}

// nextId yields deterministic unique task ids for the wired stack.
func (w *wiredStack) nextId() shared.TaskId {
	w.idSeq++
	return shared.TaskId(fmt.Sprintf("ITCOV-WU-%d", w.idSeq))
}

// seedTask creates one pending task through the real CreateTask use case.
func (w *wiredStack) seedTask(t *testing.T, tt task.Type, orderRef string, cpt time.Time, cap string) *task.Task {
	t.Helper()
	created, err := w.create.Execute(context.Background(), tt, shared.NewCPT(cpt),
		shared.OrderRef(orderRef), shared.NewCapabilitySet(shared.Capability(cap)), false, false)
	if err != nil {
		t.Fatalf("create task %s: %v", orderRef, err)
	}
	return created
}

// countEvents reports how many published events carry the given name.
func (w *wiredStack) countEvents(name string) int {
	n := 0
	for _, e := range w.publisher.Events() {
		if e.EventName() == name {
			n++
		}
	}
	return n
}

// TestUsecases_TaskLifecycleRoundTrip drives the aggregate's full lifecycle
// — create, claim, complete — against the real repos and UnitOfWork,
// asserting both the persisted state and the published events at each step.
func TestUsecases_TaskLifecycleRoundTrip(t *testing.T) {
	w := newWiredUsecases(t)
	ctx := context.Background()

	if _, err := w.register.Execute(ctx, "itcov-station-1", []string{"pick"}, ""); err != nil {
		t.Fatalf("register station: %v", err)
	}

	// Two pending PICK tasks with different CPTs; the claim policy must
	// take the earliest-CPT one first.
	early := w.seedTask(t, task.Pick, "ITCOV-ORD-1", wiringEpoch.Add(2*time.Hour), "pick")
	late := w.seedTask(t, task.Pick, "ITCOV-ORD-2", wiringEpoch.Add(3*time.Hour), "pick")

	if depth, err := w.depth.Execute(ctx, task.Pick); err != nil || depth != 2 {
		t.Fatalf("queue depth after two creates = %d, %v; want 2", depth, err)
	}
	if n := w.countEvents("TaskCreated"); n != 2 {
		t.Fatalf("expected 2 TaskCreated events, got %d", n)
	}

	claimed, err := w.claim.Execute(ctx, "itcov-station-1", task.Pick)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Id() != early.Id() {
		t.Fatalf("claim must take the earliest-CPT task %s, got %s", early.Id(), claimed.Id())
	}
	if depth, err := w.depth.Execute(ctx, task.Pick); err != nil || depth != 1 {
		t.Fatalf("queue depth after one claim = %d, %v; want 1", depth, err)
	}
	if n := w.countEvents("TaskClaimed"); n != 1 {
		t.Fatalf("expected 1 TaskClaimed event, got %d", n)
	}

	// Completion by the owning station persists Completed and publishes.
	if err := w.complete.Execute(ctx, claimed.Id(), "itcov-station-1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	stored, err := w.tasks.FindById(ctx, claimed.Id())
	if err != nil || stored == nil {
		t.Fatalf("find completed task: %v, %v", stored, err)
	}
	if stored.Status() != task.Completed {
		t.Fatalf("expected persisted status Completed, got %s", stored.Status())
	}
	if n := w.countEvents("TaskCompleted"); n != 1 {
		t.Fatalf("expected 1 TaskCompleted event, got %d", n)
	}

	// At-most-once: a second completion is rejected and publishes nothing.
	if err := w.complete.Execute(ctx, claimed.Id(), "itcov-station-1"); err == nil {
		t.Fatal("second completion must be rejected")
	}
	if n := w.countEvents("TaskCompleted"); n != 1 {
		t.Fatalf("rejected completion must not publish, saw %d TaskCompleted", n)
	}

	// The other task is untouched by the round trip.
	other, err := w.tasks.FindById(ctx, late.Id())
	if err != nil || other == nil || other.Status() != task.Pending {
		t.Fatalf("unclaimed task must remain Pending, got %+v (%v)", other, err)
	}
}

// TestUsecases_ClaimRespectsStationCapabilities proves the capability gate
// over the real repos: a station that lacks the task's required capability
// gets no claim even though the queue is non-empty.
func TestUsecases_ClaimRespectsStationCapabilities(t *testing.T) {
	w := newWiredUsecases(t)
	ctx := context.Background()

	if _, err := w.register.Execute(ctx, "itcov-station-pack", []string{"pack"}, ""); err != nil {
		t.Fatalf("register station: %v", err)
	}
	w.seedTask(t, task.Pick, "ITCOV-ORD-CAP", wiringEpoch.Add(time.Hour), "pick")

	if _, err := w.claim.Execute(ctx, "itcov-station-pack", task.Pick); err != usecases.ErrNoClaimableTask {
		t.Fatalf("claim without the required capability must fail with ErrNoClaimableTask, got %v", err)
	}
	if n := w.countEvents("TaskClaimed"); n != 0 {
		t.Fatalf("rejected claim must publish nothing, saw %d TaskClaimed", n)
	}
}

// TestUsecases_LeaseExpiryReturnsTaskToPool drives the clock-driven
// ExpireLeases sweep against real persisted claims: a lapsed lease frees
// the task back to Pending, publishes LeaseExpired, and makes it claimable
// again.
func TestUsecases_LeaseExpiryReturnsTaskToPool(t *testing.T) {
	w := newWiredUsecases(t)
	ctx := context.Background()

	if _, err := w.register.Execute(ctx, "itcov-station-2", []string{"pick"}, ""); err != nil {
		t.Fatalf("register station: %v", err)
	}
	created := w.seedTask(t, task.Pick, "ITCOV-ORD-EXP", wiringEpoch.Add(time.Hour), "pick")

	if _, err := w.claim.Execute(ctx, "itcov-station-2", task.Pick); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Nothing to sweep while the lease is active.
	if freed, err := w.expire.Execute(ctx); err != nil || freed != 0 {
		t.Fatalf("sweep with an active lease must free 0, got %d (%v)", freed, err)
	}

	// Past the lease: the sweep frees exactly this task.
	w.clock.Advance(2 * w.claim.LeaseDuration)
	freed, err := w.expire.Execute(ctx)
	if err != nil || freed != 1 {
		t.Fatalf("sweep after expiry must free 1, got %d (%v)", freed, err)
	}
	stored, err := w.tasks.FindById(ctx, created.Id())
	if err != nil || stored == nil || stored.Status() != task.Pending {
		t.Fatalf("expired task must return to Pending, got %+v (%v)", stored, err)
	}
	if n := w.countEvents("LeaseExpired"); n != 1 {
		t.Fatalf("expected 1 LeaseExpired event, got %d", n)
	}

	// The freed task is claimable again, by the same station.
	if _, err := w.claim.Execute(ctx, "itcov-station-2", task.Pick); err != nil {
		t.Fatalf("re-claim after expiry: %v", err)
	}
}

// TestUsecases_CompleteRejectsNonOwner proves the ownership invariant over
// the real rows: a station that does not hold the claim cannot complete,
// and the failed attempt changes nothing.
func TestUsecases_CompleteRejectsNonOwner(t *testing.T) {
	w := newWiredUsecases(t)
	ctx := context.Background()

	if _, err := w.register.Execute(ctx, "itcov-owner", []string{"pick"}, ""); err != nil {
		t.Fatalf("register owner: %v", err)
	}
	if _, err := w.register.Execute(ctx, "itcov-intruder", []string{"pick"}, ""); err != nil {
		t.Fatalf("register intruder: %v", err)
	}
	created := w.seedTask(t, task.Pick, "ITCOV-ORD-OWN", wiringEpoch.Add(time.Hour), "pick")

	if _, err := w.claim.Execute(ctx, "itcov-owner", task.Pick); err != nil {
		t.Fatalf("claim: %v", err)
	}

	if err := w.complete.Execute(ctx, created.Id(), "itcov-intruder"); err == nil {
		t.Fatal("completion by a non-owning station must be rejected")
	}
	stored, err := w.tasks.FindById(ctx, created.Id())
	if err != nil || stored == nil || stored.Status() != task.Claimed {
		t.Fatalf("rejected completion must leave the task Claimed, got %+v (%v)", stored, err)
	}
	if n := w.countEvents("TaskCompleted"); n != 0 {
		t.Fatalf("rejected completion must publish nothing, saw %d TaskCompleted", n)
	}
}
