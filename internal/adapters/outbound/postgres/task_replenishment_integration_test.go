//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

const (
	repSKU     = "SKU-1"
	repFromBin = "WH1-RSV-AMB-B02-03-02-C"
	repToBin   = "WH1-FWD-AMB-A01-01-01-A"
)

func replenishMove(t *testing.T) task.ReplenishmentMove {
	t.Helper()
	m, err := task.NewReplenishmentMove("req-9", repSKU, repFromBin, repToBin, 24, "URGENT")
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	return m
}

// TestTaskRepo_ReplenishmentRoundTrip proves migration 0020 (ADR 0050): a
// REPLENISH task's planned move round-trips on every read path, the executed
// result recorded by CompleteReplenishment round-trips once the completion is
// saved, and a non-REPLENISH task rehydrates with neither.
func TestTaskRepo_ReplenishmentRoundTrip(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	cpt := shared.NewCPT(now.Add(time.Hour))

	move := replenishMove(t)
	rep := task.NewReplenishmentTask("integration-task-replenish", cpt, "replenish-req-9", shared.NewCapabilitySet("replenish"), move)
	plain := task.New("integration-task-not-replenish", task.Pick, cpt, "wu-plain", shared.NewCapabilitySet("pick"), false, false)
	for _, tk := range []*task.Task{rep, plain} {
		if err := repo.Save(ctx, tk); err != nil {
			t.Fatalf("save %s: %v", tk.Id(), err)
		}
	}

	got, err := repo.FindById(ctx, "integration-task-replenish")
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if got.Type() != task.Replenish || got.Replenishment() == nil || *got.Replenishment() != move {
		t.Fatalf("round-trip: type %s move %+v, want REPLENISH %+v", got.Type(), got.Replenishment(), move)
	}
	if got.ReplenishmentResult() != nil || got.Relocation() != nil {
		t.Fatalf("a pending REPLENISH task has no result and no relocation, got %+v / %+v", got.ReplenishmentResult(), got.Relocation())
	}

	// Bulk read path (scanTasks) carries the move too.
	list, err := repo.FindClaimableByType(ctx, task.Replenish, now)
	if err != nil || len(list) != 1 || list[0].Replenishment() == nil || list[0].Replenishment().Ref() != "req-9" {
		t.Fatalf("FindClaimableByType = %v err=%v, want one task with request req-9", list, err)
	}

	// Claim, complete with the operator's count (below the plan), save, reload.
	if err := got.Claim("s1", shared.NewCapabilitySet("replenish"), now, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := got.CompleteReplenishment("s1", now.Add(time.Second), task.RelocationConfirmation{Quantity: 20, FromBin: repFromBin}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := repo.Save(ctx, got); err != nil {
		t.Fatalf("save completed: %v", err)
	}
	done, err := repo.FindById(ctx, "integration-task-replenish")
	if err != nil || done == nil {
		t.Fatalf("find completed: %v %v", done, err)
	}
	want := task.ReplenishmentResult{Quantity: 20, FromBin: repFromBin, ToBin: repToBin}
	if done.Status() != task.Completed || done.ReplenishmentResult() == nil || *done.ReplenishmentResult() != want {
		t.Fatalf("completed round-trip: %s %+v, want COMPLETED %+v", done.Status(), done.ReplenishmentResult(), want)
	}
	if *done.Replenishment() != move {
		t.Fatalf("the planned move must survive the completion, got %+v", done.Replenishment())
	}

	other, err := repo.FindById(ctx, "integration-task-not-replenish")
	if err != nil || other == nil {
		t.Fatalf("find plain: %v %v", other, err)
	}
	if other.Replenishment() != nil || other.ReplenishmentResult() != nil {
		t.Fatalf("a PICK task carries no replenishment, got %+v / %+v", other.Replenishment(), other.ReplenishmentResult())
	}
}

// TestTaskRepo_ReplenishmentColumnsAreNullForOtherTasks pins the additive
// migration's storage rule: no replenishment column is written for any other
// task type, a RELOCATE task included (NULL, never "" or 0).
func TestTaskRepo_ReplenishmentColumnsAreNullForOtherTasks(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	cpt := shared.NewCPT(time.Now().Add(time.Hour))
	reloc, err := task.NewRelocationMove("plan-7", 0, "Assign", "SKU-1", "", "FWD-B")
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	for _, tk := range []*task.Task{
		task.New("integration-task-null-rep-pack", task.Pack, cpt, "wu-null", shared.NewCapabilitySet("pack"), false, false),
		task.NewRelocationTask("integration-task-null-rep-reloc", cpt, "slot-plan-7-move-0", shared.NewCapabilitySet("relocate"), reloc),
	} {
		if err := repo.Save(ctx, tk); err != nil {
			t.Fatalf("save: %v", err)
		}
		var nonNull int
		err := pool.QueryRow(ctx, `
			SELECT num_nonnulls(replenishment_request_id, replenishment_sku, replenishment_from_bin, replenishment_to_bin,
			       replenishment_quantity, replenishment_priority, replenished_quantity, replenished_from_bin, replenished_to_bin)
			FROM tasks WHERE id = $1`, string(tk.Id())).Scan(&nonNull)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if nonNull != 0 {
			t.Fatalf("%d replenishment columns are non-NULL on a %s task, want 0", nonNull, tk.Type())
		}
	}
}

// TestMigration0020_UpAndDown applies migration 0020 on top of a database at
// 0019 holding existing tasks, proves those rows survive with NULL
// replenishment columns and that a REPLENISH row can be stored, then steps
// down one migration and proves the columns are gone and the older rows
// remain, and finally steps back up. It runs on its own throwaway container,
// never the shared one the other tests assume is at the latest version.
func TestMigration0020_UpAndDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("fulfillment_execution"),
		tcpostgres.WithUsername("fulfillment"),
		tcpostgres.WithPassword("fulfillment"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	m, err := migrate.New("file://../../../../migrations", url)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	t.Cleanup(func() { _, _ = m.Close() })
	if err := m.Migrate(19); err != nil {
		t.Fatalf("migrate to 0019: %v", err)
	}

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	insertPick := func(id string) {
		t.Helper()
		_, err := conn.Exec(ctx, `INSERT INTO tasks (id, task_type, status, cpt, order_ref, required_capabilities) VALUES ($1, 'PICK', 'PENDING', now() + interval '1 hour', 'wu-old', ARRAY['pick'])`, id)
		if err != nil {
			t.Fatalf("insert pre-migration task: %v", err)
		}
	}
	insertPick("old-task-1")

	columns := func() int {
		t.Helper()
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'tasks' AND (column_name LIKE 'replenishment\_%' OR column_name LIKE 'replenished\_%')`).Scan(&n); err != nil {
			t.Fatalf("count columns: %v", err)
		}
		return n
	}
	if got := columns(); got != 0 {
		t.Fatalf("at 0019 there are %d replenishment columns, want 0", got)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate up: %v", err)
	}
	if got := columns(); got != 9 {
		t.Fatalf("after 0020 there are %d replenishment columns, want 9", got)
	}
	var nonNull int
	if err := conn.QueryRow(ctx, `SELECT num_nonnulls(replenishment_request_id, replenishment_sku, replenishment_quantity, replenished_quantity) FROM tasks WHERE id = 'old-task-1'`).Scan(&nonNull); err != nil || nonNull != 0 {
		t.Fatalf("a pre-migration row must read NULL replenishment columns: nonNull=%d err=%v", nonNull, err)
	}

	// The migrated schema stores a REPLENISH task through the repository.
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	repo := postgres.NewTaskRepo(pool)
	rep := task.NewReplenishmentTask("new-replenish-1", shared.NewCPT(time.Now().Add(time.Hour)), "replenish-req-9", shared.NewCapabilitySet("replenish"), replenishMove(t))
	if err := repo.Save(ctx, rep); err != nil {
		t.Fatalf("save on the migrated schema: %v", err)
	}
	if err := m.Steps(-1); err != nil {
		t.Fatalf("migrate down one step: %v", err)
	}
	if got := columns(); got != 0 {
		t.Fatalf("after the down step there are %d replenishment columns, want 0", got)
	}
	var survivors int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE id IN ('old-task-1', 'new-replenish-1')`).Scan(&survivors); err != nil || survivors != 2 {
		t.Fatalf("the down step must keep the task rows, got %d err=%v", survivors, err)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("migrate up again: %v", err)
	}
	if got := columns(); got != 9 {
		t.Fatalf("after re-applying 0020 there are %d replenishment columns, want 9", got)
	}
}

func createReplenish(t *testing.T, s *outboxStack, pub usecasesPublisher) (*task.Task, error) {
	t.Helper()
	uc := &usecases.CreateTask{Tasks: s.tasks, Publisher: pub, Clock: s.clock, NewId: taskIds("rp"), UnitOfWork: s.uow}
	return uc.ExecuteReplenishment(context.Background(), shared.NewCPT(s.clock.t.Add(time.Hour)), "replenish-req-9", shared.NewCapabilitySet("replenish", "forklift"), replenishMove(t))
}

func claimReplenish(s *outboxStack) (*task.Task, error) {
	claim := &usecases.ClaimNext{Tasks: s.tasks, Stations: s.stations, Publisher: s.pub, Clock: s.clock, UnitOfWork: s.uow, LeaseDuration: time.Minute}
	return claim.Execute(context.Background(), "s1", task.Replenish)
}

// Release: the REPLENISH task row, its analytics TaskCreated and its
// integration TaskAvailable (both bins) commit together.
func TestReplenish_Release_CommitsTaskAndTaskAvailableTogether(t *testing.T) {
	s := newOutboxStack(t)
	tk, err := createReplenish(t, s, s.pub)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := integrationRows(t, s, typeAvailable); got != 1 {
		t.Fatalf("expected 1 integration TaskAvailable row, got %d", got)
	}
	var value []byte
	if err := s.pool.QueryRow(context.Background(), "SELECT value FROM outbox_events WHERE topic = $1 AND event_type = $2", "warehouse.fulfillment.events", typeAvailable).Scan(&value); err != nil {
		t.Fatalf("read row: %v", err)
	}
	for _, want := range []string{
		`"task_id":"` + string(tk.Id()) + `"`, `"task_type":"REPLENISH"`, `"site_id":"WH1"`,
		`"from_location":"` + repFromBin + `"`, `"to_location":"` + repToBin + `"`,
		`"required_capabilities":["forklift","replenish"]`,
	} {
		if !bytes.Contains(value, []byte(want)) {
			t.Errorf("TaskAvailable payload lacks %s: %s", want, value)
		}
	}
}

// Atomicity of the release: a failure after the outbox rows were inserted
// leaves neither the REPLENISH task nor any row.
func TestReplenish_Release_RollsBackTaskAndEventsTogether(t *testing.T) {
	s := newOutboxStack(t)
	if _, err := createReplenish(t, s, insertThenFail{inner: s.pub}); err == nil {
		t.Fatal("expected the forced failure")
	}
	if found, err := s.tasks.FindById(context.Background(), "rp1"); err != nil || found != nil {
		t.Fatalf("the task row survived the rollback: %v err=%v", found, err)
	}
	if got := countOutbox(t, s.pool, "true"); got != 0 {
		t.Fatalf("outbox rows survived the rollback: %d", got)
	}
}

func seedClaimedReplenish(t *testing.T, s *outboxStack) *task.Task {
	t.Helper()
	if err := s.stations.Save(context.Background(), station.New("s1", shared.NewCapabilitySet("replenish", "forklift"))); err != nil {
		t.Fatalf("station: %v", err)
	}
	if _, err := createReplenish(t, s, s.pub); err != nil {
		t.Fatalf("create: %v", err)
	}
	claimed, err := claimReplenish(s)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	return claimed
}

func completeReplenish(s *outboxStack, pub usecasesPublisher, id shared.TaskId) error {
	complete := &usecases.CompleteTask{Tasks: s.tasks, Publisher: pub, Clock: s.clock, UnitOfWork: s.uow}
	return complete.ExecuteWith(context.Background(), id, "s1", task.RelocationConfirmation{Quantity: 20})
}

// Completion: the executed result and the integration TaskCompleted (with
// request_ref and the confirmed quantity) commit together.
func TestReplenish_Complete_CommitsResultAndTaskCompletedTogether(t *testing.T) {
	s := newOutboxStack(t)
	claimed := seedClaimedReplenish(t, s)
	if err := completeReplenish(s, s.pub, claimed.Id()); err != nil {
		t.Fatalf("complete: %v", err)
	}
	done, err := s.tasks.FindById(context.Background(), claimed.Id())
	if err != nil || done == nil || done.Status() != task.Completed || done.ReplenishmentResult() == nil || done.ReplenishmentResult().Quantity != 20 {
		t.Fatalf("completed task = %+v err=%v", done, err)
	}
	var value []byte
	if err := s.pool.QueryRow(context.Background(), "SELECT value FROM outbox_events WHERE topic = $1 AND event_type = $2", "warehouse.fulfillment.events", "com.warehouse.wes.fulfillment-execution.task.TaskCompleted").Scan(&value); err != nil {
		t.Fatalf("read TaskCompleted row: %v", err)
	}
	for _, want := range []string{
		`"task_type":"REPLENISH"`, `"sku":"` + repSKU + `"`, `"from_bin":"` + repFromBin + `"`,
		`"to_bin":"` + repToBin + `"`, `"quantity":20`, `"request_ref":"req-9"`,
	} {
		if !bytes.Contains(value, []byte(want)) {
			t.Errorf("TaskCompleted payload lacks %s: %s", want, value)
		}
	}
	if bytes.Contains(value, []byte("relocation_ref")) {
		t.Errorf("a REPLENISH completion must not carry relocation_ref: %s", value)
	}
}

// Atomicity of the completion: when the publish fails after the rows were
// written, the task is still Claimed with no result and no TaskCompleted row.
func TestReplenish_Complete_RollsBackResultAndEventsTogether(t *testing.T) {
	s := newOutboxStack(t)
	claimed := seedClaimedReplenish(t, s)
	before := countOutbox(t, s.pool, "true")
	if err := completeReplenish(s, insertThenFail{inner: s.pub}, claimed.Id()); err == nil {
		t.Fatal("expected the forced failure")
	}
	found, err := s.tasks.FindById(context.Background(), claimed.Id())
	if err != nil || found == nil || found.Status() != task.Claimed || found.ReplenishmentResult() != nil {
		t.Fatalf("the completion survived the rollback: %+v err=%v", found, err)
	}
	if got := countOutbox(t, s.pool, "true"); got != before {
		t.Fatalf("outbox rows survived the rollback: %d, want %d", got, before)
	}
}

// Replay: completing again is the existing already-completed error and writes
// nothing more.
func TestReplenish_Complete_ReplayAddsNothing(t *testing.T) {
	s := newOutboxStack(t)
	claimed := seedClaimedReplenish(t, s)
	if err := completeReplenish(s, s.pub, claimed.Id()); err != nil {
		t.Fatalf("complete: %v", err)
	}
	rows := countOutbox(t, s.pool, "true")
	if err := completeReplenish(s, s.pub, claimed.Id()); !errors.Is(err, task.ErrAlreadyCompleted) {
		t.Fatalf("replayed completion = %v, want ErrAlreadyCompleted", err)
	}
	if got := countOutbox(t, s.pool, "true"); got != rows {
		t.Fatalf("a replayed completion wrote %d new outbox rows", got-rows)
	}
}
