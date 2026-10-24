//go:build integration

// Integration tests for the Idempotency-Key contract of POST
// /tasks/{id}/scans (docs/docs/adr/0055-scan-idempotency-and-claimable-break-case.md)
// against a real Postgres 16 (the package's shared testcontainers instance),
// through the REAL chi router over the real Postgres task repo, PackView and
// UnitOfWork. The floor client's outbox resends a scan with the SAME key
// after a lost response; the scan must then be answered from the stored
// outcome and never counted twice.
package http_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/packview"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

const (
	scanEachGTIN = "00012345678905"
	scanStation  = "station-scan"
)

type scanFixture struct {
	pool   *pgxpool.Pool
	router http.Handler
	tasks  *postgres.TaskRepo
}

// newScanFixture wires the real router over Postgres with a claimed PICK task
// "pick-1" planning `planned` eaches of SKU-1, and the EACH GTIN in the PackView.
func newScanFixture(t *testing.T, planned int) *scanFixture {
	t.Helper()
	pool := idempotencyDB(t)
	ctx := context.Background()
	tasks := postgres.NewTaskRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	packs := packview.NewPostgresStore(pool)
	now := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)

	if _, err := packs.UpsertIfNewer(ctx, ports.PackRecord{
		GTIN: scanEachGTIN, SKU: "SKU-1", Level: task.PackLevelEach, UnitsPerPack: 1, Version: 1, EffectiveFrom: now,
	}); err != nil {
		t.Fatalf("seed pack view: %v", err)
	}
	plan, err := task.NewPickPlan("SKU-1", planned, "EACH", "BIN-1")
	if err != nil {
		t.Fatalf("pick plan: %v", err)
	}
	pick := task.New("pick-1", task.Pick, shared.NewCPT(now.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false).
		WithPickPlan(&plan, 0)
	if err := pick.Claim(scanStation, shared.NewCapabilitySet("pick"), now, time.Hour*24*365*10); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := tasks.Save(ctx, pick); err != nil {
		t.Fatalf("save task: %v", err)
	}

	handlers := &inboundhttp.Handlers{
		ConfirmPickScan: &usecases.ConfirmPickScan{Tasks: tasks, Packs: packs, Clock: fixedClockAt(now), UnitOfWork: uow},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &scanFixture{
		pool:   pool,
		tasks:  tasks,
		router: inboundhttp.NewRouter(handlers, logger, inboundhttp.WithIdempotencyPool(pool)),
	}
}

func (f *scanFixture) scan(taskID, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/tasks/"+taskID+"/scans", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

func (f *scanFixture) picked(t *testing.T) int {
	t.Helper()
	got, err := f.tasks.FindById(context.Background(), "pick-1")
	if err != nil || got == nil {
		t.Fatalf("reload task: %v (%v)", err, got)
	}
	return got.PickedEaches()
}

func scanBody(count int) string {
	b, _ := json.Marshal(map[string]any{"stationId": scanStation, "gtin": scanEachGTIN, "count": count})
	return string(b)
}

func problemType(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var p struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem %q: %v", rec.Body.String(), err)
	}
	return p.Type[strings.LastIndex(p.Type, "/")+1:]
}

// A lost response retried with the same key and body replays the stored
// answer verbatim and does not count the scan again.
func TestScanIdempotency_ReplaySameKeySameBody_CountsOnce(t *testing.T) {
	f := newScanFixture(t, 10)

	first := f.scan("pick-1", "scan-key-1", scanBody(2))
	if first.Code != http.StatusOK {
		t.Fatalf("first scan = %d (%s), want 200", first.Code, first.Body.String())
	}
	replay := f.scan("pick-1", "scan-key-1", scanBody(2))
	if replay.Code != http.StatusOK || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay = %d %s, want the stored 200 %s", replay.Code, replay.Body.String(), first.Body.String())
	}
	if got := f.picked(t); got != 2 {
		t.Fatalf("pickedEaches = %d after a replayed scan, want 2 (counted once)", got)
	}
	var res struct{ AddedEaches, PickedEaches, PlannedEaches int }
	if err := json.Unmarshal(replay.Body.Bytes(), &res); err != nil || res.PickedEaches != 2 || res.AddedEaches != 2 || res.PlannedEaches != 10 {
		t.Fatalf("replayed answer = %s (%v), want added 2 / picked 2 / planned 10", replay.Body.String(), err)
	}
}

// Each physical scan has its own key: two scans with different keys both count.
func TestScanIdempotency_DifferentKeys_BothCount(t *testing.T) {
	f := newScanFixture(t, 10)

	if rec := f.scan("pick-1", "scan-key-a", scanBody(1)); rec.Code != http.StatusOK {
		t.Fatalf("scan a = %d (%s)", rec.Code, rec.Body.String())
	}
	rec := f.scan("pick-1", "scan-key-b", scanBody(1))
	if rec.Code != http.StatusOK {
		t.Fatalf("scan b = %d (%s)", rec.Code, rec.Body.String())
	}
	if got := f.picked(t); got != 2 {
		t.Fatalf("pickedEaches = %d, want 2 (two scans, two keys)", got)
	}
	if !strings.Contains(rec.Body.String(), `"pickedEaches":2`) {
		t.Fatalf("second answer = %s, want pickedEaches 2", rec.Body.String())
	}
}

// The same key with a different body is the repo's existing conflict: 422
// idempotency-key-reused, and nothing is counted.
func TestScanIdempotency_SameKeyDifferentBody_Is422AndCountsNothing(t *testing.T) {
	f := newScanFixture(t, 10)

	if rec := f.scan("pick-1", "scan-key-1", scanBody(1)); rec.Code != http.StatusOK {
		t.Fatalf("first scan = %d (%s)", rec.Code, rec.Body.String())
	}
	rec := f.scan("pick-1", "scan-key-1", scanBody(3))
	if rec.Code != http.StatusUnprocessableEntity || problemType(t, rec) != "idempotency-key-reused" {
		t.Fatalf("conflicting reuse = %d %s, want 422 idempotency-key-reused", rec.Code, rec.Body.String())
	}
	if got := f.picked(t); got != 1 {
		t.Fatalf("pickedEaches = %d, want 1", got)
	}
}

// A key is not a retry on another task: the body carries no task id, so the
// path is part of the request identity.
func TestScanIdempotency_SameKeyOtherTask_Is422(t *testing.T) {
	f := newScanFixture(t, 10)

	if rec := f.scan("pick-1", "scan-key-1", scanBody(1)); rec.Code != http.StatusOK {
		t.Fatalf("first scan = %d (%s)", rec.Code, rec.Body.String())
	}
	rec := f.scan("pick-2", "scan-key-1", scanBody(1))
	if rec.Code != http.StatusUnprocessableEntity || problemType(t, rec) != "idempotency-key-reused" {
		t.Fatalf("same key on another task = %d %s, want 422 idempotency-key-reused", rec.Code, rec.Body.String())
	}
}

func TestScanIdempotency_MissingKey_Is400AndCountsNothing(t *testing.T) {
	f := newScanFixture(t, 10)

	rec := f.scan("pick-1", "", scanBody(1))
	if rec.Code != http.StatusBadRequest || problemType(t, rec) != "idempotency-key-required" {
		t.Fatalf("no key = %d %s, want 400 idempotency-key-required", rec.Code, rec.Body.String())
	}
	if got := f.picked(t); got != 0 {
		t.Fatalf("pickedEaches = %d, want 0", got)
	}
}

// A rejected scan is stored too: its retry replays the same 409 and the
// over-quantity guard still holds for a genuinely new key.
func TestScanIdempotency_RejectedScanReplaysTheSameProblem(t *testing.T) {
	f := newScanFixture(t, 2)

	if rec := f.scan("pick-1", "scan-key-ok", scanBody(2)); rec.Code != http.StatusOK {
		t.Fatalf("filling scan = %d (%s)", rec.Code, rec.Body.String())
	}
	over := f.scan("pick-1", "scan-key-over", scanBody(1))
	if over.Code != http.StatusConflict || problemType(t, over) != "pick-over-quantity" {
		t.Fatalf("over-quantity = %d %s, want 409 pick-over-quantity", over.Code, over.Body.String())
	}
	replay := f.scan("pick-1", "scan-key-over", scanBody(1))
	if replay.Code != over.Code || replay.Body.String() != over.Body.String() {
		t.Fatalf("replayed rejection = %d %s, want %d %s", replay.Code, replay.Body.String(), over.Code, over.Body.String())
	}
	if got := f.picked(t); got != 2 {
		t.Fatalf("pickedEaches = %d, want 2", got)
	}
}
