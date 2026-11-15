//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
)

// The reports binary is the READER composition root of the throughput data
// product (ADR-0012): it owns no schema, opens the analytical database
// read-only and serves GET /healthz, GET /reports/throughput and
// GET /reports/throughput/freshness (the documented interface of
// docs/docs/analytics/throughput-report.md, contract ADR-0043). This test
// boots the real run() against an EMPTY testcontainers Postgres that the
// test migrates with the repo's own analytics migrations (the writer owns
// them; the reader never migrates), seeds facts through the repo's own
// analytics writer (the PostgresProjection the projector uses), and asserts
// the documented report answers from those facts.

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(120 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// stopOnSignal delivers SIGTERM to this process until run() returns: the
// entry point's real shutdown path drains the HTTP server on SIGINT/SIGTERM.
// The test registers its own signal.Notify BEFORE starting run(), so the
// signal is never left to the process's default disposition during the
// window before run()'s own Notify.
func stopOnSignal(t *testing.T, done <-chan error) error {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
		select {
		case err := <-done:
			return err
		case <-time.After(300 * time.Millisecond):
		}
	}
	t.Fatal("run() did not return after SIGTERM")
	return nil
}

type throughputRow struct {
	TaskType                  string  `json:"taskType"`
	StationId                 string  `json:"stationId"`
	HourBucket                string  `json:"hourBucket"`
	Completions               int     `json:"completions"`
	AvgClaimToCompleteSeconds float64 `json:"avgClaimToCompleteSeconds"`
	LeaseExpiries             int     `json:"leaseExpiries"`
	WeighCheckDiverts         int     `json:"weighCheckDiverts"`
	PackagesManifested        int     `json:"packagesManifested"`
	PackagesOnTimeToCPT       int     `json:"packagesOnTimeToCPT"`
	PackagesLateToCPT         int     `json:"packagesLateToCPT"`
}

// TestReportsBootsOnMigratedAnalyticsAndServesTheSeededFacts seeds a
// claim->complete pair (75s apart) and one lease expiry through the
// PostgresProjection, boots the real run(), and asserts the documented
// answers: /healthz (the probe), the throughput report row for the seeded
// facts (completions, the derived claim-to-complete average, the hour
// bucket), and the freshness lag; then a SIGTERM is a clean drain.
func TestReportsBootsOnMigratedAnalyticsAndServesTheSeededFacts(t *testing.T) {
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("fulfillment_analytics"),
		tcpostgres.WithUsername("fx_analytics"),
		tcpostgres.WithPassword("fx_analytics"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}

	// The writer (cmd/fulfillment-projector) owns the analytics schema
	// (ADR-0012); the reader boots on an already-migrated database, so the
	// test runs the repo's own migration step exactly once, like the
	// projector's boot does.
	if err := postgres.Migrate(dsn, "../../migrations/analytics"); err != nil {
		t.Fatalf("migrate analytics: %v", err)
	}

	// Seed facts with the repo's own analytics writer — the same
	// PostgresProjection the projector drives from the analytics topic.
	pool, err := analyticsstore.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("analytics pool: %v", err)
	}
	defer pool.Close()
	projection := analyticsstore.NewPostgresProjection(pool)
	at := time.Date(2026, 11, 13, 11, 0, 0, 0, time.UTC)
	seed := []struct {
		apply func() error
	}{
		{func() error {
			return projection.ApplyTaskClaimed(ctx, "rep-it-claim-1", "T-REP-1", "PICK", "ST-REP-A", at)
		}},
		{func() error {
			return projection.ApplyTaskCompleted(ctx, "rep-it-complete-1", "T-REP-1", "PICK", "ST-REP-A", at.Add(75*time.Second))
		}},
		{func() error {
			return projection.ApplyLeaseExpired(ctx, "rep-it-expiry-1", "T-REP-2", "PACK", "ST-REP-B", at.Add(10*time.Minute))
		}},
	}
	for _, s := range seed {
		if err := s.apply(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	httpAddr := freeAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", dsn)
	t.Setenv("HTTP_ADDR", httpAddr)
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	defer signal.Stop(sig)

	done := make(chan error, 1)
	go func() { done <- run() }()

	base := "http://" + httpAddr
	waitUntil(t, "GET /healthz = 200 (the documented probe)", func() bool {
		resp, err := http.Get(base + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	// The documented report answers from the seeded facts: exactly one row
	// for the PICK station (completions=1, avg=75s at the hour bucket),
	// and one for the PACK lease expiry.
	q := url.Values{}
	q.Set("from", at.Add(-time.Hour).Format(time.RFC3339))
	q.Set("to", at.Add(2*time.Hour).Format(time.RFC3339))
	resp, err := http.Get(base + "/reports/throughput?" + q.Encode())
	if err != nil {
		t.Fatalf("GET /reports/throughput: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /reports/throughput = %d, want 200", resp.StatusCode)
	}
	var report struct {
		Rows []throughputRow `json:"rows"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	rows := map[string]throughputRow{}
	for _, r := range report.Rows {
		rows[r.StationId] = r
	}
	pick, ok := rows["ST-REP-A"]
	if !ok {
		t.Fatalf("no row for ST-REP-A: %+v", report.Rows)
	}
	if pick.TaskType != "PICK" || pick.Completions != 1 || pick.AvgClaimToCompleteSeconds != 75 || pick.HourBucket != at.Format(time.RFC3339) {
		t.Errorf("PICK row = %+v, want completions=1 avg=75s bucket=%s", pick, at.Format(time.RFC3339))
	}
	pack, ok := rows["ST-REP-B"]
	if !ok || pack.TaskType != "PACK" || pack.LeaseExpiries != 1 || pack.Completions != 0 {
		t.Errorf("PACK row = %+v (ok=%v), want leaseExpiries=1 completions=0", pack, ok)
	}

	// Freshness: lagSeconds is now minus the most recently applied event's
	// age — a non-negative number over the seeded facts.
	resp2, err := http.Get(base + "/reports/throughput/freshness")
	if err != nil {
		t.Fatalf("GET /reports/throughput/freshness: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET /reports/throughput/freshness = %d, want 200", resp2.StatusCode)
	}
	var freshness struct {
		LagSeconds float64 `json:"lagSeconds"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&freshness); err != nil {
		t.Fatalf("decode freshness: %v", err)
	}
	// The seeded occurrences are instants in 2026-11; the lag against the
	// real now is large but must be finite and non-negative.
	if freshness.LagSeconds < 0 {
		t.Errorf("lagSeconds = %v, want >= 0", freshness.LagSeconds)
	}

	if err := stopOnSignal(t, done); err != nil {
		t.Fatalf("a SIGTERMed reports reader must drain cleanly, got %v", err)
	}
}
