//go:build integration

package analyticsstore_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/analytics/report"
)

// These tests run against a throwaway analytics Postgres the test binary
// starts itself via testcontainers — never an external
// ANALYTICS_DATABASE_URL, never t.Skip. One container is started in
// TestMain, migrated once with the analytics migrations, and shared by every
// test in the package (containers are slow to boot); isolation comes from
// each test using unique station ids or truncating what it asserts on.

var analyticsURL string

// TestMain owns the package-wide Postgres lifecycle.
func TestMain(m *testing.M) {
	os.Exit(runWithAnalyticsPostgres(m))
}

func runWithAnalyticsPostgres(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("fulfillment_analytics"),
		tcpostgres.WithUsername("fx_analytics"),
		tcpostgres.WithPassword("fx_analytics"),
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

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection string: %v\n", err)
		return 1
	}
	if err := postgres.Migrate(url, "../../../../migrations/analytics"); err != nil {
		fmt.Fprintf(os.Stderr, "migrate analytics: %v\n", err)
		return 1
	}
	analyticsURL = url
	return m.Run()
}

func TestPostgresProjectionAndReport_RoundTrip(t *testing.T) {
	url := analyticsURL

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	// Isolate this run's rows.
	base := time.Now().UTC().Truncate(time.Hour)
	taskType := "PICK-INT"
	station := "st-int-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM throughput_rollup WHERE station_id = $1`, station)
		_, _ = pool.Exec(ctx, `DELETE FROM analytics_pending_claims WHERE station_id = $1`, station)
	})

	proj := analyticsstore.NewPostgresProjection(pool)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	// claim -> complete twice with the same event ids: idempotent.
	apply := func() {
		must(proj.ApplyTaskClaimed(ctx, "int-claim", "Tint", taskType, station, base))
		must(proj.ApplyTaskCompleted(ctx, "int-complete", "Tint", taskType, station, base.Add(60*time.Second)))
	}
	apply()
	apply()

	rdr := analyticsstore.NewPostgresReport(pool)
	rep, err := rdr.Query(ctx, report.ReportQuery{
		From:        base.Add(-time.Hour),
		To:          base.Add(time.Hour),
		StationId:   station,
		Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rep.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rep.Rows))
	}
	if rep.Rows[0].Completions != 1 {
		t.Errorf("Completions = %d, want 1 (idempotent)", rep.Rows[0].Completions)
	}
	if rep.Rows[0].AvgClaimToCompleteSeconds != 60 {
		t.Errorf("AvgClaimToCompleteSeconds = %v, want 60", rep.Rows[0].AvgClaimToCompleteSeconds)
	}

	lag, err := rdr.FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag: %v", err)
	}
	if lag < 0 {
		t.Errorf("lag = %v, want >= 0", lag)
	}
}

// TestReadOnlyPool_RejectsWrites asserts the reader pool is genuinely
// read-only: an attempt to write through it must be rejected by Postgres.
func TestReadOnlyPool_RejectsWrites(t *testing.T) {
	url := analyticsURL

	roPool, err := analyticsstore.NewReadOnlyPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewReadOnlyPool: %v", err)
	}
	t.Cleanup(roPool.Close)

	ctx := context.Background()
	_, err = roPool.Exec(ctx,
		`INSERT INTO throughput_rollup (task_type, station_id, hour_bucket) VALUES ($1, $2, $3)`,
		"RO", "ro-station", time.Now().UTC().Truncate(time.Hour))
	if err == nil {
		t.Fatal("expected read-only pool to reject INSERT, but it succeeded")
	}

	// The read side still works over the same read-only pool.
	rdr := analyticsstore.NewPostgresReport(roPool)
	if _, err := rdr.FreshnessLag(ctx); err != nil {
		t.Fatalf("FreshnessLag over read-only pool: %v", err)
	}
}

// TestFreshnessLag_EmptyStore covers the NULL path: max(occurred_at) over an
// empty table returns a single NULL row (not zero rows), which must be read as
// a zero lag rather than a scan error.
func TestFreshnessLag_EmptyStore(t *testing.T) {
	url := analyticsURL

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	// Ensure the processed-events table is empty so max() yields NULL.
	if _, err := pool.Exec(ctx, `TRUNCATE analytics_processed_events`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	lag, err := analyticsstore.NewPostgresReport(pool).FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag on empty store: %v", err)
	}
	if lag != 0 {
		t.Fatalf("empty-store lag = %v, want 0", lag)
	}
}

// TestPostgresProjectionAndReport_OnTimeToCPT_RoundTrip proves the
// on-time-to-CPT columns (ADR-0026, migration 0002) round-trip for real
// against a live Postgres: packages_manifested/packages_on_time_cpt/
// packages_late_cpt accumulate correctly across on-time and late
// manifests, idempotently on eventId, on the SAME (task_type, station_id,
// hour_bucket) grain the existing rollup uses.
func TestPostgresProjectionAndReport_OnTimeToCPT_RoundTrip(t *testing.T) {
	url := analyticsURL

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Hour)
	taskType := "SLAM-INT"
	station := "st-ontime-int-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM throughput_rollup WHERE station_id = $1`, station)
	})

	proj := analyticsstore.NewPostgresProjection(pool)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	// Two on-time, one late, applied twice with the SAME event ids
	// (duplicate delivery): idempotent, so counts must reflect one logical
	// occurrence each.
	apply := func() {
		must(proj.ApplyPackageManifested(ctx, "int-manifest-1-"+station, taskType, station, base, true))
		must(proj.ApplyPackageManifested(ctx, "int-manifest-2-"+station, taskType, station, base.Add(time.Minute), true))
		must(proj.ApplyPackageManifested(ctx, "int-manifest-3-"+station, taskType, station, base.Add(2*time.Minute), false))
	}
	apply()
	apply()

	rdr := analyticsstore.NewPostgresReport(pool)
	rep, err := rdr.Query(ctx, report.ReportQuery{
		From:        base.Add(-time.Hour),
		To:          base.Add(time.Hour),
		StationId:   station,
		Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rep.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 (station=%s)", len(rep.Rows), station)
	}
	row := rep.Rows[0]
	if row.PackagesManifested != 3 {
		t.Errorf("PackagesManifested = %d, want 3 (idempotent)", row.PackagesManifested)
	}
	if row.PackagesOnTimeToCPT != 2 {
		t.Errorf("PackagesOnTimeToCPT = %d, want 2", row.PackagesOnTimeToCPT)
	}
	if row.PackagesLateToCPT != 1 {
		t.Errorf("PackagesLateToCPT = %d, want 1", row.PackagesLateToCPT)
	}
	// Existing columns from the same events (none applied here) must stay
	// at their zero default — additive migration, no cross-contamination.
	if row.Completions != 0 {
		t.Errorf("Completions = %d, want 0 (no TaskCompleted applied in this test)", row.Completions)
	}
}
