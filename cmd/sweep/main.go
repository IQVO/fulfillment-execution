// Command sweep is the entrypoint of the two sweep CronJobs (expire-leases,
// sweep-cpt-misses; ADR-0003, ADR-0025, ADR-0037). It POSTs the sweep
// endpoint of this release's own Service with a per-attempt timeout and a
// bounded, backed-off retry, exits non-zero when it gives up, and asks the
// Istio sidecar to quit on EVERY exit path, so an unavailable or rolling API
// fails the Job fast instead of hanging it until activeDeadlineSeconds
// (ADR 0048).
//
// Configuration (all from the environment, rendered by the chart):
//
//	SWEEP_URL              endpoint to POST (required)
//	SWEEP_REQUEST_TIMEOUT  per-attempt timeout, Go duration (default 15s)
//	SWEEP_MAX_ATTEMPTS     attempts before giving up (default 4)
//	SWEEP_RETRY_BACKOFF    first backoff, doubled per attempt (default 1s)
//	SWEEP_MAX_BACKOFF      backoff cap (default 8s)
//	SIDECAR_QUIT_URL       Istio quit endpoint (default
//	                       http://127.0.0.1:15020/quitquitquit; empty disables)
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	r := runner{client: &http.Client{}, logger: logger, sleep: sleepCtx}
	os.Exit(r.runMain(context.Background(), os.LookupEnv))
}
