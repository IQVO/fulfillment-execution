---
id: 0048-sweep-cronjobs-fail-fast
slug: /adr/0048-sweep-cronjobs-fail-fast
title: 48. The sweep CronJobs fail fast with a bounded retry and quit the sidecar on every exit
sidebar_label: "48. Sweep CronJobs fail fast"
description: "ADR 0048 — the expire-leases and sweep-cpt-misses CronJobs run the /app/sweep entrypoint: per-attempt timeout, bounded backed-off retry, non-zero exit on give-up and the Istio sidecar quit on every exit path, with a render-time guard that the retry budget fits activeDeadlineSeconds. Amends ADR 0037."
---

# 48. The sweep CronJobs fail fast with a bounded retry and quit the sidecar on every exit

## Status

Accepted (2026-10-13). Amends [ADR-0037](./0037-sweeps-scheduled-by-cronjob-default-on.md)
(how the CronJob calls the endpoint; the schedules and the default-on decision
are unchanged). Final-gaps decision E4.

## Context

ADR-0037's CronJobs ran busybox `wget -T 30 --post-data=` against the api
Service. In the warehouse kind cluster, both Jobs repeatedly ended in
`DeadlineExceeded` after `activeDeadlineSeconds` (120 s), with the pod deleted
and no logs, whenever the api was unavailable or rolling. It happened to both
sweeps at the same time, and it happened again during a rollout. The inline
`wget`:

- had no bounded retry, so a rolling api gave the sweep one chance;
- had no overall bound we could prove: busybox's `-T` is a read timeout, and
  a connection that the mesh accepted but never answered was not cut reliably;
- left no log line explaining the failure;
- never told the Istio sidecar to exit. The namespace runs Istio 1.30 native
  sidecars, which Kubernetes stops when the main container ends, so the quit
  is not needed on that path. A classic (non-native) sidecar keeps the pod
  Running until the deadline unless it is told to quit.

## Decision

1. The Job runs a dedicated entrypoint, `cmd/sweep` (`/app/sweep` in the same
   image). It POSTs `SWEEP_URL` with an empty body:
   - each attempt is bounded end to end by `SWEEP_REQUEST_TIMEOUT` (default
     15 s; context deadline on connect, request and response);
   - connect errors, timeouts, `429` and `5xx` are retried, up to
     `SWEEP_MAX_ATTEMPTS` (default 4), after a backoff that starts at
     `SWEEP_RETRY_BACKOFF` (1 s) and doubles up to `SWEEP_MAX_BACKOFF` (8 s);
   - any other non-2xx response is not retried;
   - exit codes: `0` success, `1` give-up or rejection, `2` bad configuration.
     A non-zero exit with `backoffLimit: 0` FAILS the Job, and its logs
     survive (`failedJobsHistoryLimit: 3`).
2. On EVERY exit path (success, give-up, rejection, configuration error) the
   entrypoint POSTs `SIDECAR_QUIT_URL` (default
   `http://127.0.0.1:15020/quitquitquit`, the Istio agent's quit endpoint).
   The call is best effort and bounded by 2 s, and a refused connection (no
   sidecar) is ignored. An empty value disables it.
3. The chart renders the settings from `sweeps.requestTimeoutSeconds`,
   `sweeps.maxAttempts`, `sweeps.retryBackoffSeconds`,
   `sweeps.maxBackoffSeconds` and `sweeps.sidecarQuitUrl`. It fails the render
   when the worst case, `maxAttempts × requestTimeoutSeconds + (maxAttempts −
   1) × maxBackoffSeconds`, is not below `activeDeadlineSeconds`. The
   defaults give 4 × 15 + 3 × 8 = 84 s against 120 s, so the Job always ends
   on its own exit code, never on the deadline.

## Consequences

- An unavailable api now produces a failed Job with a log line
  (`sweep: failed; exiting non-zero so the Job fails fast`) within about 84
  seconds. A short rollout is absorbed by the retries. The next scheduled run
  (one minute or five minutes later) tries again, as before.
- The unit tests in `cmd/sweep/sweep_test.go` cover the give-up path against
  an unreachable endpoint, a server that never answers, no retry on 4xx, and
  the quit call on every path. The chart test
  `charts/fulfillment-execution/tests/test_sweeps_cronjob.py` asserts
  `backoffLimit: 0`, the deadline, the entrypoint, the env, the quit URL and
  the budget guard.
- The image carries one more small static binary.
