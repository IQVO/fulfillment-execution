---
id: 0037-sweeps-scheduled-by-cronjob-default-on
slug: /adr/0037-sweeps-scheduled-by-cronjob-default-on
title: 37. The two sweeps are scheduled by Helm CronJobs, on by default
sidebar_label: 37. Sweeps on by default
description: "ADR 0037 — the Helm chart's expire-leases (every minute) and sweep-cpt-misses (every 5 minutes) CronJobs are enabled by default, with concurrencyPolicy Forbid and per-environment schedules; no in-process ticker is added."
---

# 37. The two sweeps are scheduled by Helm CronJobs, on by default

## Status

Accepted. Decided 2026-10-06 (audit decision 12a). Supplements
[ADR-0003](./0003-lease-based-at-most-once-claiming.md) (names a CronJob as the
sweep's caller) and [ADR-0025](./0025-cpt-missed-sweep-and-package-manifested.md)
(rejected an in-process ticker). It changes a chart default only; no REST,
event or database contract changes.

## Context

`POST /tasks/expire-leases` and `POST /tasks/sweep-cpt-misses` are
Clock-driven but externally triggered: nothing in the service calls them on a
timer, and ADR-0025 rejected an in-process ticker. The chart already shipped
the external scheduler as two Kubernetes `CronJob`s, but behind
`sweeps.enabled`, **default false**. A release that did not opt in therefore
ran with no scheduler at all:

- leases were only freed lazily (on the next claim/renew/complete), so
  queue-depth stayed wrong and `LeaseExpired` was never emitted for abandoned
  work (ADR-0003);
- tasks stuck past their CPT were never reported as `TaskCPTMissed`, so
  order-management could not re-promise those orders (ADR-0025).

A running system needs both.

## Decision

1. **`sweeps.enabled` defaults to `true`.** Each sweep keeps its own
   `enabled` toggle; `sweeps.enabled=false` restores the old "an external
   caller drives the endpoints" behaviour.
2. **Cadence.**
   - `expire-leases`: `* * * * *` (every minute). The default lease is 5
     minutes (ADR-0003), so a lapsed lease is freed and `LeaseExpired` is
     raised within roughly one minute of lapsing; the sweep is cheap (one scan
     of the claimed set).
   - `sweep-cpt-misses`: `*/5 * * * *` (every 5 minutes). It re-publishes
     `TaskCPTMissed` for **every** still-open overdue task on **every** pass
     (ADR-0025 §4), so its Kafka volume is *(overdue tasks) × (passes)* —
     **`TaskCPTMissed` volume scales linearly with this cadence**. Five minutes
     bounds that amplification while still telling order-management within a
     few minutes.
   Both schedules are values (`sweeps.expireLeases.schedule`,
   `sweeps.cptMisses.schedule`) and can be changed per environment.
3. **Job shape.** `concurrencyPolicy: Forbid` (two passes of one sweep never
   overlap), `startingDeadlineSeconds: 60` (a missed slot is skipped rather
   than queued behind the next), `backoffLimit: 0`,
   `activeDeadlineSeconds: 120`, `successfulJobsHistoryLimit: 1`,
   `failedJobsHistoryLimit: 3` (the last two now values).
4. **Invocation.** The Job reuses the service image and runs busybox
   `wget -q -O - -T 30 --post-data= http://<service>:<port>/tasks/<sweep>`.
   This is correct for these endpoints: both handlers
   (`PostExpireLeases`, `PostSweepCPTMisses`) read neither a body nor a
   `Content-Type`, and neither route is behind the Idempotency-Key middleware
   (ADR-0028 wraps `POST /tasks` only). `--post-data=` makes busybox issue a
   `POST` with `Content-Length: 0`; busybox `wget` exits non-zero on an HTTP
   error status, so a failing sweep fails the Job. The rendered command is
   asserted by `charts/fulfillment-execution/tests/test_sweeps_cronjob.py`.
5. **No in-process ticker** is added (ADR-0025 stands).

## Consequences

### Easier

- A default install frees abandoned work and reports missed CPTs without any
  extra operator step.
- One place (the chart values) to tune each environment's cadence.

### Harder

- **`TaskCPTMissed` volume is now non-zero by default** and grows with the
  overdue backlog and the CPT cadence. Consumers must already be idempotent on
  their business key (ADR-0025); environments with a large backlog should
  lengthen `sweeps.cptMisses.schedule`.
- **One CronJob pod per minute** per release for the lease sweep. Resource
  requests are small (`sweeps.resources`), history is bounded.
- **Multiple releases of the chart pointing at one database** would each run
  the sweeps; the passes are idempotent but would repeat `TaskCPTMissed`.
  Disable `sweeps.enabled` on all but one such release.
- The Job needs `wget` in the service image (alpine/busybox today). A
  distroless image would need a different invocation.
