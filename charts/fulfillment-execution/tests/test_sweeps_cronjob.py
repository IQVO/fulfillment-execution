#!/usr/bin/env python3
"""Assert the sweep CronJobs (ADR-0003, ADR-0025, ADR-0037) render correctly.

- DEFAULT values render one CronJob per sweep (sweeps are ON by default):
  expire-leases every minute, sweep-cpt-misses every 5 minutes, each POSTing
  an empty body to this release's own Service at the documented endpoint,
  with concurrencyPolicy Forbid and bounded start/active deadlines + history.
- sweeps.enabled=false renders no CronJob.
- A single sweep can be switched off, and the schedule is overridable.
- The CronJob pods must never match a Deployment's Service selector.

Run: python3 charts/fulfillment-execution/tests/test_sweeps_cronjob.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]


def render(extra_args: list[str]) -> list[dict]:
    out = subprocess.run(
        ["helm", "template", "fulfillment-execution", str(CHART_DIR), *extra_args],
        capture_output=True, text=True, check=True,
    ).stdout
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(out) if d]


def cronjobs(docs: list[dict]) -> dict[str, dict]:
    return {d["metadata"]["name"]: d for d in docs if d.get("kind") == "CronJob"}


def main() -> int:
    failures: list[str] = []

    if cronjobs(render(["--set", "sweeps.enabled=false"])):
        failures.append("sweeps.enabled=false must render no CronJob")

    # Defaults: sweeps are ON (ADR-0037) -- no --set needed.
    docs = render([])
    jobs = cronjobs(docs)
    want = {
        "fulfillment-execution-expire-leases": ("/tasks/expire-leases", "* * * * *"),
        "fulfillment-execution-sweep-cpt-misses": ("/tasks/sweep-cpt-misses", "*/5 * * * *"),
    }
    if set(jobs) != set(want):
        failures.append(f"expected CronJobs {sorted(want)} by default, got {sorted(jobs)}")
    for name, (path, schedule) in want.items():
        job = jobs.get(name)
        if not job:
            continue
        spec = job["spec"]
        if spec["schedule"] != schedule:
            failures.append(f"{name}: default schedule must be {schedule!r}, got {spec['schedule']!r}")
        if spec["concurrencyPolicy"] != "Forbid":
            failures.append(f"{name}: concurrencyPolicy must be Forbid")
        if not spec.get("startingDeadlineSeconds"):
            failures.append(f"{name}: startingDeadlineSeconds must be set")
        for key in ("successfulJobsHistoryLimit", "failedJobsHistoryLimit"):
            if key not in spec:
                failures.append(f"{name}: {key} must be set")
        job_spec = spec["jobTemplate"]["spec"]
        if job_spec.get("backoffLimit") != 0 or not job_spec.get("activeDeadlineSeconds"):
            failures.append(f"{name}: Job must have backoffLimit 0 and an activeDeadlineSeconds")
        pod = job_spec["template"]["spec"]
        if pod["restartPolicy"] != "Never":
            failures.append(f"{name}: restartPolicy must be Never")
        # The exact invocation: busybox wget, an explicit EMPTY POST body
        # (--post-data=), the in-cluster Service URL last. The handlers read
        # no body and need no Content-Type, so this is accepted.
        cmd = pod["containers"][0]["command"]
        if cmd[0] != "wget":
            failures.append(f"{name}: must use busybox wget, got {cmd[0]}")
        if cmd[-1] != f"http://fulfillment-execution:80{path}":
            failures.append(f"{name}: wrong target URL {cmd[-1]}")
        if "--post-data=" not in cmd:
            failures.append(f"{name}: must POST an explicit empty body (--post-data=)")
        if cmd.index("-T") + 1 >= len(cmd) or cmd[cmd.index("-T") + 1] != "30":
            failures.append(f"{name}: request timeout (-T 30) missing")

    # CronJob pods must not be selectable by any Service in the chart.
    services = [d for d in docs if d.get("kind") == "Service"]
    for name, job in jobs.items():
        labels = job["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["labels"]
        for svc in services:
            sel = svc["spec"].get("selector") or {}
            if sel and all(labels.get(k) == v for k, v in sel.items()):
                failures.append(f"{name}: pods match Service {svc['metadata']['name']}")

    docs = render([
        "--set", "sweeps.cptMisses.enabled=false",
        "--set", "sweeps.expireLeases.schedule=*/2 * * * *",
    ])
    jobs = cronjobs(docs)
    if set(jobs) != {"fulfillment-execution-expire-leases"}:
        failures.append(f"cptMisses.enabled=false must drop that CronJob, got {sorted(jobs)}")
    elif jobs["fulfillment-execution-expire-leases"]["spec"]["schedule"] != "*/2 * * * *":
        failures.append("sweeps.expireLeases.schedule must be overridable")

    docs = render(["--set", "sweeps.cptMisses.schedule=*/15 * * * *"])
    jobs = cronjobs(docs)
    cpt = jobs.get("fulfillment-execution-sweep-cpt-misses")
    if not cpt or cpt["spec"]["schedule"] != "*/15 * * * *":
        failures.append("sweeps.cptMisses.schedule must be overridable")

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1
    print("PASS: sweep CronJobs are ON by default (every minute / every 5 minutes, Forbid, "
          "empty-body wget POST); sweeps.enabled=false and per-sweep toggle/schedule override "
          "work, pods unreachable via Services")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
