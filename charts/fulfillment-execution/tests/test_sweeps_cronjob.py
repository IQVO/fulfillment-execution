#!/usr/bin/env python3
"""Assert the sweep CronJobs (ADR-0003, ADR-0025, ADR-0037) render correctly.

- DEFAULT values render one CronJob per sweep (sweeps are ON by default):
  expire-leases every minute, sweep-cpt-misses every 5 minutes, each POSTing
  an empty body to this release's own Service at the documented endpoint,
  with concurrencyPolicy Forbid and bounded start/active deadlines + history.
- sweeps.enabled=false renders no CronJob.
- A single sweep can be switched off, and the schedule is overridable.
- The CronJob pods must never match a Deployment's Service selector.
- Fail fast (ADR 0048): the Job runs /app/sweep (per-attempt timeout, bounded
  backed-off retry, non-zero exit on give-up, Istio sidecar quit on every exit
  path), backoffLimit 0, an activeDeadlineSeconds, and the retry budget
  (attempts x timeout + (attempts - 1) x max backoff) fits inside that
  deadline; a values set that breaks the budget fails the render.

Run: python3 charts/fulfillment-execution/tests/test_sweeps_cronjob.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]
# Postgres is the only runtime (ADR 0052): the chart refuses to render without
# a DATABASE_URL source.
REQUIRED = ["--set", "database.url=postgres://u@example.invalid:5432/db"]


def render_raw(extra_args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["helm", "template", "fulfillment-execution", str(CHART_DIR), *REQUIRED, *extra_args],
        capture_output=True, text=True, check=False,
    )


def render(extra_args: list[str]) -> list[dict]:
    proc = render_raw(extra_args)
    if proc.returncode != 0:
        raise SystemExit(f"FAIL: helm template {extra_args} failed: {proc.stderr}")
    out = proc.stdout
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(out) if d]


def cronjobs(docs: list[dict]) -> dict[str, dict]:
    return {d["metadata"]["name"]: d for d in docs if d.get("kind") == "CronJob"}


WANT_ENV = {
    "SWEEP_REQUEST_TIMEOUT": "15s",
    "SWEEP_MAX_ATTEMPTS": "4",
    "SWEEP_RETRY_BACKOFF": "1s",
    "SWEEP_MAX_BACKOFF": "8s",
    "SIDECAR_QUIT_URL": "http://127.0.0.1:15020/quitquitquit",
}


def seconds(value: str) -> int:
    if not value.endswith("s") or not value[:-1].isdigit():
        raise ValueError(f"expected whole seconds like '15s', got {value!r}")
    return int(value[:-1])


def check_fail_fast(name: str, path: str, job_spec: dict) -> list[str]:
    """ADR 0048: the sweep entrypoint fails fast and the budget fits the deadline."""
    failures: list[str] = []
    if job_spec.get("backoffLimit") != 0:
        failures.append(f"{name}: backoffLimit must be 0, got {job_spec.get('backoffLimit')!r}")
    deadline = job_spec.get("activeDeadlineSeconds")
    if not deadline:
        failures.append(f"{name}: activeDeadlineSeconds must be set")
        return failures
    container = job_spec["template"]["spec"]["containers"][0]
    if container.get("command") != ["/app/sweep"]:
        failures.append(f"{name}: command must be ['/app/sweep'] (the fail-fast entrypoint), "
                        f"got {container.get('command')!r}")
    env = {e["name"]: e.get("value") for e in container.get("env") or []}
    if env.get("SWEEP_URL") != f"http://fulfillment-execution:80{path}":
        failures.append(f"{name}: SWEEP_URL must target the release Service, got {env.get('SWEEP_URL')!r}")
    for key, value in WANT_ENV.items():
        if env.get(key) != value:
            failures.append(f"{name}: {key} must default to {value!r}, got {env.get(key)!r}")
    try:
        attempts = int(env.get("SWEEP_MAX_ATTEMPTS") or "0")
        budget = attempts * seconds(env.get("SWEEP_REQUEST_TIMEOUT") or "") + \
            (attempts - 1) * seconds(env.get("SWEEP_MAX_BACKOFF") or "")
    except ValueError as exc:
        failures.append(f"{name}: unparsable retry settings: {exc}")
        return failures
    if budget >= deadline:
        failures.append(f"{name}: retry budget {budget}s must be below activeDeadlineSeconds {deadline}s")
    return failures


def check_budget_guard() -> list[str]:
    """A values set whose retry budget exceeds the deadline must fail the render."""
    failures: list[str] = []
    proc = render_raw(["--set", "sweeps.requestTimeoutSeconds=60"])
    if proc.returncode == 0:
        failures.append("requestTimeoutSeconds=60 (budget 4x60+3x8 > 120) must fail the render")
    elif "activeDeadlineSeconds" not in proc.stderr:
        failures.append(f"budget guard must name activeDeadlineSeconds, got: {proc.stderr.strip()}")
    docs = render(["--set", "sweeps.sidecarQuitUrl="])
    for name, job in cronjobs(docs).items():
        container = job["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
        env = {e["name"]: e.get("value") for e in container.get("env") or []}
        if env.get("SIDECAR_QUIT_URL", None) != "":
            failures.append(f"{name}: sweeps.sidecarQuitUrl='' must render SIDECAR_QUIT_URL empty "
                            f"(quit disabled), got {env.get('SIDECAR_QUIT_URL')!r}")
    return failures


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
        pod = job_spec["template"]["spec"]
        if pod["restartPolicy"] != "Never":
            failures.append(f"{name}: restartPolicy must be Never")
        failures.extend(check_fail_fast(name, path, job_spec))

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

    failures.extend(check_budget_guard())

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1
    print("PASS: sweep CronJobs are ON by default (every minute / every 5 minutes, Forbid, "
          "/app/sweep fail-fast entrypoint, retry budget inside the deadline, sidecar quit); sweeps.enabled=false and per-sweep toggle/schedule override "
          "work, pods unreachable via Services")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
