#!/usr/bin/env python3
"""Assert the opt-in sweep CronJobs (ADR-0003, ADR-0025) render correctly.

- Default values render NO CronJob (today's behaviour is unchanged).
- sweeps.enabled=true renders one CronJob per sweep, each POSTing to this
  release's own Service at the documented endpoint.
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

    if cronjobs(render([])):
        failures.append("default values must render no CronJob")

    docs = render(["--set", "sweeps.enabled=true"])
    jobs = cronjobs(docs)
    want = {
        "fulfillment-execution-expire-leases": "/tasks/expire-leases",
        "fulfillment-execution-sweep-cpt-misses": "/tasks/sweep-cpt-misses",
    }
    if set(jobs) != set(want):
        failures.append(f"expected CronJobs {sorted(want)}, got {sorted(jobs)}")
    for name, path in want.items():
        job = jobs.get(name)
        if not job:
            continue
        spec = job["spec"]
        if spec["concurrencyPolicy"] != "Forbid":
            failures.append(f"{name}: concurrencyPolicy must be Forbid")
        pod = spec["jobTemplate"]["spec"]["template"]["spec"]
        if pod["restartPolicy"] != "Never":
            failures.append(f"{name}: restartPolicy must be Never")
        cmd = pod["containers"][0]["command"]
        url = cmd[-1]
        if url != f"http://fulfillment-execution:80{path}":
            failures.append(f"{name}: wrong target URL {url}")
        if "--post-data=" not in cmd:
            failures.append(f"{name}: must POST (--post-data=)")

    # CronJob pods must not be selectable by any Service in the chart.
    services = [d for d in docs if d.get("kind") == "Service"]
    for name, job in jobs.items():
        labels = job["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["labels"]
        for svc in services:
            sel = svc["spec"].get("selector") or {}
            if sel and all(labels.get(k) == v for k, v in sel.items()):
                failures.append(f"{name}: pods match Service {svc['metadata']['name']}")

    docs = render([
        "--set", "sweeps.enabled=true",
        "--set", "sweeps.cptMisses.enabled=false",
        "--set", "sweeps.expireLeases.schedule=*/2 * * * *",
    ])
    jobs = cronjobs(docs)
    if set(jobs) != {"fulfillment-execution-expire-leases"}:
        failures.append(f"cptMisses.enabled=false must drop that CronJob, got {sorted(jobs)}")
    elif jobs["fulfillment-execution-expire-leases"]["spec"]["schedule"] != "*/2 * * * *":
        failures.append("sweeps.expireLeases.schedule must be overridable")

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1
    print("PASS: sweep CronJobs are off by default; enabled -> POST to both endpoints, "
          "per-sweep toggle and schedule override work, pods unreachable via Services")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
