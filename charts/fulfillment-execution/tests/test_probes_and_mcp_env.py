#!/usr/bin/env python3
"""Assert the OLTP probes follow ADR-0029 and the MCP pod can publish events.

- The OLTP Deployment's readinessProbe must hit /readyz (it flips to 503 as the
  first step of graceful shutdown and, since ADR 0052, while Postgres or Kafka
  is unreachable); startup/liveness stay on /healthz.
- The MCP Deployment must carry EVENT_PUBLISHER and KAFKA_BROKERS so
  complete_task writes the same outbox rows as REST (ADR-0008, ADR-0020).
  EVENT_PUBLISHER is config.eventPublisher (kafka, the only accepted value);
  there is no per-MCP override any more.

Run: python3 charts/fulfillment-execution/tests/test_probes_and_mcp_env.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]
# Postgres is the only runtime (ADR 0052): the chart refuses to render without
# a DATABASE_URL source.
REQUIRED = ["--set", "database.url=postgres://u@example.invalid:5432/db"]


def render(extra_args: list[str]) -> list[dict]:
    out = subprocess.run(
        ["helm", "template", "fulfillment-execution", str(CHART_DIR), *REQUIRED, *extra_args],
        capture_output=True, text=True, check=True,
    ).stdout
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(out) if d]


def container(docs: list[dict], name: str) -> dict:
    for d in docs:
        if d.get("kind") == "Deployment" and d["metadata"]["name"] == name:
            return d["spec"]["template"]["spec"]["containers"][0]
    raise SystemExit(f"FAIL: Deployment {name} not rendered")


def env(c: dict) -> dict:
    return {e["name"]: e.get("value") for e in c.get("env", [])}


def main() -> int:
    failures: list[str] = []

    docs = render(["--set", "mcp.enabled=true"])
    oltp = container(docs, "fulfillment-execution")
    if oltp["readinessProbe"]["httpGet"]["path"] != "/readyz":
        failures.append("OLTP readinessProbe must use /readyz")
    for probe in ("startupProbe", "livenessProbe"):
        if oltp[probe]["httpGet"]["path"] != "/healthz":
            failures.append(f"OLTP {probe} must stay on /healthz")

    mcp_env = env(container(docs, "fulfillment-execution-mcp"))
    for key in ("EVENT_PUBLISHER", "KAFKA_BROKERS"):
        if not mcp_env.get(key):
            failures.append(f"MCP deployment must set {key}")
    if mcp_env.get("EVENT_PUBLISHER") != "kafka":
        failures.append("MCP EVENT_PUBLISHER must be kafka by default")

    docs = render(["--set", "mcp.enabled=true", "--set", "config.eventPublisher=kafka"])
    if env(container(docs, "fulfillment-execution-mcp")).get("EVENT_PUBLISHER") != "kafka":
        failures.append("MCP EVENT_PUBLISHER must come from config.eventPublisher")

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1
    print("PASS: OLTP readiness on /readyz; MCP publishes via EVENT_PUBLISHER/KAFKA_BROKERS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
