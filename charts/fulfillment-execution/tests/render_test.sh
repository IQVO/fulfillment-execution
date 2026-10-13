#!/usr/bin/env bash
# Chart render assertions for charts/fulfillment-execution (run in CI by the
# chart-tests job, next to the Python chart tests in this directory).
#
# Run: bash charts/fulfillment-execution/tests/render_test.sh
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release=fulfillment-execution

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

api_deployment="$(helm template "$release" "$chart_dir" --show-only templates/deployment.yaml)"
configmap="$(helm template "$release" "$chart_dir" --show-only templates/configmap.yaml)"

# D12 (2026-10-12, ADR-0042): the outbox relay attempt cap is a chart value,
# rendered into the ConfigMap and the api env, default 20, tunable.
grep -q 'OUTBOX_MAX_ATTEMPTS: "20"' <<<"$configmap" || fail "OUTBOX_MAX_ATTEMPTS must default to 20 in the ConfigMap (ADR-0042)"
grep -A1 'name: OUTBOX_MAX_ATTEMPTS' <<<"$api_deployment" | grep -q 'value: "20"' \
  || fail "api Deployment must set OUTBOX_MAX_ATTEMPTS (default 20)"
tuned="$(helm template "$release" "$chart_dir" --set config.outboxMaxAttempts=5 --show-only templates/configmap.yaml)"
grep -q 'OUTBOX_MAX_ATTEMPTS: "5"' <<<"$tuned" || fail "config.outboxMaxAttempts is not rendered into the ConfigMap"

echo "PASS: fulfillment-execution chart render assertions"
