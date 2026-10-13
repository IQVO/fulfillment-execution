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
configmap_name="$(awk '/^  name:/ {print $2; exit}' <<<"$configmap")"
[[ -n "$configmap_name" ]] || fail "could not read the ConfigMap name"

# D14 (2026-10-12): the api Deployment -- the only workload whose binary
# (cmd/execution) reads the ConfigMap's keys -- mounts it with envFrom, or a
# value that is only in the ConfigMap silently does nothing.
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q 'configMapRef:' \
  || fail "api Deployment does not mount the ConfigMap via envFrom"
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q "name: $configmap_name" \
  || fail "api Deployment envFrom does not reference ConfigMap $configmap_name"

# The other workloads read their own explicit env and must NOT inherit the
# api's HTTP_ADDR (reports would bind the api address) or OTEL_SERVICE_NAME.
for template in reports-deployment.yaml projector-deployment.yaml mcp-deployment.yaml frontend-deployment.yaml; do
  rendered="$(helm template "$release" "$chart_dir" --set analytics.enabled=true --set mcp.enabled=true --set frontend.enabled=true --show-only "templates/$template")"
  if grep -q 'configMapRef:' <<<"$rendered"; then
    fail "$template must not mount the api ConfigMap"
  fi
done

# D12 (2026-10-12, ADR-0042): the outbox relay attempt cap is a chart value,
# rendered into the ConfigMap and the api env, default 20, tunable.
grep -q 'OUTBOX_MAX_ATTEMPTS: "20"' <<<"$configmap" || fail "OUTBOX_MAX_ATTEMPTS must default to 20 in the ConfigMap (ADR-0042)"
grep -A1 'name: OUTBOX_MAX_ATTEMPTS' <<<"$api_deployment" | grep -q 'value: "20"' \
  || fail "api Deployment must set OUTBOX_MAX_ATTEMPTS (default 20)"
tuned="$(helm template "$release" "$chart_dir" --set config.outboxMaxAttempts=5 --show-only templates/configmap.yaml)"
grep -q 'OUTBOX_MAX_ATTEMPTS: "5"' <<<"$tuned" || fail "config.outboxMaxAttempts is not rendered into the ConfigMap"

echo "PASS: fulfillment-execution chart render assertions"
