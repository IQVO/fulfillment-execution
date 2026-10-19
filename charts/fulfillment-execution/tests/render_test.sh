#!/usr/bin/env bash
# Chart render assertions for charts/fulfillment-execution (run in CI by the
# chart-tests job, next to the Python chart tests in this directory).
#
# Postgres and Kafka are the only runtime (ADR 0052): the chart refuses to
# render without a database source, always renders KAFKA_BROKERS and one
# consumer group per Kafka consumer, never renders a permissive/off/log mode,
# and still renders with the values warehouse-infra really passes
# (tests/warehouse-infra-values.yaml).
#
# Run: bash charts/fulfillment-execution/tests/render_test.sh
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$chart_dir/../.." && pwd)"
release=fulfillment-execution
# Dummy DSN, no password: the chart refuses to render without a database source.
base=(--set database.url=postgres://u@example.invalid:5432/db)
everything=(--set analytics.enabled=true
  --set analytics.database.projectorUrl=postgres://projector@example.invalid:5432/a
  --set analytics.database.reportsUrl=postgres://reports@example.invalid:5432/a
  --set mcp.enabled=true --set frontend.enabled=true)

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

render() { helm template "$release" "$chart_dir" "${base[@]}" "$@"; }

# env_value DOC NAME: the value of the first `- name: NAME` entry in DOC.
env_value() {
  awk -v n="$2" '$0 ~ "- name: " n "$" {found=1; next} found && /value:/ {gsub(/^ *value: *"?|"?$/, ""); print; exit} found {exit}' <<<"$1"
}

api_deployment="$(render --show-only templates/deployment.yaml)"
configmap="$(render --show-only templates/configmap.yaml)"
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
  rendered="$(render "${everything[@]}" --show-only "templates/$template")"
  if grep -q 'configMapRef:' <<<"$rendered"; then
    fail "$template must not mount the api ConfigMap"
  fi
done

# D12 (2026-10-12, ADR-0042): the outbox relay attempt cap is a chart value,
# rendered into the ConfigMap and the api env, default 20, tunable.
grep -q 'OUTBOX_MAX_ATTEMPTS: "20"' <<<"$configmap" || fail "OUTBOX_MAX_ATTEMPTS must default to 20 in the ConfigMap (ADR-0042)"
grep -A1 'name: OUTBOX_MAX_ATTEMPTS' <<<"$api_deployment" | grep -q 'value: "20"' \
  || fail "api Deployment must set OUTBOX_MAX_ATTEMPTS (default 20)"
tuned="$(render --set config.outboxMaxAttempts=5 --show-only templates/configmap.yaml)"
grep -q 'OUTBOX_MAX_ATTEMPTS: "5"' <<<"$tuned" || fail "config.outboxMaxAttempts is not rendered into the ConfigMap"

# --- real Postgres and real Kafka only (ADR 0052) -----------------------
all_docs="$(render "${everything[@]}")"
notes="$(helm template "$release" "$chart_dir" "${base[@]}" "${everything[@]}" --show-only templates/NOTES.txt 2>/dev/null || true)"

[[ "$(env_value "$api_deployment" EVENT_PUBLISHER)" == "kafka" ]] || fail "EVENT_PUBLISHER must default to kafka"
grep -q '^  EVENT_PUBLISHER: "kafka"' <<<"$configmap" || fail "the ConfigMap must carry EVENT_PUBLISHER=kafka"
[[ "$(env_value "$api_deployment" KAFKA_BROKERS)" == "kafka:9092" ]] || fail "KAFKA_BROKERS is not rendered from kafka.brokers"
[[ "$(env_value "$api_deployment" WORK_RELEASED_CONSUMER_GROUP)" == "fulfillment-execution" ]] \
  || fail "WORK_RELEASED_CONSUMER_GROUP must default to the group the binary used before (fulfillment-execution)"
[[ "$(env_value "$api_deployment" PRODUCT_CLASSIFICATION_CONSUMER_GROUP)" == "fulfillment-execution-product-classification" ]] \
  || fail "PRODUCT_CLASSIFICATION_CONSUMER_GROUP is not rendered by default"
[[ "$(env_value "$api_deployment" TASK_INTERLEAVING_CONSUMER_GROUP)" == "fulfillment-execution-interleaving" ]] \
  || fail "TASK_INTERLEAVING_CONSUMER_GROUP is not rendered by default"
grep -A4 'name: DATABASE_URL$' <<<"$api_deployment" | grep -q 'secretKeyRef:' || fail "DATABASE_URL must come from a Secret (secretKeyRef)"
if grep -qE '^  (MIGRATIONS_)?DATABASE_URL:' <<<"$configmap"; then fail "a DSN must never land in the ConfigMap"; fi

tuned_groups="$(render --set config.workReleasedConsumerGroup=g1 --set config.productClassificationConsumerGroup=g2 \
  --set config.taskInterleavingConsumerGroup=g3 --show-only templates/deployment.yaml)"
[[ "$(env_value "$tuned_groups" WORK_RELEASED_CONSUMER_GROUP)" == "g1" ]] || fail "config.workReleasedConsumerGroup is not rendered"
[[ "$(env_value "$tuned_groups" PRODUCT_CLASSIFICATION_CONSUMER_GROUP)" == "g2" ]] || fail "config.productClassificationConsumerGroup is not rendered"
[[ "$(env_value "$tuned_groups" TASK_INTERLEAVING_CONSUMER_GROUP)" == "g3" ]] || fail "config.taskInterleavingConsumerGroup is not rendered"

# No permissive / off / log value and no in-memory mention anywhere.
if grep -qiE 'value: "?(permissive|off|log)"?$' <<<"$all_docs"; then fail "the render carries permissive/off/log, which the binaries reject at boot"; fi
if grep -q 'DATABASE_URL: ""' <<<"$all_docs"; then fail "an empty DATABASE_URL must never be rendered"; fi
if grep -qi 'in-memory' <<<"$all_docs"$'\n'"$notes"; then fail "the render mentions an in-memory mode"; fi

# Every variable a binary reports as "<VAR> is required" is rendered into the
# Deployment (or CronJob) that runs that binary, under both the defaults and
# the warehouse-infra values.
infra=(--values "$chart_dir/tests/warehouse-infra-values.yaml")
infra_all="$(helm template "$release" "$chart_dir" "${infra[@]}")" || fail "the warehouse-infra values no longer render"
for pair in "execution:deployment.yaml" "mcp:mcp-deployment.yaml" "fulfillment-projector:projector-deployment.yaml" "fulfillment-reports:reports-deployment.yaml" "sweep:sweeps-cronjob.yaml"; do
  bin="${pair%%:*}"
  template="${pair##*:}"
  vars="$(grep -ho '"[A-Z][A-Z_]* is required' $(ls "$repo_dir"/cmd/"$bin"/*.go | grep -v '_test\.go$') | sed -E 's/^"//; s/ is required//' | sort -u)"
  [[ -n "$vars" ]] || fail "found no mandatory variables in cmd/$bin; the scan is looking in the wrong place"
  for source in default infra; do
    if [[ "$source" == default ]]; then
      rendered="$(render "${everything[@]}" --show-only "templates/$template")"
    else
      rendered="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only "templates/$template")"
    fi
    for var in $vars; do
      grep -q "name: $var\$" <<<"$rendered" || fail "the $bin workload ($source values) does not render $var, which cmd/$bin requires"
    done
  done
done

# warehouse-infra values: Kafka from _platform.yaml, the DSN from the existing
# secret, every consumer group once (a duplicate env entry breaks ArgoCD's diff).
infra_api="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/deployment.yaml)"
[[ "$(env_value "$infra_api" KAFKA_BROKERS)" == "kafka.warehouse-systems.svc.cluster.local:9092" ]] || fail "infra values: KAFKA_BROKERS"
[[ "$(env_value "$infra_api" EVENT_PUBLISHER)" == "kafka" ]] || fail "infra values: EVENT_PUBLISHER"
[[ "$(env_value "$infra_api" WORK_RELEASED_CONSUMER_GROUP)" == "fulfillment-execution" ]] || fail "infra values: WORK_RELEASED_CONSUMER_GROUP"
[[ "$(env_value "$infra_api" PRODUCT_CLASSIFICATION_CONSUMER_GROUP)" == "fulfillment-execution-product-classification" ]] || fail "infra values: PRODUCT_CLASSIFICATION_CONSUMER_GROUP"
[[ "$(env_value "$infra_api" TASK_INTERLEAVING_CONSUMER_GROUP)" == "fulfillment-execution-interleaving" ]] || fail "infra values: TASK_INTERLEAVING_CONSUMER_GROUP"
grep -q 'fulfillment-execution-db' <<<"$infra_api" || fail "infra values: DATABASE_URL must come from the fulfillment-execution-db secret"
dups="$(grep -E '^ {12}- name: ' <<<"$infra_api" | sort | uniq -d)"
[[ -z "$dups" ]] || fail "infra values: duplicate env entries in the api Deployment: $dups"
infra_proj="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/projector-deployment.yaml)"
[[ "$(env_value "$infra_proj" KAFKA_BROKERS)" == "kafka.warehouse-systems.svc.cluster.local:9092" ]] || fail "infra values: projector KAFKA_BROKERS"
infra_mcp="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/mcp-deployment.yaml)"
[[ "$(env_value "$infra_mcp" EVENT_PUBLISHER)" == "kafka" ]] || fail "infra values: mcp EVENT_PUBLISHER"
for component in api mcp analytics-projector analytics-reports; do
  grep -q "app.kubernetes.io/component: $component" <<<"$infra_all" || fail "infra values: component $component not rendered"
done

# The chart's own readinessProbe is /readyz (now a real dependency check).
grep -A3 'readinessProbe:' <<<"$api_deployment" | grep -A2 'httpGet:' | grep -q 'path: /readyz' || fail "readinessProbe must hit /readyz"

# --- the chart refuses to render without its mandatory inputs -----------
expect_refusal() {
  local why="$1" needle="$2"
  shift 2
  local err
  if err="$(helm template "$release" "$chart_dir" "$@" 2>&1 >/dev/null)"; then
    fail "the chart must refuse to render: $why"
  fi
  grep -qF -- "$needle" <<<"$err" || fail "the refusal for '$why' does not name '$needle': $err"
}
expect_refusal "no database source" "database.url or database.existingSecret"
expect_refusal "mcp without a database" "database.url or database.existingSecret" --set mcp.enabled=true
expect_refusal "empty kafka.brokers" "kafka.brokers is required" "${base[@]}" --set kafka.brokers=
expect_refusal "empty WORK_RELEASED group" "config.workReleasedConsumerGroup is required" "${base[@]}" --set config.workReleasedConsumerGroup=

echo "PASS: fulfillment-execution chart render assertions"
