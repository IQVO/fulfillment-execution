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
  --set mcp.enabled=true --set a2a.enabled=true --set frontend.enabled=true)

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
for template in reports-deployment.yaml projector-deployment.yaml mcp-deployment.yaml a2a-deployment.yaml frontend-deployment.yaml; do
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
[[ "$(env_value "$api_deployment" PACK_VIEW_CONSUMER_GROUP)" == "fulfillment-execution-pack-view" ]] \
  || fail "PACK_VIEW_CONSUMER_GROUP is not rendered by default"
[[ "$(env_value "$api_deployment" INVENTORY_STORAGE_BASE_URL)" == "http://inventory-storage.warehouse-systems.svc.cluster.local:80" ]] \
  || fail "INVENTORY_STORAGE_BASE_URL is not rendered by default"
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
for pair in "execution:deployment.yaml" "mcp:mcp-deployment.yaml" "a2a:a2a-deployment.yaml" "fulfillment-projector:projector-deployment.yaml" "fulfillment-reports:reports-deployment.yaml" "sweep:sweeps-cronjob.yaml"; do
  bin="${pair%%:*}"
  template="${pair##*:}"
  vars="$(grep -ho '"[A-Z][A-Z0-9_]* is required' $(ls "$repo_dir"/cmd/"$bin"/*.go | grep -v '_test\.go$') | sed -E 's/^"//; s/ is required//' | sort -u)"
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
infra_a2a="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/a2a-deployment.yaml)"
[[ "$(env_value "$infra_a2a" EVENT_PUBLISHER)" == "kafka" ]] || fail "infra values: a2a EVENT_PUBLISHER"
helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/a2a-route.yaml | grep -q "replacePrefixMatch: /$" || fail "infra values: the a2a route must rewrite to /"
for component in api mcp a2a analytics-projector analytics-reports; do
  grep -q "app.kubernetes.io/component: $component" <<<"$infra_all" || fail "infra values: component $component not rendered"
done

# --- the A2A agent (ADR-0059) ----------------------------------------------
a2a_dep="$(render --set a2a.enabled=true --show-only templates/a2a-deployment.yaml)"
grep -A4 'name: DATABASE_URL$' <<<"$a2a_dep" | grep -q 'secretKeyRef:' || fail "the a2a DATABASE_URL must come from a Secret"
[[ "$(env_value "$a2a_dep" KAFKA_BROKERS)" == "kafka:9092" ]] || fail "the a2a KAFKA_BROKERS is not rendered from kafka.brokers"
[[ "$(env_value "$a2a_dep" EVENT_PUBLISHER)" == "kafka" ]] || fail "the a2a EVENT_PUBLISHER must be kafka"
[[ "$(env_value "$a2a_dep" A2A_ADDR)" == ":8093" ]] || fail "the a2a agent must listen on :8093 (8090 is mcp, 8091 the projector admin, 8092 the reports API)"
grep -q 'command: \["/app/a2a"\]' <<<"$a2a_dep" || fail "the a2a Deployment must run /app/a2a"
# The Agent Card must advertise a dialable absolute URL: it defaults to the
# release's in-cluster Service DNS (namespace-derived, so pattern-match) and
# stays overridable.
a2a_url="$(env_value "$a2a_dep" A2A_AGENT_URL || true)" # a missing var must reach the fail message below, not abort under set -e
[[ "$a2a_url" =~ ^http://[a-z0-9.-]+-a2a\.[a-z0-9-]+\.svc\.cluster\.local:8093/$ ]] || fail "A2A_AGENT_URL default is not the in-cluster Service DNS: $a2a_url"
a2a_override="$(render --set a2a.enabled=true --set a2a.agentUrl=http://gw.example/api/a2a/fulfillment-execution/ --show-only templates/a2a-deployment.yaml)"
[[ "$(env_value "$a2a_override" A2A_AGENT_URL || true)" == "http://gw.example/api/a2a/fulfillment-execution/" ]] || fail "a2a.agentUrl does not override A2A_AGENT_URL"
# A2A is off by default: no a2a resource renders unless asked for.
if render --show-only templates/a2a-deployment.yaml 2>/dev/null | grep -q 'kind: Deployment'; then fail "the a2a Deployment rendered with default values"; fi
# The a2a agent is a dedicated pod: its Service selects only the a2a component.
a2a_svc="$(render --set a2a.enabled=true --show-only templates/a2a-service.yaml)"
grep -A6 '^  selector:' <<<"$a2a_svc" | grep -q 'app.kubernetes.io/component: a2a' || fail "the a2a Service must select the a2a component only"
# It has no business with the reports service (that is an mcp-only tool).
if grep -q 'REPORTS_BASE_URL' <<<"$a2a_dep"; then fail "the a2a Deployment must not carry REPORTS_BASE_URL"; fi

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
expect_refusal "a2a without a database" "database.url or database.existingSecret" --set a2a.enabled=true
expect_refusal "a2a without kafka brokers" "kafka.brokers is required" "${base[@]}" --set a2a.enabled=true --set kafka.brokers=
expect_refusal "empty kafka.brokers" "kafka.brokers is required" "${base[@]}" --set kafka.brokers=
expect_refusal "empty WORK_RELEASED group" "config.workReleasedConsumerGroup is required" "${base[@]}" --set config.workReleasedConsumerGroup=

echo "PASS: fulfillment-execution chart render assertions"
