#!/usr/bin/env bash
# Contract-test the REST API with Schemathesis (property-based testing
# against apis/openapi.yaml): builds the service, boots it with its
# in-memory adapters on a loopback port, waits for /healthz, generates
# valid AND invalid requests for every operation, and asserts the
# responses conform to the spec (status codes, content types, response
# schemas; positive data accepted, negative data rejected).
#
# Mirrors the `contract` job in .github/workflows/ci.yml — same pinned
# Schemathesis version, same flags — so a local pass means a CI pass.
#
# Unlike inventory-storage's sibling script, this service refuses to boot
# without a process-path catalogue file (cmd/execution/main.go loads it as
# a boot-time invariant — a missing or empty file is fatal, never a
# partial catalogue). The script therefore writes the same four-path
# catalogue warehouse-infra publishes
# (config/process-paths/sortable-fc.yaml) into its temp dir and points
# PATH_CATALOGUE_FILE at it.
#
# Requires `st` on PATH:
#   python3 -m pip install --user 'schemathesis==4.28.0'
set -euo pipefail

SCHEMATHESIS_VERSION="4.28.0"
PORT="${CONTRACT_PORT:-18082}"
BASE_URL="http://127.0.0.1:${PORT}"
MAX_EXAMPLES="${CONTRACT_MAX_EXAMPLES:-100}"

if ! command -v st >/dev/null 2>&1; then
  echo "schemathesis (st) is not installed (or not on PATH)."
  echo "Install the exact version CI pins:"
  echo "  python3 -m pip install --user 'schemathesis==${SCHEMATHESIS_VERSION}'"
  exit 1
fi

cd "$(dirname "$0")/.."
TMP="$(mktemp -d)"
BIN="${TMP}/execution"
go build -o "$BIN" ./cmd/execution

cat >"${TMP}/process-paths.yaml" <<'EOF'
building: contract-test
paths:
  - id: PICK
    matchPrefix: pick
    direct: true
    requiredCapabilities: [pick]
  - id: PACK
    matchPrefix: pack
    direct: true
    requiredCapabilities: [pack]
  - id: REBIN
    matchPrefix: rebin
    direct: true
    requiredCapabilities: [rebin]
  - id: SLAM
    matchPrefix: slam
    direct: true
    requiredCapabilities: [slam]
EOF

HTTP_ADDR="127.0.0.1:${PORT}" PATH_CATALOGUE_FILE="${TMP}/process-paths.yaml" "$BIN" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true; rm -rf "${TMP}"' EXIT

# Wait for the server to report healthy (up to ~10s).
for _ in $(seq 1 50); do
  if curl -sf "${BASE_URL}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -sf "${BASE_URL}/healthz" >/dev/null # fail loudly if it never came up

# sealPackage is excluded: success requires the path task to be a PACK
# task whose active claim is owned by exactly this stationId — resource
# state that OpenAPI 3.0.3 cannot express in a request schema. Against
# any other task a schema-valid body correctly returns 422
# wrong-task-type or 409 task-not-owner, which Schemathesis's
# positive-response check cannot know is documented behaviour. The
# conditional IS tested — by the unit tests
# (TestSealPackage_ReturnsErrWrongTaskType in
# internal/application/usecases/usecases_test.go) and the BDD scenarios
# (features/pack_slam.feature, features/task_guards.feature) — this
# exclusion only stops Schemathesis generating the
# unexpressible-but-invalid combinations.
st run apis/openapi.yaml \
  --url "${BASE_URL}" \
  --max-examples "${MAX_EXAMPLES}" \
  --workers 4 \
  --exclude-operation-id sealPackage
