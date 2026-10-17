---
id: tools
title: MCP tools, resources and prompts
sidebar_label: MCP tools
description: Every tool, resource and prompt the cmd/mcp server exposes over Streamable HTTP — input fields as advertised by tools/list, read-only or write annotation, output shape and error behaviour — captured from a live run.
---

# MCP tools, resources and prompts

`cmd/mcp` is a separate binary that serves the Model Context Protocol over
**Streamable HTTP** only, at both `/` and `/mcp` (plus `GET /healthz`), on
`MCP_ADDR` (default `:8090`). It is unauthenticated
([ADR-0022](../adr/0022-remove-rest-mcp-auth.md)) and reuses the same use
cases and the same database as the REST service
([ADR-0008](../adr/0008-mcp-inbound-adapter.md)). The fleet rules it follows
are on the [Governance Charter](./governance-charter.md).

The schemas below were captured from `tools/list` against a locally built
`cmd/mcp` (go-sdk v1.8.0, protocol version `2025-06-18`), not written from
memory. Every schema is strict (`additionalProperties: false`). Optional
filters carry `omitempty`, so the SDK lists only the truly required fields in
`required` (gap fix D16; before it, every field was required and a call that
left an optional filter out was rejected). The sensor
`TestMCPOptionalToolInputsCarryOmitempty` keeps it that way.

Server info: name `fulfillment-execution-mcp`, version `1.0.0`. Its
`instructions` string lists the read tools, names `complete_task` as the one
WRITE tool (it publishes `TaskCompleted`; confirm before calling it) and
points at the read-only `triage_backlog` prompt. It no longer says
"Read-only access" (gap fix D17, pinned by
`TestEvalConformance_InstructionsNameTheWriteTool`).

## Tools

| Tool | Annotation | Registered | Input (required unless marked optional) | Output |
| --- | --- | --- | --- | --- |
| `get_queue_status` | read-only | always | `processPath` (string): `PICK`, `PACK`, `SLAM`, `RELOCATE` or `REPLENISH` | `{"processPath","depth"}`: number of Pending tasks (same as REST `GET /queues/{taskType}/depth`) |
| `find_claimable_work` | read-only | always | `processPath` (string): `PICK`, `PACK`, `SLAM`, `RELOCATE` or `REPLENISH` | `{"processPath","candidateCount","best"}`; `best` is the earliest-CPT claimable task or `null` when nothing is claimable (not an error). A RELOCATE `best` carries `relocation` `{planId, moveSeq, kind, sku, fromBin?, toBin?, ref}` ([ADR-0047](../adr/0047-relocate-tasks-execute-slot-plan-moves.md)); a REPLENISH `best` carries `replenishment` `{requestId, sku, fromBin, toBin, quantity, priority}` ([ADR-0050](../adr/0050-replenish-tasks-execute-pick-face-replenishment.md)) |
| `diagnose_stuck_tasks` | read-only | always | `withinSeconds` (integer): flag claimed tasks whose lease expires within this many seconds; `0` = only already expired | `{"count","tasks":[{taskId,type,leaseStationId,leaseExpiry,reason}]}` |
| `complete_task` | **write**: `readOnlyHint: false`, `destructiveHint: true`, `idempotentHint: false` | always | `taskId` (string), `stationId` (string): the station holding the active claim; optional, RELOCATE and REPLENISH tasks only: `quantity` (integer, at least 1), `fromBin`, `toBin` (strings): the operator's count and the scanned bin (RELOCATE: the side the move left open; REPLENISH: an optional scan that must equal the planned bin) ([ADR-0047](../adr/0047-relocate-tasks-execute-slot-plan-moves.md), [ADR-0050](../adr/0050-replenish-tasks-execute-pick-face-replenishment.md)) | `{"taskId","stationId","completed":true}`. Runs `CompleteTask` exactly like `POST /tasks/{id}/complete`: publishes `TaskCompleted` (through `outbox_events` with Postgres and `EVENT_PUBLISHER=kafka`) and increments `fulfillment.tasks.completed`. |
| `get_fulfillment_throughput_report` | read-only | only when `REPORTS_BASE_URL` is set | `from`, `to` (RFC 3339, `[from, to)`); optional: `taskType` (`PICK`, `PACK`, `SLAM`), `stationId`, `granularity` (`hour`, the default) | `{"rows":[...]}`, the body of `GET /reports/throughput` on `cmd/fulfillment-reports` |
| `get_on_time_to_cpt` | read-only | only when `REPORTS_BASE_URL` is set | `from`, `to` (RFC 3339); optional: `taskType`, `stationId` | `{"from","to","packagesManifested","packagesOnTimeToCPT","packagesLateToCPT","onTimeRate"}` summed over the window; `onTimeRate` is `0` when nothing was manifested ([ADR-0026](../adr/0026-on-time-to-cpt-kpi.md)) |

Read-only tools carry `readOnlyHint: true`. No tool takes a station
capability set, and no tool can claim, release or create work.

### Errors

Failures come back as a tool result with `isError: true` and the error text,
not as a JSON-RPC error. Examples from the local run:

- `get_queue_status` with `REBIN`:
  `unknown process path "REBIN": must be PICK, PACK, SLAM, RELOCATE or REPLENISH` (the
  MCP queue tools accept only those paths).
- `get_on_time_to_cpt` with the reports service down:
  `reports client: request: Get "http://127.0.0.1:1/reports/throughput?...": dial tcp 127.0.0.1:1: connect: connection refused`.
- `complete_task` surfaces the domain error verbatim: `task: already completed`,
  `task: station does not own the claim`, `task: not claimed`,
  `usecases: task not found`; an empty `taskId` or `stationId` gives
  `taskId and stationId are required`. A RELOCATE task without the
  confirmation gives `task: a RELOCATE task completes only with the confirmed
  quantity and the bins the move left open`; a bad scan gives `task: invalid
  relocation confirmation: ...`; a REPLENISH task without the quantity gives
  `task: a REPLENISH task completes only with the confirmed quantity` and a bad
  quantity or scan `task: invalid replenishment confirmation: ...`; these inputs
  on another task give
  `task: quantity, from_bin and to_bin apply only to RELOCATE and REPLENISH tasks`. The tool
  description tells the model never to invent these values.

Each call runs inside an OTel span `mcp.tool <name>` with attribute
`mcp.tool.name` (see [Observability](../operations/observability.md#traces)).

## Resources

Static resources only; `resources/templates/list` is empty.

| URI | MIME type | Content |
| --- | --- | --- |
| `queue://fulfillment/PICK/status` | `application/json` | `{"processPath":"PICK","depth":N}` |
| `queue://fulfillment/PACK/status` | `application/json` | same for PACK |
| `queue://fulfillment/SLAM/status` | `application/json` | same for SLAM |

## Prompts

| Prompt | Arguments | What it returns |
| --- | --- | --- |
| `triage_backlog` | none | A user message with the backlog-triage procedure: call `get_queue_status` for PICK, PACK and SLAM, `find_claimable_work` for the deepest queue, `diagnose_stuck_tasks` with `withinSeconds=0` and then a small window; how to read the results; when to escalate; and that triage is read-only. |

## Calling it

```bash
# 1. initialize: the response carries the Mcp-Session-Id header
curl -sS -i -X POST localhost:8090/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'

# 2. call a tool with that session id
curl -sS -X POST localhost:8090/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H "Mcp-Session-Id: $SESSION" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_queue_status","arguments":{"processPath":"PICK"}}}'
```

Real response to step 2 on an empty in-memory store:

```text
event: message
data: {"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"{\"depth\":0,\"processPath\":\"PICK\"}"}],"structuredContent":{"depth":0,"processPath":"PICK"}}}
```

Sessions live in the process's memory, so the chart keeps the MCP Deployment
at one replica with no HPA. Without `DATABASE_URL`, `cmd/mcp` has its own
in-memory store and sees none of the tasks a separately running
`cmd/execution` holds.

## Who calls it

warehouse-ops-agent reads `FULFILLMENT_EXECUTION_MCP_ENDPOINT`
(`internal/config/config.go` in that repo); warehouse-infra fills it from
`local.mcp_endpoint["fulfillment-execution"]` when `deploy_mcp_servers` is on
(`terraform/ops-agent.tf`).
