# MCP behavioral evals for the fulfillment-execution tool surface.
#
# Each scenario drives tools/call over the REAL Streamable HTTP handler
# with a connected SDK client — exactly the call a model host makes — and
# pins the structured result and the state side effects. Arguments are
# deliberately model-realistic: extra keys, wrong types, unknown ids.
#
# The report tools are wired to a fake reports REST client in the harness
# (the real server registers them only when REPORTS_BASE_URL is set), so
# their contracts are evaluated here rather than left to production.
# Pinned reality: the SDK-derived schemas mark EVERY report-tool argument
# required — including taskType/stationId, whose descriptions call them
# optional filters — so "no filter" is an explicit empty string, never an
# omitted key.
#
# Derived from the tool contracts documented in:
#   - internal/adapters/inbound/mcp/tools.go, report_tool.go and
#     on_time_to_cpt_tool.go (tool descriptions and semantics: queue depth
#     counts Pending only; best = earliest-CPT candidate or null; the
#     write tool is bounded by the at-most-once and ownership invariants)
#   - docs/docs/mcp/governance-charter.md (§2 tool curation: intent-level
#     tools; §4 write tools must be safe-by-construction)
#   - docs/docs/adr/0008-mcp-inbound-adapter.md (this repo is the fleet's
#     reference MCP implementation) and ADR-0026 (on-time-to-CPT KPI)

Feature: MCP tool behavioral evals
  The fulfillment-execution MCP tools expose this bounded context to AI
  agents: queue depth per process path, the most urgent claimable task,
  lapsing-lease diagnostics, a write tool that completes a claimed task,
  and two curated throughput data-product tools. An agent relying on them
  must get the same semantics the REST API guarantees, through the
  schema-decoded argument path a model host actually uses.

  Background:
    Given the MCP server is running with the canonical eval state (earliest PICK claimed by s1, one PICK and one PACK still pending)

  Scenario: Queue depth counts Pending tasks only
    The claimed PICK task is out of the pool, so PICK depth is 1, not 2.
    When I call the tool "get_queue_status" with argument "processPath" = "PICK"
    Then the tool call succeeds
    And the structured result field "processPath" is "PICK"
    And the structured result field "depth" is 1
    When I call the tool "get_queue_status" with argument "processPath" = "SLAM"
    Then the tool call succeeds
    And the structured result field "depth" is 0

  Scenario: An unknown process path is a clean tool error
    When I call the tool "get_queue_status" with argument "processPath" = "FLYING"
    Then the tool call reports a problem mentioning "must be PICK, PACK or SLAM"

  Scenario: Model chatter in the arguments is rejected, not ignored
    The typed tool schemas are strict (additionalProperties: false, the
    SDK default): a host forwarding stray model-generated keys gets a
    clean schema validation error rather than a silent ignore.
    When I call the tool "get_queue_status" with arguments
      | processPath   | PICK                |
      | model_chatter | maybe backed up?    |
      | step          | 2                   |
    Then the tool call reports a problem mentioning "model_chatter"

  Scenario: A wrong-typed argument is rejected without coercion
    When I call the tool "get_queue_status" with argument "processPath" = 42
    Then the tool call does not succeed silently

  Scenario: The best claimable task is the earliest-CPT candidate
    With the earliest PICK claimed and gone from the pool, the best (and
    only) candidate is the later PICK.
    When I call the tool "find_claimable_work" with argument "processPath" = "PICK"
    Then the tool call succeeds
    And the structured result field "candidateCount" is 1
    And the structured result field "best.type" is "PICK"
    And the structured result field "best.orderRef" is "o-late"
    And the structured result field "best.cpt" is "2026-01-01T14:00:00Z"

  Scenario: An empty queue reports zero candidates and a null best
    Pinned: "nothing claimable" is a successful, well-formed answer
    (best: null), not an error and not an omitted field.
    When I call the tool "find_claimable_work" with argument "processPath" = "SLAM"
    Then the tool call succeeds
    And the structured result field "candidateCount" is 0
    And the structured result field "best" is null

  Scenario: An expiring lease is flagged with a reason
    The seeded claim's lease expires five minutes after it was taken, so
    a ten-minute window must flag it.
    When I call the tool "diagnose_stuck_tasks" with arguments
      | withinSeconds | 600 |
    Then the tool call succeeds
    And the structured result field "count" is 1
    And the structured result array field "tasks" has an entry whose "leaseStationId" is "s1"
    And the structured result array field "tasks" has an entry whose "leaseExpiry" is "2026-01-01T12:05:00Z"

  Scenario: A healthy lease is not flagged
    withinSeconds 0 means "only already-expired leases"; the seeded lease
    still has five minutes to run.
    When I call the tool "diagnose_stuck_tasks" with arguments
      | withinSeconds | 0 |
    Then the tool call succeeds
    And the structured result field "count" is 0
    And the structured result array field "tasks" has 0 entries

  Scenario: Completing a claimed task publishes TaskCompleted and clears the diagnostics
    When I call the tool "complete_task" with the seeded claimed task id and station "s1"
    Then the tool call succeeds
    And the structured result field "completed" is true
    And the structured result field "stationId" is "s1"
    And the domain event "TaskCompleted" was published
    When I call the tool "diagnose_stuck_tasks" with arguments
      | withinSeconds | 600 |
    Then the tool call succeeds
    And the structured result field "count" is 0

  Scenario: A second completion is rejected by the at-most-once invariant
    When I call the tool "complete_task" with the seeded claimed task id and station "s1"
    Then the tool call succeeds
    When I call the tool "complete_task" with the seeded claimed task id and station "s1"
    Then the tool call reports a problem mentioning "already completed"

  Scenario: A station that does not own the claim is rejected
    When I call the tool "complete_task" with the seeded claimed task id and station "intruder"
    Then the tool call reports a problem mentioning "does not own the claim"

  Scenario: Completing an unknown task is a clean tool error
    When I call the tool "complete_task" with arguments
      | taskId    | t-does-not-exist |
      | stationId | s1               |
    Then the tool call reports a problem mentioning "not found"

  Scenario: An empty task id is a clean tool error
    When I call the tool "complete_task" with arguments
      | taskId    |    |
      | stationId | s1 |
    Then the tool call reports a problem mentioning "taskId and stationId are required"

  Scenario: Throughput report rows pass through from the reports service
    Given the reports service returns throughput rows
      | taskType | packagesManifested | packagesOnTimeToCPT | packagesLateToCPT |
      | PICK     | 0                  | 0                   | 0                 |
      | SLAM     | 3                  | 2                   | 1                 |
    When I call the tool "get_fulfillment_throughput_report" with arguments
      | from        | 2026-06-01T00:00:00Z |
      | to          | 2026-06-02T00:00:00Z |
      | taskType    | SLAM                 |
      | granularity | hour                 |
      | stationId   |                      |
    Then the tool call succeeds
    And the structured result array field "rows" has 2 entries
    And the structured result array field "rows" has an entry whose "taskType" is "SLAM"
    And the structured result array field "rows" has an entry whose "packagesManifested" is 3

  Scenario: On-time-to-CPT aggregates buckets into one rate
    Given the reports service returns throughput rows
      | taskType | packagesManifested | packagesOnTimeToCPT | packagesLateToCPT |
      | SLAM     | 3                  | 2                   | 1                 |
      | SLAM     | 1                  | 1                   | 0                 |
    When I call the tool "get_on_time_to_cpt" with arguments
      | from      | 2026-06-01T00:00:00Z |
      | to        | 2026-06-02T00:00:00Z |
      | taskType  |                      |
      | stationId |                      |
    Then the tool call succeeds
    And the structured result field "packagesManifested" is 4
    And the structured result field "packagesOnTimeToCPT" is 3
    And the structured result field "packagesLateToCPT" is 1
    And the structured result field "onTimeRate" is the decimal 0.75

  Scenario: A missing report window is a clean tool error
    The schema lets empty strings through; the handler is the layer that
    demands a real RFC3339 window.
    When I call the tool "get_on_time_to_cpt" with arguments
      | from      | |
      | to        | |
      | taskType  | |
      | stationId | |
    Then the tool call reports a problem mentioning "from and to are required"

  Scenario: A reports-service failure surfaces as a clean tool error
    The tool does not swallow an upstream outage: a failing reports REST
    call becomes a tool-level error, never an empty-but-successful report.
    Given the reports service is failing
    When I call the tool "get_fulfillment_throughput_report" with arguments
      | from      | 2026-06-01T00:00:00Z |
      | to        | 2026-06-02T00:00:00Z |
      | taskType  |                      |
      | stationId |                      |
    Then the tool call reports a problem mentioning "reports client"
