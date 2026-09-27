# Derived from docs/docs/adr/0018-installed-capacity-read-endpoint.md,
# section "Decision", and apis/openapi.yaml operation getInstalledCapacity
# (GET /capacity/{capability}): a read-only projection over the Station
# registry counting how many currently-registered stations hold the
# capability, regardless of occupancy. An unrecognized capability is not
# an error — it has an installed count of 0, the same "count over existing
# rows" contract getQueueDepth uses for an unknown taskType.
Feature: Installed capacity read model
  The installed capacity endpoint projects how many stations CAN work a
  process path — the raw installed ceiling workforce-management's
  CommitShiftPlan enforces plannedHeads against — never how many are
  staffed right now.

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: Installed capacity counts registered stations holding the capability
    Given a Station "pick-01" is registered with capabilities "pick"
    And a Station "pick-02" is registered with capabilities "pick,slam"
    And a Station "pack-01" is registered with capabilities "pack"
    Then the installed capacity for "pick" is 2
    And the installed capacity for "slam" is 1
    And the installed capacity for "pack" is 1

  @bdd
  Scenario: An unrecognized capability has an installed capacity of zero
    Then the installed capacity for "rebin" is 0
