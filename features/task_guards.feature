# Derived from apis/openapi.yaml per-operation error responses (RFC 7807
# application/problem+json problem types per ADR-0005) and
# .claude/rules/ubiquitous-language.md, section "Aggregates & invariants":
# claim-next against an unregistered station (404 station-not-found), the
# no-double-complete invariant (409 task-already-completed), only the lease
# owner may renew (409 task-not-owner), renewing with no active claim
# (409 task-not-claimed), sealing without scanned contents
# (422 package-no-scanned-contents), and a repeat SLAM pass
# (409 package-already-processed).
Feature: Task and package lifecycle guards
  The documented failing paths of the task and package lifecycle: guards
  that reject acting on a task nobody owns, acting twice, or sealing and
  re-processing a package — each answered with a typed Problem Details
  document rather than a silent success.

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: Claiming next task from an unregistered station is rejected
    Given a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    When Station "ghost-01" calls claimNext for task type "PICK"
    Then the response status is 404
    And the response is a Problem Details document of type "station-not-found"

  @bdd
  Scenario: Completing an already-completed task is rejected
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-01" completes the claimed Task
    When Station "pick-01" completes the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "task-already-completed"
    And exactly 1 "TaskCompleted" domain event is recorded

  @bdd
  Scenario: Renewing a lease without owning the claim is rejected
    Given a Station "pick-01" is registered with capabilities "pick"
    And a Station "pick-02" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-02" renews the lease on the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-owner"

  @bdd
  Scenario: Renewing the lease on an unclaimed task is rejected
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    When Station "pick-01" tries to renew the lease on the Task for order "order-1"
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-claimed"

  @bdd
  Scenario: Sealing a package without scanned contents is rejected
    Given a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents ""
    Then the response status is 422
    And the response is a Problem Details document of type "package-no-scanned-contents"
    And no "PackageSealed" domain event is recorded

  @bdd
  Scenario: Running the SLAM weigh-check twice on the same package is rejected
    Given a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    When the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    Then the response status is 409
    And the response is a Problem Details document of type "package-already-processed"
    And exactly 1 "LabelApplied" domain event is recorded
