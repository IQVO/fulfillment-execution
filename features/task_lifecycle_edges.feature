# Task lifecycle edges around leases, ownership and capability matching.
#
# Derived from:
#   - apis/openapi.yaml: POST /stations/{id}/claim-next leases the
#     earliest-CPT pending task of the requested type the station can work
#     (409 no-claimable-task otherwise); POST /tasks/{id}/renew-lease and
#     /complete need the claiming station (409 task-not-owner) and a live
#     claim (409 task-not-claimed); POST /tasks/expire-leases frees every
#     lapsed lease and returns the task to the pool
#   - .claude/rules/domain-model.md: a Task requires ALL of its
#     requiredCapabilities from the claiming Station; a lease lasts 5
#     minutes and is renewable only by its owner

Feature: Working a task through its lease
  As a station
  I want to hold a task only while my lease lives and only if I can do the work
  So that nobody completes work they do not hold and nothing is stranded

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: A station needs every capability the task requires
    Given a Station "pick-01" is registered with capabilities "pick"
    And a Station "pick-02" is registered with capabilities "pick,hazmat"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick,hazmat"
    When Station "pick-01" calls claimNext for task type "PICK"
    Then the response status is 409
    And the response is a Problem Details document of type "no-claimable-task"
    When Station "pick-02" calls claimNext for task type "PICK"
    Then the response status is 200
    And the claimed Task is the one for order "order-1"

  @bdd
  Scenario: Claiming one task type never takes another type's work
    Given a Station "multi-01" is registered with capabilities "pick,pack"
    And a "PACK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pack"
    When Station "multi-01" calls claimNext for task type "PICK"
    Then the response status is 409
    And the response is a Problem Details document of type "no-claimable-task"
    When Station "multi-01" calls claimNext for task type "PACK"
    Then the response status is 200

  @bdd
  Scenario: Completing after the lease lapsed is a task-not-claimed problem
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When the clock advances by 6 minutes
    And Station "pick-01" completes the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-claimed"
    And no "TaskCompleted" domain event is recorded

  @bdd
  Scenario: Renewing after the lease lapsed is a task-not-claimed problem
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When the clock advances by 6 minutes
    And Station "pick-01" renews the lease on the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-claimed"

  @bdd
  Scenario: After an expiry sweep another station takes over and the first loses its claim
    Given a Station "pick-01" is registered with capabilities "pick"
    And a Station "pick-02" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When the clock advances by 6 minutes
    And the lease expiry sweep runs
    And Station "pick-02" calls claimNext for task type "PICK"
    Then the response status is 200
    And the claimed Task is leased to Station "pick-02"
    When Station "pick-01" completes the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-owner"

  @bdd
  Scenario: The new owner can complete the task after a takeover
    Given a Station "pick-01" is registered with capabilities "pick"
    And a Station "pick-02" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    And the clock advances by 6 minutes
    And the lease expiry sweep runs
    And Station "pick-02" has claimed the next "PICK" Task
    When Station "pick-02" completes the claimed Task
    Then the response status is 204
    And exactly 1 "TaskCompleted" domain event is recorded
    And the queue depth for "PICK" is 0

  @bdd
  Scenario: Each renewal restarts the five-minute lease
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When the clock advances by 4 minutes
    And Station "pick-01" renews the lease on the claimed Task
    And the clock advances by 4 minutes
    And Station "pick-01" renews the lease on the claimed Task
    Then the response status is 204
    When the clock advances by 4 minutes
    And the lease expiry sweep runs
    Then 0 leases were freed

  @bdd
  Scenario: A claim is announced once and a refused claim announces nothing
    Given a Station "pick-01" is registered with capabilities "pick"
    When Station "pick-01" calls claimNext for task type "PICK"
    Then the response status is 409
    And no "TaskClaimed" domain event is recorded
    Given a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    When Station "pick-01" calls claimNext for task type "PICK"
    Then exactly 1 "TaskClaimed" domain events are recorded

  @bdd
  Scenario: Tasks are handed out earliest CPT first regardless of creation order
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-late" with a CPT 90 minutes from now requiring capabilities "pick"
    And a "PICK" Task for order "order-early" with a CPT 10 minutes from now requiring capabilities "pick"
    And a "PICK" Task for order "order-mid" with a CPT 40 minutes from now requiring capabilities "pick"
    When Station "pick-01" has claimed the next "PICK" Task
    Then the claimed Task is the one for order "order-early"
    When Station "pick-01" completes the claimed Task
    And Station "pick-01" has claimed the next "PICK" Task
    Then the claimed Task is the one for order "order-mid"
