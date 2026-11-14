# task_lease_rules.feature: scenarios derived from the context documentation
# (warehouse-docs wes/docs/contexts/fulfillment-execution: business-context,
# use-cases, aggregate-design-canvas invariants T1-T6 and corrective policies).
# Each scenario names the rule it pins; the existing suite covers the happy
# paths, these pin the boundaries and the policies.
Feature: The lease rules of the Task aggregate and its two sweeps

  Background:
    Given a running Fulfillment Execution service

  # --- ClaimNext ---------------------------------------------------------------

  @bdd
  Scenario: A claim holds the task for exactly five minutes
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 120 minutes from now requiring capabilities "pick"
    When Station "pick-01" calls claimNext for task type "PICK"
    Then the response status is 200
    And the response field "status" is "CLAIMED"
    And the response field "leaseExpiry" is "2026-01-01T12:05:00Z"

  @bdd
  Scenario: A station holding more capabilities than the task requires can still take it
    Given a Station "multi-01" is registered with capabilities "pick,hazmat,fragile"
    And a "PICK" Task for order "order-1" with a CPT 120 minutes from now requiring capabilities "pick"
    When Station "multi-01" calls claimNext for task type "PICK"
    Then the response status is 200
    And the claimed Task is the one for order "order-1"

  @bdd
  Scenario: Claiming does not require the station to be occupied
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 120 minutes from now requiring capabilities "pick"
    When Station "pick-01" calls claimNext for task type "PICK"
    Then the response status is 200
    When Station "pick-01" completes the claimed Task
    Then the response status is 204

  # --- Lease boundaries (T3, T5, T6) ---------------------------------------------

  @bdd
  Scenario Outline: A lease is still live one second before its expiry
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 120 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When the clock advances by 299 seconds
    And <action>
    Then the response status is 204

    Examples:
      | action                                                |
      | Station "pick-01" completes the claimed Task          |
      | Station "pick-01" renews the lease on the claimed Task |

  @bdd
  Scenario Outline: A lease has lapsed from its expiry instant on
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 120 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When the clock advances by 300 seconds
    And <action>
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-claimed"
    And no "TaskCompleted" domain event is recorded

    Examples:
      | action                                                |
      | Station "pick-01" completes the claimed Task          |
      | Station "pick-01" renews the lease on the claimed Task |

  @bdd
  Scenario: A lapsed claim is taken over by the next station without waiting for the sweep
    Given a Station "pick-01" is registered with capabilities "pick"
    And a Station "pick-02" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 120 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When the clock advances by 6 minutes
    And Station "pick-02" calls claimNext for task type "PICK"
    Then the response status is 200
    And the claimed Task is leased to Station "pick-02"
    When Station "pick-01" tries to renew the lease on the Task for order "order-1"
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-owner"

  @bdd
  Scenario: A completed task rejects every further renewal
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 120 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-01" completes the claimed Task
    When Station "pick-01" renews the lease on the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "task-already-completed"

  # --- Lease expiry sweep ----------------------------------------------------------

  @bdd
  Scenario Outline: The sweep frees a lease at its expiry instant and not before
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 120 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When the clock advances by <seconds> seconds
    And the lease expiry sweep runs
    Then <freed> leases were freed
    And the queue depth for "PICK" is <depth>

    Examples:
      | seconds | freed | depth |
      | 299     | 0     | 0     |
      | 300     | 1     | 1     |

  @bdd
  Scenario: The sweep frees only the lapsed leases and never frees one twice
    Given a Station "pick-01" is registered with capabilities "pick"
    And a Station "pick-02" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And a "PICK" Task for order "order-2" with a CPT 40 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-02" has claimed the next "PICK" Task
    And the clock advances by 4 minutes
    And Station "pick-01" tries to renew the lease on the Task for order "order-1"
    When the clock advances by 2 minutes
    And the lease expiry sweep runs
    Then 1 lease was freed
    And exactly 1 "LeaseExpired" domain event is recorded
    And the queue depth for "PICK" is 1
    When the lease expiry sweep runs
    Then 0 leases were freed
    And exactly 1 "LeaseExpired" domain event is recorded
    When Station "pick-01" tries to renew the lease on the Task for order "order-1"
    Then the response status is 204

  @bdd
  Scenario: A task freed by the sweep re-enters the pool as a new TaskAvailable fact
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 120 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    And exactly 1 "TaskAvailable" domain event is recorded
    When the clock advances by 6 minutes
    And the lease expiry sweep runs
    Then exactly 1 "LeaseExpired" domain event is recorded
    And exactly 2 "TaskAvailable" domain events are recorded

  # --- CPT-missed sweep ------------------------------------------------------------

  @bdd
  Scenario Outline: A task is reported as missing its CPT from the CPT instant and not before
    Given a "PICK" Task for order "order-1" with a CPT 5 minutes from now requiring capabilities "pick"
    When the clock advances by <seconds> seconds
    And the CPT-missed sweep runs
    Then <reported> tasks were reported as CPT-missed

    Examples:
      | seconds | reported |
      | 299     | 0        |
      | 300     | 1        |

  @bdd
  Scenario: The CPT sweep reports every open overdue task, claimed or pending, and nothing else
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-done" with a CPT 5 minutes from now requiring capabilities "pick"
    And a "PICK" Task for order "order-held" with a CPT 6 minutes from now requiring capabilities "pick"
    And a "PICK" Task for order "order-idle" with a CPT 7 minutes from now requiring capabilities "pick"
    And a "PICK" Task for order "order-future" with a CPT 60 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-01" completes the claimed Task
    And the clock advances by 3 minutes
    And Station "pick-01" has claimed the next "PICK" Task
    When the clock advances by 4 minutes
    And the CPT-missed sweep runs
    Then 2 tasks were reported as CPT-missed
    And exactly 2 "TaskCPTMissed" domain events are recorded
    And the "TaskCPTMissed" domain event carries:
      | OrderRef | order-held |
    And the "TaskCPTMissed" domain event carries:
      | OrderRef | order-idle |
