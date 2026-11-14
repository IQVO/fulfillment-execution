# integration_events_rules.feature: scenarios derived from the context
# documentation (warehouse-docs wes/docs/contexts/fulfillment-execution:
# domain-events "Wire conventions", "The catalogue", "Why the events stay thin",
# "TaskCPTMissed re-fires", the Rebin events that stay in-process; use-cases
# RunSlam). The recorded domain events are run through the real integration
# encoder (Publisher.Encode), the same code the transactional outbox uses, so
# the published wire contract is asserted without a broker.
Feature: What this context publishes on the integration topic

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: A published event is a CloudEvents 1.0 envelope whose subject is the aggregate id and Kafka key
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-01" completes the claimed Task
    When the recorded events are encoded for the integration topic
    Then the integration topic carries 1 "TaskCompleted" message
    And the "TaskCompleted" integration message has "specversion" equal to "1.0"
    And the "TaskCompleted" integration message has "type" equal to "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"
    And the "TaskCompleted" integration message has "source" equal to "/warehouse/fulfillment-execution"
    And the "TaskCompleted" integration message has "dataschema" equal to "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1"
    And the "TaskCompleted" integration message has "subject" equal to "task-1"
    And the "TaskCompleted" integration message is keyed by its subject
    And the "TaskCompleted" integration message has "data.task_id" equal to "task-1"
    And the "TaskCompleted" integration message has "data.station_id" equal to "pick-01"

  @bdd
  Scenario: The associate checked in when the completion is published is named on it
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And worker "assoc-77" has checked in at Station "pick-01"
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-01" completes the claimed Task
    When the recorded events are encoded for the integration topic
    Then the "TaskCompleted" integration message has "data.associate_id" equal to "assoc-77"

  @bdd
  Scenario: A station nobody is checked in at publishes no associate
    Given a Station "pick-01" is registered with capabilities "pick"
    And a Station "robot-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And worker "assoc-77" has checked in at Station "pick-01"
    And Station "pick-01" checks out
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-01" completes the claimed Task
    When the recorded events are encoded for the integration topic
    Then the "TaskCompleted" integration message has "data.task_id" equal to "task-1"
    And the "TaskCompleted" integration message has no "data.associate_id"

  @bdd
  Scenario: The completion carries how long the task was held, measured from its claim
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    And the clock advances by 3 minutes
    And Station "pick-01" completes the claimed Task
    When the recorded events are encoded for the integration topic
    Then the "TaskCompleted" integration message has "data.duration_seconds" equal to "180"

  @bdd
  Scenario: A passing SLAM publishes only the manifest on the integration topic
    Given a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    When the recorded events are encoded for the integration topic
    Then the integration topic carries 1 "PackageManifested" message
    And the integration topic carries 0 "PackageSealed" messages
    And the integration topic carries 0 "LabelApplied" messages
    And the integration topic carries 0 "PackageDiverted" messages
    And the integration topic carries 0 "WeightDiscrepancyDetected" messages
    And the "PackageManifested" integration message has "type" equal to "com.warehouse.wes.fulfillment-execution.package.PackageManifested"
    And the "PackageManifested" integration message has "subject" equal to "pkg-1"
    And the "PackageManifested" integration message has "data.package_id" equal to "pkg-1"
    And the "PackageManifested" integration message has "data.order_ref" equal to "order-1"

  @bdd
  Scenario: A diverted package publishes the measurement and the routing decision and no manifest
    Given a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 1.80 against an expected weight of 2.00
    When the recorded events are encoded for the integration topic
    Then the integration topic carries 1 "WeightDiscrepancyDetected" message
    And the integration topic carries 1 "PackageDiverted" message
    And the integration topic carries 0 "PackageManifested" messages
    And the "WeightDiscrepancyDetected" integration message has "data.package_id" equal to "pkg-1"
    And the "WeightDiscrepancyDetected" integration message has "data.expected_g" equal to "2"
    And the "WeightDiscrepancyDetected" integration message has "data.actual_g" equal to "1.8"
    And the "PackageDiverted" integration message has "subject" equal to "pkg-1"

  @bdd
  Scenario: The Rebin facts never leave the process
    When line "line-1" of order "order-1" arrives at Rebin; the order needs lines "line-1"
    Then exactly 1 "ItemArrivedAtRebin" domain event is recorded
    And exactly 1 "OrderConsolidated" domain event is recorded
    When the recorded events are encoded for the integration topic
    Then the integration topic carries nothing

  @bdd
  Scenario: Each CPT-missed pass publishes the order, the leg and the CPT under a new event id
    Given a "PICK" Task for order "order-1" with a CPT 5 minutes from now requiring capabilities "pick"
    When the clock advances by 6 minutes
    And the CPT-missed sweep runs
    And the CPT-missed sweep runs
    And the recorded events are encoded for the integration topic
    Then the integration topic carries 2 "TaskCPTMissed" messages
    And the "TaskCPTMissed" integration message has "data.order_ref" equal to "order-1"
    And the "TaskCPTMissed" integration message has "data.task_type" equal to "PICK"
    And the "TaskCPTMissed" integration message has "data.cpt" equal to "2026-01-01T12:05:00Z"
    And the "TaskCPTMissed" integration message has "subject" equal to "task-1"
    And the "TaskCPTMissed" integration messages have distinct CloudEvents ids
