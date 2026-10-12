# Derived from apis/asyncapi.yaml (WorkReleased), the inbound Kafka consumer
# contract in docs/docs/adr/0004-kafka-integration-events-and-envelope.md,
# docs/docs/adr/0017-process-path-catalogue-as-configuration.md,
# docs/docs/adr/0011-gift-wrap-handling-flag.md and
# docs/docs/adr/0036-transfer-task-types-and-facts.md. WorkReleased facts do
# not arrive over REST, so each scenario seeds them through the Kafka
# consumer's message handler (the same ApplyWorkReleased use case production
# runs) and then asserts through REST or the recorded domain events.
Feature: WorkReleased consumer
  Work Planning releases work over Kafka. Each distinct CloudEvents id becomes
  exactly one Task whose type and capabilities come from the process-path
  catalogue; anything the contract does not allow is rejected (dead-lettered)
  rather than defaulted.

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: A release for an unrecognized process path is rejected and creates no task
    When work planning publishes a WorkReleased event "evt-1" with:
      | path_id      | picking-station |
      | work_unit_id | wu-1            |
      | ref          | order-1         |
      | cpt_minutes  | 30              |
    Then the consumer rejects the message
    And the order "wu-1" has 0 "PICK" Tasks
    And the queue depth for "PICK" is 0
    And no "TaskCreated" domain event is recorded

  @bdd
  Scenario: A redelivered WorkReleased event creates the task only once
    When work planning publishes a WorkReleased event "evt-1" with:
      | path_id      | pick-zone-a |
      | work_unit_id | wu-1        |
      | ref          | order-1     |
      | cpt_minutes  | 30          |
    Then the consumer accepts the message
    When work planning publishes the WorkReleased event "evt-1" again
    Then the consumer accepts the message
    And the order "wu-1" has 1 "PICK" Task
    And the queue depth for "PICK" is 1
    And exactly 1 "TaskCreated" domain event is recorded

  @bdd
  Scenario: A released PACK task carries its packing hints through to the sealed package
    Given a Station "pack-01" is registered with capabilities "pack"
    When work planning publishes a WorkReleased event "evt-2" with:
      | path_id      | PACK-lane-2 |
      | work_unit_id | wu-2        |
      | ref          | order-2     |
      | cpt_minutes  | 45          |
      | fragile      | true        |
      | gift_wrap    | true        |
    Then the consumer accepts the message
    And the order "wu-2" has 1 "PACK" Task
    And the "PACK" Task for order "wu-2" requires capabilities "pack"
    And the "PACK" Task for order "wu-2" is flagged fragile true and gift wrap true
    And the "PACK" Task for order "wu-2" is due 45 minutes after the scenario start
    Given Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-1"
    Then the response status is 201
    And the sealed Package is fragile true, gift wrap true and routed to the "FRAGILE_NO_TILT" lane

  @bdd
  Scenario Outline: Completing transfer work publishes exactly the transfer fact its work kind selects
    Given a Station "<station>" is registered with capabilities "<capability>"
    When work planning publishes a WorkReleased event "evt-t1" with:
      | path_id      | <path_id>   |
      | work_unit_id | wu-t1       |
      | ref          | demand-9    |
      | cpt_minutes  | 30          |
      | transfer_ref | transfer-7  |
      | work_kind    | <work_kind> |
      | site_id      | SITE-A      |
      | sku          | sku-42      |
      | quantity     | 4           |
    Then the consumer accepts the message
    And no transfer fact is recorded
    Given Station "<station>" has claimed the next "<task_type>" Task
    When Station "<station>" completes the claimed Task
    Then the response status is 204
    And exactly 1 "<fact>" domain event is recorded
    And the "<fact>" domain event carries:
      | TaskId      | task-1      |
      | TransferRef | transfer-7  |
      | DemandId    | demand-9    |
      | WorkUnitId  | wu-t1       |
      | WorkKind    | <work_kind> |
      | SiteId      | SITE-A      |
      | SKU         | sku-42      |
      | Quantity    | 4           |
    And exactly 1 "TaskCompleted" domain event is recorded

    Examples:
      | path_id    | task_type | station   | capability | work_kind         | fact               |
      | pick-1     | PICK      | pick-01   | pick       | TRANSFER_PICK     | TransferPicked     |
      | dispatch-1 | DISPATCH  | disp-01   | dispatch   | TRANSFER_DISPATCH | TransferDispatched |
      | arrival-1  | ARRIVAL   | arr-01    | arrival    | TRANSFER_ARRIVAL  | TransferArrived    |

  @bdd
  Scenario: A transfer release with an unknown work kind is rejected and creates no task
    When work planning publishes a WorkReleased event "evt-t2" with:
      | path_id      | pick-1            |
      | work_unit_id | wu-t2             |
      | ref          | demand-9          |
      | cpt_minutes  | 30                |
      | transfer_ref | transfer-7        |
      | work_kind    | TRANSFER_TELEPORT |
    Then the consumer rejects the message
    And the order "wu-t2" has 0 "PICK" Tasks
    And no "TaskCreated" domain event is recorded
