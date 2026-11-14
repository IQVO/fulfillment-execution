# work_released_rules.feature: scenarios derived from the context documentation
# (warehouse-docs wes/docs/contexts/fulfillment-execution: use-cases #18
# "ApplyWorkReleased", aggregate-design-canvas Task description (sourceOrderId,
# sourceLineNo), the troubleshooting "Where released work can get lost" flow,
# ADR-0040 and ADR-0041). The facts arrive over Kafka, so they are seeded through
# the consumer's message handler and asserted through REST and the integration
# encoder.
Feature: What the WorkReleased consumer accepts and what it tells the rest of the fleet afterwards

  Background:
    Given a running Fulfillment Execution service
    And a Station "pick-01" is registered with capabilities "pick"

  @bdd
  Scenario: A CloudEvent of another type on the topic is ignored
    When work planning publishes a CloudEvent "evt-x" of type "com.warehouse.wes.work-planning.workunit.WorkCancelled"
    Then the consumer accepts the message
    And the queue depth for "PICK" is 0
    And no "TaskCreated" domain event is recorded

  @bdd
  Scenario Outline: A message that is not a CloudEvent is rejected and creates no task
    When work planning publishes the raw message <payload>
    Then the consumer rejects the message
    And the queue depth for "PICK" is 0
    And no "TaskCreated" domain event is recorded

    Examples:
      | payload                                                                                           |
      | {"event_id":"evt-1","path_id":"pick-zone-a","work_unit_id":"wu-1","ref":"order-1"}                 |
      | {"specversion":"1.0","id":"evt-1","type":"com.warehouse.wes.work-planning.workunit.WorkReleased"   |

  @bdd
  Scenario: Completing released order work tells work planning the work unit and order management the order line
    When work planning publishes a WorkReleased event "evt-1" with:
      | path_id      | pick-zone-a    |
      | work_unit_id | order-5-line-2 |
      | ref          | order-5        |
      | line_no      | 2              |
      | cpt_minutes  | 30             |
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-01" completes the claimed Task
    And the recorded events are encoded for the integration topic
    Then the "TaskCompleted" integration message has "data.work_unit_id" equal to "order-5-line-2"
    And the "TaskCompleted" integration message has "data.order_ref" equal to "order-5"
    And the "TaskCompleted" integration message has "data.line_no" equal to "2"
    And the "TaskCompleted" integration message has "data.task_type" equal to "PICK"

  @bdd
  Scenario Outline: The source line is published only when it is a real line number within 1..2147483647
    When work planning publishes a WorkReleased event "evt-1" with:
      | path_id      | pick-zone-a    |
      | work_unit_id | order-5-line-2 |
      | ref          | order-5        |
      | line_no      | <line_no>      |
      | cpt_minutes  | 30             |
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-01" completes the claimed Task
    And the recorded events are encoded for the integration topic
    Then the response status is 204
    And <assertion>
    And the "TaskCompleted" integration message has "data.order_ref" equal to "order-5"

    Examples:
      | line_no    | assertion                                                                       |
      | 1          | the "TaskCompleted" integration message has "data.line_no" equal to "1"          |
      | 2147483647 | the "TaskCompleted" integration message has "data.line_no" equal to "2147483647" |
      | 2147483648 | the "TaskCompleted" integration message has no "data.line_no"                    |
      | 0          | the "TaskCompleted" integration message has no "data.line_no"                    |

  @bdd
  Scenario: Transfer work is not order work and publishes neither an order nor a line
    When work planning publishes a WorkReleased event "evt-t1" with:
      | path_id      | pick-1        |
      | work_unit_id | wu-t1         |
      | ref          | demand-9      |
      | line_no      | 3             |
      | cpt_minutes  | 30            |
      | transfer_ref | transfer-7    |
      | work_kind    | TRANSFER_PICK |
      | site_id      | SITE-A        |
      | sku          | sku-42        |
      | quantity     | 4             |
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-01" completes the claimed Task
    And the recorded events are encoded for the integration topic
    Then the "TaskCompleted" integration message has "data.work_unit_id" equal to "wu-t1"
    And the "TaskCompleted" integration message has no "data.order_ref"
    And the "TaskCompleted" integration message has no "data.line_no"
    And the integration topic carries 1 "TransferPicked" message
    And the "TransferPicked" integration message has "type" equal to "com.warehouse.wes.fulfillment-execution.transfer.TransferPicked"
    And the "TransferPicked" integration message has "data.transfer_ref" equal to "transfer-7"
    And the "TransferPicked" integration message has "data.demand_id" equal to "demand-9"
    And the "TransferPicked" integration message has "data.work_unit_id" equal to "wu-t1"
    And the "TransferPicked" integration message has "data.work_kind" equal to "TRANSFER_PICK"
    And the "TransferPicked" integration message has "data.site_id" equal to "SITE-A"
    And the "TransferPicked" integration message has "data.sku" equal to "sku-42"
    And the "TransferPicked" integration message has "data.quantity" equal to "4"
    And the "TransferPicked" integration message is keyed by its subject
