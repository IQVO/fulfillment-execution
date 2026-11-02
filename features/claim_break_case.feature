# Derived from apis/openapi.yaml: AnyTaskType includes BREAK_CASE (ADR 0055),
# so claimNextTask accepts it. A BREAK_CASE task is released by work planning
# (WorkReleased.data.case_break, ADR 0054), never created through POST /tasks,
# and completes only through POST /tasks/{id}/case-breaks.
Feature: A station claims a BREAK_CASE task through claim-next
  An each pick that finds only a sealed case gets a BREAK_CASE task. The
  station with the break capability pulls it like any other task.

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: A station with the break capability claims a released BREAK_CASE task
    Given a Station "break-01" is registered with capabilities "break"
    When work planning publishes a WorkReleased event "evt-b1" with:
      | path_id                  | break-zone-a |
      | work_unit_id             | wu-b1        |
      | ref                      | order-b1     |
      | cpt_minutes              | 20           |
      | case_break.lpn           | LPN-1        |
      | case_break.sku           | SKU-1        |
      | case_break.loose_bin     | BIN-1        |
      | case_break.pick_task_id  | pick-1       |
    Then the consumer accepts the message
    And the queue depth for "BREAK_CASE" is 1
    When Station "break-01" calls claimNext for task type "BREAK_CASE"
    Then the response status is 200
    And the response field "type" is "BREAK_CASE"
    And the response field "status" is "CLAIMED"

  @bdd
  Scenario: A station without the break capability gets no claimable task
    Given a Station "pick-01" is registered with capabilities "pick"
    When work planning publishes a WorkReleased event "evt-b2" with:
      | path_id                  | break-zone-a |
      | work_unit_id             | wu-b2        |
      | ref                      | order-b2     |
      | cpt_minutes              | 20           |
      | case_break.lpn           | LPN-2        |
      | case_break.sku           | SKU-1        |
      | case_break.loose_bin     | BIN-1        |
      | case_break.pick_task_id  | pick-2       |
    Then the consumer accepts the message
    When Station "pick-01" calls claimNext for task type "BREAK_CASE"
    Then the response status is 409
    And the response is a Problem Details document of type "no-claimable-task"
    And the queue depth for "BREAK_CASE" is 1

  @bdd
  Scenario: A claimed BREAK_CASE task cannot be completed through the plain complete
    Given a Station "break-01" is registered with capabilities "break"
    When work planning publishes a WorkReleased event "evt-b3" with:
      | path_id                  | break-zone-a |
      | work_unit_id             | wu-b3        |
      | ref                      | order-b3     |
      | cpt_minutes              | 20           |
      | case_break.lpn           | LPN-3        |
      | case_break.sku           | SKU-1        |
      | case_break.loose_bin     | BIN-1        |
      | case_break.pick_task_id  | pick-3       |
    Then the consumer accepts the message
    Given Station "break-01" has claimed the next "BREAK_CASE" Task
    When Station "break-01" completes the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "case-break-completion-required"

  @bdd
  Scenario: BREAK_CASE is not creatable through POST /tasks
    When a POST is sent to "/tasks" with the JSON body {"type":"BREAK_CASE","cpt":"2026-01-01T18:00:00Z","orderRef":"order-x","requiredCapabilities":["break"]}
    Then the response status is 400
