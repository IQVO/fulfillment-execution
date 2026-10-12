# Derived from apis/openapi.yaml operation sweepCPTMisses ("for each whose CPT
# is at or before now" over every task still open, i.e. not Completed) and
# docs/docs/adr/0025-cpt-missed-sweep-and-package-manifested.md (TaskCPTMissed
# carries the order, task type and CPT order-management keys its re-promise on).
Feature: CPT-missed sweep boundaries
  The sweep reports a task whose CPT is at or before now, never one that has
  already been completed, and the reported event names the order and the leg
  that missed.

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: A task whose CPT is exactly now is reported with its order, type and CPT
    Given a "PICK" Task for order "order-1" with a CPT 5 minutes from now requiring capabilities "pick"
    When the clock advances by 5 minutes
    And the CPT-missed sweep runs
    Then the response status is 200
    And 1 task was reported as CPT-missed
    And the "TaskCPTMissed" domain event carries:
      | TaskId   | task-1               |
      | OrderRef | order-1              |
      | TaskType | PICK                 |
      | CPT      | 2026-01-01T12:05:00Z |

  @bdd
  Scenario: A completed task past its CPT is not reported
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 5 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-01" completes the claimed Task
    When the clock advances by 10 minutes
    And the CPT-missed sweep runs
    Then the response status is 200
    And 0 tasks were reported as CPT-missed
    And no "TaskCPTMissed" domain event is recorded
