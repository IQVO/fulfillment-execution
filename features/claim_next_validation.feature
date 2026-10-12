# Derived from apis/openapi.yaml operation claimNextTask: ClaimNextRequest.taskType
# is the TaskType enum (PICK, PACK, SLAM, REBIN, DISPATCH, ARRIVAL) and "400" is
# components.responses.BadRequest. A task type outside the enum is a malformed
# request; it must not be answered as "no claimable task" (409).
Feature: claimNext rejects a task type outside the TaskType enum
  A station asking for a kind of work the contract does not define has made a
  malformed request. An empty queue for a real task type is a different answer.

  Background:
    Given a running Fulfillment Execution service
    And a Station "pick-01" is registered with capabilities "pick"

  @bdd
  Scenario: A task type outside the enum is a bad request, not an empty queue
    Given a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    When Station "pick-01" calls claimNext for task type "BOGUS"
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
    And no "TaskClaimed" domain event is recorded
    And the queue depth for "PICK" is 1

  @bdd
  Scenario: The task type is case-sensitive
    Given a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    When Station "pick-01" calls claimNext for task type "pick"
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"

  @bdd
  Scenario: A valid task type with nothing to claim is still no-claimable-task
    When Station "pick-01" calls claimNext for task type "PACK"
    Then the response status is 409
    And the response is a Problem Details document of type "no-claimable-task"
