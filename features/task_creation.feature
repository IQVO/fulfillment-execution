# Derived from apis/openapi.yaml operation createTask (POST /tasks) and its
# TaskType / CreateTaskRequest schemas, and from the TaskResponse description
# of giftWrap (read-only over HTTP, ADR-0011).
Feature: Task creation over HTTP
  POST /tasks puts a validated unit of work into the pending pool. The task
  type must be one of the published TaskType values, and the packing hint
  giftWrap has no HTTP ingestion path.

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: A task with a type outside the TaskType enum is rejected
    When a POST is sent to "/tasks" with the JSON body {"type":"BOGUS","cpt":"2026-01-01T13:00:00Z","orderRef":"order-1","requiredCapabilities":["pick"]}
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
    And the Problem Details document has no instance
    And the order "order-1" has 0 "BOGUS" Tasks
    And no "TaskCreated" domain event is recorded

  @bdd
  Scenario: A created task starts pending and unleased, and giftWrap cannot be set over HTTP
    When a POST is sent to "/tasks" with the JSON body {"type":"DISPATCH","cpt":"2026-01-01T13:00:00Z","orderRef":"order-1","requiredCapabilities":["dispatch"],"fragile":true,"giftWrap":true}
    Then the response status is 201
    And the response header "Location" is "/tasks/task-1"
    And the response field "id" is "task-1"
    And the response field "type" is "DISPATCH"
    And the response field "status" is "PENDING"
    And the response field "fragile" is true
    And the response field "giftWrap" is false
    And the response has no "leaseStationId" field
    And the response has no "leaseExpiry" field
    And the queue depth for "DISPATCH" is 1
    And a "TaskCreated" domain event is recorded
