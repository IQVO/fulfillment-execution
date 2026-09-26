# Derived from .claude/rules/api-and-integration.md, section "The `orderRef`
# cross-service contract", and apis/openapi.yaml operation getTasksByOrderRef
# (GET /tasks?orderRef=): returns every task recorded for the reference
# including retried legs, an unrecognized orderRef is not an error (empty
# array, same convention as getQueueDepth), and a missing orderRef query
# parameter is a 400 invalid-request problem.
Feature: Task lookup by order reference
  GET /tasks?orderRef= is the read side backing the cross-service Order
  Lifecycle console screen: it traces an order reference back to every
  task this service created for it, array-shaped and side-effect-free.

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: Looking up an order reference returns every task recorded for it
    Given a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    When the tasks for order "order-1" are looked up
    Then the response status is 200
    And the lookup returns 2 tasks for order "order-1"

  @bdd
  Scenario: An unknown order reference returns an empty array
    When the tasks for order "order-unknown" are looked up
    Then the response status is 200
    And the lookup returns 0 tasks for order "order-unknown"

  @bdd
  Scenario: Looking up tasks without an order reference is rejected
    When tasks are looked up without an orderRef
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
