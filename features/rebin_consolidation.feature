# Derived from apis/openapi.yaml operation arriveAtRebin (POST /rebin/arrivals)
# and docs/docs/adr/0016-rebin-and-order-consolidation.md: the Rebin fan-in
# gates the pick -> pack handoff, the first arrival fixes the order's required
# line set, arrivals are idempotent per (orderRef, lineId), and the PACK task
# is created exactly once by the arrival that completes the set.
Feature: Rebin order consolidation
  A multi-line order picked from different pods only becomes packable once
  every required line has arrived at Rebin. The arrival that completes the
  set creates the order's PACK task through the normal task pool.

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: A multi-line order is packable only after every line has arrived
    When line "line-1" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2"
    Then the response status is 204
    And the order "order-1" has 0 "PACK" Tasks
    And the queue depth for "PACK" is 0
    And no "OrderConsolidated" domain event is recorded
    When line "line-1" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2"
    Then the response status is 204
    And the order "order-1" has 0 "PACK" Tasks
    When line "line-2" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2"
    Then the response status is 204
    And the order "order-1" has 1 "PACK" Task
    And the "PACK" Task for order "order-1" has status "PENDING"
    And the queue depth for "PACK" is 1
    And an "ItemArrivedAtRebin" domain event is recorded
    And exactly 1 "OrderConsolidated" domain event is recorded
    And the "OrderConsolidated" domain event carries:
      | OrderRef | order-1 |

  @bdd
  Scenario: A single-line order is consolidated on its first arrival and a later duplicate adds nothing
    When line "line-1" of order "order-1" arrives at Rebin; the order needs lines "line-1"
    Then the response status is 204
    And the order "order-1" has 1 "PACK" Task
    And the "ItemArrivedAtRebin" domain event carries:
      | OrderRef | order-1 |
      | LineId   | line-1  |
    When line "line-1" of order "order-1" arrives at Rebin; the order needs lines "line-1"
    Then the response status is 204
    And the order "order-1" has 1 "PACK" Task
    And exactly 1 "OrderConsolidated" domain event is recorded
    And exactly 1 "TaskCreated" domain event is recorded

  @bdd
  Scenario: A line outside the order's required set is rejected and consolidates nothing
    When line "line-9" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2"
    Then the response status is 422
    And the response is a Problem Details document of type "rebin-unknown-line"
    And the order "order-1" has 0 "PACK" Tasks
    And no "ItemArrivedAtRebin" domain event is recorded
    And no "OrderConsolidated" domain event is recorded

  @bdd
  Scenario: The first arrival fixes the required line set for the order
    Given line "line-1" of order "order-1" has arrived at Rebin; the order needs lines "line-1,line-2"
    When line "line-3" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2,line-3"
    Then the response status is 422
    And the response is a Problem Details document of type "rebin-unknown-line"
    When line "line-2" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2,line-3"
    Then the response status is 204
    And the order "order-1" has 1 "PACK" Task
    And exactly 1 "OrderConsolidated" domain event is recorded

  @bdd
  Scenario: The PACK task takes its hints from the arrival that completes the set
    Given line "line-1" of order "order-1" has arrived at Rebin; the order needs lines "line-1,line-2"
    When line "line-2" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2" and its PACK task is due in 90 minutes, requires capabilities "pack,hazmat", fragile true, gift wrap true
    Then the response status is 204
    And the "PACK" Task for order "order-1" requires capabilities "pack,hazmat"
    And the "PACK" Task for order "order-1" is flagged fragile true and gift wrap true
    And the "PACK" Task for order "order-1" is due 90 minutes after the scenario start
    And a Station "pack-01" is registered with capabilities "pack"
    And a Station "pack-hz" is registered with capabilities "pack,hazmat"
    When Station "pack-01" calls claimNext for task type "PACK"
    Then the response status is 409
    And the response is a Problem Details document of type "no-claimable-task"
    When Station "pack-hz" calls claimNext for task type "PACK"
    Then the response status is 200
    And the claimed Task is a "PACK" Task for order "order-1"
    And the claimed Task is leased to Station "pack-hz"

  @bdd
  Scenario: An arrival without required lines or PACK capabilities is rejected as an invalid request
    When a POST is sent to "/rebin/arrivals" with the JSON body {"orderRef":"order-1","lineId":"line-1","requiredLineIds":[],"packCpt":"2026-01-01T13:00:00Z","packRequiredCapabilities":["pack"]}
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
    And the Problem Details document has no instance
    When a POST is sent to "/rebin/arrivals" with the JSON body {"orderRef":"order-1","lineId":"line-1","requiredLineIds":["line-1"],"packCpt":"2026-01-01T13:00:00Z","packRequiredCapabilities":[]}
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
    And no "ItemArrivedAtRebin" domain event is recorded
    And the order "order-1" has 0 "PACK" Tasks
