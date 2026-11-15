# rebin_rules.feature: scenarios derived from the context documentation
# (warehouse-docs wes/docs/contexts/fulfillment-execution: use-cases
# "ArriveAtRebin", aggregate-design-canvas OrderConsolidation C1-C2 and its
# corrective policies, ADR-0016).
Feature: The Rebin fan-in of the OrderConsolidation aggregate

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: Lines may arrive in any order and the last one to arrive completes the order
    When line "line-2" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2"
    Then the response status is 204
    And the order "order-1" has 0 "PACK" Tasks
    When line "line-1" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2"
    Then the response status is 204
    And the order "order-1" has 1 "PACK" Task
    And exactly 2 "ItemArrivedAtRebin" domain events are recorded
    And exactly 1 "OrderConsolidated" domain event is recorded
    And exactly 1 "TaskCreated" domain event is recorded

  @bdd
  Scenario: Once the order is consolidated nothing more happens for it
    Given line "line-1" of order "order-1" has arrived at Rebin; the order needs lines "line-1,line-2,line-3"
    And line "line-2" of order "order-1" has arrived at Rebin; the order needs lines "line-1,line-2,line-3"
    And line "line-3" of order "order-1" has arrived at Rebin; the order needs lines "line-1,line-2,line-3"
    When line "line-1" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2,line-3"
    Then the response status is 204
    And the order "order-1" has 1 "PACK" Task
    And exactly 3 "ItemArrivedAtRebin" domain events are recorded
    And exactly 1 "OrderConsolidated" domain event is recorded
    And exactly 1 "TaskCreated" domain event is recorded
    When line "line-9" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2,line-3"
    Then the response status is 422
    And the response is a Problem Details document of type "rebin-unknown-line"
    And the order "order-1" has 1 "PACK" Task

  @bdd
  Scenario: A redelivered arrival while the order is still collecting adds no second arrival fact
    # aggregate-design-canvas, OrderConsolidation: "Redelivered arrivals are
    # idempotent no-ops" and "one event per line plus one at completion".
    # ArriveAtRebin returns before saving or publishing when the line has
    # already arrived, at any stage of the consolidation.
    When line "line-1" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2"
    And line "line-1" of order "order-1" arrives at Rebin; the order needs lines "line-1,line-2"
    Then the response status is 204
    And exactly 1 "ItemArrivedAtRebin" domain event is recorded
    And the order "order-1" has 0 "PACK" Tasks

  @bdd
  Scenario: An order whose last line never arrives is never packed on its own
    Given line "line-1" of order "order-1" has arrived at Rebin; the order needs lines "line-1,line-2,line-3"
    And line "line-2" of order "order-1" has arrived at Rebin; the order needs lines "line-1,line-2,line-3"
    When the clock advances by 2880 minutes
    And the lease expiry sweep runs
    And the CPT-missed sweep runs
    Then 0 tasks were reported as CPT-missed
    And the order "order-1" has 0 "PACK" Tasks
    And no "OrderConsolidated" domain event is recorded
    And the queue depth for "PACK" is 0
