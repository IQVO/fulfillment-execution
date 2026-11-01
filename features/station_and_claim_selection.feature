# Derived from apis/openapi.yaml operations registerStation ("replaces its
# capability set"), getInstalledCapacity ("regardless of occupancy") and
# claimNextTask ("the earliest-CPT task a station is equipped for", skipping
# candidates it cannot take), and from the 404 task-not-found responses of
# completeTask and sealPackage.
Feature: Station recertification, capacity and claim selection
  Re-registering a station recertifies it, installed capacity ignores who is
  checked in, claimNext skips work a station cannot do, and acting on a task
  that does not exist is a typed 404.

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: Re-registering a station replaces its capability set
    Given a Station "st-1" is registered with capabilities "pick"
    And a Station "st-2" is registered with capabilities "pick,pack"
    And the installed capacity for "pick" is 2
    When a Station "st-1" is registered with capabilities "pack"
    Then the response status is 201
    And the installed capacity for "pick" is 1
    And the installed capacity for "pack" is 2

  @bdd
  Scenario: Claiming next skips an earlier task the station cannot take and leases the next best one
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-hazmat" with a CPT 10 minutes from now requiring capabilities "pick,hazmat"
    And a "PICK" Task for order "order-plain" with a CPT 40 minutes from now requiring capabilities "pick"
    When Station "pick-01" calls claimNext for task type "PICK"
    Then the response status is 200
    And the claimed Task is the one for order "order-plain"
    And the claimed Task is leased to Station "pick-01"
    And the queue depth for "PICK" is 1

  @bdd
  Scenario: Completing or sealing for a task that does not exist is a task-not-found problem
    When a POST is sent to "/tasks/ghost-task/completions" with the JSON body {"stationId":"pick-01"}
    Then the response status is 404
    And the response is a Problem Details document of type "task-not-found"
    And the Problem Details document has instance "/tasks/ghost-task/completions"
    When a POST is sent to "/tasks/ghost-task/package-seals" with the JSON body {"stationId":"pack-01","contents":["sku-1"]}
    Then the response status is 404
    And the response is a Problem Details document of type "task-not-found"
    And the Problem Details document has instance "/tasks/ghost-task/package-seals"
    And no "TaskCompleted" domain event is recorded
    And no "PackageSealed" domain event is recorded
