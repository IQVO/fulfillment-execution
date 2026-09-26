# Derived from docs/docs/adr/0014-labor-performance-integration-hooks.md,
# section "Check-in/check-out: application + HTTP only, no domain change",
# and apis/openapi.yaml operations checkInStation / checkOutStation
# (POST /stations/{stationId}/check-in and /check-out): the Station
# aggregate's one-occupant-at-a-time invariant — a second check-in is a
# 409 station-occupied problem, checking out clears the occupant, checking
# out an unoccupied station is a 409 station-not-occupied problem, and
# neither endpoint publishes a domain event.
Feature: Station occupancy
  A Station holds at most one occupant at a time. Checking in records the
  worker or robot that a later TaskCompleted event reports best-effort as
  associateId; check-in and check-out are operational state, not
  labor-performance domain events in their own right.

  Background:
    Given a running Fulfillment Execution service
    And a Station "pick-01" is registered with capabilities "pick"

  @bdd
  Scenario: Checking a worker in occupies the station without publishing a domain event
    When worker "worker-42" checks in at Station "pick-01"
    Then the response status is 200
    And the Station response shows the station is occupied
    And no domain events are recorded

  @bdd
  Scenario: A second check-in is rejected
    Given worker "worker-42" has checked in at Station "pick-01"
    When worker "worker-43" checks in at Station "pick-01"
    Then the response status is 409
    And the response is a Problem Details document of type "station-occupied"

  @bdd
  Scenario: Checking out clears the occupant
    Given worker "worker-42" has checked in at Station "pick-01"
    When Station "pick-01" checks out
    Then the response status is 200
    And the Station response shows the station is unoccupied

  @bdd
  Scenario: Checking out an unoccupied station is rejected
    When Station "pick-01" checks out
    Then the response status is 409
    And the response is a Problem Details document of type "station-not-occupied"
