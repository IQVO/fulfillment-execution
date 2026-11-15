# station_rules.feature: scenarios derived from the context documentation
# (warehouse-docs wes/docs/contexts/fulfillment-execution: use-cases
# "RegisterStation", business-context "does not own location", the 422
# station-location-not-workcenter troubleshooting rows, ADR-0024). The
# facility-layout role lookup is opt-in and fail-open.
Feature: Registering a station at a facility-layout location

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: A station cannot be registered at a location facility-layout knows is not a work centre
    Given facility-layout says location "STG-A-01" is a "Storage"
    When a POST is sent to "/stations" with the JSON body {"stationId":"pick-01","capabilities":["pick"],"locationCode":"STG-A-01"}
    Then the response status is 422
    And the response is a Problem Details document of type "station-location-not-workcenter"
    And the installed capacity for "pick" is 0

  @bdd
  Scenario Outline: A location code is accepted when it is a work centre or when facility-layout cannot say
    Given <facility-layout>
    When a POST is sent to "/stations" with the JSON body {"stationId":"pick-01","capabilities":["pick"],"locationCode":"WC-01"}
    Then the response status is 201
    And the response field "locationCode" is "WC-01"
    And the installed capacity for "pick" is 1

    Examples:
      | facility-layout                                              |
      | facility-layout says location "WC-01" is a "WorkCenter"      |
      | facility-layout does not know any location                   |
      | facility-layout is unreachable                               |

  @bdd
  Scenario: A refused re-registration leaves the existing registration untouched
    Given a Station "pick-01" is registered with capabilities "pick"
    And facility-layout says location "STG-A-01" is a "Storage"
    When a POST is sent to "/stations" with the JSON body {"stationId":"pick-01","capabilities":["pack"],"locationCode":"STG-A-01"}
    Then the response status is 422
    And the installed capacity for "pick" is 1
    And the installed capacity for "pack" is 0
