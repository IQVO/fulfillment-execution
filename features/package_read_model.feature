# Derived from docs/docs/adr/0033-package-read-model.md and
# apis/openapi.yaml operations getPackage (GET /packages/{id}) and
# getPackagesByOrderRef (GET /packages?orderRef=): the SLAM weigh-check
# answers 204 for both outcomes, so the Package read model is how a caller
# tells LABELED from DIVERTED; an unknown id is a 404 package-not-found
# problem; the orderRef lookup mirrors GET /tasks?orderRef=.
Feature: Package read model
  The SLAM operator and the outbound dock loader read a Package back to see
  whether it was labeled (truck, by sort lane) or diverted (problem-solve).

  Background:
    Given a running Fulfillment Execution service
    And a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"

  @bdd
  Scenario: A labeled package reads back as LABELED
    Given the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    When the Package is read back
    Then the response status is 200
    And the Package reads back with status "LABELED" for order "order-1"

  @bdd
  Scenario: A diverted package reads back as DIVERTED
    Given the SLAM weigh-check runs on the Package with an actual weight of 2.50 against an expected weight of 2.00
    When the Package is read back
    Then the response status is 200
    And the Package reads back with status "DIVERTED" for order "order-1"

  @bdd
  Scenario: Reading an unknown package is a package-not-found problem
    When the Package "does-not-exist" is read
    Then the response status is 404
    And the response is a Problem Details document of type "package-not-found"

  @bdd
  Scenario: Looking up an order reference returns its packages
    When the packages for order "order-1" are looked up
    Then the response status is 200
    And the package lookup returns 1 package for order "order-1"

  @bdd
  Scenario: An unknown order reference returns no packages
    When the packages for order "order-unknown" are looked up
    Then the response status is 200
    And the package lookup returns 0 packages for order "order-unknown"

  @bdd
  Scenario: Looking up packages without an order reference is rejected
    When packages are looked up without an orderRef
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
