# Derived from apis/openapi.yaml operation sealPackage (409 task-not-claimed /
# task-not-owner / package-segregation-violation, 422 for a non-Pack task) and
# docs/docs/adr/0038-seal-package-expired-lease-is-not-claimed.md,
# docs/docs/adr/0010-package-segregation-and-sort-lane.md and
# docs/docs/adr/0039-product-classification-local-copy.md. Product
# classifications arrive from product-master over Kafka, so they are seeded
# through the ProductClassified consumer's handler.
Feature: Sealing a package: claim and segregation guards
  Only the station holding an active claim on a Pack task may seal a package,
  and items whose DOT hazard classes may not share a carton are refused.

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario: Sealing with an expired lease is a task-not-claimed problem and seals nothing
    Given a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When the clock advances by 6 minutes
    And Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-1"
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-claimed"
    And no "PackageSealed" domain event is recorded
    When the packages for order "order-1" are looked up
    Then the package lookup returns 0 packages for order "order-1"

  @bdd
  Scenario: Sealing a package for a task that is not a Pack task is rejected
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" seals a Package for the claimed Task with scanned contents "sku-1"
    Then the response status is 422
    And the response is a Problem Details document of type "wrong-task-type"
    And no "PackageSealed" domain event is recorded

  @bdd
  Scenario: Items with incompatible DOT hazard classes cannot share a package
    Given product-master classifies SKU "sku-flammable" under DOT hazard class 3
    And product-master classifies SKU "sku-oxidizer" under DOT hazard class 5
    And a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-flammable,sku-oxidizer"
    Then the response status is 409
    And the response is a Problem Details document of type "package-segregation-violation"
    And no "PackageSealed" domain event is recorded

  @bdd
  Scenario: Compatible hazmat items and unclassified items seal into a hazmat-lane package
    Given product-master classifies SKU "sku-flammable" under DOT hazard class 3
    And product-master classifies SKU "sku-corrosive" under DOT hazard class 8
    And a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-flammable,sku-corrosive,sku-plain"
    Then the response status is 201
    And the sealed Package is fragile false, gift wrap false and routed to the "HAZMAT_LANE" lane
    And a "PackageSealed" domain event is recorded
