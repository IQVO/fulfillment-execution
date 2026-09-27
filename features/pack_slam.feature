Feature: Pack and SLAM
  The Pack path turns an order into a sealed carton, then the SLAM
  weigh-check either applies the shipping label or diverts the Package when
  the actual weight strays outside tolerance of the expected weight.

  Background:
    Given a running Fulfillment Execution service
    And a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task

  @bdd
  Scenario: Sealing a package with scanned contents succeeds
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-1,sku-2"
    Then the response status is 201
    And the sealed Package has status "SEALED"
    And the sealed Package holds scanned contents "sku-1,sku-2"
    And a "PackageSealed" domain event is recorded

  @bdd
  Scenario: SLAM weigh-check within tolerance applies a label
    Given Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    When the SLAM weigh-check runs on the Package with an actual weight of 2.02 against an expected weight of 2.00
    Then the response status is 204
    And a "LabelApplied" domain event is recorded
    And no "WeightDiscrepancyDetected" domain event is recorded
    And no "PackageDiverted" domain event is recorded

  # Derived from docs/docs/adr/0025-cpt-missed-sweep-and-package-manifested.md
  # (PackageManifested is raised alongside LabelApplied on a successful
  # SLAM pass) and .claude/rules/ubiquitous-language.md use case 6
  # (RunSlam -> LabelApplied + PackageManifested, or
  # WeightDiscrepancyDetected + PackageDiverted — not manifested).
  @bdd
  Scenario: A passing SLAM weigh-check manifests the package
    Given Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    When the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    Then the response status is 204
    And a "LabelApplied" domain event is recorded
    And a "PackageManifested" domain event is recorded
    And no "PackageDiverted" domain event is recorded

  @bdd
  Scenario: SLAM weigh-check outside tolerance diverts the package
    Given Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    When the SLAM weigh-check runs on the Package with an actual weight of 2.50 against an expected weight of 2.00
    Then the response status is 204
    And a "WeightDiscrepancyDetected" domain event is recorded
    And a "PackageDiverted" domain event is recorded
    And no "LabelApplied" domain event is recorded
