# Derived from apis/openapi.yaml operation runSlam (tolerance 0.05, same unit
# for both weights; a discrepancy raises WeightDiscrepancyDetected +
# PackageDiverted and is not manifested) and
# .claude/rules/ubiquitous-language.md use case 6.
Feature: SLAM weigh-check tolerance
  The weigh-check is symmetric: a package lighter than expected is diverted
  exactly like a heavier one, and the discrepancy event records both weights.

  Background:
    Given a running Fulfillment Execution service
    And a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"

  @bdd
  Scenario: A package lighter than expected is diverted with the discrepancy recorded
    When the SLAM weigh-check runs on the Package with an actual weight of 1.80 against an expected weight of 2.00
    Then the response status is 204
    And the "WeightDiscrepancyDetected" domain event carries:
      | PackageId      | pkg-1 |
      | ExpectedWeight | 2     |
      | ActualWeight   | 1.8   |
    And a "PackageDiverted" domain event is recorded
    And no "LabelApplied" domain event is recorded
    And no "PackageManifested" domain event is recorded
    When the Package is read back
    Then the Package reads back with status "DIVERTED" for order "order-1"
