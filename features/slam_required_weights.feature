# Derived from apis/openapi.yaml operation runSlam: RunSlamRequest lists
# actualWeight and expectedWeight as required, and "400" is
# components.responses.BadRequest. The schema also documents that an explicit
# null weight is accepted and treated as 0, so only an ABSENT weight is
# rejected. A rejected request must not run the weigh-check: otherwise a
# package nobody weighed is labelled as if it had passed.
Feature: SLAM weigh-check requires both weights

  Background:
    Given a running Fulfillment Execution service
    And a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"

  @bdd
  Scenario: A weigh-check with no weights is rejected and the package stays unprocessed
    When a POST is sent to "/packages/pkg-1/slam-runs" with the JSON body {}
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
    And the Problem Details document has instance "/packages/pkg-1/slam-runs"
    And no "LabelApplied" domain event is recorded
    And no "WeightDiscrepancyDetected" domain event is recorded
    When the Package is read back
    Then the Package reads back with status "SEALED" for order "order-1"

  @bdd
  Scenario: A weigh-check missing one weight is rejected
    When a POST is sent to "/packages/pkg-1/slam-runs" with the JSON body {"actualWeight":2.0}
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
    And no "LabelApplied" domain event is recorded

  @bdd
  Scenario: A rejected weigh-check can be retried with both weights
    Given a POST is sent to "/packages/pkg-1/slam-runs" with the JSON body {"expectedWeight":2.0}
    And the response status is 400
    When the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    Then the response status is 204
    And a "LabelApplied" domain event is recorded

  @bdd
  Scenario: Explicit null weights are accepted and read as zero, as the schema documents
    When a POST is sent to "/packages/pkg-1/slam-runs" with the JSON body {"actualWeight":null,"expectedWeight":null}
    Then the response status is 204
    And a "LabelApplied" domain event is recorded
