# pack_slam_rules.feature: scenarios derived from the context documentation
# (warehouse-docs wes/docs/contexts/fulfillment-execution: use-cases "SealPackage"
# and "RunSlam", aggregate-design-canvas Package invariants P1-P6 and corrective
# policies, troubleshooting 409/422 rows, ADR-0010, ADR-0038, ADR-0039).
Feature: The Pack and SLAM rules of the Package aggregate

  Background:
    Given a running Fulfillment Execution service
    And a Station "pack-01" is registered with capabilities "pack"

  # --- SLAM tolerance and single run (P4, P5) ----------------------------------------

  @bdd
  Scenario Outline: The weigh-check diverts only when the weights differ by more than 0.05
    Given a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    When the SLAM weigh-check runs on the Package with an actual weight of <actual> against an expected weight of <expected>
    Then the response status is 204
    When the Package is read back
    Then the Package reads back with status "<status>" for order "order-1"

    Examples:
      | expected | actual | status   |
      | 2.00     | 2.04   | LABELED  |
      | 2.00     | 1.96   | LABELED  |
      | 0.10     | 0.05   | LABELED  |
      | 2.00     | 2.06   | DIVERTED |
      | 2.00     | 1.94   | DIVERTED |
      | 0.10     | 0.04   | DIVERTED |

  @bdd
  Scenario: A diverted package is never re-weighed
    Given a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.50 against an expected weight of 2.00
    When the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    Then the response status is 409
    And the response is a Problem Details document of type "package-already-processed"
    When the Package is read back
    Then the Package reads back with status "DIVERTED" for order "order-1"
    And exactly 1 "PackageDiverted" domain event is recorded
    And no "LabelApplied" domain event is recorded

  # --- SealPackage: idempotent on the task id, guarded by the claim ---------------------

  @bdd
  Scenario: A retried seal answers the package sealed first and seals nothing twice
    Given a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-1,sku-2"
    Then the response status is 201
    And the response field "id" is "pkg-1"
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-9"
    Then the response status is 201
    And the response field "id" is "pkg-1"
    And the sealed Package holds scanned contents "sku-1,sku-2"
    And exactly 1 "PackageSealed" domain event is recorded
    When the packages for order "order-1" are looked up
    Then the package lookup returns 1 package for order "order-1"

  @bdd
  Scenario: A seal retried after the lease lapsed still answers the package sealed first
    Given a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    When the clock advances by 6 minutes
    And Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-1"
    Then the response status is 201
    And the response field "id" is "pkg-1"
    And exactly 1 "PackageSealed" domain event is recorded

  @bdd
  Scenario: Another station cannot seal a package for a task whose claim it does not hold
    Given a Station "pack-02" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-02" seals a Package for the claimed Task with scanned contents "sku-1"
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-owner"
    And no "PackageSealed" domain event is recorded
    When the packages for order "order-1" are looked up
    Then the package lookup returns 0 packages for order "order-1"

  @bdd
  Scenario: A task nobody has claimed cannot be sealed
    Given a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    When a POST is sent to "/tasks/task-1/package-seals" with the JSON body {"stationId":"pack-01","contents":["sku-1"]}
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-claimed"
    And no "PackageSealed" domain event is recorded

  @bdd
  Scenario Outline: The seal is allowed until the lease expiry instant and refused from it
    Given a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When the clock advances by <seconds> seconds
    And Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-1"
    Then the response status is <status>
    And exactly <sealed> "PackageSealed" domain events are recorded

    Examples:
      | seconds | status | sealed |
      | 299     | 201    | 1      |
      | 300     | 409    | 0      |

  @bdd
  Scenario: A sealed carton awaiting its weigh-check reads back as SEALED
    Given a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    When the Package is read back
    Then the Package reads back with status "SEALED" for order "order-1"
    And no "LabelApplied" domain event is recorded

  # --- sort lane (ADR-0010) --------------------------------------------------------------

  @bdd
  Scenario Outline: The sort lane puts hazmat before fragile and gift wrap never changes it
    Given product-master classifies SKU "sku-flammable" under DOT hazard class 3
    When work planning publishes a WorkReleased event "evt-l" with:
      | path_id      | pack-lane-1 |
      | work_unit_id | wu-l        |
      | ref          | order-l     |
      | cpt_minutes  | 45          |
      | fragile      | <fragile>   |
      | gift_wrap    | <gift_wrap> |
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" seals a Package for the claimed Task with scanned contents "<contents>"
    Then the response status is 201
    And the sealed Package is fragile <fragile>, gift wrap <gift_wrap> and routed to the "<lane>" lane

    Examples:
      | fragile | gift_wrap | contents      | lane            |
      | false   | false     | sku-plain     | STANDARD        |
      | true    | false     | sku-plain     | FRAGILE_NO_TILT |
      | false   | true      | sku-plain     | STANDARD        |
      | true    | true      | sku-flammable | HAZMAT_LANE     |

  # --- DOT hazard segregation (P6) -----------------------------------------------------------

  @bdd
  Scenario Outline: Hazard classes that may not share a carton are refused whichever is scanned first
    Given product-master classifies SKU "sku-a" under DOT hazard class <class_a>
    And product-master classifies SKU "sku-b" under DOT hazard class <class_b>
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-a,sku-b"
    Then the response status is 409
    And the response is a Problem Details document of type "package-segregation-violation"
    And no "PackageSealed" domain event is recorded

    Examples:
      | class_a | class_b | rule                                                    |
      | 1       | 1       | explosives never share a package, not even with each other |
      | 1       | 9       | class 9 is the one class that still conflicts with explosives |
      | 9       | 1       | the same pair in the other scan order                   |
      | 3       | 5       | flammable liquid with an oxidizer                       |
      | 5       | 3       | the same pair in the other scan order                   |
      | 2       | 3       | divisions collapse to the conservative worst case       |

  @bdd
  Scenario Outline: Hazard classes the regulation lets share a carton are sealed into a hazmat-lane package
    Given product-master classifies SKU "sku-a" under DOT hazard class <class_a>
    And product-master classifies SKU "sku-b" under DOT hazard class <class_b>
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-a,sku-b"
    Then the response status is 201
    And the sealed Package is fragile false, gift wrap false and routed to the "HAZMAT_LANE" lane

    Examples:
      | class_a | class_b | rule                                              |
      | 3       | 3       | two items of one class                            |
      | 3       | 8       | flammable liquid with a corrosive                 |
      | 9       | 3       | class 9 is broadly compatible                     |
      | 9       | 9       | two class 9 items                                 |

  @bdd
  Scenario: A new item is checked against every item already in the carton, not only the last one
    Given product-master classifies SKU "sku-flammable" under DOT hazard class 3
    And product-master classifies SKU "sku-misc" under DOT hazard class 9
    And product-master classifies SKU "sku-oxidizer" under DOT hazard class 5
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-flammable,sku-misc,sku-oxidizer"
    Then the response status is 409
    And the response is a Problem Details document of type "package-segregation-violation"

  @bdd
  Scenario: A seal refused for segregation seals nothing and the same claim can still seal a compliant carton
    Given product-master classifies SKU "sku-flammable" under DOT hazard class 3
    And product-master classifies SKU "sku-oxidizer" under DOT hazard class 5
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-flammable,sku-oxidizer"
    Then the response status is 409
    When the packages for order "order-1" are looked up
    Then the package lookup returns 0 packages for order "order-1"
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-flammable"
    Then the response status is 201
    And exactly 1 "PackageSealed" domain event is recorded

  # --- the classification lookup is a soft dependency (fail-open) -------------------------------

  @bdd
  Scenario: A lookup failure for one SKU treats it as unclassified and the seal goes ahead
    Given the classification lookup fails for SKU "sku-unreadable"
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-unreadable,sku-plain"
    Then the response status is 201
    And the sealed Package is fragile false, gift wrap false and routed to the "STANDARD" lane

  @bdd
  Scenario: A lookup failure for one SKU does not relax the segregation of the others
    Given product-master classifies SKU "sku-flammable" under DOT hazard class 3
    And product-master classifies SKU "sku-oxidizer" under DOT hazard class 5
    And the classification lookup fails for SKU "sku-unreadable"
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-flammable,sku-unreadable,sku-oxidizer"
    Then the response status is 409
    And the response is a Problem Details document of type "package-segregation-violation"

  @bdd
  Scenario: An item with no recorded hazard class is neither segregated nor routed to the hazmat lane
    Given product-master classifies SKU "sku-no-class" as Hazmat without a DOT hazard class
    And product-master classifies SKU "sku-fragile" as Fragile only
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-no-class,sku-fragile"
    Then the response status is 201
    And the sealed Package is fragile false, gift wrap false and routed to the "STANDARD" lane

  @bdd
  Scenario: A newer classification of a SKU replaces the one the copy holds
    Given product-master classifies SKU "sku-flammable" under DOT hazard class 3
    And product-master classifies SKU "sku-oxidizer" under DOT hazard class 5
    And product-master reclassifies SKU "sku-flammable" under DOT hazard class 9 as version 2 in event "reclassified-1"
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-flammable,sku-oxidizer"
    Then the response status is 201

  @bdd
  Scenario: A stale classification delivered late never replaces a newer one
    Given product-master classifies SKU "sku-oxidizer" under DOT hazard class 5
    And product-master reclassifies SKU "sku-flammable" under DOT hazard class 9 as version 2 in event "reclassified-2"
    And product-master reclassifies SKU "sku-flammable" under DOT hazard class 3 as version 1 in event "late-1"
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-flammable,sku-oxidizer"
    Then the response status is 201
