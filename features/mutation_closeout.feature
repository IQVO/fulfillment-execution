# mutation_closeout.feature: the rules whose mutants SURVIVED the first
# mutation pass over the documentation-derived scenarios (re-proof on the
# merged code). Each scenario names the one rule a surviving mutant had been
# free to break; the production comparison it pins is named in the PR table.
Feature: Rules the documentation-derived scenarios left unpinned

  Background:
    Given a running Fulfillment Execution service
    And a Station "pack-01" is registered with capabilities "pack"
    And a Station "pick-01" is registered with capabilities "pick"

  # --- request validation: openapi CompleteTaskRequest.quantity "minimum: 1;
  # a value below 1 is a 400" - the validation answer, not the domain's 422 rows.

  @bdd
  Scenario Outline: A completion quantity of <quantity> is a 400 whatever the task type
    Given a Station "<station>" is registered with capabilities "<capability>"
    And <release>
    And Station "<station>" has claimed the next "<type>" Task
    When Station "<station>" completes the claimed Task with the confirmation {"quantity":<quantity>}
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
    And the "<type>" Task for order "wu-1" has status "CLAIMED"
    And no "TaskCompleted" domain event is recorded

    Examples:
      | station  | capability | type      | quantity | release                                                                                                                                                                              |
      | reloc-01 | relocate   | RELOCATE  | 0        | work planning has released a RELOCATE move "Relocate" of SKU "SKU-1001" in plan "plan-7" step 3 as work unit "wu-1" from bin "WH1-FWD-A01-01" to bin "WH1-FWD-B02-04"                |
      | repl-01  | replenish  | REPLENISH | 0        | work planning has released a REPLENISH request "req-9001" of 24 units of SKU "SKU-2002" from bin "WH1-RES-C07-02" to bin "WH1-FWD-A01-01" with priority "NORMAL" as work unit "wu-1" |
      | pick-01  | pick       | PICK      | 0        | a "PICK" Task for order "wu-1" with a CPT 30 minutes from now requiring capabilities "pick"                                                                                         |
      | pick-01  | pick       | PICK      | -1       | a "PICK" Task for order "wu-1" with a CPT 30 minutes from now requiring capabilities "pick"                                                                                         |

  # --- the lease is renewed from the renewal instant (RenewLease) ----------------

  @bdd
  Scenario Outline: A lease renewed two minutes into the claim runs five minutes from the renewal
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 120 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    And the clock advances by 120 seconds
    And Station "pick-01" renews the lease on the claimed Task
    When the clock advances by <seconds> seconds
    And Station "pick-01" completes the claimed Task
    Then the response status is <status>

    Examples:
      | seconds | status |
      | 299     | 204    |
      | 300     | 409    |

  # --- a released move is trimmed before it is checked (NewRelocationMove) ---------

  @bdd
  Scenario: Every string of a released move is trimmed before it is stored
    Given work planning has released a RELOCATE move " Relocate " of SKU " SKU-1001 " in plan " plan-7 " step 3 as work unit "wu-trim" from bin " WH1-FWD-A01-01 " to bin " WH1-FWD-B02-04 "
    Then the order "wu-trim" has 1 "RELOCATE" Task
    And the task for order "wu-trim" shows "relocation.planId" as "plan-7"
    And the task for order "wu-trim" shows "relocation.kind" as "Relocate"
    And the task for order "wu-trim" shows "relocation.sku" as "SKU-1001"
    And the task for order "wu-trim" shows "relocation.fromBin" as "WH1-FWD-A01-01"
    And the task for order "wu-trim" shows "relocation.toBin" as "WH1-FWD-B02-04"
    And the task for order "wu-trim" shows "relocation.ref" as "plan-7:3"

  # --- the WorkReleased pairing rules of ADR 0054 -----------------------------------

  @bdd
  Scenario: Work on the break-case path without its case break is rejected and creates no task
    When work planning publishes a WorkReleased event "evt-bc" for work unit "wu-bc" on path "break-zone-a" without any move object
    Then the consumer rejects the message
    And the order "wu-bc" has 0 "BREAK_CASE" Tasks
    And no "TaskCreated" domain event is recorded

  @bdd
  Scenario: A case break released on a path that is not the break-case path is rejected
    When work planning publishes a WorkReleased event "evt-bc2" for work unit "wu-bc2" on path "pick-zone-a" with the "case_break" object {"lpn":"LPN-1","sku":"SKU-1","loose_bin":"BIN-1","pick_task_id":"pick-1"}
    Then the consumer rejects the message
    And the order "wu-bc2" has 0 "PICK" Tasks
    And no "TaskCreated" domain event is recorded

  @bdd
  Scenario Outline: A pick plan released on the <path> path is rejected, only PICK work carries one
    When work planning publishes a WorkReleased event "evt-pk" for work unit "wu-pk" on path "<path>" with the "pick" object {"sku":"SKU-1","quantity":2,"unit":"EACH"}
    Then the consumer rejects the message
    And the order "wu-pk" has 0 "<type>" Tasks
    And no "TaskCreated" domain event is recorded

    Examples:
      | path        | type |
      | pack-zone-a | PACK |
      | slam-zone-a | SLAM |

  @bdd
  Scenario: A PICK released with a pick plan is not completed until the scans add up to the plan
    Given work planning publishes a WorkReleased event "evt-pp" for work unit "wu-pp" on path "pick-zone-a" with the "pick" object {"sku":"SKU-1","quantity":2,"unit":"EACH"}
    And Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" completes the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "pick-incomplete"
    And no "TaskCompleted" domain event is recorded

  # --- a failed application leaves no claim behind (ApplyWorkReleased, no UnitOfWork) ---

  @bdd
  Scenario: A release that failed leaves its event id unclaimed, so the redelivery is applied
    When work planning publishes a WorkReleased event "evt-retry" for work unit "wu-retry" on path "relocate-slotting" without any move object
    Then the consumer rejects the message
    And the order "wu-retry" has 0 "RELOCATE" Tasks
    When work planning publishes a WorkReleased event "evt-retry" for work unit "wu-retry" on path "relocate-slotting" with the "relocation" object {"plan_id":"plan-7","move_seq":1,"kind":"Relocate","sku":"SKU-1001","from_bin":"A-01","to_bin":"B-02"}
    Then the consumer accepts the message
    And the order "wu-retry" has 1 "RELOCATE" Task

  # --- values the consumer ignores are logged, never fatal (ADR 0041, ADR 0065) -------

  @bdd
  Scenario Outline: A line number of <line_no> on order work is <outcome>
    Given the WorkReleased consumer's log is captured
    When work planning publishes a WorkReleased event "evt-ln" with:
      | path_id      | pick-zone-a    |
      | work_unit_id | order-5-line-2 |
      | ref          | order-5        |
      | line_no      | <line_no>      |
      | cpt_minutes  | 30             |
    Then the consumer accepts the message
    And the order "order-5-line-2" has 1 "PICK" Task
    And <log>

    Examples:
      | line_no    | outcome                                  | log                                                 |
      | 2147483647 | accepted silently                        | the captured log has no WARN                        |
      | 2147483648 | ignored with a WARN naming the event     | the captured log has a WARN containing "evt-ln"     |
      | 0          | ignored without a WARN, like an absent one | the captured log has no WARN                      |

  @bdd
  Scenario: A line number above the maximum on transfer work is not an order line and is not warned about
    Given the WorkReleased consumer's log is captured
    When work planning publishes a WorkReleased event "evt-tl" with:
      | path_id      | pick-1        |
      | work_unit_id | wu-t9         |
      | ref          | demand-9      |
      | line_no      | 2147483648    |
      | cpt_minutes  | 30            |
      | transfer_ref | transfer-7    |
      | work_kind    | TRANSFER_PICK |
      | site_id      | SITE-A        |
      | sku          | sku-42        |
      | quantity     | 4             |
    Then the consumer accepts the message
    And the captured log has no WARN

  @bdd
  Scenario: A malformed order site is ignored with a WARN and the task is still created
    Given the WorkReleased consumer's log is captured
    And a Station "pack-01" is registered with capabilities "pack"
    When work planning publishes a WorkReleased event "evt-site" with:
      | path_id      | pack-zone-a |
      | work_unit_id | wu-s1       |
      | ref          | order-s1    |
      | cpt_minutes  | 60          |
      | site_id      | atl-1       |
    Then the consumer accepts the message
    And the order "wu-s1" has 1 "PACK" Task
    And the captured log has a WARN containing "evt-site"
    When Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    And the recorded events are encoded for the integration topic
    Then the "PackageManifested" integration message has no "data.site_id"
    And the "PackageManifested" integration message has "data.source_order_id" equal to "order-s1"

  # --- what a PackageManifested tells outbound-shipping (ADR 0061, ADR 0065) --------

  @bdd
  Scenario: The manifest of an order with a SLAM leg takes its cpt, site and source order from the SLAM leg
    Given a Station "pack-01" is registered with capabilities "pack"
    And work planning publishes a WorkReleased event "evt-pack" with:
      | path_id      | pack-zone-a |
      | work_unit_id | wu-m1       |
      | ref          | order-pack  |
      | cpt_minutes  | 60          |
      | site_id      | DFW2        |
    And work planning publishes a WorkReleased event "evt-slam" with:
      | path_id      | slam-zone-a |
      | work_unit_id | wu-m1       |
      | ref          | order-slam  |
      | cpt_minutes  | 90          |
      | site_id      | ATL1        |
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    When the recorded events are encoded for the integration topic
    Then the "PackageManifested" integration message has "data.order_ref" equal to "wu-m1"
    And the "PackageManifested" integration message has "data.cpt" equal to "2026-01-01T13:30:00Z"
    And the "PackageManifested" integration message has "data.site_id" equal to "ATL1"
    And the "PackageManifested" integration message has "data.source_order_id" equal to "order-slam"

  @bdd
  Scenario: Without a SLAM leg the manifest takes its cpt, site and source order from the PACK task
    Given a Station "pack-01" is registered with capabilities "pack"
    And work planning publishes a WorkReleased event "evt-pack" with:
      | path_id      | pack-zone-a |
      | work_unit_id | wu-m2       |
      | ref          | order-pack  |
      | cpt_minutes  | 60          |
      | site_id      | DFW2        |
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    When the recorded events are encoded for the integration topic
    Then the "PackageManifested" integration message has "data.cpt" equal to "2026-01-01T13:00:00Z"
    And the "PackageManifested" integration message has "data.site_id" equal to "DFW2"
    And the "PackageManifested" integration message has "data.source_order_id" equal to "order-pack"

  @bdd
  Scenario Outline: A PACK task that names no source order answers with the bare order id of its work unit
    Given a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "<order_ref>" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    When the recorded events are encoded for the integration topic
    Then the "PackageManifested" integration message has "data.source_order_id" equal to "<source_order_id>"
    And the "PackageManifested" integration message has "data.cpt" equal to "2026-01-01T13:00:00Z"
    And the "PackageManifested" integration message has no "data.site_id"

    Examples:
      | order_ref      | source_order_id |
      | order-7-line-3 | order-7         |
      | order-8        | order-8         |

  @bdd
  Scenario: The manifest site is the first leg that names one, not simply the first leg
    Given a Station "pack-01" is registered with capabilities "pack"
    And work planning publishes a WorkReleased event "evt-pack" with:
      | path_id      | pack-zone-a |
      | work_unit_id | wu-m4       |
      | ref          | order-pack  |
      | cpt_minutes  | 60          |
      | site_id      | DFW2        |
    And work planning publishes a WorkReleased event "evt-slam" with:
      | path_id      | slam-zone-a |
      | work_unit_id | wu-m4       |
      | ref          | order-slam  |
      | cpt_minutes  | 90          |
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    When the recorded events are encoded for the integration topic
    Then the "PackageManifested" integration message has "data.site_id" equal to "DFW2"
    And the "PackageManifested" integration message has "data.cpt" equal to "2026-01-01T13:30:00Z"

  @bdd
  Scenario: A PICK task of the same work unit never stands in for the PACK leg of the manifest
    Given a Station "pick-01" is registered with capabilities "pick"
    And a Station "pack-01" is registered with capabilities "pack"
    And work planning publishes a WorkReleased event "evt-pick" with:
      | path_id      | pick-zone-a |
      | work_unit_id | wu-m5       |
      | ref          | order-pick  |
      | cpt_minutes  | 30          |
      | site_id      | ATL1        |
    And work planning publishes a WorkReleased event "evt-pack" with:
      | path_id      | pack-zone-a |
      | work_unit_id | wu-m5       |
      | ref          | order-pack  |
      | cpt_minutes  | 60          |
      | site_id      | DFW2        |
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    When the recorded events are encoded for the integration topic
    Then the "PackageManifested" integration message has "data.cpt" equal to "2026-01-01T13:00:00Z"
    And the "PackageManifested" integration message has "data.site_id" equal to "DFW2"
    And the "PackageManifested" integration message has "data.source_order_id" equal to "order-pack"

  @bdd
  Scenario: A manifest encoded with no task repository wired still ships, without any correlation
    Given a Station "pack-01" is registered with capabilities "pack"
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    When the recorded events are encoded without a task repository
    Then the integration topic carries 1 "PackageManifested" message
    And the "PackageManifested" integration message has "data.order_ref" equal to "order-1"
    And the "PackageManifested" integration message has no "data.cpt"
    And the "PackageManifested" integration message has no "data.site_id"
    And the "PackageManifested" integration message has no "data.source_order_id"

  @bdd
  Scenario: The site of a Rebin PACK task comes from the order's PICK tasks and from no other leg
    Given a Station "pack-01" is registered with capabilities "pack"
    And work planning publishes a WorkReleased event "evt-pick" with:
      | path_id      | pick-zone-a     |
      | work_unit_id | order-r3-line-1 |
      | ref          | order-r3        |
      | line_no      | 1               |
      | cpt_minutes  | 30              |
    And work planning publishes a WorkReleased event "evt-slam" with:
      | path_id      | slam-zone-a |
      | work_unit_id | wu-slam3    |
      | ref          | order-r3    |
      | cpt_minutes  | 45          |
      | site_id      | DFW2        |
    And line "line-1" of order "order-r3" has arrived at Rebin; the order needs lines "line-1"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    When the recorded events are encoded for the integration topic
    Then the "PackageManifested" integration message has "data.order_ref" equal to "order-r3"
    And the "PackageManifested" integration message has no "data.site_id"

  @bdd
  Scenario: The PACK task a Rebin consolidation creates carries the order and the site of its PICK tasks
    Given a Station "pack-01" is registered with capabilities "pack"
    And work planning publishes a WorkReleased event "evt-pick" with:
      | path_id      | pick-zone-a |
      | work_unit_id | order-r1-line-1 |
      | ref          | order-r1    |
      | line_no      | 1           |
      | cpt_minutes  | 30          |
      | site_id      | ATL1        |
    And line "line-1" of order "order-r1" has arrived at Rebin; the order needs lines "line-1"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    When the recorded events are encoded for the integration topic
    Then the "PackageManifested" integration message has "data.order_ref" equal to "order-r1"
    And the "PackageManifested" integration message has "data.source_order_id" equal to "order-r1"
    And the "PackageManifested" integration message has "data.site_id" equal to "ATL1"
    And the "PackageManifested" integration message has "data.cpt" equal to "2026-01-01T13:00:00Z"

  @bdd
  Scenario: A Rebin consolidation of an order whose PICK tasks name no site creates a PACK task without one
    Given a Station "pack-01" is registered with capabilities "pack"
    And work planning publishes a WorkReleased event "evt-pick" with:
      | path_id      | pick-zone-a     |
      | work_unit_id | order-r2-line-1 |
      | ref          | order-r2        |
      | line_no      | 1               |
      | cpt_minutes  | 30              |
    And line "line-1" of order "order-r2" has arrived at Rebin; the order needs lines "line-1"
    And Station "pack-01" has claimed the next "PACK" Task
    And Station "pack-01" sealed a Package for the claimed Task with scanned contents "sku-1"
    And the SLAM weigh-check runs on the Package with an actual weight of 2.00 against an expected weight of 2.00
    When the recorded events are encoded for the integration topic
    Then the "PackageManifested" integration message has "data.source_order_id" equal to "order-r2"
    And the "PackageManifested" integration message has no "data.site_id"

  # --- the whole DOT segregation matrix (P6, ADR 0010, 49 CFR 177.848 collapsed to the nine classes) ---
  # Transcribed from the matrix documented in pack/segregation.go: class 1 conflicts with every class
  # including itself, class 9 conflicts with class 1 only, the matrix is symmetric, so each unordered
  # pair appears in both scan orders.

  @bdd
  Scenario Outline: Hazard class <class_a> scanned before class <class_b> may not share a carton
    Given product-master classifies SKU "sku-a" under DOT hazard class <class_a>
    And product-master classifies SKU "sku-b" under DOT hazard class <class_b>
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-a,sku-b"
    Then the response status is 409
    And the response is a Problem Details document of type "package-segregation-violation"
    And no "PackageSealed" domain event is recorded

    Examples:
      | class_a | class_b |
      | 1       | 1       |
      | 1       | 2       |
      | 1       | 3       |
      | 1       | 4       |
      | 1       | 5       |
      | 1       | 6       |
      | 1       | 7       |
      | 1       | 8       |
      | 1       | 9       |
      | 2       | 1       |
      | 2       | 3       |
      | 2       | 4       |
      | 2       | 5       |
      | 2       | 6       |
      | 2       | 7       |
      | 2       | 8       |
      | 3       | 1       |
      | 3       | 2       |
      | 3       | 5       |
      | 3       | 6       |
      | 4       | 1       |
      | 4       | 2       |
      | 4       | 6       |
      | 4       | 8       |
      | 5       | 1       |
      | 5       | 2       |
      | 5       | 3       |
      | 5       | 6       |
      | 5       | 8       |
      | 6       | 1       |
      | 6       | 2       |
      | 6       | 3       |
      | 6       | 4       |
      | 6       | 5       |
      | 6       | 8       |
      | 7       | 1       |
      | 7       | 2       |
      | 8       | 1       |
      | 8       | 2       |
      | 8       | 4       |
      | 8       | 5       |
      | 8       | 6       |
      | 9       | 1       |

  @bdd
  Scenario Outline: Hazard class <class_a> scanned before class <class_b> may share a carton
    Given product-master classifies SKU "sku-a" under DOT hazard class <class_a>
    And product-master classifies SKU "sku-b" under DOT hazard class <class_b>
    And a "PACK" Task for order "order-1" with a CPT 60 minutes from now requiring capabilities "pack"
    And Station "pack-01" has claimed the next "PACK" Task
    When Station "pack-01" seals a Package for the claimed Task with scanned contents "sku-a,sku-b"
    Then the response status is 201

    Examples:
      | class_a | class_b |
      | 2       | 2       |
      | 2       | 9       |
      | 3       | 3       |
      | 3       | 4       |
      | 3       | 7       |
      | 3       | 8       |
      | 3       | 9       |
      | 4       | 3       |
      | 4       | 4       |
      | 4       | 5       |
      | 4       | 7       |
      | 4       | 9       |
      | 5       | 4       |
      | 5       | 5       |
      | 5       | 7       |
      | 5       | 9       |
      | 6       | 6       |
      | 6       | 7       |
      | 6       | 9       |
      | 7       | 3       |
      | 7       | 4       |
      | 7       | 5       |
      | 7       | 6       |
      | 7       | 7       |
      | 7       | 8       |
      | 7       | 9       |
      | 8       | 3       |
      | 8       | 7       |
      | 8       | 8       |
      | 8       | 9       |
      | 9       | 2       |
      | 9       | 3       |
      | 9       | 4       |
      | 9       | 5       |
      | 9       | 6       |
      | 9       | 7       |
      | 9       | 8       |
      | 9       | 9       |
