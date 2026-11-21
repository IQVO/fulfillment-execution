# pick_scan_rules.feature: scenarios derived from the context documentation
# (repo docs/docs/ddd/use-cases.md rows for ConfirmPickScan and
# CompleteCaseBreak; ADR-0054 "Case and each pick scans; BREAK_CASE task";
# ADR-0055 "Idempotent pick scans; BREAK_CASE is claimable"). The scan and
# case-break commands are the part of the documented surface the suite never
# drove: every earlier scenario stops at the refusal (pick-incomplete,
# case-break-completion-required, not-case-break-task) and no scenario ever
# confirms a pick, adds eaches, or breaks a case.
Feature: Pick scans confirm eaches; a case break completes through inventory-storage

  Background:
    Given a running Fulfillment Execution service

  # --- eaches arithmetic (ADR-0054: "quantities stay in eaches") ----------------

  @bdd
  Scenario: An EACH scan adds its count to the picked eaches
    Given product-master has declared the EACH GTIN "2345600001117" of SKU "SKU-1"
    And a Station "pick-01" is registered with capabilities "pick"
    And work planning publishes a WorkReleased event "evt-s1" for work unit "wu-s1" on path "pick-zone-a" with the "pick" object {"sku":"SKU-1","quantity":6,"unit":"EACH"}
    Then the consumer accepts the message
    Given Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" scans "2345600001117" against the claimed Task
    Then the response status is 200
    And the response at "addedEaches" is "1"
    And the response at "pickedEaches" is "1"
    And the response at "plannedEaches" is "6"
    When Station "pick-01" scans "2345600001117" x 3 against the claimed Task
    Then the response at "addedEaches" is "3"
    And the response at "pickedEaches" is "4"

  @bdd
  Scenario: A CASE scan adds count times the units per pack in eaches
    Given product-master has declared the CASE GTIN "12345600002104" of SKU "SKU-1" with 12 units per pack
    And a Station "pick-01" is registered with capabilities "pick"
    And work planning publishes a WorkReleased event "evt-s2" for work unit "wu-s2" on path "pick-zone-a" with the "pick" object {"sku":"SKU-1","quantity":24,"unit":"CASE"}
    Then the consumer accepts the message
    Given Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" scans "12345600002104" x 2 against the claimed Task
    Then the response status is 200
    And the response at "level" is "CASE"
    And the response at "addedEaches" is "24"
    And the response at "pickedEaches" is "24"
    And the response at "plannedEaches" is "24"

  @bdd
  Scenario: A pick completes once its scans add up to the planned quantity
    Given product-master has declared the EACH GTIN "2345600001117" of SKU "SKU-1"
    And a Station "pick-01" is registered with capabilities "pick"
    And work planning publishes a WorkReleased event "evt-s3" for work unit "wu-s3" on path "pick-zone-a" with the "pick" object {"sku":"SKU-1","quantity":2,"unit":"EACH"}
    Then the consumer accepts the message
    Given Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" completes the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "pick-incomplete"
    When Station "pick-01" scans "2345600001117" against the claimed Task
    And Station "pick-01" scans "2345600001117" against the claimed Task
    And Station "pick-01" completes the claimed Task
    Then the response status is 204
    And exactly 1 "TaskCompleted" domain event is recorded
    When Station "pick-01" scans "2345600001117" against the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "task-already-completed"

  # --- a rejected scan changes nothing (ADR-0054) -------------------------------

  @bdd
  Scenario: A scan past the planned quantity is refused and adds nothing
    Given product-master has declared the EACH GTIN "2345600001117" of SKU "SKU-1"
    And a Station "pick-01" is registered with capabilities "pick"
    And work planning publishes a WorkReleased event "evt-s4" for work unit "wu-s4" on path "pick-zone-a" with the "pick" object {"sku":"SKU-1","quantity":2,"unit":"EACH"}
    Then the consumer accepts the message
    Given Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" scans "2345600001117" x 2 against the claimed Task
    Then the response status is 200
    When Station "pick-01" scans "2345600001117" against the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "pick-over-quantity"
    When Station "pick-01" completes the claimed Task
    Then the response status is 204

  @bdd
  Scenario: A scan naming another SKU's GTIN is refused
    Given product-master has declared the EACH GTIN "2345600001117" of SKU "SKU-1"
    And product-master has declared the EACH GTIN "2345600009991" of SKU "SKU-9"
    And a Station "pick-01" is registered with capabilities "pick"
    And work planning publishes a WorkReleased event "evt-s5" for work unit "wu-s5" on path "pick-zone-a" with the "pick" object {"sku":"SKU-1","quantity":2,"unit":"EACH"}
    Then the consumer accepts the message
    Given Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" scans "2345600009991" against the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "scan-sku-mismatch"
    When Station "pick-01" scans "2345600001117" against the claimed Task
    Then the response at "pickedEaches" is "1"

  @bdd
  Scenario: A scan at the wrong pack level is refused
    Given product-master has declared the EACH GTIN "2345600001117" of SKU "SKU-1"
    And product-master has declared the CASE GTIN "12345600002104" of SKU "SKU-1" with 12 units per pack
    And a Station "pick-01" is registered with capabilities "pick"
    And work planning publishes a WorkReleased event "evt-s6" for work unit "wu-s6" on path "pick-zone-a" with the "pick" object {"sku":"SKU-1","quantity":2,"unit":"EACH"}
    Then the consumer accepts the message
    Given Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" scans "12345600002104" against the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "scan-level-mismatch"
    When Station "pick-01" scans "2345600001117" x 2 against the claimed Task
    Then the response at "pickedEaches" is "2"

  @bdd
  Scenario Outline: A scan no pack level carries, or that is not a GTIN at all, is refused
    Given product-master has declared the EACH GTIN "2345600001117" of SKU "SKU-1"
    And a Station "pick-01" is registered with capabilities "pick"
    And work planning publishes a WorkReleased event "evt-s7" for work unit "wu-s7" on path "pick-zone-a" with the "pick" object {"sku":"SKU-1","quantity":2,"unit":"EACH"}
    Then the consumer accepts the message
    Given Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" scans "<gtin>" against the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "<slug>"

    Examples:
      | gtin          | slug                |
      | 6789012345678 | unknown-gtin        |
      | 2345600001110 | invalid-scanned-gtin |

  @bdd
  Scenario: A scan on a pick that carries no plan is refused
    Given product-master has declared the EACH GTIN "2345600001117" of SKU "SKU-1"
    And a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-s8" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" scans "2345600001117" against the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "no-pick-plan"

  @bdd
  Scenario: A scan by a station that does not hold the claim is refused
    Given product-master has declared the EACH GTIN "2345600001117" of SKU "SKU-1"
    And a Station "pick-01" is registered with capabilities "pick"
    And a Station "pick-02" is registered with capabilities "pick"
    And work planning publishes a WorkReleased event "evt-s9" for work unit "wu-s9" on path "pick-zone-a" with the "pick" object {"sku":"SKU-1","quantity":2,"unit":"EACH"}
    Then the consumer accepts the message
    Given Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-02" scans "2345600001117" against the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-owner"
    And the "PICK" Task for order "wu-s9" has status "CLAIMED"

  # --- the case break (ADR-0054: POST /tasks/{id}/case-breaks) ------------------

  @bdd
  Scenario: A claimed case break completes through inventory-storage and publishes its completion
    Given a Station "break-01" is registered with capabilities "break"
    And work planning publishes a WorkReleased event "evt-b9" with:
      | path_id                  | break-zone-a |
      | work_unit_id             | wu-b9        |
      | ref                      | order-b9     |
      | cpt_minutes              | 20           |
      | case_break.lpn           | LPN-9        |
      | case_break.sku           | SKU-1        |
      | case_break.loose_bin     | BIN-LOOSE    |
      | case_break.pick_task_id  | pick-9       |
    Then the consumer accepts the message
    Given Station "break-01" has claimed the next "BREAK_CASE" Task
    When Station "break-01" breaks the case of the claimed Task
    Then the response status is 204
    And the "BREAK_CASE" Task for order "wu-b9" has status "COMPLETED"
    And exactly 1 "TaskCompleted" domain event is recorded
    And inventory-storage received 1 case break for LPN "LPN-9" into bin "BIN-LOOSE" under the idempotency key "break-case:task-1"

  @bdd
  Scenario: An unreachable inventory-storage leaves the case claimed, and a retry never breaks twice
    Given a Station "break-01" is registered with capabilities "break"
    And work planning publishes a WorkReleased event "evt-b10" with:
      | path_id                  | break-zone-a  |
      | work_unit_id             | wu-b10        |
      | ref                      | order-b10     |
      | cpt_minutes              | 20            |
      | case_break.lpn           | LPN-10        |
      | case_break.sku           | SKU-1         |
      | case_break.loose_bin     | BIN-LOOSE     |
      | case_break.pick_task_id  | pick-10       |
    Then the consumer accepts the message
    Given Station "break-01" has claimed the next "BREAK_CASE" Task
    And inventory-storage is unreachable for case breaks
    When Station "break-01" breaks the case of the claimed Task
    Then the response status is 503
    And the response is a Problem Details document of type "case-break-unavailable"
    And the "BREAK_CASE" Task for order "wu-b10" has status "CLAIMED"
    And no "TaskCompleted" domain event is recorded
    Given inventory-storage is reachable for case breaks again
    When Station "break-01" breaks the case of the claimed Task
    Then the response status is 204
    And inventory-storage received 2 case breaks for LPN "LPN-10" into bin "BIN-LOOSE" under the idempotency key "break-case:task-1"

  @bdd
  Scenario: A case break by a station that does not hold the claim is refused
    Given a Station "break-01" is registered with capabilities "break"
    And a Station "break-02" is registered with capabilities "break"
    And work planning publishes a WorkReleased event "evt-b11" with:
      | path_id                  | break-zone-a  |
      | work_unit_id             | wu-b11        |
      | ref                      | order-b11     |
      | cpt_minutes              | 20            |
      | case_break.lpn           | LPN-11        |
      | case_break.sku           | SKU-1         |
      | case_break.loose_bin     | BIN-LOOSE     |
      | case_break.pick_task_id  | pick-11       |
    Then the consumer accepts the message
    Given Station "break-01" has claimed the next "BREAK_CASE" Task
    When Station "break-02" breaks the case of the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-owner"
    And the "BREAK_CASE" Task for order "wu-b11" has status "CLAIMED"
    And inventory-storage received 0 case breaks for LPN "LPN-11" into bin "BIN-LOOSE" under the idempotency key "break-case:task-1"

  @bdd
  Scenario: A case break on a task that is not a case break is refused
    Given a Station "break-01" is registered with capabilities "break,pick"
    And a "PICK" Task for order "order-b12" with a CPT 30 minutes from now requiring capabilities "pick"
    Given Station "break-01" has claimed the next "PICK" Task
    When Station "break-01" breaks the case of the claimed Task
    Then the response status is 409
    And the response is a Problem Details document of type "not-case-break-task"
    And the "PICK" Task for order "order-b12" has status "CLAIMED"
