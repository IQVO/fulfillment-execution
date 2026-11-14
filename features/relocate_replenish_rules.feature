# relocate_replenish_rules.feature: scenarios derived from the context
# documentation (warehouse-docs wes/docs/contexts/fulfillment-execution:
# business-context "Relocate" and "Replenish", use-cases #1, #4, #18 and the
# two "CompleteTask on a ... task" notes, aggregate-design-canvas, and the
# troubleshooting rows for the 422 slugs and the dead-lettered releases;
# ADR-0047 and ADR-0050). The move tasks are the part of the documentation the
# rest of the suite never touched: RELOCATE and REPLENISH work arrives only
# through WorkReleased and completes only with the operator's confirmation.
Feature: Relocate and replenish work: the operator confirms what was physically moved

  Background:
    Given a running Fulfillment Execution service

  # --- creation ------------------------------------------------------------------

  @bdd
  Scenario Outline: A RELOCATE or REPLENISH task cannot be created through POST /tasks
    When a POST is sent to "/tasks" with the JSON body {"type":"<type>","cpt":"2026-01-01T13:00:00Z","orderRef":"order-1","requiredCapabilities":["<capability>"]}
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
    And the queue depth for "<type>" is 0
    And no "TaskCreated" domain event is recorded

    Examples:
      | type      | capability |
      | RELOCATE  | relocate   |
      | REPLENISH | replenish  |

  @bdd
  Scenario: A released Relocate move becomes a RELOCATE task that carries the planned move
    Given work planning has released a RELOCATE move "Relocate" of SKU "SKU-1001" in plan "plan-7" step 3 as work unit "wu-reloc-1" from bin "WH1-FWD-A01-01" to bin "WH1-FWD-B02-04"
    Then the order "wu-reloc-1" has 1 "RELOCATE" Task
    And the "RELOCATE" Task for order "wu-reloc-1" has status "PENDING"
    And the "RELOCATE" Task for order "wu-reloc-1" requires capabilities "relocate"
    And the queue depth for "RELOCATE" is 1
    And the task for order "wu-reloc-1" shows "relocation.planId" as "plan-7"
    And the task for order "wu-reloc-1" shows "relocation.moveSeq" as "3"
    And the task for order "wu-reloc-1" shows "relocation.kind" as "Relocate"
    And the task for order "wu-reloc-1" shows "relocation.sku" as "SKU-1001"
    And the task for order "wu-reloc-1" shows "relocation.fromBin" as "WH1-FWD-A01-01"
    And the task for order "wu-reloc-1" shows "relocation.toBin" as "WH1-FWD-B02-04"
    And the task for order "wu-reloc-1" shows "relocation.ref" as "plan-7:3"
    And the task for order "wu-reloc-1" shows nothing at "relocation.executed"

  @bdd
  Scenario Outline: A move leaves the side the operator scans out of the planned move
    Given work planning has released a RELOCATE move "<kind>" of SKU "SKU-1001" in plan "plan-7" step 4 as work unit "wu-reloc-1" from bin "<from>" to bin "<to>"
    Then the task for order "wu-reloc-1" shows "relocation.kind" as "<kind>"
    And the task for order "wu-reloc-1" shows nothing at "relocation.<open>"

    Examples:
      | kind   | from           | to             | open    |
      | Assign |                | WH1-FWD-A01-01 | fromBin |
      | Vacate | WH1-FWD-A01-01 |                | toBin   |

  @bdd
  Scenario Outline: A station without the move capability gets no RELOCATE or REPLENISH work
    Given a Station "pick-01" is registered with capabilities "pick"
    And <release>
    When Station "pick-01" calls claimNext for task type "<type>"
    Then the response status is 409
    And the response is a Problem Details document of type "no-claimable-task"
    And the queue depth for "<type>" is 1

    Examples:
      | type      | release                                                                                                                                                                      |
      | RELOCATE  | work planning has released a RELOCATE move "Relocate" of SKU "SKU-1001" in plan "plan-7" step 3 as work unit "wu-1" from bin "WH1-FWD-A01-01" to bin "WH1-FWD-B02-04"     |
      | REPLENISH | work planning has released a REPLENISH request "req-9001" of 24 units of SKU "SKU-2002" from bin "WH1-RES-C07-02" to bin "WH1-FWD-A01-01" with priority "NORMAL" as work unit "wu-1" |

  # --- completing a RELOCATE task (ADR-0047) -----------------------------------------

  @bdd
  Scenario Outline: Completing a RELOCATE move publishes the executed bins, the operator's scan filling the side the move left open
    Given a Station "reloc-01" is registered with capabilities "relocate"
    And work planning has released a RELOCATE move "<kind>" of SKU "SKU-1001" in plan "plan-7" step 4 as work unit "wu-reloc-1" from bin "<from>" to bin "<to>"
    And Station "reloc-01" has claimed the next "RELOCATE" Task
    When Station "reloc-01" completes the claimed Task with the confirmation <confirmation>
    Then the response status is 204
    And the task for order "wu-reloc-1" shows "status" as "COMPLETED"
    And the task for order "wu-reloc-1" shows "relocation.executed.quantity" as "<quantity>"
    And the task for order "wu-reloc-1" shows "relocation.executed.fromBin" as "<executed_from>"
    And the task for order "wu-reloc-1" shows "relocation.executed.toBin" as "<executed_to>"
    When the recorded events are encoded for the integration topic
    Then the integration topic carries 1 "TaskCompleted" message
    And the "TaskCompleted" integration message has "data.task_type" equal to "RELOCATE"
    And the "TaskCompleted" integration message has "data.sku" equal to "SKU-1001"
    And the "TaskCompleted" integration message has "data.from_bin" equal to "<executed_from>"
    And the "TaskCompleted" integration message has "data.to_bin" equal to "<executed_to>"
    And the "TaskCompleted" integration message has "data.quantity" equal to "<quantity>"
    And the "TaskCompleted" integration message has "data.relocation_ref" equal to "plan-7:4"
    And the "TaskCompleted" integration message has no "data.request_ref"

    Examples:
      | kind     | from           | to             | confirmation                                                       | quantity | executed_from  | executed_to    |
      | Relocate | WH1-FWD-A01-01 | WH1-FWD-B02-04 | {"quantity":12}                                                    | 12       | WH1-FWD-A01-01 | WH1-FWD-B02-04 |
      | Assign   |                | WH1-FWD-A01-01 | {"quantity":6,"fromBin":"WH1-RES-C07-02"}                          | 6        | WH1-RES-C07-02 | WH1-FWD-A01-01 |
      | Assign   |                | WH1-FWD-A01-01 | {"quantity":6,"fromBin":"WH1-RES-C07-02","toBin":"WH1-FWD-A01-01"} | 6        | WH1-RES-C07-02 | WH1-FWD-A01-01 |
      | Vacate   | WH1-FWD-A01-01 |                | {"quantity":6,"toBin":"WH1-RES-C07-02"}                            | 6        | WH1-FWD-A01-01 | WH1-RES-C07-02 |

  @bdd
  Scenario Outline: A confirmation that does not fit the planned move is refused and the task stays claimed
    Given a Station "reloc-01" is registered with capabilities "relocate"
    And work planning has released a RELOCATE move "<kind>" of SKU "SKU-1001" in plan "plan-7" step 5 as work unit "wu-reloc-1" from bin "<from>" to bin "<to>"
    And Station "reloc-01" has claimed the next "RELOCATE" Task
    When Station "reloc-01" completes the claimed Task with the confirmation <confirmation>
    Then the response status is 422
    And the response is a Problem Details document of type "invalid-relocation-confirmation"
    And the "RELOCATE" Task for order "wu-reloc-1" has status "CLAIMED"
    And no "TaskCompleted" domain event is recorded

    Examples:
      | kind     | from           | to             | confirmation                              |
      | Assign   |                | WH1-FWD-A01-01 | {"quantity":6}                            |
      | Vacate   | WH1-FWD-A01-01 |                | {"quantity":6}                            |
      | Relocate | WH1-FWD-A01-01 | WH1-FWD-B02-04 | {"quantity":6,"fromBin":"WH1-FWD-A09-09"} |
      | Relocate | WH1-FWD-A01-01 | WH1-FWD-B02-04 | {"quantity":6,"toBin":"WH1-FWD-B09-09"}   |
      | Assign   |                | WH1-FWD-A01-01 | {"quantity":6,"fromBin":"WH1-FWD-A01-01"} |
      | Vacate   | WH1-FWD-A01-01 |                | {"quantity":6,"toBin":"WH1-FWD-A01-01"}   |

  @bdd
  Scenario Outline: A plain completion of a move task is refused until the operator confirms
    Given a Station "<station>" is registered with capabilities "<capability>"
    And <release>
    And Station "<station>" has claimed the next "<type>" Task
    When Station "<station>" completes the claimed Task
    Then the response status is 422
    And the response is a Problem Details document of type "<slug>"
    And the "<type>" Task for order "wu-1" has status "CLAIMED"
    And no "TaskCompleted" domain event is recorded
    When Station "<station>" completes the claimed Task with the confirmation <confirmation>
    Then the response status is 204
    And exactly 1 "TaskCompleted" domain event is recorded

    Examples:
      | station  | capability | type      | slug                                | confirmation   | release                                                                                                                                                                              |
      | reloc-01 | relocate   | RELOCATE  | relocation-confirmation-required    | {"quantity":12} | work planning has released a RELOCATE move "Relocate" of SKU "SKU-1001" in plan "plan-7" step 3 as work unit "wu-1" from bin "WH1-FWD-A01-01" to bin "WH1-FWD-B02-04"                 |
      | repl-01  | replenish  | REPLENISH | replenishment-confirmation-required | {"quantity":24} | work planning has released a REPLENISH request "req-9001" of 24 units of SKU "SKU-2002" from bin "WH1-RES-C07-02" to bin "WH1-FWD-A01-01" with priority "NORMAL" as work unit "wu-1" |

  @bdd
  Scenario: The claim rules run before the confirmation is looked at
    Given a Station "reloc-01" is registered with capabilities "relocate"
    And a Station "reloc-02" is registered with capabilities "relocate"
    And work planning has released a RELOCATE move "Assign" of SKU "SKU-1001" in plan "plan-7" step 5 as work unit "wu-reloc-1" from bin "" to bin "WH1-FWD-A01-01"
    And Station "reloc-01" has claimed the next "RELOCATE" Task
    When Station "reloc-02" completes the claimed Task with the confirmation {"quantity":6}
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-owner"
    When the clock advances by 6 minutes
    And Station "reloc-01" completes the claimed Task with the confirmation {"quantity":6,"fromBin":"WH1-RES-C07-02"}
    Then the response status is 409
    And the response is a Problem Details document of type "task-not-claimed"
    And no "TaskCompleted" domain event is recorded

  @bdd
  Scenario Outline: Confirmation inputs sent for any other task type are refused
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" completes the claimed Task with the confirmation <confirmation>
    Then the response status is 422
    And the response is a Problem Details document of type "not-relocation-task"
    And the "PICK" Task for order "order-1" has status "CLAIMED"
    And no "TaskCompleted" domain event is recorded

    Examples:
      | confirmation                              |
      | {"quantity":3}                            |
      | {"fromBin":"WH1-FWD-A01-01"}              |
      | {"toBin":"WH1-RES-C07-02"}                |

  @bdd
  Scenario Outline: A move task is never completed by a count below one or by a scanned bin without a count
    Given a Station "<station>" is registered with capabilities "<capability>"
    And <release>
    And Station "<station>" has claimed the next "<type>" Task
    When Station "<station>" completes the claimed Task with the confirmation <confirmation>
    Then <check>
    And the "<type>" Task for order "wu-1" has status "CLAIMED"
    And no "TaskCompleted" domain event is recorded

    Examples:
      | station  | capability | type      | confirmation                 | check                          | release                                                                                                                                                                              |
      | reloc-01 | relocate   | RELOCATE  | {"quantity":0}               | the response is a client error | work planning has released a RELOCATE move "Relocate" of SKU "SKU-1001" in plan "plan-7" step 3 as work unit "wu-1" from bin "WH1-FWD-A01-01" to bin "WH1-FWD-B02-04"                |
      | reloc-01 | relocate   | RELOCATE  | {"quantity":-3}              | the response is a client error | work planning has released a RELOCATE move "Relocate" of SKU "SKU-1001" in plan "plan-7" step 3 as work unit "wu-1" from bin "WH1-FWD-A01-01" to bin "WH1-FWD-B02-04"                |
      | reloc-01 | relocate   | RELOCATE  | {"fromBin":"WH1-FWD-A01-01"} | the response status is 422     | work planning has released a RELOCATE move "Relocate" of SKU "SKU-1001" in plan "plan-7" step 3 as work unit "wu-1" from bin "WH1-FWD-A01-01" to bin "WH1-FWD-B02-04"                |
      | repl-01  | replenish  | REPLENISH | {"quantity":0}               | the response is a client error | work planning has released a REPLENISH request "req-9001" of 24 units of SKU "SKU-2002" from bin "WH1-RES-C07-02" to bin "WH1-FWD-A01-01" with priority "NORMAL" as work unit "wu-1" |
      | repl-01  | replenish  | REPLENISH | {"quantity":-3}              | the response is a client error | work planning has released a REPLENISH request "req-9001" of 24 units of SKU "SKU-2002" from bin "WH1-RES-C07-02" to bin "WH1-FWD-A01-01" with priority "NORMAL" as work unit "wu-1" |
      | repl-01  | replenish  | REPLENISH | {"fromBin":"WH1-RES-C07-02"} | the response status is 422     | work planning has released a REPLENISH request "req-9001" of 24 units of SKU "SKU-2002" from bin "WH1-RES-C07-02" to bin "WH1-FWD-A01-01" with priority "NORMAL" as work unit "wu-1" |

  # --- completing a REPLENISH task (ADR-0050) ----------------------------------------

  @bdd
  Scenario: A released replenishment request becomes a REPLENISH task with its planned move
    Given work planning has released a REPLENISH request "req-9001" of 24 units of SKU "SKU-2002" from bin "WH1-RES-C07-02" to bin "WH1-FWD-A01-01" with priority "URGENT" as work unit "wu-rep-1"
    Then the order "wu-rep-1" has 1 "REPLENISH" Task
    And the "REPLENISH" Task for order "wu-rep-1" requires capabilities "replenish"
    And the task for order "wu-rep-1" shows "replenishment.requestId" as "req-9001"
    And the task for order "wu-rep-1" shows "replenishment.sku" as "SKU-2002"
    And the task for order "wu-rep-1" shows "replenishment.fromBin" as "WH1-RES-C07-02"
    And the task for order "wu-rep-1" shows "replenishment.toBin" as "WH1-FWD-A01-01"
    And the task for order "wu-rep-1" shows "replenishment.quantity" as "24"
    And the task for order "wu-rep-1" shows "replenishment.priority" as "URGENT"
    And the task for order "wu-rep-1" shows nothing at "replenishment.executed"

  @bdd
  Scenario Outline: A replenishment records the operator's count against the planned bins and the request reference
    Given a Station "repl-01" is registered with capabilities "replenish"
    And work planning has released a REPLENISH request "req-9001" of 24 units of SKU "SKU-2002" from bin "WH1-RES-C07-02" to bin "WH1-FWD-A01-01" with priority "NORMAL" as work unit "wu-rep-1"
    And Station "repl-01" has claimed the next "REPLENISH" Task
    When Station "repl-01" completes the claimed Task with the confirmation {"quantity":<counted>}
    Then the response status is 204
    And the task for order "wu-rep-1" shows "replenishment.quantity" as "24"
    And the task for order "wu-rep-1" shows "replenishment.executed.quantity" as "<counted>"
    And the task for order "wu-rep-1" shows "replenishment.executed.fromBin" as "WH1-RES-C07-02"
    And the task for order "wu-rep-1" shows "replenishment.executed.toBin" as "WH1-FWD-A01-01"
    When the recorded events are encoded for the integration topic
    Then the "TaskCompleted" integration message has "data.task_type" equal to "REPLENISH"
    And the "TaskCompleted" integration message has "data.sku" equal to "SKU-2002"
    And the "TaskCompleted" integration message has "data.from_bin" equal to "WH1-RES-C07-02"
    And the "TaskCompleted" integration message has "data.to_bin" equal to "WH1-FWD-A01-01"
    And the "TaskCompleted" integration message has "data.quantity" equal to "<counted>"
    And the "TaskCompleted" integration message has "data.request_ref" equal to "req-9001"
    And the "TaskCompleted" integration message has no "data.relocation_ref"

    Examples:
      | counted |
      | 24      |
      | 20      |

  @bdd
  Scenario Outline: A scanned bin is optional on a replenishment and accepted when it equals the plan
    Given a Station "repl-01" is registered with capabilities "replenish"
    And work planning has released a REPLENISH request "req-9001" of 24 units of SKU "SKU-2002" from bin "WH1-RES-C07-02" to bin "WH1-FWD-A01-01" with priority "NORMAL" as work unit "wu-rep-1"
    And Station "repl-01" has claimed the next "REPLENISH" Task
    When Station "repl-01" completes the claimed Task with the confirmation <confirmation>
    Then the response status is 204
    And the task for order "wu-rep-1" shows "replenishment.executed.fromBin" as "WH1-RES-C07-02"
    And the task for order "wu-rep-1" shows "replenishment.executed.toBin" as "WH1-FWD-A01-01"

    Examples:
      | confirmation                                                                 |
      | {"quantity":24,"fromBin":"WH1-RES-C07-02"}                                   |
      | {"quantity":24,"toBin":"WH1-FWD-A01-01"}                                     |
      | {"quantity":24,"fromBin":"WH1-RES-C07-02","toBin":"WH1-FWD-A01-01"}          |

  @bdd
  Scenario Outline: A scanned bin that differs from the plan is refused and the task stays claimed
    Given a Station "repl-01" is registered with capabilities "replenish"
    And work planning has released a REPLENISH request "req-9001" of 24 units of SKU "SKU-2002" from bin "WH1-RES-C07-02" to bin "WH1-FWD-A01-01" with priority "NORMAL" as work unit "wu-rep-1"
    And Station "repl-01" has claimed the next "REPLENISH" Task
    When Station "repl-01" completes the claimed Task with the confirmation <confirmation>
    Then the response status is 422
    And the response is a Problem Details document of type "invalid-replenishment-confirmation"
    And the "REPLENISH" Task for order "wu-rep-1" has status "CLAIMED"
    And no "TaskCompleted" domain event is recorded

    Examples:
      | confirmation                                |
      | {"quantity":24,"fromBin":"WH1-RES-C09-09"}  |
      | {"quantity":24,"toBin":"WH1-FWD-A09-09"}    |

  @bdd
  Scenario Outline: A replenishment is claimed by earliest CPT and never by its priority
    Given a Station "repl-01" is registered with capabilities "replenish"
    And work planning has released a REPLENISH request "req-normal" of 24 units of SKU "SKU-2002" from bin "WH1-RES-C07-02" to bin "WH1-FWD-A01-01" with priority "NORMAL" as work unit "wu-normal" due in 30 minutes
    And work planning has released a REPLENISH request "req-urgent" of 12 units of SKU "SKU-2003" from bin "WH1-RES-C07-03" to bin "WH1-FWD-A01-02" with priority "URGENT" as work unit "wu-urgent" due in <urgent_due> minutes
    When Station "repl-01" calls claimNext for task type "REPLENISH"
    Then the response status is 200
    And the claimed Task is a "REPLENISH" Task for order "<first>"

    Examples:
      | urgent_due | first     |
      | 60         | wu-normal |
      | 10         | wu-urgent |

  # --- the WorkReleased contract of the two move paths ---------------------------------

  @bdd
  Scenario Outline: A released move is accepted only when it keeps the relocation contract
    When work planning publishes a WorkReleased event "evt-r" for work unit "wu-r" on path "relocate-slotting" with the "relocation" object <move>
    Then the consumer <verdict> the message
    And the order "wu-r" has <tasks> "RELOCATE" Tasks
    And exactly <created> "TaskCreated" domain events are recorded

    Examples:
      | move                                                                                                    | verdict | tasks | created |
      | {"plan_id":"plan-7","kind":"Relocate","sku":"SKU-1001","from_bin":"A-01","to_bin":"B-02"}               | rejects | 0     | 0       |
      | {"plan_id":"plan-7","move_seq":-1,"kind":"Relocate","sku":"SKU-1001","from_bin":"A-01","to_bin":"B-02"} | rejects | 0     | 0       |
      | {"plan_id":"plan-7","move_seq":1,"kind":"Teleport","sku":"SKU-1001","from_bin":"A-01","to_bin":"B-02"}  | rejects | 0     | 0       |
      | {"plan_id":"plan-7","move_seq":1,"kind":"relocate","sku":"SKU-1001","from_bin":"A-01","to_bin":"B-02"}  | rejects | 0     | 0       |
      | {"plan_id":"plan-7","move_seq":1,"kind":"Relocate","sku":"SKU-1001","from_bin":"A-01"}                  | rejects | 0     | 0       |
      | {"plan_id":"plan-7","move_seq":1,"kind":"Relocate","sku":"SKU-1001","to_bin":"B-02"}                    | rejects | 0     | 0       |
      | {"plan_id":"plan-7","move_seq":1,"kind":"Assign","sku":"SKU-1001","from_bin":"A-01"}                    | rejects | 0     | 0       |
      | {"plan_id":"plan-7","move_seq":1,"kind":"Vacate","sku":"SKU-1001","to_bin":"B-02"}                      | rejects | 0     | 0       |
      | {"plan_id":"plan-7","move_seq":1,"kind":"Relocate","sku":"SKU-1001","from_bin":"A-01","to_bin":"A-01"}  | rejects | 0     | 0       |
      | {"plan_id":" ","move_seq":1,"kind":"Relocate","sku":"SKU-1001","from_bin":"A-01","to_bin":"B-02"}       | rejects | 0     | 0       |
      | {"plan_id":"plan-7","move_seq":1,"kind":"Relocate","sku":" ","from_bin":"A-01","to_bin":"B-02"}         | rejects | 0     | 0       |
      | {"plan_id":"plan-7","move_seq":0,"kind":"Relocate","sku":"SKU-1001","from_bin":"A-01","to_bin":"B-02"}  | accepts | 1     | 1       |
      | {"plan_id":"plan-7","move_seq":1,"kind":"Assign","sku":"SKU-1001","from_bin":"R-07","to_bin":"B-02"}    | accepts | 1     | 1       |
      | {"plan_id":"plan-7","move_seq":1,"kind":"Vacate","sku":"SKU-1001","from_bin":"A-01","to_bin":"R-07"}    | accepts | 1     | 1       |

  @bdd
  Scenario Outline: A move released on the wrong path is rejected and creates no task
    When work planning publishes a WorkReleased event "evt-p" for work unit "wu-p" on path "<path>" with the "<key>" object <object>
    Then the consumer rejects the message
    And the order "wu-p" has 0 "<type>" Tasks
    And no "TaskCreated" domain event is recorded

    Examples:
      | path              | key           | object                                                                                                              | type      |
      | pick-zone-a       | relocation    | {"plan_id":"plan-7","move_seq":1,"kind":"Relocate","sku":"SKU-1001","from_bin":"A-01","to_bin":"B-02"}              | PICK      |
      | replenish-pickface | relocation    | {"plan_id":"plan-7","move_seq":1,"kind":"Relocate","sku":"SKU-1001","from_bin":"A-01","to_bin":"B-02"}             | REPLENISH |
      | pick-zone-a       | replenishment | {"request_id":"req-9001","sku":"SKU-2002","from_bin":"R-07","to_bin":"F-01","quantity":24,"priority":"NORMAL"}      | PICK      |
      | relocate-slotting | replenishment | {"request_id":"req-9001","sku":"SKU-2002","from_bin":"R-07","to_bin":"F-01","quantity":24,"priority":"NORMAL"}      | RELOCATE  |

  @bdd
  Scenario Outline: Work on a move path without its move is rejected and creates no task
    When work planning publishes a WorkReleased event "evt-m" for work unit "wu-m" on path "<path>" without any move object
    Then the consumer rejects the message
    And the order "wu-m" has 0 "<type>" Tasks
    And no "TaskCreated" domain event is recorded

    Examples:
      | path               | type      |
      | relocate-slotting  | RELOCATE  |
      | replenish-pickface | REPLENISH |

  @bdd
  Scenario Outline: A replenishment request that breaks the contract is rejected and creates no task
    When work planning publishes a WorkReleased event "evt-q" for work unit "wu-q" on path "replenish-pickface" with the "replenishment" object <request>
    Then the consumer rejects the message
    And the order "wu-q" has 0 "REPLENISH" Tasks
    And no "TaskCreated" domain event is recorded

    Examples:
      | request                                                                                                              |
      | {"request_id":" ","sku":"SKU-2002","from_bin":"R-07","to_bin":"F-01","quantity":24,"priority":"NORMAL"}              |
      | {"request_id":"req-9001","sku":" ","from_bin":"R-07","to_bin":"F-01","quantity":24,"priority":"NORMAL"}              |
      | {"request_id":"req-9001","sku":"SKU-2002","from_bin":" ","to_bin":"F-01","quantity":24,"priority":"NORMAL"}          |
      | {"request_id":"req-9001","sku":"SKU-2002","from_bin":"R-07","to_bin":" ","quantity":24,"priority":"NORMAL"}          |
      | {"request_id":"req-9001","sku":"SKU-2002","from_bin":"R-07","to_bin":"R-07","quantity":24,"priority":"NORMAL"}       |
      | {"request_id":"req-9001","sku":"SKU-2002","from_bin":"R-07","to_bin":"F-01","quantity":0,"priority":"NORMAL"}        |
      | {"request_id":"req-9001","sku":"SKU-2002","from_bin":"R-07","to_bin":"F-01","quantity":-5,"priority":"NORMAL"}       |
      | {"request_id":"req-9001","sku":"SKU-2002","from_bin":"R-07","to_bin":"F-01","priority":"NORMAL"}                     |
      | {"request_id":"req-9001","sku":"SKU-2002","from_bin":"R-07","to_bin":"F-01","quantity":24,"priority":"HIGH"}         |
      | {"request_id":"req-9001","sku":"SKU-2002","from_bin":"R-07","to_bin":"F-01","quantity":24,"priority":"urgent"}       |
      | {"request_id":"req-9001","sku":"SKU-2002","from_bin":"R-07","to_bin":"F-01","quantity":24}                           |

  @bdd
  Scenario Outline: The replenishment request id may be 200 characters long and not one more
    When work planning publishes a REPLENISH request whose id has <length> characters as work unit "wu-len"
    Then the consumer <verdict> the message
    And the order "wu-len" has <tasks> "REPLENISH" Tasks

    Examples:
      | length | verdict | tasks |
      | 200    | accepts | 1     |
      | 201    | rejects | 0     |
