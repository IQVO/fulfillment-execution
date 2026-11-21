# task_pool_rules.feature: scenarios derived from the context documentation
# (repo docs/docs/ddd/domain-events.md rows TaskAvailable / TaskClaimed /
# ItemPicked; ADR-0049 task pool events; ADR-0062 pick tasks announced to the
# open pool; ADR-0063 work batches behind PICK_BATCH_MODE; ADR-0058 the
# work-request delegation). The pool-announcement wire contract, the
# omit-when-unknown placement rule, the mode gates of the batch and delegation
# surfaces, and the "ItemPicked is defined but never raised" hotspot are the
# documented rules no earlier scenario pinned.
Feature: What the open-task pool is told, and the surfaces that are off by default

  Background:
    Given a running Fulfillment Execution service

  # --- TaskAvailable on the wire (ADR-0049, ADR-0062) ---------------------------

  @bdd
  Scenario: A released pick that names its site and pick face is announced to the open pool
    Given work planning publishes a WorkReleased event "evt-a1" for work unit "wu-a1" on path "pick-zone-a" with the "pick" object {"sku":"SKU-1","quantity":2,"unit":"EACH","site_id":"SITE-A","from_bin":"FACE-01"}
    Then the consumer accepts the message
    When the recorded events are encoded for the integration topic
    Then the integration topic carries 1 "TaskAvailable" message
    And the "TaskAvailable" integration message has "type" equal to "com.warehouse.wes.fulfillment-execution.task.TaskAvailable"
    And the "TaskAvailable" integration message has "subject" equal to "task-1"
    And the "TaskAvailable" integration message is keyed by its subject
    And the "TaskAvailable" integration message has "data.task_id" equal to "task-1"
    And the "TaskAvailable" integration message has "data.task_type" equal to "PICK"
    And the "TaskAvailable" integration message has "data.site_id" equal to "SITE-A"
    And the "TaskAvailable" integration message has "data.from_location" equal to "FACE-01"
    And the "TaskAvailable" integration message has "data.to_location" equal to "FACE-01"
    And the "TaskAvailable" integration message has "data.required_capabilities.0" equal to "pick"
    And the "TaskAvailable" integration message has "data.priority_band" equal to "PICK:2026-01-01T13:00:00Z"
    And the "TaskAvailable" integration message has "data.cpt" equal to "2026-01-01T13:00:00Z"
    And the "TaskAvailable" integration message has "data.available_at" equal to "2026-01-01T12:00:00Z"

  @bdd
  Scenario Outline: A released pick missing its site or its pick face is not announced
    Given work planning publishes a WorkReleased event "evt-a2" for work unit "wu-a2" on path "pick-zone-a" with the "pick" object {"sku":"SKU-1","quantity":2,"unit":"EACH"<extra>}
    Then the consumer accepts the message
    When the recorded events are encoded for the integration topic
    Then the integration topic carries 0 "TaskAvailable" messages
    And the order "wu-a2" has 1 "PICK" Task

    Examples:
      | extra                      |
      | ,"site_id":"SITE-A"        |
      | ,"from_bin":"FACE-01"      |

  @bdd
  Scenario: A released move is announced from its source bin to its destination
    Given work planning has released a RELOCATE move "Relocate" of SKU "SKU-1001" in plan "plan-7" step 3 as work unit "wu-a3" from bin "WH1-FWD-Z1-A01-B02-L3-P04" to bin "WH1-FWD-Z1-A02-B05-L1-P01"
    When the recorded events are encoded for the integration topic
    Then the integration topic carries 1 "TaskAvailable" message
    And the "TaskAvailable" integration message has "data.task_type" equal to "RELOCATE"
    And the "TaskAvailable" integration message has "data.site_id" equal to "WH1"
    And the "TaskAvailable" integration message has "data.from_location" equal to "WH1-FWD-Z1-A01-B02-L3-P04"
    And the "TaskAvailable" integration message has "data.to_location" equal to "WH1-FWD-Z1-A02-B05-L1-P01"
    And the "TaskAvailable" integration message has "data.required_capabilities.0" equal to "relocate"
    And the "TaskAvailable" integration message has "data.priority_band" equal to "RELOCATE:2026-01-01T13:00:00Z"

  @bdd
  Scenario: A claim leaves the pool carrying the station and the claim time
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-a4" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When the recorded events are encoded for the integration topic
    Then the integration topic carries 1 "TaskClaimed" message
    And the "TaskClaimed" integration message has "type" equal to "com.warehouse.wes.fulfillment-execution.task.TaskClaimed"
    And the "TaskClaimed" integration message has "data.task_id" equal to "task-1"
    And the "TaskClaimed" integration message has "data.task_type" equal to "PICK"
    And the "TaskClaimed" integration message has "data.station_id" equal to "pick-01"
    And the "TaskClaimed" integration message has "data.claimed_at" equal to "2026-01-01T12:00:00Z"
    And the "TaskClaimed" integration message is keyed by its subject

  @bdd
  Scenario: A completed pick publishes no item-level fact
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-a5" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    And Station "pick-01" completes the claimed Task
    Then exactly 1 "TaskCompleted" domain event is recorded
    And no "ItemPicked" domain event is recorded

  # --- the surfaces that are off unless configured (ADR-0063, ADR-0058) --------

  @bdd
  Scenario: With pick batching off the work-batch surface answers off and the pool is untouched
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-a6" with a CPT 30 minutes from now requiring capabilities "pick"
    When a POST is sent to "/stations/pick-01/work-batches" without a body
    Then the response status is 503
    And the response is a Problem Details document of type "batch-mode-off"
    When a POST is sent to "/work-batches/batch-9/lease-renewals" without a body
    Then the response status is 503
    And the response is a Problem Details document of type "batch-mode-off"
    When a GET is sent to "/work-batches/batch-9"
    Then the response status is 404
    And the response is a Problem Details document of type "batch-not-found"
    And the queue depth for "PICK" is 1
    When Station "pick-01" has claimed the next "PICK" Task
    Then the response status is 200
    And no "WorkBatchFormed" domain event is recorded

  @bdd
  Scenario: A work request is explicitly off while the planning delegation is not configured
    When a POST is sent to "/paths/PICK/work-requests" without a body
    Then the response status is 503
    And the response is a Problem Details document of type "work-planning-not-configured"
