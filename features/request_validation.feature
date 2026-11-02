# Request validation of every write endpoint, and the operational probes.
#
# Derived from:
#   - apis/openapi.yaml: POST /tasks ("type" in the TaskType enum, "cpt",
#     "orderRef", at least one "requiredCapabilities"), POST /stations
#     (stationId, at least one capability), POST /stations/{id}/check-ins
#     (occupantId), POST /tasks/{id}/lease-renewals and /completions (stationId),
#     POST /tasks/{id}/package-seals (stationId) -- a missing required field
#     or an unreadable body is 400 invalid-request, an RFC 7807 problem
#   - POST /stations 201 with a Location of /stations/{id}; locationCode is
#     omitted entirely (never defaulted to "") when the station has none
#   - GET /queues/{taskType}/depth and /capacity/{capability}: read-only
#     projections; POST /task-lease-expirations -> {"freed": n} and
#     POST /cpt-miss-sweeps -> {"reported": n}
#   - /readyz (graceful shutdown): 200 {"status":"ready"} while serving, 503
#     {"status":"not_ready"} once shutdown begins; /healthz never flips

Feature: Rejecting malformed requests and reporting health
  As the fleet's execution service
  I want every write validated at the boundary
  So that a malformed request never reaches the domain or leaves a trace

  Background:
    Given a running Fulfillment Execution service

  @bdd
  Scenario Outline: A task missing a required field is rejected
    When a POST is sent to "/tasks" with the JSON body <body>
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
    And no "TaskCreated" domain event is recorded

    Examples:
      | body                                                                                         |
      | {"cpt":"2026-01-01T13:00:00Z","orderRef":"order-1","requiredCapabilities":["pick"]}          |
      | {"type":"PICK","orderRef":"order-1","requiredCapabilities":["pick"]}                         |
      | {"type":"PICK","cpt":"2026-01-01T13:00:00Z","requiredCapabilities":["pick"]}                 |
      | {"type":"PICK","cpt":"2026-01-01T13:00:00Z","orderRef":"order-1"}                            |
      | {"type":"PICK","cpt":"2026-01-01T13:00:00Z","orderRef":"order-1","requiredCapabilities":[]}  |
      | {"type":"pick","cpt":"2026-01-01T13:00:00Z","orderRef":"order-1","requiredCapabilities":["pick"]} |
      | {"type":"PICK","cpt":"not-a-time","orderRef":"order-1","requiredCapabilities":["pick"]}      |
      | {"type":"PICK","cpt":                                                                        |

  @bdd
  Scenario Outline: Every task type in the enum can be created
    When a POST is sent to "/tasks" with the JSON body {"type":"<type>","cpt":"2026-01-01T13:00:00Z","orderRef":"order-1","requiredCapabilities":["<capability>"]}
    Then the response status is 201
    And the response field "type" is "<type>"
    And the response field "status" is "PENDING"
    And the queue depth for "<type>" is 1

    Examples:
      | type     | capability |
      | PICK     | pick       |
      | PACK     | pack       |
      | SLAM     | slam       |
      | REBIN    | rebin      |
      | DISPATCH | dispatch   |
      | ARRIVAL  | arrival    |

  @bdd
  Scenario Outline: Registering a station needs an id and at least one capability
    When a POST is sent to "/stations" with the JSON body <body>
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"

    Examples:
      | body                                   |
      | {"capabilities":["pick"]}              |
      | {"stationId":"pick-01"}                |
      | {"stationId":"pick-01","capabilities":[]} |
      | {"stationId":"pick-01","capabilities": |

  @bdd
  Scenario: A registered station is addressable and not occupied
    When a POST is sent to "/stations" with the JSON body {"stationId":"pick-01","capabilities":["pick"]}
    Then the response status is 201
    And the response header "Location" is "/stations/pick-01"
    And the response field "id" is "pick-01"
    And the response field "occupied" is false
    And the response has no "locationCode" field
    And the installed capacity for "pick" is 1

  @bdd
  Scenario: A station location code is echoed when one was given
    When a POST is sent to "/stations" with the JSON body {"stationId":"pick-01","capabilities":["pick"],"locationCode":"A-01-02"}
    Then the response status is 201
    And the response field "locationCode" is "A-01-02"

  @bdd
  Scenario Outline: A check-in needs an occupant
    Given a Station "pick-01" is registered with capabilities "pick"
    When a POST is sent to "/stations/pick-01/check-ins" with the JSON body <body>
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"
    When worker "worker-1" checks in at Station "pick-01"
    Then the response status is 200
    And the Station response shows the station is occupied

    Examples:
      | body              |
      | {}                |
      | {"occupantId":""} |
      | {"occupantId":    |

  @bdd
  Scenario: Checking in at a station that was never registered is not found
    When a POST is sent to "/stations/ghost/check-ins" with the JSON body {"occupantId":"worker-1"}
    Then the response status is 404
    And the response is a Problem Details document of type "station-not-found"

  @bdd
  Scenario Outline: Renewing, completing or sealing needs the claiming station
    When a POST is sent to "/tasks/task-1/<action>" with the JSON body <body>
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"

    Examples:
      | action        | body                  |
      | lease-renewals | {}                    |
      | lease-renewals | {"stationId":""}      |
      | completions  | {}                    |
      | completions  | {"stationId":""}      |
      | package-seals | {"contents":["sku-1"]} |
      | package-seals | {"stationId":"","contents":["sku-1"]} |

  @bdd
  Scenario: A claim request with an unreadable body is rejected
    When a POST is sent to "/stations/pick-01/task-claims" with the JSON body {"taskType":
    Then the response status is 400
    And the response is a Problem Details document of type "invalid-request"

  @bdd
  Scenario: The queue depth counts only pending tasks of that type
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And a "PICK" Task for order "order-2" with a CPT 60 minutes from now requiring capabilities "pick"
    And a "PACK" Task for order "order-3" with a CPT 30 minutes from now requiring capabilities "pack"
    Then the queue depth for "PICK" is 2
    And the queue depth for "PACK" is 1
    When Station "pick-01" has claimed the next "PICK" Task
    Then the queue depth for "PICK" is 1
    And the queue depth for "PACK" is 1

  @bdd
  Scenario: An empty queue has depth zero
    When a GET is sent to "/queues/SLAM/depth"
    Then the response status is 200
    And the response field "taskType" is "SLAM"
    And the response field "depth" is the number 0

  @bdd
  Scenario: A task completed no longer counts towards the queue depth
    Given a Station "pick-01" is registered with capabilities "pick"
    And a "PICK" Task for order "order-1" with a CPT 30 minutes from now requiring capabilities "pick"
    And Station "pick-01" has claimed the next "PICK" Task
    When Station "pick-01" completes the claimed Task
    Then the queue depth for "PICK" is 0

  @bdd
  Scenario: The sweeps report what they did as a count
    When a POST is sent to "/task-lease-expirations" without a body
    Then the response status is 200
    And the response field "freed" is the number 0
    When a POST is sent to "/cpt-miss-sweeps" without a body
    Then the response status is 200
    And the response field "reported" is the number 0

  @bdd
  Scenario: Several stations holding a capability each add to its installed capacity
    Given a Station "pack-01" is registered with capabilities "pack"
    And a Station "pack-02" is registered with capabilities "pack"
    And a Station "pack-03" is registered with capabilities "pack,slam"
    When a GET is sent to "/capacity/pack"
    Then the response status is 200
    And the response field "capability" is "pack"
    And the response field "installed" is the number 3

  @bdd
  Scenario: A station that gives up a capability stops counting towards it
    Given a Station "pack-01" is registered with capabilities "pack,slam"
    When a POST is sent to "/stations" with the JSON body {"stationId":"pack-01","capabilities":["pack"]}
    Then the installed capacity for "slam" is 0
    And the installed capacity for "pack" is 1

  @bdd
  Scenario: Liveness and readiness while serving
    When a GET is sent to "/healthz"
    Then the response status is 200
    When a GET is sent to "/readyz"
    Then the response status is 200
    And the response field "status" is "ready"

  @bdd
  Scenario: Readiness flips to not ready on graceful shutdown while liveness stays up
    When the service begins graceful shutdown
    And a GET is sent to "/readyz"
    Then the response status is 503
    And the response field "status" is "not_ready"
    When a GET is sent to "/healthz"
    Then the response status is 200
