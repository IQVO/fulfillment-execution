---
id: context-relationships
title: Bounded-context relationships
sidebar_label: Context relationships
sidebar_position: 5
description: How this bounded context relates to the other warehouse-systems services and to WCS, using Evans/Vernon context-mapping vocabulary — Customer/Supplier, Open Host Service, Published Language, Conformist, Anti-Corruption Layer.
---

# Bounded-context relationships

This page is the *strategic* view — which context-mapping pattern governs each
edge, and why. The *technical* view — actual topics, envelopes and wiring
status — is on the [Context map](../ecosystem/context-map.md) and
[Integration contracts](../ecosystem/integration-contracts.md).

## Vocabulary

Using the standard Evans/Vernon patterns as the platform reference uses them:

| Pattern | Meaning |
| --- | --- |
| **Customer/Supplier (C/S)** | Upstream and downstream teams negotiate; downstream's needs are a real input to upstream's planning. |
| **Open Host Service (OHS)** | Upstream publishes a stable, general-purpose protocol for many downstreams. |
| **Published Language (PL)** | A shared, documented interchange format — here, the CloudEvents-typed event catalogue plus the OpenAPI contract. |
| **Conformist (CF)** | Downstream adopts upstream's model wholesale, with no translation layer. |
| **Anti-Corruption Layer (ACL)** | Downstream translates at the boundary so the upstream model never leaks inward. |

## Edge by edge

### `wes-work-planning` → `fulfillment-execution` — **Customer/Supplier + ACL**

`wes-work-planning` is the conductor: it turns a shift's charge into a plan and
releases work continuously (waveless). When it releases a unit, it publishes
`WorkReleased` on `warehouse.work-planning.events`. This service consumes it
and creates a `Task`.

**Why Customer/Supplier and not Conformist:** the relationship is genuinely
bidirectional in planning terms. This service's queue depth is exactly the
buffer telemetry Work Planning flow-balances on, and the completion feedback
this service publishes is a required input to Work Planning's own progress
model. Neither side can do its job without the other's signal — that is the
Customer/Supplier shape, not a one-way conformity.

**The ACL is real, not nominal.** `internal/adapters/inbound/kafka/consumer.go`
decodes the envelope and maps its fields into this context's own vocabulary:

| From `WorkReleased.data` | Becomes | Via |
| --- | --- | --- |
| `path_id` | `task.Type` | process-path catalogue, longest `matchPrefix` wins; unknown id is a hard error ([ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md)) |
| `work_unit_id` | `shared.OrderRef` | direct |
| `cpt` | `shared.CPT` | `shared.NewCPT` |
| *(from the matched path)* | `shared.CapabilitySet` | the path definition's `requiredCapabilities` |
| `fragile`, `gift_wrap` | `Task.Fragile`, `Task.GiftWrap` | direct, optional (default `false`) |

Note that `data.ref` is decoded but **not** used — the deliberate choice was
`work_unit_id` as the correlation key, because that is what Work Planning's
`RecordCompletion` expects back. No upstream struct crosses the boundary. This
is precisely the discipline the reference model demands: *"WES's `Task` is
built FROM WMS's released work, not shared with it."*

### `fulfillment-execution` → `wes-work-planning` — **Customer/Supplier (feedback edge)**

The direction reverses for completions. This service publishes `TaskCompleted`
to `warehouse.fulfillment.events`; `wes-work-planning` consumes it and calls
its own `RecordCompletion(workUnitId)`.

This closes what the repo's own integration notes call the
**drum-buffer-rope feedback edge: Execution → Orchestration**. Without it, the
conductor would be releasing work into a void with no confirmation that any of
it landed, and its plan-versus-actual would be pure guesswork.

The enrichment step matters strategically: the domain event carries only
`TaskId`/`StationId`, and the adapter backfills `work_unit_id` via a
`TaskRepo` lookup. The *correlation key the consumer needs* is an integration
concern and is added at the boundary — the domain model is not reshaped to
serve a downstream's join.

### `fulfillment-execution` → WCS / equipment — **Customer/Supplier + Conformist, behind an ACL**

Strategically, this service is upstream of WCS: it decides that a carton
should be sealed and labelled, and WCS drives the conveyor, the print-and-apply
head and the check-weigher that do it.

The platform reference classifies WCS as a **Generic Subdomain** — *"Buy,
don't build — device orchestration is rarely a competitive advantage"* — which
determines the pattern on this edge. At its lower boundary a WCS is inevitably
**Conformist** to each vendor's PLC protocol; the correct defence is an
**Anti-Corruption Layer** on this side, so that vendor protocol shapes never
climb up into the `Task` or `Package` model.

:::info[Not wired today]
There is **no WCS integration in this repository**: no adapter, no topic, no
command channel. `apis/openapi.yaml` says so explicitly — driving physical
equipment "is a separate command channel," out of scope for this API. The edge
is drawn on the context map because it is the real strategic shape of the
system, and it is labelled as not-yet-built rather than implied to exist.
:::

### `fulfillment-execution` → `labor-performance` and `order-management` — **Published Language**

The same integration topic carries more than the Work Planning feedback edge.
`labor-performance` consumes `TaskCompleted` for per-associate attribution
(the claiming associate is carried on the wire,
[ADR-0014](../adr/0014-labor-performance-integration-hooks.md)).
`order-management`'s `RepromiseOrder` consumer reads `TaskCPTMissed` and
`PackageManifested` to close its promise feedback loop
([ADR-0025](../adr/0025-cpt-missed-sweep-and-package-manifested.md)). Neither
downstream is consulted about this service's model; they read the published
event catalogue as-is — Published Language, not Customer/Supplier.

### `workforce-management` → `fulfillment-execution` — **Open Host Service (one read)**

`workforce-management` owns "who is on shift, on which process path, at what
rate." It **stops at the path boundary** — its own charter says it "never links
an associate to a specific task — dispatch of individual tasks to a claiming
station belongs to Fulfillment Execution." From this side, the mirror-image
rule holds: `Station.occupant` is an opaque `OccupantId` with no roster, no
certifications, no shift window behind it.

The one technical edge is a read:
[ADR-0018](../adr/0018-installed-capacity-read-endpoint.md) added
`GET /capacity/{capability}`, which returns how many registered stations can
serve a capability so shift plans are bounded by physical station count.
That is an Open Host read of this service's own `Station` pool — no shared
type, no event, and no coupling of dispatch to shift planning. The two
contexts still change at **completely different cadences** — shifts versus
seconds.

They also share a **published language** for capability names — `pick`,
`pack`, `slam`, `rebin` — declared once in the process-path catalogue. That is
a shared *vocabulary*, not a shared type.

### `fulfillment-execution` → `inventory-storage` — **Separate Ways**

`inventory-storage` is the WMS-tier authority on stock reality. This service
does **not** consume its events: stock reality reaches it transitively, via
what `wes-work-planning` chooses to release. A `Task` carries an `orderRef`,
never a bin.

The former direct edge, a synchronous `GET /products/{sku}/classification`
per scanned SKU (`PRODUCT_CLASSIFICATION_MODE=http`, ADR-0010), was removed:
classification moved to product-master (below). In the other direction,
inventory-storage can consume this service's `TaskCompleted` (its additive
`order_ref`, [ADR-0040](../adr/0040-task-completed-carries-order-ref.md)) to
confirm an order's picks; that consumer is off by default on its side.

### `product-master` → `fulfillment-execution` — **Published Language + ACL over a local copy (opt-in)**

With `PRODUCT_CLASSIFICATION_MODE=kafka`, a consumer of
`warehouse.product-master.events` keeps `product_classification_copy` current
from `ProductClassified` events (version-guarded, deduped on the CloudEvents
`id`), and `SealPackage` reads each scanned SKU's DOT hazard class from it to
enforce package segregation
([ADR-0039](../adr/0039-product-classification-local-copy.md),
[ADR-0010](../adr/0010-package-segregation-and-sort-lane.md)). The row is
translated into a local `ClassificationInfo` at the port; the default
permissive adapter skips the lookup entirely.

### `fulfillment-execution` → `facility-layout` — **Conformist behind an ACL (one opt-in lookup)**

`facility-layout` is a **Generic Subdomain** owning the physical warehouse
map, intended as an **Open Host Service** for physical-location truth. A
`Station` may now carry an optional `locationCode`; with
`LOCATION_ROLE_MODE=http`, `RegisterStation` resolves it via
`GET /locations/{locationCode}` and rejects a known non-WorkCenter role
([ADR-0024](../adr/0024-station-location-code-and-workcenter-role-check.md)).
This service conforms to facility-layout's location vocabulary for that one
check, translated at `ports.LocationRoleLookup`. A `Task` still says *what*
work and *by when*, never *where*.

### `process-path-management` → `fulfillment-execution` — **Published Language, Conformist (opt-in)**

`process-path-management` owns the declared process-path catalogue. With
`PATH_CATALOGUE_SOURCE=kafka`, `internal/adapters/outbound/kafkacatalog`
replays its `ProcessPathCreated` / `ProcessPathUpdated` /
`ProcessPathDeactivated` events into an in-memory `pathcatalog.Catalogue`
and blocks readiness until the replay catches up. This side conforms to the
upstream's path definition (`path_id`, `match_prefix`,
`required_capabilities`, ...) without negotiating it — Conformist on a
Published Language. With the default `file` source the same shape arrives
as a YAML file instead and there is no runtime edge
([ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md)).

### `warehouse-ops-agent` → `fulfillment-execution` — **Open Host Service (MCP + REST reads)**

`warehouse-ops-agent` calls this service's MCP server (`cmd/mcp`,
Streamable HTTP) and hosts the console BFF that calls
`GET /tasks?orderRef=` for the cross-service Order Lifecycle screen
([ADR-0013](../adr/0013-fulfillment-mfe-console-adoption.md)). Both are
general-purpose, unauthenticated surfaces this side publishes for any
client ([ADR-0022](../adr/0022-remove-rest-mcp-auth.md)); the caller
conforms to them.

## Summary

| Edge | Pattern | Wired today? |
| --- | --- | --- |
| `wes-work-planning` → this | Customer/Supplier, ACL on this side | **Yes** — Kafka `warehouse.work-planning.events` |
| this → `wes-work-planning` | Customer/Supplier (feedback) | **Yes** — Kafka `warehouse.fulfillment.events` |
| this → `labor-performance` | Published Language | **Yes** — Kafka `warehouse.fulfillment.events` (`TaskCompleted`) |
| this → `order-management` | Published Language | **Yes** — Kafka `warehouse.fulfillment.events` (`TaskCPTMissed`, `PackageManifested`) |
| `workforce-management` → this | Open Host Service | **Yes** — HTTP `GET /capacity/{capability}` |
| `warehouse-ops-agent` → this | Open Host Service | **Yes** — MCP server and HTTP `GET /tasks?orderRef=` |
| `process-path-management` → this | Published Language, Conformist on this side | Opt-in — Kafka `warehouse.process-path-management.events` (`PATH_CATALOGUE_SOURCE=kafka`) |
| `product-master` → this | Published Language, ACL over a local copy on this side | **Yes** in the cluster — Kafka `warehouse.product-master.events` (`ProductClassified`, `PRODUCT_CLASSIFICATION_MODE=kafka`; binary default `permissive`) |
| this → `inventory-storage` | Published Language (one event consumed there) | Wired there, off by default — inventory-storage can consume `TaskCompleted` (with `order_ref`, ADR-0040) to confirm picks. No call from this side: the former HTTP classification lookup was removed (ADR-0039) |
| this → `facility-layout` | Conformist behind ACL | Opt-in — HTTP location-role lookup |
| this → WCS / equipment | Customer/Supplier + Conformist behind ACL | No — strategic only |

The technical view of the same edges, with the U/D direction and the
adapter evidence for each, is the [Context map](../ecosystem/context-map.md).
