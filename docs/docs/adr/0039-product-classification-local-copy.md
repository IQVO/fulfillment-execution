---
id: 0039-product-classification-local-copy
slug: /adr/0039-product-classification-local-copy
title: 39. Product classification from a local copy of product-master events
sidebar_label: "39. Classification: local copy"
description: "ADR 0039 — SealPackage's ProductClassificationLookup reads a local Postgres copy fed by product-master's ProductClassified events on warehouse.product-master.events instead of calling inventory-storage over REST. PRODUCT_CLASSIFICATION_MODE becomes kafka|permissive; http is removed and rejected at boot. Supersedes the live-lookup part of ADR-0010."
---

# 39. Product classification from a local copy of product-master events

## Status

Accepted. Decided 2026-10-06 (product-master migration, stage D).
**Supersedes the live-lookup part of [ADR-0010](./0010-package-segregation-and-sort-lane.md)**
(the `productclassification` HTTP client to inventory-storage, its
`PRODUCT_CLASSIFICATION_MODE=http` switch and `INVENTORY_STORAGE_BASE_URL`).
ADR-0010's segregation matrix, `SortLane`, the port and its fail-open
semantics are unchanged. Also retires the classification breaker that
[ADR-0029](./0029-resilience-circuit-breakers-retry-dlq-shutdown.md) added
(the facility-layout breaker stays). Companion decisions: product-master
ADR 0001 (product-master owns `ProductClassification`), product-master
ADR 0003 (migration, stage D: readers move to local copies) and
inventory-storage ADR 0034 (hand-over of classification ownership).

## Context

`SealPackage` looks up every scanned SKU's classification to enforce
same-package DOT hazard segregation (ADR-0010). Until now the lookup was a
synchronous `GET /products/{sku}/classification` to inventory-storage, behind
retry and a circuit breaker, selected by `PRODUCT_CLASSIFICATION_MODE=http`.

Classification ownership moved to the new **product-master** context
(product-master ADR 0001). product-master publishes the full classification
of a SKU as `com.warehouse.wms.product-master.product.ProductClassified` on
`warehouse.product-master.events` (key and `subject` = SKU), with the product
aggregate `version` after the change, and asks every reader to keep a local
copy keyed by SKU instead of calling it (its AsyncAPI, "Local copies, not
lookups"). inventory-storage stops being the source of truth and deprecates
its `GET` endpoint until stage E (inventory-storage ADR 0034).

The forces:

- A seal-time REST call made pack stations depend on another context's
  availability; ADR-0010 accepted that a lookup error silently skipped the
  segregation check for that SKU.
- The fleet rule is event-carried state transfer between contexts: CloudEvents
  1.0, dispatch on the full `type`, dedupe on `id` in the same transaction as
  the effect, commit the offset afterwards.
- product-master has no "unclassify" event in v1, and messages for one SKU can
  be redelivered or arrive after a newer one was applied.

## Decision

### 1. The port stays; the adapter changes

`ports.ProductClassificationLookup` and `SealPackage` are unchanged. A new
outbound adapter, `internal/adapters/outbound/productclassificationcopy`,
implements the port from a local table and maps a row to exactly what the
HTTP client returned:

| copy | `ClassificationInfo` | was (HTTP client) |
| --- | --- | --- |
| row present | `Known=true`, `Hazmat`/`Fragile` from `handling_tags`, `DOTHazardClass` from `dot_hazard_class` (0 when unset) | `200` |
| no row | `Known=false`, nil error | `404` |
| read fails | error | transport error / unexpected status |

`SealPackage` therefore keeps its semantics: an unknown SKU or a failed
read is treated as unclassified for that SKU only (fail-open per item), and
the segregation check fails closed inside one package.

Three implementations: `PostgresStore` (production), `MemoryStore` (when
`DATABASE_URL` is unset, as every other repo in this service supports) and
`PermissiveLookup` (`permissive` mode, never classified). The HTTP client
package, its breaker wiring and its tests are deleted.

### 2. The local copy

Migration `0015_product_classification_copy`:

```sql
CREATE TABLE product_classification_copy (
    sku               TEXT PRIMARY KEY,
    handling_tags     TEXT[] NOT NULL DEFAULT '{}',
    temperature_class TEXT,
    dot_hazard_class  INTEGER CHECK (dot_hazard_class BETWEEN 1 AND 9),
    version           BIGINT NOT NULL CHECK (version >= 1),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

One row per SKU. A message is applied only when its `version` is greater
than the stored one (insert when absent): an `INSERT ... ON CONFLICT (sku) DO
UPDATE ... WHERE stored.version < incoming.version`. The payload is a
full-state replacement, so an applied message overwrites every column
(omitted optional fields become `NULL`).

### 3. The consumer

`internal/adapters/inbound/kafka.ProductClassifiedConsumer` reads
`warehouse.product-master.events`:

- consumer group from env `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` (stable and
  shared by replicas; never a literal); a new group starts at the earliest
  offset so the copy receives the whole history;
- `FetchMessage`, then `usecases.ApplyProductClassified`, which claims the
  CloudEvents `id` in the existing `processed_events` table and runs the
  version-guarded upsert in ONE `UnitOfWork`, then `CommitMessages`;
- only the full type `com.warehouse.wms.product-master.product.ProductClassified`
  is applied; every other type on the topic is ignored and committed past;
- a message that is not a valid CloudEvent, or a `ProductClassified` whose
  payload breaks the contract (no `sku`, `version < 1`, `dot_hazard_class`
  outside 1-9, undecodable data), is logged at WARN and committed past;
- a transient failure (database) is retried on the same message with capped
  backoff (200 ms to 5 s) until it succeeds or the process stops; the offset
  is never committed on failure.

There is no DLQ: nothing deterministic is retried, and a transient failure
must not lose the update.

### 4. Configuration and boot

| `PRODUCT_CLASSIFICATION_MODE` | effect |
| --- | --- |
| unset / `permissive` (default) | `PermissiveLookup`; no consumer |
| `kafka` | copy adapter + consumer; requires `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` |
| `http` | **boot error**: the mode was removed |
| anything else | boot error |

The mode is validated first in `cmd/execution`, before anything dials. The
consumer starts with the other consumers and stops in the existing shutdown
order: readiness flips, the HTTP server drains, the relay stops, the consumer
context is cancelled and the process waits for every consumer loop, then the
readers close, then the pool. `INVENTORY_STORAGE_BASE_URL` is no longer read
(classification was its only use here).

`cmd/mcp` does not build `SealPackage` or the lookup, so it gets neither the
copy adapter nor a consumer. `cmd/fulfillment-projector` and
`cmd/fulfillment-reports` are unaffected.

The Helm chart has no dedicated values for consumer groups or for this mode;
both env vars are supplied through `extraEnv` (warehouse-infra
`sync_edge_env`), which must switch from `PRODUCT_CLASSIFICATION_MODE=http` +
`INVENTORY_STORAGE_BASE_URL` to `PRODUCT_CLASSIFICATION_MODE=kafka` +
`PRODUCT_CLASSIFICATION_CONSUMER_GROUP` in the same rollout, otherwise the pod
fails at boot by design.

## Consequences

### Easier

- Sealing no longer calls another context: the lookup is a primary-key read
  on the service's own database, with no breaker, retry or timeout to tune.
- A pack station keeps enforcing segregation while product-master or
  inventory-storage is down.
- A stale deployment that still sets `http` fails loudly at boot instead of
  silently running without classifications.

### Harder

- **Eventual consistency.** A classification changed in product-master is
  visible here only after the consumer applies it (normally well under a
  second; longer during consumer lag or an outage). A seal in that window uses
  the previous classification, or none for a SKU never seen.
- **First deployment.** Until product-master has imported and republished the
  legacy classifications (product-master ADR 0003, stages A-B) and this
  consumer has caught up, older SKUs are unknown and seal as unclassified,
  exactly as the permissive mode did. Check the consumer lag before judging
  the feature.
- **No unclassify.** product-master v1 has no event that removes a
  classification, so a row is only ever replaced, never deleted.
- A second consumer group on the shared broker to operate and monitor, and a
  second table that duplicates product-master data.
- The in-memory copy (no `DATABASE_URL`) is rebuilt only when the consumer
  group is new; with a reused group a restarted process starts empty. Local
  development only.
