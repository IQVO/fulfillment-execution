---
id: ddd-artifacts
title: DDD artifacts (ddd-crew)
sidebar_label: Overview
sidebar_position: 0
description: Index of the ddd-crew DDD artifact pack for Fulfillment Execution — core domain chart, bounded context canvas, context map, aggregate design canvas, domain message flow, EventStorming, ubiquitous language, UML class / ER / sequence diagrams and domain events — all derived from the code on develop.
---

# DDD artifacts (ddd-crew)

This pack describes the Fulfillment Execution bounded context with the
[ddd-crew](https://github.com/ddd-crew) modelling tools, plus three UML-style
views. Every diagram is Mermaid, every page is derived from the code in this
repository, and each diagram has a **Source:** line naming the files it was
drawn from.

| Artifact | Page | ddd-crew tool / notation |
| --- | --- | --- |
| Core domain chart | [Core domain chart](./core-domain-chart.md) | [Core Domain Charts](https://github.com/ddd-crew/core-domain-charts) |
| Bounded context canvas | [Bounded context canvas](./bounded-context-canvas.md) | [Bounded Context Canvas v5](https://github.com/ddd-crew/bounded-context-canvas) |
| Context map | [Context map](../ecosystem/context-map.md) (lives in *Ecosystem*) | [Context Mapping](https://github.com/ddd-crew/context-mapping) |
| Aggregate design canvas | [Aggregate design canvas](./aggregate-design-canvas.md) | [Aggregate Design Canvas v1.1](https://github.com/ddd-crew/aggregate-design-canvas) |
| Domain message flow | [Domain message flow](./domain-message-flow.md) | [Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling) |
| EventStorming | [EventStorming](./eventstorming.md) | [EventStorming glossary & cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet), design level |
| Ubiquitous language | [Ubiquitous language](../business-context/ubiquitous-language.md) (lives in *Business context*) | glossary with code mapping |
| UML class diagram | [Class diagram](./class-diagram.md) | UML class diagram + hexagonal ports/adapters view |
| ER diagram | [Entity-relationship diagram](./entity-relationship.md) | ER diagram of the final migration schema |
| Sequence diagrams | [Sequence diagrams](./sequence-diagrams.md) | UML sequence diagrams per use case |
| Domain events | [Domain events](./domain-events.md) | event catalogue (full CloudEvents types, topics, consumers) |

The narrative DDD pages sit next to the pack and go deeper on single topics:
[Subdomain classification](./subdomain-classification.md),
[Aggregates & invariants](./aggregates-and-invariants.md),
[Use cases & ports](./use-cases.md) and
[Context relationships](./context-relationships.md).

## Sources of truth

When a page and the code disagree, the code wins. The pack was drawn from:

| Concern | Source of truth |
| --- | --- |
| Aggregates, invariants, value objects, events | `internal/domain/**` (`task`, `station`, `package` (Go package `pack`), `consolidation`, `pathcatalog`, `shared`) |
| Use cases and ports | `internal/application/usecases/*.go`, `internal/application/ports/*.go` |
| REST surface | `internal/adapters/inbound/http/router.go` and `apis/openapi.yaml` (kept 1:1 by `openapi_routes_test.go`) |
| MCP surface | `internal/adapters/inbound/mcp/*.go`, `cmd/mcp/main.go` |
| Kafka published | `internal/adapters/outbound/kafka/publisher.go`, `analytics_publisher.go`, `internal/adapters/kafka/cloudevents/cloudevents.go`, `apis/asyncapi.yaml` |
| Kafka consumed | `internal/adapters/inbound/kafka/consumer.go`, `internal/adapters/outbound/kafkacatalog/consumer.go`, `internal/adapters/inbound/kafka/analytics_consumer.go` |
| Persistence | `migrations/*.up.sql`, `migrations/analytics/*.up.sql`, `internal/adapters/outbound/postgres/*.go` |
| Decisions | [Architecture decisions](../adr/index.md) (ADR-0001 to ADR-0035) |

The fleet-level aggregate of these pages lives in the `warehouse-docs`
repository; this repository is the source those pages are synced from.
