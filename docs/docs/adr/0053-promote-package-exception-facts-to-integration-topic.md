---
id: 0053-promote-package-exception-facts-to-integration-topic
slug: /adr/0053-promote-package-exception-facts-to-integration-topic
title: 53. Promote PackageDiverted and WeightDiscrepancyDetected to the integration topic
sidebar_label: "53. Promote package exception facts to the integration topic"
description: "ADR 0053 — the two SLAM package exception facts, PackageDiverted and WeightDiscrepancyDetected, are published on warehouse.fulfillment.events as well as warehouse.fulfillment.analytics, with byte-identical data, subject and type, so the exception-management context can consume them; LeaseExpired is not promoted."
---

# 53. Promote PackageDiverted and WeightDiscrepancyDetected to the integration topic

## Status

Accepted (2026-10-21). Amends
[ADR-0032](./0032-cloudevents-mandatory-envelope.md) (the integration
publish list; one line added there per type) — that record's body is not
edited beyond the catalogue lines. Consumer:
exception-management ("Problem Solve", a new WES bounded context).

## Context

When SLAM's weigh-check finds a weight out of tolerance the package is
diverted to the problem-solve area instead of being labelled. The domain
raises two facts: `WeightDiscrepancyDetected` (the measurement that caused
the divert) and `PackageDiverted` (the divert itself). Until now both were
published only on `warehouse.fulfillment.analytics`, the internal topic
[ADR-0012](./0012-analytical-data-product.md) reserves for this service's
own projector; as [ADR-0033](./0033-package-read-model.md) records, "the
only signal was the domain events" — nothing outside this service could
see a divert.

The exception-management context exists to be that outside consumer: it
opens a problem-solve case on the divert fact and needs the measurement
that explains it. A context may not consume another context's analytics
topic — that channel is a private data-product feed, not an integration
contract — so the facts must move to the integration topic to be
consumable at all.

## Decision

We will publish `PackageDiverted` and `WeightDiscrepancyDetected` on
`warehouse.fulfillment.events` in addition to
`warehouse.fulfillment.analytics`:

- Same CloudEvents `type` strings
  (`com.warehouse.wes.fulfillment-execution.package.PackageDiverted`,
  `...package.WeightDiscrepancyDetected`); the integration `dataschema`
  is `urn:warehouse:fulfillment-execution:events:<Event>:v1` per
  ADR-0032's topic-side rule.
- Same `subject` and Kafka key (the package id) and **byte-identical
  `data`**: `{"package_id"}` and `{"package_id","expected_g","actual_g"}`.
  The wire test asserts the `data` bytes of the integration encoder equal
  the analytics encoder's for the same domain event, not merely the same
  fields.
- Both occurrences leave through the same transactional outbox
  ([ADR-0020](./0020-transactional-outbox.md)); `apis/asyncapi.yaml`
  declares both messages on the integration channel, and the catalogue in
  ADR-0032 lists both types as integration + analytics.
- `LabelApplied`, `PackageSealed` and the other analytics-only facts are
  **not** promoted; `LeaseExpired` is explicitly out of scope.

## Consequences

- Exception-management can consume the divert fact from an integration
  topic under the fleet's consumer rules (full-type dispatch, CloudEvents
  `id` dedupe, offset after effect), with no change on our side when it
  does.
- The integration topic carries two more low-volume types; consumers that
  ignore unknown types (fleet rule) are unaffected, and every existing
  consumer already ignores them.
- Two encoders now produce the same payload shape; the byte-identity test
  keeps them from drifting, and a breaking payload change still needs a
  `.v2` type + dataschema per ADR-0032's versioning rule — on both topics.
- What becomes harder: the analytics projector's contract is no longer a
  private matter — the promoted types' payloads are now a cross-context
  contract owned jointly with exception-management.
