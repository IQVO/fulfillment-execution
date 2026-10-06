---
id: 0015-wcs-equipment-anti-corruption-seam
title: 15. Structural anti-corruption-layer seam for the (unbuilt) WCS tier
sidebar_label: 15. WCS anti-corruption seam
sidebar_position: 15
description: An unimplemented EquipmentCommandPort outbound port, so the documented refusal to drive WCS/equipment directly is enforced by the compiler, not only by prose in openapi.yaml and the context map.
---

# 15. Structural anti-corruption-layer seam for the (unbuilt) WCS tier

## Status

**Accepted.**

## Context

This service's own [context map](../ecosystem/context-map.md) and
`apis/openapi.yaml` both state that fulfillment-execution does not drive
physical WCS/equipment (conveyors, print-and-apply heads, checkweighers)
directly — that relationship is documented as **Customer/Supplier +
Conformist behind an Anti-Corruption Layer**, strategic but "not built,"
because the WCS tier is explicitly out of this platform's scope (buy,
don't build).

Today that boundary exists only as prose: a paragraph in `context-map.md`
and a sentence in the OpenAPI description. Nothing in the codebase
prevents a future contributor from reaching directly for equipment
vocabulary — a divert code, a chute id, an encoder tick — inside a use
case or domain type the moment a WCS integration is actually scoped. The
[arch-go fitness tests](./0006-arch-go-architecture-fitness-tests.md)
enforce the hexagonal dependency *direction* (domain must not depend on
adapters), but they have nothing to enforce here yet, because there is no
package for a future WCS adapter to violate.

Forces:

- **Equipment lifecycles change on a different cycle than domain
  lifecycles.** A conveyor, sorter, or robotic cell gets replaced on a
  capital-project cadence entirely independent of when this service's
  business rules change. The reference material this platform's DDD work
  draws from calls this out directly: equipment vocabulary is corrosive to
  business models if it is allowed to leak upward.
- **There is no equipment to integrate yet**, and speculatively designing
  a rich command API for hardware that does not exist in this platform
  would be exactly the kind of premature abstraction `CLAUDE.md`'s
  YAGNI discipline elsewhere in this fleet warns against.
- **The boundary is real regardless of whether it is implemented.** The
  context map already draws it as a dashed strategic edge — "the real
  shape of the system, not because anything implements it." A structural
  placeholder makes that shape visible in the dependency graph the same
  way the fitness tests make the hexagonal layers visible, rather than
  living only in a markdown file that can drift from the code.

### Alternatives considered

**Leave it as documentation only.** Free, and the status quo. Costs
nothing today and risks everything the moment someone actually starts a
WCS integration under time pressure and reaches for the nearest concrete
type.

**Design a full `EquipmentCommandPort` API now** (divert, label-print,
weigh-check commands). Rejected: there is no real equipment behind it to
validate the shape against, and every method added speculatively is a
guess that will likely be wrong once real hardware vocabulary shows up —
the same mistake ADR-0009/0011 avoided by stamping only facts this service
already needed, not facts it might need.

## Decision

**Add `ports.EquipmentCommandPort`, an outbound port interface in
`internal/application/ports/equipment.go`, with no adapter and
deliberately no callable methods yet.**

```go
type EquipmentCommandPort interface {
	_reservedForFutureWCSIntegration(ctx context.Context)
}
```

The unexported placeholder method exists only so the interface is not
empty (an empty interface is satisfied by everything, which defeats the
purpose of a seam); it is never intended to be called and is not wired
into any use case today. When a WCS integration is actually scoped, this
interface gains real methods named in equipment-agnostic business
vocabulary (e.g. `RequestDivert`, `RequestLabelPrint` — not `SetChuteN` or
`SendEncoderTick`), and exactly one outbound adapter under
`internal/adapters/outbound/wcs/` implements it, translating this
service's vocabulary into whatever the real equipment control system
speaks. No other package may import equipment-specific types — the
existing `arch-go` fitness tests already forbid any package but `cmd` from
wiring adapters together, so this rule is inherited for free rather than
needing a new fitness-test rule today.

## Consequences

### Easier

- **The ACL boundary is now visible in the type system**, not only in a
  markdown file. A future contributor scoping a real WCS integration has
  an obvious, named place to add methods, rather than having to invent
  the seam from scratch under delivery pressure.
- **No speculative API surface to get wrong.** Zero real methods means
  zero chance of designing the wrong equipment vocabulary before any real
  equipment exists to validate it against.
- **Costs nothing at runtime.** No adapter, no wiring in `cmd/execution/main.go`,
  no behavior change to any existing use case.

### Harder

- **The seam is unverified.** Because nothing implements or calls it, this
  port cannot be exercised by any test today — its value is purely
  structural/documentary until a real integration exists. This is a
  known, accepted limitation, not an oversight.
- **A second migration for readers.** Someone reading `context-map.md`'s
  "not wired" callout for WCS should also now be told to check this file;
  the two must be kept consistent, and this ADR is the forcing function.
- **Naming the real methods later is still real work.** This decision
  does not reduce the amount of design needed when a WCS integration is
  actually scoped — it only guarantees that design lands behind a seam
  instead of leaking into domain types.
