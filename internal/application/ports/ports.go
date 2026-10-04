// Package ports declares the outbound interfaces the application layer
// depends on: repositories, event publishing, and time. Adapters implement
// these; the application layer never depends on adapters directly.
package ports

import (
	"context"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/consolidation"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TaskRepo persists and retrieves Task aggregates.
type TaskRepo interface {
	Save(ctx context.Context, t *task.Task) error
	FindById(ctx context.Context, id shared.TaskId) (*task.Task, error)
	// FindClaimableByType returns Pending or lease-expired tasks of the
	// given type, ordered by earliest CPT first, for ClaimNext to scan.
	FindClaimableByType(ctx context.Context, taskType task.Type, now time.Time) ([]*task.Task, error)
	// SaveClaim persists t's new Claimed state ONLY if the stored task is
	// still claimable at now (Pending, or Claimed with a lease expired at
	// or before now) -- a compare-and-set on the row. It reports false,
	// with no error and nothing written, when another station's claim got
	// there first. This is what makes the claim at-most-once across
	// concurrent ClaimNext calls: FindClaimableByType is a plain read, so
	// several stations can load the same Pending task at the same instant.
	SaveClaim(ctx context.Context, t *task.Task, now time.Time) (bool, error)
	// FindAllClaimed returns every task currently in the Claimed state, for
	// the lease-expiry sweep.
	FindAllClaimed(ctx context.Context) ([]*task.Task, error)
	// FindOpenPastCPT returns every task still open (Pending or Claimed —
	// i.e. not Completed, see task.Task.IsCPTMissed) whose CPT is at or
	// before now, for the CPT-missed sweep (ADR-0025). Order is
	// unspecified; a sweep with nothing overdue returns an empty slice,
	// not an error.
	FindOpenPastCPT(ctx context.Context, now time.Time) ([]*task.Task, error)
	CountByTypeAndStatus(ctx context.Context, taskType task.Type, status task.Status) (int, error)
	// FindByOrderRef returns every task created for orderRef — a PICK,
	// PACK, and SLAM leg is typically one each, but a leg may appear more
	// than once if it was retried (e.g. a new task created after a prior
	// one's lease expired without completion), so callers must treat this
	// as a set of tasks per order, not a single result. Order is
	// unspecified; an unknown orderRef returns an empty slice, not an
	// error.
	FindByOrderRef(ctx context.Context, orderRef shared.OrderRef) ([]*task.Task, error)
}

// StationRepo persists and retrieves Station aggregates.
type StationRepo interface {
	Save(ctx context.Context, s *station.Station) error
	FindById(ctx context.Context, id shared.StationId) (*station.Station, error)
	// CountByCapability returns how many currently-registered stations
	// hold capability, regardless of occupancy. This is a raw
	// installed-capacity count (stations that CAN work the path), not a
	// staffing count (associates actively working it right now) — see
	// GetInstalledCapacity's own doc comment for the distinction this
	// use case depends on.
	CountByCapability(ctx context.Context, capability shared.Capability) (int, error)
}

// PackageRepo persists and retrieves Package aggregates.
type PackageRepo interface {
	Save(ctx context.Context, p *pack.Package) error
	FindById(ctx context.Context, id shared.PackageId) (*pack.Package, error)
	// FindByTaskId returns the Package already sealed for taskId, or nil
	// (not an error) if no Package has been sealed for that task yet.
	// SealPackage uses this to make POST /tasks/{id}/seal-package
	// idempotent: a retried call returns the already-sealed Package
	// instead of creating a duplicate (see SealPackage.Execute).
	FindByTaskId(ctx context.Context, taskId shared.TaskId) (*pack.Package, error)
	// FindByOrderRef returns every Package sealed for orderRef — normally
	// one carton per order, but a multi-carton order (or a re-pack after a
	// SLAM diversion) yields several, so callers must treat this as a set.
	// Ordered by package id for a deterministic response; an unknown
	// orderRef returns an empty slice, not an error. Backs
	// GET /packages?orderRef= (ADR-0033), mirroring TaskRepo.FindByOrderRef.
	FindByOrderRef(ctx context.Context, orderRef shared.OrderRef) ([]*pack.Package, error)
}

// OrderConsolidationRepo persists and retrieves OrderConsolidation
// aggregates, one per order, tracking Rebin fan-in. FindByOrderRef
// returns nil (not an error) when no consolidation tracker exists yet for
// orderRef — the use case treats that as "not yet started" and creates
// one on first arrival, so no separate "does one exist" call is needed.
type OrderConsolidationRepo interface {
	Save(ctx context.Context, oc *consolidation.OrderConsolidation) error
	FindByOrderRef(ctx context.Context, orderRef shared.OrderRef) (*consolidation.OrderConsolidation, error)
	// FindByOrderRefForUpdate is FindByOrderRef for a read-modify-write:
	// inside a UnitOfWork it first takes a per-order lock held until the
	// scope ends, so concurrent arrivals for one order are serialized and
	// none is lost (ADR-0034). That includes the very first arrival, when no
	// row exists yet to lock. Outside a UnitOfWork it behaves like
	// FindByOrderRef.
	FindByOrderRefForUpdate(ctx context.Context, orderRef shared.OrderRef) (*consolidation.OrderConsolidation, error)
}

// PathCatalogue is the outbound port for the fleet's declared process-path
// catalogue (see warehouse-infra's config/process-paths/*.yaml, loaded by
// the filecatalog adapter). It replaces the WorkReleased consumer's old
// path_id-prefix-convention guess with a real, validated lookup: an
// unrecognized path_id is now a hard error, not a silent default to
// task.Pick. See the process-path-catalogue-as-configuration ADR.
type PathCatalogue interface {
	Lookup(id string) (pathcatalog.PathDefinition, error)
}

// EventPublisher publishes domain events raised by use cases.
type EventPublisher interface {
	Publish(ctx context.Context, events ...shared.DomainEvent) error
}

// UnitOfWork brackets a use case's state change and the domain events it
// raises so both commit or neither does (ADR 0020, transactional outbox).
//
// Execute runs fn inside one atomic scope. Every Repo.Save and
// EventPublisher.Publish made with the ctx handed to fn is bound to that
// same scope: if fn returns an error the scope is rolled back and nothing
// — neither the aggregate row nor the outbox rows — is visible afterwards.
// A nested Execute (fn calling another use case that itself uses the same
// UnitOfWork) joins the outer scope rather than opening a second one.
//
// Adapters that have no transactional backing (the in-memory repos, the
// log publisher) simply leave the use case's UnitOfWork field nil; the use
// cases then run Save and Publish back to back exactly as before.
type UnitOfWork interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
}

// Metrics records the business events of the task lifecycle for the
// observability pipeline. It is a port, not a direct OTel dependency, so the
// application layer stays free of vendor telemetry types; the adapter behind
// it turns each call into an OTel counter increment.
//
// Use cases treat a nil Metrics as "not instrumented" rather than an error:
// telemetry must never change what a use case decides.
type Metrics interface {
	// TaskClaimed records that one task of taskType was leased to a station.
	TaskClaimed(ctx context.Context, taskType task.Type)
	// TaskCompleted records that one task of taskType was completed.
	TaskCompleted(ctx context.Context, taskType task.Type)
}

// Clock supplies the current time, injected so use cases and lease logic are
// deterministic under test.
type Clock interface {
	Now() time.Time
}

// ProcessedEvents tracks inbound event ids already applied, so an
// at-least-once source (e.g. Kafka) can be consumed idempotently.
type ProcessedEvents interface {
	// MarkProcessed records eventId as processed if it is not already
	// present. It returns true if this call newly recorded it, false if
	// eventId was already marked processed.
	MarkProcessed(ctx context.Context, eventId string) (bool, error)
}

// ClassificationInfo is the placement/segregation-relevant subset of a
// SKU's classification, as seen from this bounded context — the result of
// the live, synchronous cross-context lookup SealPackage uses per scanned
// SKU. Known=false means "no classification info available for this SKU",
// which SealPackage treats as no constraint (fail-open) — this service
// does not own product classification and cannot assume every SKU is known
// to inventory-storage (see ADR-0010, mirroring inventory-storage's own
// SlotAttributes.Known convention for facility-layout lookups).
type ClassificationInfo struct {
	// Hazmat reports whether inventory-storage's ProductClassification
	// carries the Hazmat handling tag for this SKU.
	Hazmat bool
	// DOTHazardClass is the DOT hazard class (1-9) when Hazmat is true.
	// Zero when Hazmat is false or the class is not (yet) recorded
	// upstream — a Hazmat SKU with no recorded class is treated as having
	// no hazard class for segregation purposes (fail-open per item, not
	// per SKU-wide Hazmat flag), since the segregation check is keyed on
	// hazard class, not the Hazmat boolean alone.
	DOTHazardClass int
	// Fragile reports whether the classification carries the Fragile
	// handling tag. Not used by SealPackage today (fragile handling
	// already rides in via Task.Fragile per ADR-0009) — carried here for
	// completeness of the classification shape and potential future use.
	Fragile bool
	// Known is false when this SKU has no classification available
	// (lookup miss, unmodeled SKU, or the permissive no-op adapter).
	// SealPackage treats Known=false exactly like Hazmat=false: no
	// segregation constraint from this item.
	Known bool
}

// ProductClassificationLookup is the outbound port for the live,
// synchronous, per-scanned-item cross-context read from inventory-storage's
// GET /products/{sku}/classification endpoint, used by SealPackage to
// enforce same-package DOT hazard segregation (see ADR-0010). Unlike the
// Fragile flag (ADR-0009, stamped onto Task at release time by
// wes-work-planning), a Pack task's scanned contents are discovered live at
// the scan station — the classification of each scanned SKU must be looked
// up at seal time, not release time.
type ProductClassificationLookup interface {
	GetClassification(ctx context.Context, sku string) (ClassificationInfo, error)
}

// LocationRoleInfo is the placement-relevant subset of a facility-layout
// LocationSlot's resolved role, as seen from this bounded context — the
// result of the live, synchronous cross-context lookup RegisterStation
// uses when a caller supplies an optional locationCode (see ADR-0024).
// Known=false means "no role info available for this LocationCode",
// which RegisterStation treats as no constraint (fail-open) — this
// service does not own facility layout and cannot assume every
// LocationCode is modeled there (mirrors inventory-storage's own
// SlotAttributes.Known convention for its own facility-layout lookup).
type LocationRoleInfo struct {
	// Role is facility-layout's LocationRole string (e.g. "WorkCenter",
	// "Storage", "Dock") for this LocationCode. Only meaningful when
	// Known is true.
	Role string
	// Known is false when this LocationCode has no role available
	// (lookup miss, unmodeled location, or the permissive no-op
	// adapter).
	Known bool
}

// LocationRoleLookup is the outbound port for the live, synchronous,
// registration-time cross-context read from facility-layout's
// GET /locations/{locationCode} endpoint, used by RegisterStation to
// confirm an optional locationCode actually resolves to a WorkCenter
// (see ADR-0024). Unlike ProductClassificationLookup's per-scanned-item
// cadence, this runs once per RegisterStation call, only when a
// locationCode is supplied.
type LocationRoleLookup interface {
	GetRole(ctx context.Context, locationCode string) (LocationRoleInfo, error)
}
