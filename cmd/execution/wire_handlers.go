package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	outbounda2a "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/a2a"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/facilitylayout"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/inventorystorage"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/system"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/observability"
	"github.com/claudioed/fulfillment-execution/internal/resilience"
)

// outboundLookups bundles the two ACL lookups the HTTP use cases read:
// product classification (local copy of product-master events, ADR-0039)
// and station location role (facility-layout over HTTP, behind the shared
// circuit-breaker recorder).
type outboundLookups struct {
	classification ports.ProductClassificationLookup
	locationRole   ports.LocationRoleLookup
	// packs and caseBreaker back POST /tasks/{id}/scans and
	// POST /tasks/{id}/break-case (ADR 0054). Nil in unit tests that build a
	// server over test doubles.
	packs       ports.PackViewLookup
	caseBreaker ports.CaseBreaker
	// cases backs ship-in-own-container on POST /tasks/{id}/seal-package
	// (ADR 0056): the same local PackView as packs, read for a SKU's CASE
	// levels. Nil exactly when packs is nil.
	cases ports.CaseLevelLookup
}

// buildOutboundLookups wires the location-role lookup plus its
// circuit-breaker gauge next to the already-built classification lookup.
// circuitBreakerMetrics wires the facility-layout breaker's OnStateChange
// into the circuit_breaker.state gauge (ADR-0029), reusing the SAME OTel
// MeterProvider observability.Setup already installed rather than standing
// up a second Prometheus registry. Errors here mirror NewMetrics' contract
// (invalid instrument name only, a programming error) -- non-fatal: a nil
// recorder just means this process runs without the gauge, never without
// the breaker itself.
//
// LOCATION_ROLE_MODE is a REST sync edge to facility-layout, not a database
// or a broker: it stays out of the real-wiring migration (ADR-0024).
func buildOutboundLookups(classification ports.ProductClassificationLookup, packs ports.PackViewLookup, inventoryStorageURL string, logger *slog.Logger) outboundLookups {
	circuitBreakerMetrics, cbmErr := observability.NewCircuitBreakerMetrics()
	if cbmErr != nil {
		logger.Warn("circuit breaker metrics unavailable; breakers will run without the circuit_breaker.state gauge", "error", cbmErr)
	}
	return outboundLookups{
		classification: classification,
		packs:          packs,
		cases:          caseLevelsOf(packs),
		caseBreaker:    inventorystorage.NewBreakerClient(inventorystorage.NewClient(inventoryStorageURL, nil), circuitBreakerMetrics),
		locationRole:   buildLocationRoleLookup(getenv("LOCATION_ROLE_MODE", "permissive"), os.Getenv("FACILITY_LAYOUT_BASE_URL"), circuitBreakerMetrics, logger),
	}
}

// caseLevelsOf returns the CASE-level read side of the PackView behind packs
// (the Postgres store implements both), or nil when packs is nil or does not
// offer it.
func caseLevelsOf(packs ports.PackViewLookup) ports.CaseLevelLookup {
	cases, _ := packs.(ports.CaseLevelLookup)
	return cases
}

// wireWorkReleasedConsumer builds the WorkReleased consumer plus its
// dead-letter writer (<topic>.dlq, ADR-0004's documented gap) and returns one
// close func releasing the DLQ writer and then the consumer.
func wireWorkReleasedConsumer(brokers []string, consumerGroup string, apply *usecases.ApplyWorkReleased, logger *slog.Logger) (*inboundkafka.Consumer, func()) {
	consumer := inboundkafka.NewConsumerWithGroup(brokers, workReleasedTopic, consumerGroup, apply, logger)
	dlqWriter := wireDeadLetter(consumer, brokers, logger)
	return consumer, func() {
		_ = dlqWriter.Close()
		_ = consumer.Close()
	}
}

// buildHandlers wires every use case into its inbound HTTP handler.
// createTask is built by the caller (the WorkReleased consumer shares it)
// and passed in; everything else is constructed here.
func buildHandlers(storage storageAdapters, publisher ports.EventPublisher, clock system.Clock, metrics *observability.Metrics, classificationLookup ports.ProductClassificationLookup, locationRoleLookup ports.LocationRoleLookup, hintLookup ports.InterleavingHintLookup, createTask *usecases.CreateTask, readiness *inboundhttp.Readiness, logger *slog.Logger, planning ports.WorkPlanning) *inboundhttp.Handlers {
	return &inboundhttp.Handlers{
		CreateTask:         createTask,
		ClaimNext:          &usecases.ClaimNext{Tasks: storage.taskRepo, Stations: storage.stationRepo, Publisher: publisher, Clock: clock, Metrics: metricsPort(metrics), UnitOfWork: storage.uow, Hints: hintLookup, Logger: logger},
		RenewLease:         &usecases.RenewLease{Tasks: storage.taskRepo, Clock: clock},
		CompleteTask:       &usecases.CompleteTask{Tasks: storage.taskRepo, Publisher: publisher, Clock: clock, Metrics: metricsPort(metrics), UnitOfWork: storage.uow},
		SealPackage:        &usecases.SealPackage{Tasks: storage.taskRepo, Packages: storage.packageRepo, Publisher: publisher, Clock: clock, NewId: newPackageId, ClassificationLookup: classificationLookup, UnitOfWork: storage.uow},
		RunSlam:            &usecases.RunSlam{Packages: storage.packageRepo, Publisher: publisher, Clock: clock, UnitOfWork: storage.uow},
		GetQueueDepth:      &usecases.GetQueueDepth{Tasks: storage.taskRepo},
		ExpireLeases:       &usecases.ExpireLeases{Tasks: storage.taskRepo, Publisher: publisher, Clock: clock, UnitOfWork: storage.uow},
		RegisterStation:    &usecases.RegisterStation{Stations: storage.stationRepo, Publisher: publisher, LocationLookup: locationRoleLookup},
		GetTasksByOrderRef: &usecases.GetTasksByOrderRef{Tasks: storage.taskRepo},
		CheckInStation:     &usecases.CheckInStation{Stations: storage.stationRepo},
		CheckOutStation:    &usecases.CheckOutStation{Stations: storage.stationRepo},
		ArriveAtRebin: &usecases.ArriveAtRebin{
			Consolidations: storage.consolidationRepo,
			CreateTask:     createTask,
			Publisher:      publisher,
			Clock:          clock,
			UnitOfWork:     storage.uow,
		},
		GetInstalledCapacity: &usecases.GetInstalledCapacity{Stations: storage.stationRepo},
		SweepCPTMisses:       &usecases.SweepCPTMisses{Tasks: storage.taskRepo, Publisher: publisher, Clock: clock, UnitOfWork: storage.uow},
		// Package read model (ADR-0033): GET /packages/{id} and
		// GET /packages?orderRef=.
		GetPackage:            &usecases.GetPackage{Packages: storage.packageRepo},
		GetPackagesByOrderRef: &usecases.GetPackagesByOrderRef{Packages: storage.packageRepo},
		// A2A delegation to Work Planning (ADR-0058): nil Planning keeps the
		// endpoint answering 503 not-configured — a deployment without
		// WORK_PLANNING_A2A_URL has the surface explicitly off.
		RequestNextWork: &usecases.RequestNextWork{Planning: planning},
		// readiness backs GET /readyz (ADR-0029 §graceful shutdown):
		// flipped to not-ready as the FIRST step of shutdown, below,
		// before anything else stops.
		Readiness: readiness,
	}
}

// startWorkReleasedConsumer runs the WorkReleased consumer loop in the
// background and returns a channel closed once the loop has fully
// returned, so graceful shutdown can wait for a real stop.
func startWorkReleasedConsumer(consumer *inboundkafka.Consumer, ctx context.Context, logger *slog.Logger, brokers []string) <-chan struct{} {
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		logger.Info("kafka consumer starting", "topic", workReleasedTopic, "brokers", brokers)
		if err := consumer.Run(ctx); err != nil {
			logger.Error("kafka consumer stopped", "error", err)
		}
	}()
	return consumerDone
}

// buildWorkPlanningClient resolves the Work Planning delegation port from
// WORK_PLANNING_A2A_URL (ADR-0058). Unset means the surface is explicitly
// OFF: nil makes POST /paths/{pathId}/work-requests answer 503
// not-configured instead of dialing anything — a half-configured deployment
// must fail closed, and boot must never depend on the planning agent being
// up (the A2A client resolves the agent's card lazily, on first use).
func buildWorkPlanningClient(logger *slog.Logger, rawURL string) ports.WorkPlanning {
	if strings.TrimSpace(rawURL) == "" {
		logger.Info("work-planning delegation disabled", "reason", "WORK_PLANNING_A2A_URL unset (ADR-0058)")
		return nil
	}
	logger.Info("work-planning delegation configured", "agent", rawURL)
	return outbounda2a.NewClient(rawURL, nil)
}

// buildLocationRoleLookup selects the outbound ports.LocationRoleLookup
// adapter via LOCATION_ROLE_MODE (http|permissive), defaulting to
// "permissive" so existing tests, CI and deployments that do not set the
// env var are unaffected (see ADR-0024). "http" requires
// FACILITY_LAYOUT_BASE_URL. This is a REST sync edge to a sibling context,
// out of scope of the real-wiring migration (a follow-up).
//
// In http mode the real Client is wrapped in retry (jittered, max 3
// attempts -- GetRole is a pure read, safe to retry) plus a
// per-dependency circuit breaker (ADR-0029): on a trip, calls fall back
// to the SAME fail-open behaviour this client already had. recorder
// feeds the breaker's state transitions into the circuit_breaker.state
// gauge; nil is fine.
func buildLocationRoleLookup(mode, facilityLayoutBaseURL string, recorder resilience.StateRecorder, logger *slog.Logger) ports.LocationRoleLookup {
	if !strings.EqualFold(mode, "http") {
		return facilitylayout.NewPermissiveLookup()
	}
	logger.Info("location role lookup configured", "mode", "http", "facility_layout_base_url", facilityLayoutBaseURL, "circuit_breaker", "enabled", "retry", "enabled")
	return facilitylayout.NewBreakerClient(facilitylayout.NewClient(facilityLayoutBaseURL, nil), recorder)
}

func newTaskId() shared.TaskId {
	return shared.TaskId(uuidLike())
}

func newPackageId() shared.PackageId {
	return shared.PackageId(uuidLike())
}

// uuidLike generates a time-ordered, sufficiently-unique id without pulling
// in an external UUID dependency.
func uuidLike() string {
	return time.Now().UTC().Format("20060102T150405.000000000")
}
