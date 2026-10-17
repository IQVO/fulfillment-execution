package shared_test

import (
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

var eventNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// Every domain event must report its own EventName and the OccurredAt time
// it was constructed with — that's what read-model projections key off of.
func TestDomainEvents_NameAndOccurredAt(t *testing.T) {
	tests := []struct {
		name  string
		event shared.DomainEvent
		want  string
	}{
		{"TaskCreated", shared.NewTaskCreated("t1", eventNow), "TaskCreated"},
		{"TaskClaimed", shared.NewTaskClaimed("t1", "s1", eventNow), "TaskClaimed"},
		{"TaskAvailable", shared.NewTaskAvailable("t1", eventNow), "TaskAvailable"},
		{"LeaseExpired", shared.NewLeaseExpired("t1", eventNow), "LeaseExpired"},
		{"TaskCompleted", shared.NewTaskCompleted("t1", "s1", eventNow), "TaskCompleted"},
		{"ItemPicked", shared.NewItemPicked("t1", eventNow), "ItemPicked"},
		{"PackageSealed", shared.NewPackageSealed("p1", eventNow), "PackageSealed"},
		{"WeightDiscrepancyDetected", shared.NewWeightDiscrepancyDetected("p1", 2.0, 2.5, eventNow), "WeightDiscrepancyDetected"},
		{"LabelApplied", shared.NewLabelApplied("p1", eventNow), "LabelApplied"},
		{"PackageDiverted", shared.NewPackageDiverted("p1", eventNow), "PackageDiverted"},
		{"TaskCPTMissed", shared.NewTaskCPTMissed("t1", "order-1", "PICK", eventNow.Add(-time.Hour), eventNow), "TaskCPTMissed"},
		{"PackageManifested", shared.NewPackageManifested("p1", "order-1", eventNow), "PackageManifested"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.event.EventName(); got != tt.want {
				t.Fatalf("EventName() = %q, want %q", got, tt.want)
			}
			if !tt.event.OccurredAt().Equal(eventNow) {
				t.Fatalf("OccurredAt() = %v, want %v", tt.event.OccurredAt(), eventNow)
			}
		})
	}
}

func TestNewTaskClaimed_CarriesTaskAndStationId(t *testing.T) {
	e := shared.NewTaskClaimed("t1", "s1", eventNow)
	if e.TaskId != "t1" {
		t.Fatalf("expected TaskId t1, got %s", e.TaskId)
	}
	if e.StationId != "s1" {
		t.Fatalf("expected StationId s1, got %s", e.StationId)
	}
}

func TestNewWeightDiscrepancyDetected_CarriesWeights(t *testing.T) {
	e := shared.NewWeightDiscrepancyDetected("p1", 2.0, 2.5, eventNow)
	if e.PackageId != "p1" {
		t.Fatalf("expected PackageId p1, got %s", e.PackageId)
	}
	if e.ExpectedWeight != 2.0 {
		t.Fatalf("expected ExpectedWeight 2.0, got %v", e.ExpectedWeight)
	}
	if e.ActualWeight != 2.5 {
		t.Fatalf("expected ActualWeight 2.5, got %v", e.ActualWeight)
	}
}

// TaskCPTMissed must carry enough for order-management's RepromiseOrder
// consumer (ADR 0014 §5) to identify the affected order/shipment-group and
// reason about which leg missed and by how much (ADR-0025).
func TestNewTaskCPTMissed_CarriesOrderTaskTypeAndCPT(t *testing.T) {
	cpt := eventNow.Add(-30 * time.Minute)
	e := shared.NewTaskCPTMissed("t1", "order-1", "PICK", cpt, eventNow)
	if e.TaskId != "t1" {
		t.Fatalf("expected TaskId t1, got %s", e.TaskId)
	}
	if e.OrderRef != "order-1" {
		t.Fatalf("expected OrderRef order-1, got %s", e.OrderRef)
	}
	if e.TaskType != "PICK" {
		t.Fatalf("expected TaskType PICK, got %s", e.TaskType)
	}
	if !e.CPT.Equal(cpt) {
		t.Fatalf("expected CPT %v, got %v", cpt, e.CPT)
	}
}

// PackageManifested must carry the OrderRef so RepromiseOrder can key its
// recompute on the same order/shipment-group identity as TaskCPTMissed.
func TestNewPackageManifested_CarriesOrderRef(t *testing.T) {
	e := shared.NewPackageManifested("p1", "order-1", eventNow)
	if e.PackageId != "p1" {
		t.Fatalf("expected PackageId p1, got %s", e.PackageId)
	}
	if e.OrderRef != "order-1" {
		t.Fatalf("expected OrderRef order-1, got %s", e.OrderRef)
	}
}
