package shared

import "time"

// DomainEvent is a past-tense fact raised by an aggregate.
type DomainEvent interface {
	EventName() string
	OccurredAt() time.Time
}

type base struct {
	Name string
	At   time.Time
}

func (b base) EventName() string     { return b.Name }
func (b base) OccurredAt() time.Time { return b.At }

// TaskCreated is raised when a new task enters the pool.
type TaskCreated struct {
	base
	TaskId TaskId
}

func NewTaskCreated(id TaskId, at time.Time) TaskCreated {
	return TaskCreated{base: base{Name: "TaskCreated", At: at}, TaskId: id}
}

// TaskClaimed is raised when a station pulls a task from the pool.
type TaskClaimed struct {
	base
	TaskId    TaskId
	StationId StationId
}

func NewTaskClaimed(id TaskId, station StationId, at time.Time) TaskClaimed {
	return TaskClaimed{base: base{Name: "TaskClaimed", At: at}, TaskId: id, StationId: station}
}

// LeaseExpired is raised when an unconfirmed claim's lease times out and the
// task returns to the pool.
type LeaseExpired struct {
	base
	TaskId TaskId
}

func NewLeaseExpired(id TaskId, at time.Time) LeaseExpired {
	return LeaseExpired{base: base{Name: "LeaseExpired", At: at}, TaskId: id}
}

// TaskCompleted is raised when a station finishes a claimed task.
type TaskCompleted struct {
	base
	TaskId    TaskId
	StationId StationId
}

func NewTaskCompleted(id TaskId, station StationId, at time.Time) TaskCompleted {
	return TaskCompleted{base: base{Name: "TaskCompleted", At: at}, TaskId: id, StationId: station}
}

// ItemPicked is raised when a Pick task records an item retrieved into a tote.
type ItemPicked struct {
	base
	TaskId TaskId
}

func NewItemPicked(id TaskId, at time.Time) ItemPicked {
	return ItemPicked{base: base{Name: "ItemPicked", At: at}, TaskId: id}
}

// PackageSealed is raised when a package's contents are scanned and it is sealed.
type PackageSealed struct {
	base
	PackageId PackageId
}

func NewPackageSealed(id PackageId, at time.Time) PackageSealed {
	return PackageSealed{base: base{Name: "PackageSealed", At: at}, PackageId: id}
}

// WeightDiscrepancyDetected is raised when SLAM finds actual weight outside tolerance.
type WeightDiscrepancyDetected struct {
	base
	PackageId      PackageId
	ExpectedWeight float64
	ActualWeight   float64
}

func NewWeightDiscrepancyDetected(id PackageId, expected, actual float64, at time.Time) WeightDiscrepancyDetected {
	return WeightDiscrepancyDetected{
		base:           base{Name: "WeightDiscrepancyDetected", At: at},
		PackageId:      id,
		ExpectedWeight: expected,
		ActualWeight:   actual,
	}
}

// LabelApplied is raised when SLAM passes the weigh-check and applies the shipping label.
type LabelApplied struct {
	base
	PackageId PackageId
}

func NewLabelApplied(id PackageId, at time.Time) LabelApplied {
	return LabelApplied{base: base{Name: "LabelApplied", At: at}, PackageId: id}
}

// PackageDiverted is raised when a package fails the SLAM weigh-check and is
// routed off the standard path instead of being labeled.
type PackageDiverted struct {
	base
	PackageId PackageId
}

func NewPackageDiverted(id PackageId, at time.Time) PackageDiverted {
	return PackageDiverted{base: base{Name: "PackageDiverted", At: at}, PackageId: id}
}

// ItemArrivedAtRebin is raised when one required line of an order reaches
// the Rebin path — the consolidation fan-in fact. OrderRef identifies the
// order; LineId identifies which required line arrived (see
// consolidation.OrderConsolidation).
type ItemArrivedAtRebin struct {
	base
	OrderRef OrderRef
	LineId   string
}

func NewItemArrivedAtRebin(orderRef OrderRef, lineId string, at time.Time) ItemArrivedAtRebin {
	return ItemArrivedAtRebin{base: base{Name: "ItemArrivedAtRebin", At: at}, OrderRef: orderRef, LineId: lineId}
}

// OrderConsolidated is raised when every required line for an order has
// arrived at Rebin — the trigger for creating that order's PACK task.
type OrderConsolidated struct {
	base
	OrderRef OrderRef
}

func NewOrderConsolidated(orderRef OrderRef, at time.Time) OrderConsolidated {
	return OrderConsolidated{base: base{Name: "OrderConsolidated", At: at}, OrderRef: orderRef}
}

// TaskCPTMissed is raised by the CPT-missed sweep (see
// usecases.SweepCPTMisses, ADR-0025) for a task still open (Pending or
// Claimed — see Task.IsCPTMissed) at or past its CPT deadline. OrderRef
// identifies the affected order/shipment-group for order-management's
// RepromiseOrder consumer (ADR 0014 §5) to key its recompute on; TaskType
// and CPT are carried so a future consumer can reason about which leg
// missed and by how much without a repo lookup back into this service.
//
// By design this event RE-FIRES on every sweep pass for as long as the
// task remains open past its CPT — the sweep does not track "already
// reported" state (see ADR-0025 and order-management's ADR 0014 §5, which
// explicitly designs its RepromiseOrder consumer to be idempotent on
// (orderId, sourceEventId) "because fulfillment-execution's missed-CPT
// sweep will re-emit on every pass").
type TaskCPTMissed struct {
	base
	TaskId   TaskId
	OrderRef OrderRef
	TaskType string
	CPT      time.Time
}

func NewTaskCPTMissed(id TaskId, orderRef OrderRef, taskType string, cpt time.Time, at time.Time) TaskCPTMissed {
	return TaskCPTMissed{
		base:     base{Name: "TaskCPTMissed", At: at},
		TaskId:   id,
		OrderRef: orderRef,
		TaskType: taskType,
		CPT:      cpt,
	}
}

// PackageManifested is raised alongside LabelApplied when a package passes
// its SLAM weigh-check (see usecases.RunSlam, ADR-0025) — the "SLAM pass"
// half of order-management ADR 0014 §5's promise feedback loop. A
// diverted package (weight outside tolerance) was NOT manifested and does
// not raise this event. OrderRef identifies the affected order for
// RepromiseOrder to key its recompute on.
type PackageManifested struct {
	base
	PackageId PackageId
	OrderRef  OrderRef
}

func NewPackageManifested(id PackageId, orderRef OrderRef, at time.Time) PackageManifested {
	return PackageManifested{base: base{Name: "PackageManifested", At: at}, PackageId: id, OrderRef: orderRef}
}

// TransferFact is the interface of the three inter-warehouse-transfer
// facts a transfer task's completion publishes (see the transfer-task-
// facts ADR). It exists so the Kafka publisher (and tests) can treat the
// selected fact uniformly while each concrete type keeps its own
// EventName.
type TransferFact interface {
	DomainEvent
	// TransferDetails returns the correlation the fact carries on the wire.
	TransferDetails() TaskTransferDetails
}

// TaskTransferDetails is the wire payload of a transfer fact — exactly the
// correlation block the completed Task carried, stamped at release time.
// It lives in shared (not task) because the domain events below and the
// Kafka payload type need it without importing the task package.
type TaskTransferDetails struct {
	TransferRef string
	DemandId    string
	WorkUnitId  string
	WorkKind    string
	SiteId      string
	SKU         string
	Quantity    int
}

// TransferPicked is raised when a task carrying a TRANSFER_PICK work kind
// completes: the transfer's stock was picked at the origin site. Consumed
// by inventory-planning's transfer saga to advance Picking -> Dispatched.
type TransferPicked struct {
	base
	TaskId TaskId
	TaskTransferDetails
}

func NewTransferPicked(id TaskId, d TaskTransferDetails, at time.Time) TransferPicked {
	return TransferPicked{base: base{Name: "TransferPicked", At: at}, TaskId: id, TaskTransferDetails: d}
}

// TransferDispatched is raised when a task carrying a TRANSFER_DISPATCH
// work kind completes: the transfer left the origin site. Consumed by
// inventory-planning's transfer saga to advance Dispatched -> InTransit.
type TransferDispatched struct {
	base
	TaskId TaskId
	TaskTransferDetails
}

func NewTransferDispatched(id TaskId, d TaskTransferDetails, at time.Time) TransferDispatched {
	return TransferDispatched{base: base{Name: "TransferDispatched", At: at}, TaskId: id, TaskTransferDetails: d}
}

// TransferArrived is raised when a task carrying a TRANSFER_ARRIVAL work
// kind completes: the transfer reached the destination site. Consumed by
// inventory-planning's transfer saga to advance InTransit -> Arrived and
// by destination receipt/stow correlation.
type TransferArrived struct {
	base
	TaskId TaskId
	TaskTransferDetails
}

func NewTransferArrived(id TaskId, d TaskTransferDetails, at time.Time) TransferArrived {
	return TransferArrived{base: base{Name: "TransferArrived", At: at}, TaskId: id, TaskTransferDetails: d}
}

// TransferDetails implements TransferFact.
func (e TransferPicked) TransferDetails() TaskTransferDetails { return e.TaskTransferDetails }

// TransferDetails implements TransferFact.
func (e TransferDispatched) TransferDetails() TaskTransferDetails { return e.TaskTransferDetails }

// TransferDetails implements TransferFact.
func (e TransferArrived) TransferDetails() TaskTransferDetails { return e.TaskTransferDetails }
