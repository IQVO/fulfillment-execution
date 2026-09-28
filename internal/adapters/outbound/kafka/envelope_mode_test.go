package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// goldenEpoch/newGoldenFixtures build a fixed, deterministic scenario per
// event type, mirroring the exact task/station setup already used by this
// package's other Encode tests, so the golden JSON captured below is
// reproducible byte-for-byte.
var goldenEpoch = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func goldenTaskCompletedPublisher(t *testing.T) *outboundkafka.Publisher {
	t.Helper()
	tasks := memory.NewTaskRepo()
	stations := memory.NewStationRepo()
	tk := task.New("task-golden", task.Pick, shared.NewCPT(goldenEpoch.Add(time.Hour)), "wu-golden", shared.NewCapabilitySet("pick"), false, false)
	if err := tk.Claim("station-golden", shared.NewCapabilitySet("pick"), goldenEpoch, 5*time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	completedAt := goldenEpoch.Add(45 * time.Second)
	if err := tk.Complete("station-golden", completedAt); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := tasks.Save(context.Background(), tk); err != nil {
		t.Fatalf("save task: %v", err)
	}
	st := station.New("station-golden", shared.NewCapabilitySet("pick"))
	if err := st.CheckIn("worker-golden"); err != nil {
		t.Fatalf("check in: %v", err)
	}
	if err := stations.Save(context.Background(), st); err != nil {
		t.Fatalf("save station: %v", err)
	}
	return outboundkafka.NewPublisherWithWriter(nil, tasks, stations, func() string { return "evt-golden-1" })
}

const (
	goldenTaskCompletedFlatJSON     = `{"event_id":"evt-golden-1","event_type":"TaskCompleted","occurred_at":"2026-01-01T12:00:45Z","source":"fulfillment-execution","data":{"task_id":"task-golden","station_id":"station-golden","work_unit_id":"wu-golden","associate_id":"worker-golden","duration_seconds":45,"task_type":"PICK"}}`
	goldenTaskCPTMissedFlatJSON     = `{"event_id":"evt-golden-2","event_type":"TaskCPTMissed","occurred_at":"2026-01-01T12:00:00Z","source":"fulfillment-execution","data":{"task_id":"task-golden","order_ref":"order-golden","task_type":"PICK","cpt":"2026-01-01T11:30:00Z"}}`
	goldenPackageManifestedFlatJSON = `{"event_id":"evt-golden-3","event_type":"PackageManifested","occurred_at":"2026-01-01T12:00:00Z","source":"fulfillment-execution","data":{"package_id":"pkg-golden","order_ref":"order-golden"}}`
)

// --- flat mode: byte-identical golden-file regression gate (ADR-0027 P4) ---
//
// These constants were captured from this repo's actual pre-ADR-0027
// Encode output (git stash the cloudevents.go/publisher.go diff, run the
// same fixtures, diff). A Publisher with no Mode set (the zero value,
// EnvelopeModeFlat) must reproduce them verbatim.

func TestEncode_FlatMode_TaskCompleted_ByteIdenticalToPreMigrationWireFormat(t *testing.T) {
	p := goldenTaskCompletedPublisher(t)
	completedAt := goldenEpoch.Add(45 * time.Second)

	encoded, err := p.Encode(context.Background(), shared.NewTaskCompleted("task-golden", "station-golden", completedAt))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("flat mode must produce exactly 1 message, got %d", len(encoded))
	}
	if got := string(encoded[0].Value); got != goldenTaskCompletedFlatJSON {
		t.Errorf("flat JSON changed:\n got:  %s\n want: %s", got, goldenTaskCompletedFlatJSON)
	}
}

func TestEncode_FlatMode_TaskCPTMissed_ByteIdenticalToPreMigrationWireFormat(t *testing.T) {
	p := outboundkafka.NewPublisherWithWriter(nil, nil, nil, func() string { return "evt-golden-2" })
	cpt := goldenEpoch.Add(-30 * time.Minute)

	encoded, err := p.Encode(context.Background(), shared.NewTaskCPTMissed("task-golden", "order-golden", "PICK", cpt, goldenEpoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("flat mode must produce exactly 1 message, got %d", len(encoded))
	}
	if got := string(encoded[0].Value); got != goldenTaskCPTMissedFlatJSON {
		t.Errorf("flat JSON changed:\n got:  %s\n want: %s", got, goldenTaskCPTMissedFlatJSON)
	}
}

func TestEncode_FlatMode_PackageManifested_ByteIdenticalToPreMigrationWireFormat(t *testing.T) {
	p := outboundkafka.NewPublisherWithWriter(nil, nil, nil, func() string { return "evt-golden-3" })

	encoded, err := p.Encode(context.Background(), shared.NewPackageManifested("pkg-golden", "order-golden", goldenEpoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("flat mode must produce exactly 1 message, got %d", len(encoded))
	}
	if got := string(encoded[0].Value); got != goldenPackageManifestedFlatJSON {
		t.Errorf("flat JSON changed:\n got:  %s\n want: %s", got, goldenPackageManifestedFlatJSON)
	}
}

// The default zero-value Mode (no Mode field set at all, exactly how every
// pre-existing Publisher literal in this package's other tests is built)
// must also behave as flat — this is the "no infra var set anywhere"
// no-op-merge guarantee ADR-0027 Phase 4 depends on.
func TestEncode_UnsetMode_DefaultsToFlat(t *testing.T) {
	p := &outboundkafka.Publisher{Writer: nil, NewId: func() string { return "evt-golden-3" }}
	encoded, err := p.Encode(context.Background(), shared.NewPackageManifested("pkg-golden", "order-golden", goldenEpoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("expected exactly 1 message for the unset/zero-value Mode, got %d", len(encoded))
	}
	if got := string(encoded[0].Value); got != goldenPackageManifestedFlatJSON {
		t.Errorf("zero-value Mode must equal flat output:\n got:  %s\n want: %s", got, goldenPackageManifestedFlatJSON)
	}
}

// An unrecognized EVENT_ENVELOPE_MODE value also falls back to flat,
// mirroring this repo's other MODE env vars' fail-safe-default behavior.
func TestParseEnvelopeMode_UnrecognizedValueDefaultsToFlat(t *testing.T) {
	for _, v := range []string{"", "  ", "bogus", "FLAT-ish"} {
		if got := outboundkafka.ParseEnvelopeMode(v); got != outboundkafka.EnvelopeModeFlat {
			t.Errorf("ParseEnvelopeMode(%q) = %q, want flat", v, got)
		}
	}
	if got := outboundkafka.ParseEnvelopeMode("CloudEvents"); got != outboundkafka.EnvelopeModeCloudEvents {
		t.Errorf("ParseEnvelopeMode case-insensitive cloudevents: got %q", got)
	}
	if got := outboundkafka.ParseEnvelopeMode("DUAL"); got != outboundkafka.EnvelopeModeDual {
		t.Errorf("ParseEnvelopeMode case-insensitive dual: got %q", got)
	}
}

// --- cloudevents mode: schema-field assertions (ADR-0027 P4) ---

type ceEnvelope[T any] struct {
	SpecVersion     string    `json:"specversion"`
	Id              string    `json:"id"`
	Type            string    `json:"type"`
	Source          string    `json:"source"`
	Subject         string    `json:"subject"`
	Time            time.Time `json:"time"`
	DataContentType string    `json:"datacontenttype"`
	Data            T         `json:"data"`
}

func TestEncode_CloudEventsMode_TaskCompleted_SchemaFields(t *testing.T) {
	p := goldenTaskCompletedPublisher(t)
	p.Mode = outboundkafka.EnvelopeModeCloudEvents
	completedAt := goldenEpoch.Add(45 * time.Second)

	encoded, err := p.Encode(context.Background(), shared.NewTaskCompleted("task-golden", "station-golden", completedAt))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("cloudevents mode must produce exactly 1 message, got %d", len(encoded))
	}
	var env ceEnvelope[outboundkafka.TaskCompletedData]
	if err := json.Unmarshal(encoded[0].Value, &env); err != nil {
		t.Fatalf("unmarshal cloudevent: %v", err)
	}
	assertCloudEventEnvelopeFields(t, env, completedAt)
	if env.Data.TaskId != "task-golden" || env.Data.WorkUnitId != "wu-golden" || env.Data.AssociateId != "worker-golden" ||
		env.Data.DurationSeconds != 45 || env.Data.TaskType != "PICK" {
		t.Errorf("data payload changed vs. flat mode's own data: %+v", env.Data)
	}
	if string(encoded[0].Key) != "task-golden" {
		t.Errorf("Key = %q, want task-golden (same as flat mode)", encoded[0].Key)
	}
}

// assertCloudEventEnvelopeFields checks the fixed CloudEvents envelope
// attributes of a TaskCompleted message in CloudEvents mode.
func assertCloudEventEnvelopeFields(t *testing.T, env ceEnvelope[outboundkafka.TaskCompletedData], completedAt time.Time) {
	t.Helper()
	if env.SpecVersion != "1.0" {
		t.Errorf("specversion = %q, want 1.0", env.SpecVersion)
	}
	if env.Id != "evt-golden-1" {
		t.Errorf("id = %q, want the same value as event_id (evt-golden-1)", env.Id)
	}
	if env.Type != "com.warehouse.wes.fulfillment-execution.task.TaskCompleted" {
		t.Errorf("type = %q, want com.warehouse.wes.fulfillment-execution.task.TaskCompleted", env.Type)
	}
	if env.Source != "/warehouse/fulfillment-execution" {
		t.Errorf("source = %q, want /warehouse/fulfillment-execution", env.Source)
	}
	if env.Subject != "task-golden" {
		t.Errorf("subject = %q, want task-golden (the task id)", env.Subject)
	}
	if !env.Time.Equal(completedAt) {
		t.Errorf("time = %v, want %v (== occurred_at)", env.Time, completedAt)
	}
	if env.DataContentType != "application/json" {
		t.Errorf("datacontenttype = %q, want application/json", env.DataContentType)
	}
}

func TestEncode_CloudEventsMode_TaskCPTMissed_SchemaFields(t *testing.T) {
	p := outboundkafka.NewPublisherWithWriter(nil, nil, nil, func() string { return "evt-golden-2" })
	p.Mode = outboundkafka.EnvelopeModeCloudEvents
	cpt := goldenEpoch.Add(-30 * time.Minute)

	encoded, err := p.Encode(context.Background(), shared.NewTaskCPTMissed("task-golden", "order-golden", "PICK", cpt, goldenEpoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("cloudevents mode must produce exactly 1 message, got %d", len(encoded))
	}
	var env ceEnvelope[outboundkafka.TaskCPTMissedData]
	if err := json.Unmarshal(encoded[0].Value, &env); err != nil {
		t.Fatalf("unmarshal cloudevent: %v", err)
	}
	if env.SpecVersion != "1.0" {
		t.Errorf("specversion = %q, want 1.0", env.SpecVersion)
	}
	if env.Id != "evt-golden-2" {
		t.Errorf("id = %q, want evt-golden-2", env.Id)
	}
	if env.Type != "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" {
		t.Errorf("type = %q, want com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed", env.Type)
	}
	if env.Source != "/warehouse/fulfillment-execution" {
		t.Errorf("source = %q, want /warehouse/fulfillment-execution", env.Source)
	}
	if env.Subject != "task-golden" {
		t.Errorf("subject = %q, want task-golden (the task id)", env.Subject)
	}
	if !env.Time.Equal(goldenEpoch) {
		t.Errorf("time = %v, want %v (== occurred_at)", env.Time, goldenEpoch)
	}
	if env.DataContentType != "application/json" {
		t.Errorf("datacontenttype = %q, want application/json", env.DataContentType)
	}
	if env.Data.TaskId != "task-golden" || env.Data.OrderRef != "order-golden" || env.Data.TaskType != "PICK" || !env.Data.Cpt.Equal(cpt) {
		t.Errorf("data payload changed vs. flat mode's own data: %+v", env.Data)
	}
}

func TestEncode_CloudEventsMode_PackageManifested_SchemaFields(t *testing.T) {
	p := outboundkafka.NewPublisherWithWriter(nil, nil, nil, func() string { return "evt-golden-3" })
	p.Mode = outboundkafka.EnvelopeModeCloudEvents

	encoded, err := p.Encode(context.Background(), shared.NewPackageManifested("pkg-golden", "order-golden", goldenEpoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("cloudevents mode must produce exactly 1 message, got %d", len(encoded))
	}
	var env ceEnvelope[outboundkafka.PackageManifestedData]
	if err := json.Unmarshal(encoded[0].Value, &env); err != nil {
		t.Fatalf("unmarshal cloudevent: %v", err)
	}
	if env.SpecVersion != "1.0" {
		t.Errorf("specversion = %q, want 1.0", env.SpecVersion)
	}
	if env.Id != "evt-golden-3" {
		t.Errorf("id = %q, want evt-golden-3", env.Id)
	}
	if env.Type != "com.warehouse.wes.fulfillment-execution.package.PackageManifested" {
		t.Errorf("type = %q, want com.warehouse.wes.fulfillment-execution.package.PackageManifested", env.Type)
	}
	if env.Source != "/warehouse/fulfillment-execution" {
		t.Errorf("source = %q, want /warehouse/fulfillment-execution", env.Source)
	}
	if env.Subject != "pkg-golden" {
		t.Errorf("subject = %q, want pkg-golden (the package id)", env.Subject)
	}
	if !env.Time.Equal(goldenEpoch) {
		t.Errorf("time = %v, want %v (== occurred_at)", env.Time, goldenEpoch)
	}
	if env.DataContentType != "application/json" {
		t.Errorf("datacontenttype = %q, want application/json", env.DataContentType)
	}
	if env.Data.PackageId != "pkg-golden" || env.Data.OrderRef != "order-golden" {
		t.Errorf("data payload changed vs. flat mode's own data: %+v", env.Data)
	}
}

// --- dual mode: 2 physical messages, same key, one of each shape ---

func TestEncode_DualMode_TaskCompleted_ProducesTwoMessagesSameKeyBothShapes(t *testing.T) {
	p := goldenTaskCompletedPublisher(t)
	p.Mode = outboundkafka.EnvelopeModeDual
	completedAt := goldenEpoch.Add(45 * time.Second)

	encoded, err := p.Encode(context.Background(), shared.NewTaskCompleted("task-golden", "station-golden", completedAt))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 2 {
		t.Fatalf("dual mode must produce exactly 2 messages, got %d", len(encoded))
	}
	if string(encoded[0].Key) != "task-golden" || string(encoded[1].Key) != "task-golden" {
		t.Fatalf("dual mode messages must share the same key: %q, %q", encoded[0].Key, encoded[1].Key)
	}

	// message 0 must be the flat shape, byte-identical to flat-mode-alone.
	if got := string(encoded[0].Value); got != goldenTaskCompletedFlatJSON {
		t.Errorf("dual mode's flat message changed:\n got:  %s\n want: %s", got, goldenTaskCompletedFlatJSON)
	}

	// message 1 must be the CloudEvents shape, structurally verified.
	assertDualCloudEventShape(t, encoded[1].Value)
	assertFlatMessageNotCloudEvent(t, encoded[0].Value)
}

// assertDualCloudEventShape checks that raw decodes as the TaskCompleted
// CloudEvents envelope with the golden identity fields and payload.
func assertDualCloudEventShape(t *testing.T, raw []byte) {
	t.Helper()
	var ce ceEnvelope[outboundkafka.TaskCompletedData]
	if err := json.Unmarshal(raw, &ce); err != nil {
		t.Fatalf("unmarshal second message as a cloudevent: %v", err)
	}
	if ce.SpecVersion != "1.0" || ce.Id != "evt-golden-1" ||
		ce.Type != "com.warehouse.wes.fulfillment-execution.task.TaskCompleted" ||
		ce.Subject != "task-golden" || ce.DataContentType != "application/json" {
		t.Errorf("dual mode's cloudevents message malformed: %+v", ce)
	}
	if ce.Data.TaskId != "task-golden" || ce.Data.WorkUnitId != "wu-golden" {
		t.Errorf("dual mode's cloudevents data changed: %+v", ce.Data)
	}
}

// assertFlatMessageNotCloudEvent explicitly confirms the flat message is
// NOT itself a valid cloudevent (no specversion key at all) — proving the
// two dual-mode messages are genuinely different shapes, not two copies
// of the same one — while still carrying the flat event_id.
func assertFlatMessageNotCloudEvent(t *testing.T, raw []byte) {
	t.Helper()
	var probe map[string]any
	if err := json.Unmarshal(raw, &probe); err != nil {
		t.Fatalf("unmarshal flat message: %v", err)
	}
	if _, hasSpecVersion := probe["specversion"]; hasSpecVersion {
		t.Errorf("dual mode's flat message must not carry specversion, got %v", probe)
	}
	if _, hasEventId := probe["event_id"]; !hasEventId {
		t.Errorf("dual mode's flat message must carry event_id, got %v", probe)
	}
}

func TestEncode_DualMode_TaskCPTMissed_ProducesTwoMessagesSameKey(t *testing.T) {
	p := outboundkafka.NewPublisherWithWriter(nil, nil, nil, func() string { return "evt-golden-2" })
	p.Mode = outboundkafka.EnvelopeModeDual
	cpt := goldenEpoch.Add(-30 * time.Minute)

	encoded, err := p.Encode(context.Background(), shared.NewTaskCPTMissed("task-golden", "order-golden", "PICK", cpt, goldenEpoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 2 {
		t.Fatalf("dual mode must produce exactly 2 messages, got %d", len(encoded))
	}
	if string(encoded[0].Key) != "task-golden" || string(encoded[1].Key) != "task-golden" {
		t.Fatalf("dual mode messages must share the same key: %q, %q", encoded[0].Key, encoded[1].Key)
	}
	if got := string(encoded[0].Value); got != goldenTaskCPTMissedFlatJSON {
		t.Errorf("dual mode's flat message changed:\n got:  %s\n want: %s", got, goldenTaskCPTMissedFlatJSON)
	}
	var ce ceEnvelope[outboundkafka.TaskCPTMissedData]
	if err := json.Unmarshal(encoded[1].Value, &ce); err != nil {
		t.Fatalf("unmarshal second message as a cloudevent: %v", err)
	}
	if ce.SpecVersion != "1.0" || ce.Type != "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" {
		t.Errorf("dual mode's cloudevents message malformed: %+v", ce)
	}
}

func TestEncode_DualMode_PackageManifested_ProducesTwoMessagesSameKey(t *testing.T) {
	p := outboundkafka.NewPublisherWithWriter(nil, nil, nil, func() string { return "evt-golden-3" })
	p.Mode = outboundkafka.EnvelopeModeDual

	encoded, err := p.Encode(context.Background(), shared.NewPackageManifested("pkg-golden", "order-golden", goldenEpoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 2 {
		t.Fatalf("dual mode must produce exactly 2 messages, got %d", len(encoded))
	}
	if string(encoded[0].Key) != "pkg-golden" || string(encoded[1].Key) != "pkg-golden" {
		t.Fatalf("dual mode messages must share the same key: %q, %q", encoded[0].Key, encoded[1].Key)
	}
	if got := string(encoded[0].Value); got != goldenPackageManifestedFlatJSON {
		t.Errorf("dual mode's flat message changed:\n got:  %s\n want: %s", got, goldenPackageManifestedFlatJSON)
	}
	var ce ceEnvelope[outboundkafka.PackageManifestedData]
	if err := json.Unmarshal(encoded[1].Value, &ce); err != nil {
		t.Fatalf("unmarshal second message as a cloudevent: %v", err)
	}
	if ce.SpecVersion != "1.0" || ce.Type != "com.warehouse.wes.fulfillment-execution.package.PackageManifested" {
		t.Errorf("dual mode's cloudevents message malformed: %+v", ce)
	}
}

// cloudEventsType is unexported; these tests are in package kafka_test so
// they cannot call it directly, but the "type" field assertions above
// already pin its exact output for every event/entity pair this publisher
// handles. NewPublisherWithMode/NewPublisherWithWriterAndMode are
// exercised here too, so the constructor surface added alongside Mode is
// covered, not just the field.

func TestNewPublisherWithWriterAndMode_SetsMode(t *testing.T) {
	p := outboundkafka.NewPublisherWithWriterAndMode(nil, nil, nil, func() string { return "evt-x" }, outboundkafka.EnvelopeModeCloudEvents)
	encoded, err := p.Encode(context.Background(), shared.NewPackageManifested("pkg-1", "order-1", goldenEpoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("expected 1 message in cloudevents mode, got %d", len(encoded))
	}
	var probe map[string]any
	if err := json.Unmarshal(encoded[0].Value, &probe); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := probe["specversion"]; !ok {
		t.Errorf("expected a cloudevents-shaped message, got %v", probe)
	}
}
