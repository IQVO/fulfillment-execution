package cloudevents

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestType(t *testing.T) {
	got := Type("task", "TaskCompleted")
	want := "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"
	if got != want {
		t.Fatalf("Type = %q, want %q", got, want)
	}
}

func TestDataSchema(t *testing.T) {
	if got, want := DataSchema(StreamEvents, "TaskCompleted", 1), "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1"; got != want {
		t.Fatalf("DataSchema(events) = %q, want %q", got, want)
	}
	if got, want := DataSchema(StreamAnalytics, "TaskCreated", 2), "urn:warehouse:fulfillment-execution:analytics:TaskCreated:v2"; got != want {
		t.Fatalf("DataSchema(analytics) = %q, want %q", got, want)
	}
}

func TestSource(t *testing.T) {
	if Source != "/warehouse/fulfillment-execution" {
		t.Fatalf("Source = %q", Source)
	}
}

func TestNew_GoldenJSON(t *testing.T) {
	b, err := New(Spec{
		ID:        "11111111-1111-4111-8111-111111111111",
		Entity:    "task",
		EventName: "TaskCompleted",
		Subject:   "task-1",
		Time:      time.Date(2026, 9, 30, 12, 0, 0, 0, time.FixedZone("x", -3*3600)),
		Stream:    StreamEvents,
		Version:   1,
		Data:      map[string]any{"task_id": "task-1"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := `{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111","source":"/warehouse/fulfillment-execution","type":"com.warehouse.wes.fulfillment-execution.task.TaskCompleted","subject":"task-1","datacontenttype":"application/json","dataschema":"urn:warehouse:fulfillment-execution:events:TaskCompleted:v1","time":"2026-09-30T15:00:00Z","data":{"task_id":"task-1"}}`
	assertJSONEqual(t, b, want)
}

func TestNew_DefaultsVersionToOne(t *testing.T) {
	b, err := New(Spec{ID: "id-1", Entity: "task", EventName: "X", Subject: "s", Time: time.Unix(0, 0), Stream: StreamAnalytics, Data: map[string]any{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.DataSchema() != "urn:warehouse:fulfillment-execution:analytics:X:v1" {
		t.Fatalf("dataschema = %q", e.DataSchema())
	}
}

func TestNew_RejectsEmptySubject(t *testing.T) {
	if _, err := New(Spec{ID: "id", Entity: "task", EventName: "X", Time: time.Now(), Stream: StreamEvents, Data: map[string]any{}}); err == nil {
		t.Fatal("expected error for empty subject")
	}
}

func TestNew_RejectsEmptyID(t *testing.T) {
	if _, err := New(Spec{Entity: "task", EventName: "X", Subject: "s", Time: time.Now(), Stream: StreamEvents, Data: map[string]any{}}); err == nil {
		t.Fatal("expected error for empty id")
	}
}

func TestContentTypeHeader(t *testing.T) {
	h := ContentTypeHeader()
	if h.Key != "content-type" || string(h.Value) != "application/cloudevents+json; charset=UTF-8" {
		t.Fatalf("header = %s: %s", h.Key, h.Value)
	}
}

func TestDecode_RoundTrip(t *testing.T) {
	b, err := New(Spec{ID: "abc", Entity: "task", EventName: "TaskCPTMissed", Subject: "task-9", Time: time.Unix(100, 0), Stream: StreamEvents, Version: 1, Data: map[string]string{"task_id": "task-9"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.ID() != "abc" || e.Subject() != "task-9" || e.Type() != "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" {
		t.Fatalf("unexpected event %v", e)
	}
	var data map[string]string
	if err := e.DataAs(&data); err != nil || data["task_id"] != "task-9" {
		t.Fatalf("DataAs: %v %v", data, err)
	}
}

func TestDecode_RejectsLegacyFlatEnvelope(t *testing.T) {
	flat := []byte(`{"event_id":"e1","event_type":"TaskCompleted","occurred_at":"2026-01-01T00:00:00Z","source":"fulfillment-execution","data":{}}`)
	if _, err := Decode(flat); !errors.Is(err, ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
}

func TestDecode_RejectsBadJSON(t *testing.T) {
	if _, err := Decode([]byte("not json")); !errors.Is(err, ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
}

func TestDecode_RejectsWrongSpecVersion(t *testing.T) {
	raw := []byte(`{"specversion":"0.3","id":"1","source":"/x","type":"t"}`)
	if _, err := Decode(raw); !errors.Is(err, ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
}

func TestDecode_RejectsMissingRequiredAttribute(t *testing.T) {
	raw := []byte(`{"specversion":"1.0","source":"/x","type":"t"}`)
	if _, err := Decode(raw); !errors.Is(err, ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
}

func assertJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("unmarshal got: %v (%s)", err, got)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if string(gb) != string(wb) {
		t.Fatalf("JSON mismatch\n got: %s\nwant: %s", gb, wb)
	}
}
