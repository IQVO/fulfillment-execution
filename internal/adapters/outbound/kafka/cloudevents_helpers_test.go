package kafka_test

import (
	"encoding/json"
	"testing"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
)

// decodeCE decodes a published message value as a CloudEvents 1.0 event via
// the production Decode (so validation is exercised too) and its data via
// DataAs into T.
func decodeCE[T any](t *testing.T, raw []byte) (ce.Event, T) {
	t.Helper()
	var data T
	e, err := cloudevents.Decode(raw)
	if err != nil {
		t.Fatalf("decode cloudevent: %v (%s)", err, raw)
	}
	if err := e.DataAs(&data); err != nil {
		t.Fatalf("cloudevent DataAs: %v", err)
	}
	return e, data
}

// assertJSONEqual compares two JSON documents structurally (key order
// insensitive), failing with both renderings on mismatch.
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

// hasContentTypeHeader reports whether hs carries the structured-mode
// CloudEvents content-type header exactly.
func hasContentTypeHeader(hs []kafkago.Header) bool {
	for _, h := range hs {
		if h.Key == "content-type" && string(h.Value) == cloudevents.MediaType {
			return true
		}
	}
	return false
}
