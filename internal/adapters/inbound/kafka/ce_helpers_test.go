package kafka_test

import (
	"encoding/json"
	"fmt"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
)

// workReleasedCE builds a CloudEvents 1.0 structured-mode message of the
// given full type, exactly as wes-work-planning publishes WorkReleased
// (ADR-0032), via the official SDK.
func workReleasedCE(id, typ, subject string, data map[string]any) []byte {
	return cloudEvent(id, "/warehouse/wes-work-planning", typ, subject,
		time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC), "urn:warehouse:wes-work-planning:events:WorkReleased:v1", data)
}

// cloudEvent builds and marshals one CloudEvents 1.0 event. It panics on a
// marshal error — fixtures are fully controlled by the tests.
func cloudEvent(id, source, typ, subject string, at time.Time, dataschema string, data any) []byte {
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource(source)
	e.SetType(typ)
	e.SetSubject(subject)
	e.SetTime(at)
	e.SetDataSchema(dataschema)
	if err := e.SetData(ce.ApplicationJSON, data); err != nil {
		panic(fmt.Sprintf("set data: %v", err))
	}
	b, err := json.Marshal(e)
	if err != nil {
		panic(fmt.Sprintf("marshal cloudevent: %v", err))
	}
	return b
}
