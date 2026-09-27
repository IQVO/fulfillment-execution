package kafka

import (
	"fmt"
	"strings"
	"time"
)

// EnvelopeMode selects which wire envelope shape(s) Publisher.Encode
// produces for each domain event, controlled by the EVENT_ENVELOPE_MODE
// environment variable (ADR-0027 Phase 4). The zero value and any
// unrecognized value both behave as EnvelopeModeFlat, so a Publisher built
// via a bare struct literal (as every pre-existing test in this package
// does) keeps writing today's byte-identical flat envelope with no source
// change required on their part.
type EnvelopeMode string

const (
	// EnvelopeModeFlat is today's Envelope shape
	// (event_id/event_type/occurred_at/source/data), byte-identical to
	// the wire format this publisher wrote before ADR-0027. Default.
	EnvelopeModeFlat EnvelopeMode = "flat"
	// EnvelopeModeCloudEvents publishes a single CloudEvents 1.0
	// structured-mode message per domain event.
	EnvelopeModeCloudEvents EnvelopeMode = "cloudevents"
	// EnvelopeModeDual publishes BOTH shapes as two physical Kafka
	// messages per domain event, on the same topic with the same key, so
	// consumers can be migrated to CloudEvents independently of this
	// publisher's own cutover (see ADR-0027 §3).
	EnvelopeModeDual EnvelopeMode = "dual"
)

// ParseEnvelopeMode parses the EVENT_ENVELOPE_MODE value v, matching
// case-insensitively (mirroring this repo's other MODE env vars, e.g.
// PRODUCT_CLASSIFICATION_MODE). "", whitespace, and any value other than
// "cloudevents"/"dual" all resolve to EnvelopeModeFlat — this env var is
// unset everywhere in infra today, so every deployed instance keeps
// running flat until a later, separate phase sets it.
func ParseEnvelopeMode(v string) EnvelopeMode {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case string(EnvelopeModeCloudEvents):
		return EnvelopeModeCloudEvents
	case string(EnvelopeModeDual):
		return EnvelopeModeDual
	default:
		return EnvelopeModeFlat
	}
}

// effectiveMode normalizes Publisher.Mode the same way ParseEnvelopeMode
// does, so a Publisher built via a raw struct literal (Mode == "") behaves
// as EnvelopeModeFlat rather than an unrecognized empty mode.
func (p *Publisher) effectiveMode() EnvelopeMode {
	switch p.Mode {
	case EnvelopeModeCloudEvents, EnvelopeModeDual:
		return p.Mode
	default:
		return EnvelopeModeFlat
	}
}

// ceSubdomain and ceBoundedContext are this service's DDD coordinates in
// the CloudEvents `type` convention already specified — and now
// implemented — in apis/asyncapi.yaml:
// com.warehouse.<subdomain>.<bounded-context>.<entity>.<Event>.
const (
	ceSubdomain      = "wes"
	ceBoundedContext = "fulfillment-execution"
	// ceSource is the CloudEvents `source` value, already used verbatim
	// in apis/asyncapi.yaml's worked examples.
	ceSource = "/warehouse/fulfillment-execution"
)

// cloudEventsType returns the reverse-DNS CloudEvents `type` string for an
// event named eventName raised by entity (e.g. "task", "package") —
// verbatim the convention already documented and exemplified in
// apis/asyncapi.yaml, not a new invention.
func cloudEventsType(entity, eventName string) string {
	return fmt.Sprintf("com.warehouse.%s.%s.%s.%s", ceSubdomain, ceBoundedContext, entity, eventName)
}

// CloudEvent is the CloudEvents 1.0 structured-mode envelope (ADR-0027).
// Data is byte-identical to the corresponding flat envelope's own Data
// payload — this migration is envelope-only.
type CloudEvent[T any] struct {
	SpecVersion     string    `json:"specversion"`
	Id              string    `json:"id"`
	Type            string    `json:"type"`
	Source          string    `json:"source"`
	Subject         string    `json:"subject"`
	Time            time.Time `json:"time"`
	DataContentType string    `json:"datacontenttype"`
	Data            T         `json:"data"`
}

// newCloudEvent builds a CloudEvent envelope. id is the same value the
// flat envelope's EventId carries (a UUID v4), entity/eventName feed
// cloudEventsType, subject is the raising aggregate's instance id, and
// occurredAt is the same domain-clock value the flat envelope's
// OccurredAt carries.
func newCloudEvent[T any](id, entity, eventName, subject string, occurredAt time.Time, data T) CloudEvent[T] {
	return CloudEvent[T]{
		SpecVersion:     "1.0",
		Id:              id,
		Type:            cloudEventsType(entity, eventName),
		Source:          ceSource,
		Subject:         subject,
		Time:            occurredAt,
		DataContentType: "application/json",
		Data:            data,
	}
}
