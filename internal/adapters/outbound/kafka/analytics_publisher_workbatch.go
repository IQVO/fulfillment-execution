package kafka

import (
	"encoding/json"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// Work-batch analytics occurrences (ADR 0063): the WorkBatch* half of
// AnalyticsPublisher's Encode map. Split out of analytics_publisher.go to
// keep that file inside the 300-code-line file-length budget.

// workBatchFormedPayload builds the analytics occurrence of a
// WorkBatchFormed event. It mirrors WorkBatchFormData field for field (the
// integration payload struct declares its fields alphabetically and
// encoding/json sorts map keys), so the `data` bytes on the integration
// topic are identical to this occurrence.
func workBatchFormedPayload(ev shared.WorkBatchFormed) map[string]any {
	members := make([]map[string]any, len(ev.Members))
	for i, m := range ev.Members {
		members[i] = map[string]any{
			attrTaskID:    string(m.TaskId),
			"stop_seq":    m.StopSeq,
			"compartment": m.Compartment,
		}
	}
	return map[string]any{
		attrBatchID:   string(ev.BatchId),
		attrStationID: string(ev.StationId),
		"cpt":         ev.Cpt,
		"members":     members,
		"formed_at":   ev.OccurredAt(),
		attrVersion:   ev.Version,
	}
}

// workBatchCompletedPayload mirrors WorkBatchCompletedData.
func workBatchCompletedPayload(ev shared.WorkBatchCompleted) map[string]any {
	return map[string]any{
		attrBatchID:    string(ev.BatchId),
		attrStationID:  string(ev.StationId),
		"member_count": ev.MemberCount,
		"walk_seconds": ev.WalkSeconds,
		"completed_at": ev.OccurredAt(),
		attrVersion:    ev.Version,
	}
}

// workBatchAbandonedPayload mirrors WorkBatchAbandonedData.
func workBatchAbandonedPayload(ev shared.WorkBatchAbandoned) map[string]any {
	return map[string]any{
		attrBatchID:    string(ev.BatchId),
		attrStationID:  string(ev.StationId),
		"member_count": ev.MemberCount,
		"abandoned_at": ev.OccurredAt(),
		attrVersion:    ev.Version,
	}
}

// marshalWorkBatch maps a WorkBatch* domain event (ADR 0063) to its
// analytics occurrence: same entity segment, event name and batch-id
// key/subject as the integration publisher, with a `data` map whose bytes
// are identical to the integration payload (the payload funcs mirror the
// wire structs field for field and encoding/json sorts map keys).
func (p *AnalyticsPublisher) marshalWorkBatch(e shared.DomainEvent) (entity, eventName, key string, data json.RawMessage, ok bool) {
	switch ev := e.(type) {
	case shared.WorkBatchFormed:
		return entityWorkBatch, "WorkBatchFormed", string(ev.BatchId), mustMarshal(workBatchFormedPayload(ev)), true
	case shared.WorkBatchCompleted:
		return entityWorkBatch, "WorkBatchCompleted", string(ev.BatchId), mustMarshal(workBatchCompletedPayload(ev)), true
	case shared.WorkBatchAbandoned:
		return entityWorkBatch, "WorkBatchAbandoned", string(ev.BatchId), mustMarshal(workBatchAbandonedPayload(ev)), true
	default:
		return "", "", "", nil, false
	}
}
