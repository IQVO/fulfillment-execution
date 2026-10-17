package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// TaskAvailableData is the payload of a published TaskAvailable event (ADR
// 0049): a task entered or re-entered the pool. It is the pinned
// consumer-first contract of task-interleaving (its ADR 0003). Field order is
// the wire order.
//
// FromLocation is where the operator goes first, ToLocation where the
// operator ends. RequiredCapabilities is what claimNext matches a station
// against, an empty array (never null) when none. Cpt is omitted when the
// task has none. PriorityBand is opaque and equal for exactly the tasks
// claimNext orders as peers. AvailableAt is the instant the task became
// claimable.
type TaskAvailableData struct {
	TaskId               string     `json:"task_id"`
	TaskType             string     `json:"task_type"`
	SiteId               string     `json:"site_id"`
	FromLocation         string     `json:"from_location"`
	ToLocation           string     `json:"to_location"`
	RequiredCapabilities []string   `json:"required_capabilities"`
	Cpt                  *time.Time `json:"cpt,omitempty"`
	PriorityBand         string     `json:"priority_band"`
	AvailableAt          time.Time  `json:"available_at"`
}

// TaskClaimedData is the payload of a published TaskClaimed integration
// event (ADR 0049): a task left the pool. The analytics-topic row of the same
// occurrence ({task_id, task_type, station_id}) is unchanged.
type TaskClaimedData struct {
	TaskId    string    `json:"task_id"`
	TaskType  string    `json:"task_type"`
	StationId string    `json:"station_id"`
	ClaimedAt time.Time `json:"claimed_at"`
}

// encodeTaskAvailable builds the TaskAvailable integration message for a task
// that just entered the pool. The payload is resolved from the task row (so
// inside the use case's transaction it sees the just-saved row): type,
// capabilities, CPT, placement. A task with no determinable placement (every
// type except RELOCATE today, ADR 0049) or that cannot be found yields no
// message: the consumer rejects an event without a site and a from_location,
// so announcing it would only add a poison message.
func (p *Publisher) encodeTaskAvailable(ctx context.Context, ev shared.TaskAvailable) ([]Encoded, error) {
	if p.Tasks == nil {
		return nil, nil
	}
	t, err := p.Tasks.FindById(ctx, ev.TaskId)
	if err != nil {
		return nil, fmt.Errorf("kafka: lookup task %s for TaskAvailable: %w", ev.TaskId, err)
	}
	if t == nil {
		return nil, nil
	}
	place, ok := t.Placement()
	if !ok {
		slog.DebugContext(ctx, "TaskAvailable not published: the task has no determinable location or site",
			"task_id", string(ev.TaskId), "task_type", string(t.Type()))
		return nil, nil
	}
	data := TaskAvailableData{
		TaskId:               string(ev.TaskId),
		TaskType:             string(t.Type()),
		SiteId:               place.SiteId,
		FromLocation:         place.FromLocation,
		ToLocation:           place.ToLocation,
		RequiredCapabilities: t.RequiredCapabilityNames(),
		PriorityBand:         t.PriorityBand(),
		AvailableAt:          ev.OccurredAt().UTC(),
	}
	if cpt := t.CPT().Time(); !cpt.IsZero() {
		cpt = cpt.UTC()
		data.Cpt = &cpt
	}
	enc, err := p.encodeIntegration(entityTask, "TaskAvailable", string(ev.TaskId), ev.OccurredAt(), data)
	if err != nil {
		return nil, err
	}
	return []Encoded{enc}, nil
}

// encodeTaskClaimed builds the TaskClaimed integration message, the richer row
// next to the analytics row of the same domain event (each encoder mints its
// own CloudEvents id, as for TaskCompleted). task_type is read off the task;
// it is empty only when the task can no longer be found.
func (p *Publisher) encodeTaskClaimed(ctx context.Context, ev shared.TaskClaimed) ([]Encoded, error) {
	taskType := ""
	if p.Tasks != nil {
		t, err := p.Tasks.FindById(ctx, ev.TaskId)
		if err != nil {
			return nil, fmt.Errorf("kafka: lookup task %s for TaskClaimed: %w", ev.TaskId, err)
		}
		if t != nil {
			taskType = string(t.Type())
		}
	}
	data := TaskClaimedData{
		TaskId:    string(ev.TaskId),
		TaskType:  taskType,
		StationId: string(ev.StationId),
		ClaimedAt: ev.OccurredAt().UTC(),
	}
	enc, err := p.encodeIntegration(entityTask, "TaskClaimed", string(ev.TaskId), ev.OccurredAt(), data)
	if err != nil {
		return nil, err
	}
	return []Encoded{enc}, nil
}
