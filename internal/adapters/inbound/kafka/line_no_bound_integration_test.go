//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// TestWorkReleased_OutOfRangeLineNoDoesNotFailTheInsert proves, against real
// Postgres (testcontainers) with the real UnitOfWork, that a WorkReleased
// whose line_no does not fit the 32-bit source_line_no column neither fails
// the tasks insert nor poisons the partition: the message is applied (nil
// error, so Run never dead-letters it), the task exists with an unknown line
// (NULL column), and the highest valid value is still stored.
func TestWorkReleased_OutOfRangeLineNoDoesNotFailTheInsert(t *testing.T) {
	pool := transferDB(t)
	ctx := context.Background()
	tasks := postgres.NewTaskRepo(pool)
	stations := postgres.NewStationRepo(pool)
	processed := postgres.NewProcessedEventsRepo(pool)
	uow := postgres.NewUnitOfWork(pool)

	ceId := 0
	integration := outboundkafka.NewPublisherWithWriter(nil, tasks, stations, func() string { ceId++; return fmt.Sprintf("ce-lb-%d", ceId) })
	pub := postgres.NewOutboxPublisher(pool, integration)

	n := 0
	newId := func() shared.TaskId { n++; return shared.TaskId(fmt.Sprintf("t-lb-%d", n)) }
	create := &usecases.CreateTask{Tasks: tasks, Publisher: pub, Clock: fixedClockAt(time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)), NewId: newId, UnitOfWork: uow}
	apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: processed, Catalogue: testCatalogue(), UnitOfWork: uow}
	consumer := &inboundkafka.Consumer{Apply: apply, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	for i, tc := range []struct {
		line json.Number
		want int
	}{
		{"2147483647", 2147483647},
		{"2147483648", 0},
		{"9223372036854775807", 0},
		{"9223372036854775808", 0},
	} {
		wu := fmt.Sprintf("ORD-LB-line-%d", i)
		raw := workReleasedJSONWithLine(fmt.Sprintf("evt-lb-%d", i), "PICK", wu, map[string]any{"line_no": tc.line, "ref": "ORD-LB"})
		if err := consumer.HandleMessage(ctx, raw); err != nil {
			t.Fatalf("line_no %s: HandleMessage failed (would retry then dead-letter): %v", tc.line, err)
		}
		list, err := tasks.FindByOrderRef(ctx, shared.OrderRef(wu))
		if err != nil || len(list) != 1 {
			t.Fatalf("line_no %s: want exactly 1 task, got %d err=%v", tc.line, len(list), err)
		}
		if got := list[0].SourceLineNo(); got != tc.want {
			t.Fatalf("line_no %s: SourceLineNo = %d, want %d", tc.line, got, tc.want)
		}
		var col *int32
		if err := pool.QueryRow(ctx, `SELECT source_line_no FROM tasks WHERE order_ref = $1`, wu).Scan(&col); err != nil {
			t.Fatalf("line_no %s: read column: %v", tc.line, err)
		}
		if tc.want == 0 && col != nil {
			t.Fatalf("line_no %s: source_line_no column = %d, want NULL", tc.line, *col)
		}
		if tc.want != 0 && (col == nil || int(*col) != tc.want) {
			t.Fatalf("line_no %s: source_line_no column = %v, want %d", tc.line, col, tc.want)
		}
	}
}
