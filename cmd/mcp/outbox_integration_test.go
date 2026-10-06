//go:build integration

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundmcp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/mcp"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TestMCPCompleteTask_WritesOutboxRowsForBothTopics is the ADR-0008/0020
// regression test for the bug where cmd/mcp built CompleteTask with a log
// publisher and no UnitOfWork: complete_task over MCP completed the task but
// never emitted TaskCompleted. It boots the REAL cmd/mcp wiring (buildStorage
// + buildPublisher + buildDeps with EVENT_PUBLISHER=kafka and a real
// Postgres from testcontainers), calls complete_task through a real MCP
// client, and asserts outbox_events holds the TaskCompleted row for BOTH the
// integration and the analytics topic, committed with the task row.
func TestMCPCompleteTask_WritesOutboxRowsForBothTopics(t *testing.T) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("fulfillment_execution"),
		tcpostgres.WithUsername("fulfillment"),
		tcpostgres.WithPassword("fulfillment"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	t.Setenv("EVENT_PUBLISHER", "kafka")
	logger := newLogger("error")

	st, closeStorage, err := buildStorage(ctx, url, url, "../../migrations", logger)
	if err != nil {
		t.Fatalf("buildStorage: %v", err)
	}
	t.Cleanup(closeStorage)
	if st.pool == nil || st.uow == nil {
		t.Fatal("a database URL must yield a pool and a UnitOfWork")
	}

	// Unreachable broker on purpose: cmd/mcp must only INSERT outbox rows
	// (relay lives in cmd/execution), so it must never touch Kafka.
	publisher, closePublisher := buildPublisher(st, []string{"127.0.0.1:1"}, logger)
	t.Cleanup(closePublisher)

	// Seed a claimed PICK task directly through the repos.
	if err := st.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick"))); err != nil {
		t.Fatalf("save station: %v", err)
	}
	clock := memory.SystemClock{}
	create := &usecases.CreateTask{
		Tasks: st.tasks, Publisher: publisher, Clock: clock, UnitOfWork: st.uow,
		NewId: func() shared.TaskId { return "mcp-t1" },
	}
	created, err := create.Execute(ctx, task.Pick, shared.NewCPT(time.Now().Add(time.Hour)), "wu-mcp", shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	claim := &usecases.ClaimNext{Tasks: st.tasks, Stations: st.stations, Publisher: publisher, Clock: clock, UnitOfWork: st.uow}
	if _, err := claim.Execute(ctx, "s1", task.Pick); err != nil {
		t.Fatalf("claim: %v", err)
	}

	srv := httptest.NewServer(newRouter(inboundmcp.Handler(inboundmcp.NewServer(buildDeps(st, publisher, nil))), "fulfillment-execution-mcp"))
	t.Cleanup(srv.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: srv.URL + "/mcp", HTTPClient: &http.Client{}}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	res, err := session.CallTool(ctx, &sdk.CallToolParams{
		Name:      "complete_task",
		Arguments: map[string]any{"taskId": string(created.Id()), "stationId": "s1"},
	})
	if err != nil {
		t.Fatalf("call complete_task: %v", err)
	}
	if res.IsError {
		t.Fatalf("complete_task returned error: %+v", res.Content)
	}

	found, err := st.tasks.FindById(ctx, created.Id())
	if err != nil || found == nil || found.Status() != task.Completed {
		t.Fatalf("task not persisted as Completed: %v err=%v", found, err)
	}

	const eventType = "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"
	for _, topic := range []string{outboundkafka.Topic, outboundkafka.AnalyticsTopic} {
		var n int
		if err := st.pool.QueryRow(ctx,
			"SELECT count(*) FROM outbox_events WHERE topic = $1 AND event_type = $2 AND published_at IS NULL",
			topic, eventType).Scan(&n); err != nil {
			t.Fatalf("count outbox rows for %s: %v", topic, err)
		}
		if n != 1 {
			t.Errorf("topic %s: %d unpublished TaskCompleted outbox rows, want exactly 1", topic, n)
		}
	}
}
