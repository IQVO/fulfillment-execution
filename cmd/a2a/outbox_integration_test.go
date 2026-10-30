//go:build integration

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inbounda2a "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/a2a"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/system"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TestA2ACompleteTask_LeavesUnpublishedOutboxRowsForBothTopics proves ADR-0059
// end to end: the delegated write goes through the SAME use case and
// transactional outbox as REST/MCP. It boots the REAL cmd/a2a wiring
// (buildStorage + buildPublisher + buildDeps with a real Postgres from
// testcontainers and an unreachable broker), calls complete_task through the
// SDK client over the real router, and asserts outbox_events holds the
// TaskCompleted row for BOTH the integration and the analytics topic,
// committed with the task row and still UNPUBLISHED (the relay stays in
// cmd/execution), while a rejected completion stores nothing.
func TestA2ACompleteTask_LeavesUnpublishedOutboxRowsForBothTopics(t *testing.T) {
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

	logger := newLogger("error")

	st, closeStorage, err := buildStorage(ctx, url, url, "../../migrations", logger)
	if err != nil {
		t.Fatalf("buildStorage: %v", err)
	}
	t.Cleanup(closeStorage)
	if st.pool == nil || st.uow == nil {
		t.Fatal("a database URL must yield a pool and a UnitOfWork")
	}

	// Unreachable broker on purpose: cmd/a2a must only INSERT outbox rows
	// (relay lives in cmd/execution), so it must never touch Kafka.
	publisher, closePublisher, err := buildPublisher(st, []string{"127.0.0.1:1"}, logger)
	if err != nil {
		t.Fatalf("buildPublisher: %v", err)
	}
	t.Cleanup(closePublisher)

	// Seed a claimed PICK task directly through the repos.
	if err := st.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick"))); err != nil {
		t.Fatalf("save station: %v", err)
	}
	clock := system.Clock{}
	create := &usecases.CreateTask{
		Tasks: st.tasks, Publisher: publisher, Clock: clock, UnitOfWork: st.uow,
		NewId: func() shared.TaskId { return "a2a-t1" },
	}
	created, err := create.Execute(ctx, task.Pick, shared.NewCPT(time.Now().Add(time.Hour)), "wu-a2a", shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	claim := &usecases.ClaimNext{Tasks: st.tasks, Stations: st.stations, Publisher: publisher, Clock: clock, UnitOfWork: st.uow}
	if _, err := claim.Execute(ctx, "s1", task.Pick); err != nil {
		t.Fatalf("claim: %v", err)
	}

	srv := httptest.NewServer(testRouterFor(st, publisher))
	t.Cleanup(srv.Close)

	client, err := a2aclient.NewFromCard(ctx, inbounda2a.Card(inbounda2a.Config{AgentURL: srv.URL + "/a2a", Version: "test"}))
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	call := func(args map[string]any) *a2a.Message {
		t.Helper()
		msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"skill": "complete_task", "arguments": args}))
		resp, err := client.SendMessage(ctx, &a2a.SendMessageRequest{Message: msg})
		if err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		reply, ok := resp.(*a2a.Message)
		if !ok || len(reply.Parts) != 2 {
			t.Fatalf("reply = %T", resp)
		}
		return reply
	}

	// A rejected completion (wrong station) stores nothing.
	rejected := call(map[string]any{"taskId": string(created.Id()), "stationId": "intruder"})
	if text, _ := rejected.Parts[1].Content.(a2a.Text); !strings.Contains(string(text), "(409)") {
		t.Fatalf("wrong-station completion = %q, want a 409 problem", text)
	}
	assertOutboxRows(t, st, 0)

	accepted := call(map[string]any{"taskId": string(created.Id()), "stationId": "s1"})
	if text, _ := accepted.Parts[1].Content.(a2a.Text); !strings.HasPrefix(string(text), "completed ") {
		t.Fatalf("completion = %q, want a completed answer", text)
	}

	found, err := st.tasks.FindById(ctx, created.Id())
	if err != nil || found == nil || found.Status() != task.Completed {
		t.Fatalf("task not persisted as Completed: %v err=%v", found, err)
	}

	assertOutboxRows(t, st, 1)
}

func testRouterFor(st storage, publisher ports.EventPublisher) http.Handler {
	deps := buildDeps(st, publisher, nil)
	card := inbounda2a.Card(inbounda2a.Config{AgentURL: "http://unused.invalid/", Version: "test"})
	return newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)), card, "fulfillment-execution-a2a")
}

// assertOutboxRows asserts exactly want UNPUBLISHED TaskCompleted rows per
// topic (integration and analytics).
func assertOutboxRows(t *testing.T, st storage, want int) {
	t.Helper()
	const eventType = "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"
	for _, topic := range []string{outboundkafka.Topic, outboundkafka.AnalyticsTopic} {
		var n int
		if err := st.pool.QueryRow(context.Background(),
			"SELECT count(*) FROM outbox_events WHERE topic = $1 AND event_type = $2 AND published_at IS NULL",
			topic, eventType).Scan(&n); err != nil {
			t.Fatalf("count outbox rows for %s: %v", topic, err)
		}
		if n != want {
			t.Errorf("topic %s: %d unpublished TaskCompleted outbox rows, want %d", topic, n, want)
		}
	}
}
