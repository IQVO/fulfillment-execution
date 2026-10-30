package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	inbounda2a "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/a2a"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

const testAgentURL = "http://fulfillment-execution-a2a.warehouse-systems.svc.cluster.local:8093/"

// testRouter builds the real router over the real A2A handler and card
// (in-memory adapters), exactly as run() wires it.
func testRouter(t *testing.T) http.Handler {
	t.Helper()
	tasks := memory.NewTaskRepo()
	deps := inbounda2a.Deps{
		GetQueueDepth: &usecases.GetQueueDepth{Tasks: tasks},
		Tasks:         tasks,
	}
	card := inbounda2a.Card(inbounda2a.Config{AgentURL: testAgentURL, Version: "test"})
	return newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)), card, "fulfillment-execution-a2a")
}

func TestHealthzNeedsNoCredentials(t *testing.T) {
	rec := httptest.NewRecorder()
	testRouter(t).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz: got %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != `{"status":"ok"}` {
		t.Fatalf("body = %q, want {\"status\":\"ok\"}", body)
	}
}

func TestAgentCardIsServedAtTheWellKnownPath(t *testing.T) {
	rec := httptest.NewRecorder()
	testRouter(t).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/agent-card.json", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET agent card: got %d, want 200", rec.Code)
	}
	var card a2a.AgentCard
	if err := json.Unmarshal(rec.Body.Bytes(), &card); err != nil {
		t.Fatalf("card is not JSON: %v", err)
	}
	if card.Name != "fulfillment-execution" || len(card.Skills) != 2 {
		t.Fatalf("card = %q with %d skills", card.Name, len(card.Skills))
	}
	if len(card.SupportedInterfaces) != 1 || card.SupportedInterfaces[0].URL != testAgentURL {
		t.Fatalf("card advertises %+v, want the configured A2A_AGENT_URL %s", card.SupportedInterfaces, testAgentURL)
	}
}

// Both mounts answer a real SendMessage over JSON-RPC, driven by the SDK
// client against a card pointing at the mount.
func TestJSONRPCMountsAnswerSkills(t *testing.T) {
	srv := httptest.NewServer(testRouter(t))
	t.Cleanup(srv.Close)

	for _, path := range []string{"/", "/a2a"} {
		t.Run(path, func(t *testing.T) {
			card := inbounda2a.Card(inbounda2a.Config{AgentURL: srv.URL + path, Version: "test"})
			client, err := a2aclient.NewFromCard(context.Background(), card)
			if err != nil {
				t.Fatalf("NewFromCard: %v", err)
			}
			msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{
				"skill": "backlog_status", "arguments": map[string]any{"processPath": "PICK"},
			}))
			resp, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: msg})
			if err != nil {
				t.Fatalf("SendMessage via %s: %v", path, err)
			}
			reply, ok := resp.(*a2a.Message)
			if !ok || len(reply.Parts) != 2 {
				t.Fatalf("reply via %s = %T", path, resp)
			}
			if text, _ := reply.Parts[1].Content.(a2a.Text); !strings.HasPrefix(string(text), "PICK: 0 pending") {
				t.Fatalf("summary via %s = %q", path, text)
			}
		})
	}
}

// The router carries the same HTTP RED instrumentation as the REST routers
// (ADR-0019): otelchimetric records http.server.request.duration labelled
// with the route pattern.
func TestRouterRecordsRequestDuration(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	rec := httptest.NewRecorder()
	testRouter(t).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz: got %d, want 200", rec.Code)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "http.server.request.duration" {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || len(hist.DataPoints) != 1 {
				t.Fatalf("unexpected histogram shape: %T %+v", m.Data, m.Data)
			}
			route, ok := hist.DataPoints[0].Attributes.Value(attribute.Key("http.route"))
			if !ok || route.AsString() != "/healthz" {
				t.Errorf("http.route = %v, want /healthz", route)
			}
			return
		}
	}
	t.Fatal("http.server.request.duration was not recorded by the A2A router")
}

// buildDeps must give CompleteTask the publisher and the UnitOfWork: a nil
// UnitOfWork is exactly the bug that made complete_task silent over MCP.
func TestBuildDepsWiresPublisherAndUnitOfWork(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://u:***@127.0.0.1:1/db?sslmode=disable") // lazy: never dials
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	st := storage{tasks: memory.NewTaskRepo(), stations: memory.NewStationRepo(), pool: pool, uow: postgres.NewUnitOfWork(pool)}
	pub, closeFn, err := buildPublisher(st, []string{"127.0.0.1:1"}, newLogger("error"))
	if err != nil {
		t.Fatalf("buildPublisher: %v", err)
	}
	defer closeFn()

	deps := buildDeps(st, pub, nil)
	if deps.CompleteTask == nil || deps.CompleteTask.Publisher != pub {
		t.Fatal("CompleteTask must publish through the shared publisher")
	}
	if deps.CompleteTask.UnitOfWork != st.uow {
		t.Fatal("CompleteTask must run Save + Publish in the shared UnitOfWork (one transaction with the outbox row)")
	}
	if deps.GetQueueDepth == nil || deps.Tasks == nil || deps.Now == nil {
		t.Fatal("read-side deps must be wired")
	}
}

func TestMetricsPortNilSafe(t *testing.T) {
	if metricsPort(nil) != nil {
		t.Fatal("a nil *Metrics must become a nil interface, not a typed nil")
	}
}

func TestUnknownPathIsNotFound(t *testing.T) {
	rec := httptest.NewRecorder()
	testRouter(t).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /nope: got %d, want 404", rec.Code)
	}
}
