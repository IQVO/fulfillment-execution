package a2a

import (
	"context"
	"errors"
	"iter"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// stubAgent serves the wes-work-planning wire contract (ADR-0046 envelope)
// on an httptest server, using the REAL SDK server side: a self-pointing
// card at the well-known path, SendMessage over JSON-RPC. Each test scripts
// the envelope the agent answers with.
func stubAgent(t *testing.T, envelope any) *httptest.Server {
	t.Helper()
	listener, err := netListen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	baseURL := "http://" + listener.Addr().String()

	executor := a2asrv.AgentExecutorFunc(func(_ context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(a2a.NewMessageForTask(a2a.MessageRoleAgent, ec, a2a.NewDataPart(envelope)), nil)
		}
	})
	handler := a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
	card := &a2a.AgentCard{
		Name:        "wes-work-planning-stub",
		Description: "test double",
		Version:     "test",
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(baseURL+"/", a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"application/json"},
		DefaultOutputModes: []string{"application/json"},
		Skills: []a2a.AgentSkill{{
			ID: "release_next_work", Name: "Release the next work unit",
			Description: "test double skill",
		}},
	}

	mux := http.NewServeMux()
	mux.Handle("/", handler)
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))

	hs := &httptest.Server{Listener: listener, Config: &http.Server{Handler: mux}}
	hs.Start()
	t.Cleanup(hs.Close)
	return hs
}

func TestReleaseNextWork_Success(t *testing.T) {
	hs := stubAgent(t, map[string]any{
		"skill": "release_next_work",
		"data": map[string]any{
			"releasedWork": map[string]any{
				"workUnitId": "wu-482910",
				"cpt":        "2026-08-21T12:00:00Z",
				"ref":        "order-77213-line-1",
			},
		},
	})
	client := NewClient(hs.URL+"/", nil)

	released, err := client.ReleaseNextWork(context.Background(), "pick-zone-a")
	if err != nil {
		t.Fatalf("ReleaseNextWork: %v", err)
	}
	if released.WorkUnitId != "wu-482910" || released.Reference != "order-77213-line-1" || released.CPT != "2026-08-21T12:00:00Z" {
		t.Errorf("released = %+v, want the admitted unit's narrow DTO", released)
	}
}

func TestReleaseNextWork_ProblemMapping(t *testing.T) {
	cases := map[string]struct {
		status int
		want   error
	}{
		"business rule (empty pool / WIP limit)": {409, ports.ErrNoPendingWork},
		"unknown path":                           {404, ports.ErrUnknownWorkPlanningPath},
		"agent internal problem":                 {500, ports.ErrWorkPlanningUnreachable},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			hs := stubAgent(t, map[string]any{
				"skill": "release_next_work",
				"problem": map[string]any{
					"type":   "https://peer/problems/x",
					"title":  "Peer rejection",
					"detail": "the peer's reason",
					"status": tc.status,
				},
			})
			client := NewClient(hs.URL, nil)

			_, err := client.ReleaseNextWork(context.Background(), "p")
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v (with the peer detail preserved)", err, tc.want)
			}
		})
	}
}

func TestReleaseNextWork_UnreachableIsTyped(t *testing.T) {
	// Nothing listens here: the card fetch must fail with the typed
	// unreachable error, never a raw transport error.
	client := NewClient("http://127.0.0.1:1", nil)

	_, err := client.ReleaseNextWork(context.Background(), "p")
	if !errors.Is(err, ports.ErrWorkPlanningUnreachable) {
		t.Fatalf("error = %v, want ErrWorkPlanningUnreachable", err)
	}
}

func TestReleaseNextWork_OffContractAnswerIsTyped(t *testing.T) {
	// An agent that answers with neither data nor problem breaks the wire
	// contract: the adapter must fail loudly, not misread.
	hs := stubAgent(t, map[string]any{"skill": "release_next_work"})
	client := NewClient(hs.URL, nil)

	_, err := client.ReleaseNextWork(context.Background(), "p")
	if !errors.Is(err, ports.ErrWorkPlanningUnreachable) {
		t.Fatalf("error = %v, want ErrWorkPlanningUnreachable", err)
	}
}

func TestBaseURLTrailingSlashNormalized(t *testing.T) {
	c := NewClient("http://agent.example.com///", nil)
	if c.baseURL != "http://agent.example.com" {
		t.Errorf("baseURL = %q, want trailing slashes trimmed", c.baseURL)
	}
}

// netListen opens an ephemeral listener so the stub can advertise its own
// URL on the card before the server starts.
func netListen() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }
