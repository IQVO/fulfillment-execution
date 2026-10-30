// Package a2a is the outbound Agent2Agent adapter: it implements the
// WorkPlanning delegation port against the wes-work-planning A2A agent
// (ADR-0058), using the official a2a-go/v2 SDK. Card discovery is real — the
// client fetches the agent's card from the well-known path and builds the
// protocol client from it — so the wire contract is the one the peer
// advertises, not an assumption.
//
// This package depends inward on the application layer's port only; it never
// touches the domain. It speaks JSON-RPC 2.0 over HTTP (the transport the
// wes-work-planning card declares) and is safe for concurrent use.
package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// wellKnownPath is the Agent Card discovery path (A2A spec); served by every
// agent in this fleet behind its Kong prefix as well as in-cluster.
const wellKnownPath = "/.well-known/agent-card.json"

// releaseSkill is the skill id the wes-work-planning agent advertises for
// admitting the next work unit (its ADR-0046 card).
const releaseSkill = "release_next_work"

// Client is the WorkPlanning port over A2A. The protocol client is resolved
// lazily from the peer's Agent Card on first use and cached: boot must not
// depend on the planning agent being up (a cold cluster starts both at
// once), but a card that cannot be resolved surfaces as
// ports.ErrWorkPlanningUnreachable on the requests that need it.
type Client struct {
	baseURL string
	http    *http.Client

	mu     sync.Mutex
	client *a2aclient.Client
}

// NewClient builds the delegation client for the planning agent reachable at
// baseURL (the agent's root, e.g. http://wes-work-planning-a2a.<ns>:8093/ or
// its Kong path). baseURL's trailing slash is normalized.
func NewClient(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: httpClient}
}

// agentEnvelope mirrors the wes-work-planning skill-answer envelope (its
// ADR-0046 wire contract): exactly one of data or problem. A test-local copy
// by design — if the peer's contract drifts, this adapter fails loudly
// instead of silently misreading.
type agentEnvelope struct {
	Skill string `json:"skill"`
	Data  struct {
		ReleasedWork *struct {
			WorkUnitId string `json:"workUnitId"`
			CPT        string `json:"cpt"`
			Ref        string `json:"ref"`
		} `json:"releasedWork"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	} `json:"problem"`
}

// ReleaseNextWork delegates "admit the next unit for path" to the planning
// agent and maps its answer onto the port's typed outcomes.
func (c *Client) ReleaseNextWork(ctx context.Context, pathId string) (ports.ReleasedWork, error) {
	client, err := c.resolve(ctx)
	if err != nil {
		return ports.ReleasedWork{}, err
	}

	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{
		"skill":     releaseSkill,
		"arguments": map[string]any{"pathId": pathId},
	}))
	resp, err := client.SendMessage(ctx, &a2a.SendMessageRequest{Message: msg})
	if err != nil {
		return ports.ReleasedWork{}, fmt.Errorf("%w: SendMessage: %v", ports.ErrWorkPlanningUnreachable, err)
	}
	agentMsg, ok := resp.(*a2a.Message)
	if !ok {
		return ports.ReleasedWork{}, fmt.Errorf("%w: agent answered %T, want a message", ports.ErrWorkPlanningUnreachable, resp)
	}
	if len(agentMsg.Parts) == 0 {
		return ports.ReleasedWork{}, fmt.Errorf("%w: agent answer carries no parts", ports.ErrWorkPlanningUnreachable)
	}
	data, ok := agentMsg.Parts[0].Content.(a2a.Data)
	if !ok {
		return ports.ReleasedWork{}, fmt.Errorf("%w: agent answer part is %T, want data", ports.ErrWorkPlanningUnreachable, agentMsg.Parts[0].Content)
	}
	raw, err := json.Marshal(data.Value)
	if err != nil {
		return ports.ReleasedWork{}, fmt.Errorf("%w: decoding answer: %v", ports.ErrWorkPlanningUnreachable, err)
	}
	var env agentEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return ports.ReleasedWork{}, fmt.Errorf("%w: answer is not the skill envelope: %v", ports.ErrWorkPlanningUnreachable, err)
	}
	if env.Problem != nil {
		return ports.ReleasedWork{}, mapProblem(env.Problem)
	}
	if env.Data.ReleasedWork == nil {
		return ports.ReleasedWork{}, fmt.Errorf("%w: answer carries no releasedWork and no problem", ports.ErrWorkPlanningUnreachable)
	}
	rw := env.Data.ReleasedWork
	return ports.ReleasedWork{WorkUnitId: rw.WorkUnitId, CPT: rw.CPT, Reference: rw.Ref}, nil
}

// mapProblem maps the peer's RFC 7807-shaped in-band problem onto this
// context's typed delegation outcomes, preserving its detail text.
func mapProblem(p *struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}) error {
	switch p.Status {
	case 404:
		return fmt.Errorf("%w: %s", ports.ErrUnknownWorkPlanningPath, p.Detail)
	case 409:
		return fmt.Errorf("%w: %s", ports.ErrNoPendingWork, p.Detail)
	default:
		return fmt.Errorf("%w: agent problem %d %s: %s", ports.ErrWorkPlanningUnreachable, p.Status, p.Title, p.Detail)
	}
}

// resolve fetches the peer's Agent Card (once, cached) and builds the
// protocol client from it.
func (c *Client) resolve(ctx context.Context) (*a2aclient.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return c.client, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+wellKnownPath, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: building card request: %v", ports.ErrWorkPlanningUnreachable, err)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: fetching agent card: %v", ports.ErrWorkPlanningUnreachable, err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: agent card fetch answered %d", ports.ErrWorkPlanningUnreachable, res.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("%w: reading agent card: %v", ports.ErrWorkPlanningUnreachable, err)
	}
	var card a2a.AgentCard
	if err := json.Unmarshal(body, &card); err != nil {
		return nil, fmt.Errorf("%w: agent card is not valid JSON: %v", ports.ErrWorkPlanningUnreachable, err)
	}
	client, err := a2aclient.NewFromCard(ctx, &card)
	if err != nil {
		return nil, fmt.Errorf("%w: building client from card: %v", ports.ErrWorkPlanningUnreachable, err)
	}
	c.client = client
	return client, nil
}

// compile-time proof the adapter satisfies the port.
var _ ports.WorkPlanning = (*Client)(nil)
