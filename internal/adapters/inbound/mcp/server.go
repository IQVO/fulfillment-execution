package mcp

import (
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewServer builds the MCP server for this bounded context with every read
// tool, the complete_task write tool, scoped resource, and workflow prompt
// registered.
func NewServer(deps Deps) *mcp.Server {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "fulfillment-execution-mcp", Version: "1.0.0"},
		&mcp.ServerOptions{
			// Gap fix D17: the server is NOT read-only. complete_task changes
			// state (and publishes TaskCompleted), so the instructions say so.
			Instructions: "Access to the fulfillment-execution work backlog. Read tools: queue depth by process path (PICK/PACK/SLAM/RELOCATE/REPLENISH), the most urgent claimable task, lapsing leases and the curated reports. One WRITE tool: complete_task completes a claimed task for the station holding the claim and publishes TaskCompleted; a RELOCATE task also needs the operator's counted quantity and scanned bins, and a REPLENISH task the operator's counted quantity; confirm before calling it. Start with the triage_backlog prompt, which is read-only.",
		},
	)

	deps.registerTools(server)
	deps.registerResources(server)
	deps.registerPrompts(server)

	return server
}

// Handler returns the Streamable HTTP handler for the MCP server. The server
// is mounted unauthenticated: this bounded context's REST and MCP surfaces
// carry no identity layer.
func Handler(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
}
