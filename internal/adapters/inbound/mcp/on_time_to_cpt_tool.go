package mcp

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// --- get_on_time_to_cpt tool ---------------------------------------------------
//
// The fulfillment-execution half of order-management ADR 0014 §6: on-time-
// to-CPT is measured where the evidence is, in this service's throughput
// analytics (PackageManifested's occurred_at vs. the originating SLAM
// task's CPT — see ADR-0026). This tool aggregates the raw
// PackagesManifested/PackagesOnTimeToCPT/PackagesLateToCPT counts the
// get_fulfillment_throughput_report tool already exposes per-hour-bucket
// into a single rate for the requested window, so a caller does not have to
// sum buckets itself to answer "what fraction of packages made their CPT".

// OnTimeToCPTToolInput is the tool's argument set (untrusted, from a model).
// TaskType/StationId are optional filters passed straight through to the
// underlying throughput query; in practice PackagesManifested is only ever
// non-zero on SLAM rows (a package inherits its originating SLAM task's
// dimensions — see ADR-0026), so an empty TaskType effectively means "SLAM
// only" without the caller needing to know that.
type OnTimeToCPTToolInput struct {
	From      string `json:"from" jsonschema:"start of the window, inclusive, RFC3339 (required)"`
	To        string `json:"to" jsonschema:"end of the window, exclusive, RFC3339 (required)"`
	TaskType  string `json:"taskType,omitempty" jsonschema:"optional process-path filter; on-time-to-CPT data only exists on SLAM rows"`
	StationId string `json:"stationId,omitempty" jsonschema:"optional station filter"`
}

// OnTimeToCPTView is the tool's aggregated result: the on-time-to-CPT KPI
// for the requested window/filters, summed across every hour bucket the
// underlying throughput report returns. OnTimeRate is derived here
// (PackagesOnTimeToCPT / PackagesManifested), never stored — consistent
// with the report's own raw-counts-not-precomputed-rates convention — and
// is 0 when PackagesManifested is 0 (no manifested packages in the window,
// not a 0% on-time rate).
type OnTimeToCPTView struct {
	From                string  `json:"from"`
	To                  string  `json:"to"`
	TaskType            string  `json:"taskType,omitempty"`
	StationId           string  `json:"stationId,omitempty"`
	PackagesManifested  int     `json:"packagesManifested"`
	PackagesOnTimeToCPT int     `json:"packagesOnTimeToCPT"`
	PackagesLateToCPT   int     `json:"packagesLateToCPT"`
	OnTimeRate          float64 `json:"onTimeRate"`
}

// getOnTimeToCPT is the tool handler: it validates the required window,
// delegates to the reports REST client via GetThroughput, and aggregates
// the per-bucket counts into a single rate.
func (d Deps) getOnTimeToCPT(ctx context.Context, in OnTimeToCPTToolInput) (OnTimeToCPTView, error) {
	return GetOnTimeToCPTForTest(ctx, d.Reports, in)
}

// GetOnTimeToCPTForTest is the tool's pure logic, factored out so it can be
// unit-tested with a fake ReportsClient independent of the MCP server
// wiring, matching get_fulfillment_throughput_report's own
// GetThroughputReportForTest convention.
func GetOnTimeToCPTForTest(ctx context.Context, client ReportsClient, in OnTimeToCPTToolInput) (OnTimeToCPTView, error) {
	if client == nil {
		return OnTimeToCPTView{}, fmt.Errorf("reports client not configured")
	}
	if in.From == "" || in.To == "" {
		return OnTimeToCPTView{}, fmt.Errorf("from and to are required (RFC3339)")
	}
	rep, err := client.GetThroughput(ctx, ThroughputQuery{
		From:      in.From,
		To:        in.To,
		TaskType:  in.TaskType,
		StationId: in.StationId,
	})
	if err != nil {
		return OnTimeToCPTView{}, err
	}

	out := OnTimeToCPTView{From: in.From, To: in.To, TaskType: in.TaskType, StationId: in.StationId}
	for _, row := range rep.Rows {
		out.PackagesManifested += row.PackagesManifested
		out.PackagesOnTimeToCPT += row.PackagesOnTimeToCPT
		out.PackagesLateToCPT += row.PackagesLateToCPT
	}
	if out.PackagesManifested > 0 {
		out.OnTimeRate = float64(out.PackagesOnTimeToCPT) / float64(out.PackagesManifested)
	}
	return out, nil
}

// registerOnTimeToCPTTool adds the curated read-only on-time-to-CPT tool. It
// is registered only when a reports client is configured (Deps.Reports !=
// nil), matching registerReportTool's own convention.
func (d Deps) registerOnTimeToCPTTool(server *mcp.Server) {
	if d.Reports == nil {
		return
	}
	readOnly := true
	addTool(server, &mcp.Tool{
		Name: "get_on_time_to_cpt",
		Description: "Return the on-time-to-CPT rate for a time window, optionally filtered by process path and station: " +
			"the fraction of manifested packages (SLAM pass) whose manifest time was at or before the originating SLAM " +
			"task's Critical Pull Time, alongside the raw manifested/on-time/late counts. Companion KPI to " +
			"order-management ADR 0014 §6 (promise-basis distribution, re-promise rate, split-shipment rate, and " +
			"promise-to-cutoff gap live there instead). Reads via the fulfillment-reports REST service.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.getOnTimeToCPT)
}
