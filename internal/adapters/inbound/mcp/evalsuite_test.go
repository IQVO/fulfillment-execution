// E3 — behavioral eval suite: Gherkin scenarios in
// testdata/features/mcp_tools.feature driven through godog, the same
// Cucumber-for-Go engine the repo-root REST acceptance suite uses. The
// suite lives inside the mcp package so the evals ship with the adapter
// they evaluate and run in the existing CI test job (`go test ./...`) with
// zero new infrastructure.
package mcp_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cucumber/godog"

	inboundmcp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/mcp"
)

// currentEvalT carries the running *testing.T into the scenario world;
// godog's ScenarioInitializer API does not hand it to step contexts, and
// the shared harness needs it for t.Cleanup. Safe here because the suite
// runs scenarios sequentially inside one test function.
var currentEvalT *testing.T

// TestMCPEvalSuite runs every Gherkin scenario under testdata/features
// against a freshly wired MCP server + client session.
func TestMCPEvalSuite(t *testing.T) {
	currentEvalT = t
	suite := godog.TestSuite{
		ScenarioInitializer: initializeMCPEvalScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"testdata/features"},
			Strict:   true,
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run MCP eval scenarios")
	}
}

// mcpEvalWorld is the per-scenario state: one harness (server + session +
// seeded repos) per scenario, rebuilt by the Background step.
type mcpEvalWorld struct {
	h *evalHarness
}

func initializeMCPEvalScenario(sc *godog.ScenarioContext) {
	w := &mcpEvalWorld{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		return ctx, nil
	})

	// Given
	sc.Step(`^the MCP server is running with the canonical eval state \(earliest PICK claimed by s1, one PICK and one PACK still pending\)$`, w.serverRunning)
	sc.Step(`^the reports service returns throughput rows$`, w.reportsReturnRows)
	sc.Step(`^the reports service is failing$`, w.reportsFailing)

	// When
	sc.Step(`^I call the tool "([^"]*)" with argument "([^"]*)" = "([^"]*)"$`, w.callWithStringArg)
	sc.Step(`^I call the tool "([^"]*)" with argument "([^"]*)" = (\d+)$`, w.callWithNumberArg)
	sc.Step(`^I call the tool "([^"]*)" with arguments$`, w.callWithTableArgs)
	sc.Step(`^I call the tool "complete_task" with the seeded claimed task id and station "([^"]*)"$`, w.callCompleteWithSeededTask)

	// Then
	sc.Step(`^the tool call succeeds$`, w.callSucceeded)
	sc.Step(`^the tool call does not succeed silently$`, w.callDidNotSucceedSilently)
	sc.Step(`^the tool call reports a problem mentioning "([^"]*)"$`, w.callErroredMentioning)
	sc.Step(`^the structured result field "([^"]*)" is "([^"]*)"$`, w.fieldIsString)
	sc.Step(`^the structured result field "([^"]*)" is (\d+)$`, w.fieldIsNumber)
	sc.Step(`^the structured result field "([^"]*)" is the decimal (\d+\.\d+)$`, w.fieldIsDecimal)
	sc.Step(`^the structured result field "([^"]*)" is true$`, w.fieldIsTrue)
	sc.Step(`^the structured result field "([^"]*)" is null$`, w.fieldIsNull)
	sc.Step(`^the structured result array field "([^"]*)" has (\d+) entries$`, w.arrayHasEntries)
	sc.Step(`^the structured result array field "([^"]*)" has an entry whose "([^"]*)" is "([^"]*)"$`, w.arrayEntryFieldIsString)
	sc.Step(`^the structured result array field "([^"]*)" has an entry whose "([^"]*)" is (\d+)$`, w.arrayEntryFieldIsNumber)
	sc.Step(`^the domain event "([^"]*)" was published$`, w.eventPublished)
}

func (w *mcpEvalWorld) serverRunning() error {
	// The harness binds its own lifecycle to the running *testing.T via
	// newEvalHarness; the world only carries the pointer.
	w.h = newEvalHarness(currentEvalT)
	return nil
}

// reportsReturnRows seeds the fake reports service with per-hour-bucket
// throughput rows (header row first: taskType, packagesManifested,
// packagesOnTimeToCPT, packagesLateToCPT).
func (w *mcpEvalWorld) reportsReturnRows(table *godog.Table) error {
	if w.h == nil {
		return fmt.Errorf("no harness — the Background step must run first")
	}
	rows := []inboundmcp.ThroughputRowView{}
	for i, row := range table.Rows {
		if i == 0 {
			continue // header
		}
		cells := row.Cells
		if len(cells) != 4 {
			return fmt.Errorf("throughput rows table needs exactly four columns, got %d", len(cells))
		}
		manifested, err := strconv.Atoi(cells[1].Value)
		if err != nil {
			return fmt.Errorf("packagesManifested column must be numeric: %w", err)
		}
		onTime, err := strconv.Atoi(cells[2].Value)
		if err != nil {
			return fmt.Errorf("packagesOnTimeToCPT column must be numeric: %w", err)
		}
		late, err := strconv.Atoi(cells[3].Value)
		if err != nil {
			return fmt.Errorf("packagesLateToCPT column must be numeric: %w", err)
		}
		rows = append(rows, inboundmcp.ThroughputRowView{
			TaskType:            cells[0].Value,
			HourBucket:          "2026-06-01T10:00:00Z",
			PackagesManifested:  manifested,
			PackagesOnTimeToCPT: onTime,
			PackagesLateToCPT:   late,
		})
	}
	w.h.reports.report = inboundmcp.ThroughputReportView{Rows: rows}
	return nil
}

func (w *mcpEvalWorld) reportsFailing() error {
	if w.h == nil {
		return fmt.Errorf("no harness — the Background step must run first")
	}
	w.h.reports.err = fmt.Errorf("reports client: unexpected status 500")
	return nil
}

func (w *mcpEvalWorld) callWithStringArg(tool, arg, value string) error {
	return w.h.callTool(context.Background(), tool, map[string]any{arg: value})
}

func (w *mcpEvalWorld) callWithNumberArg(tool, arg string, value int64) error {
	return w.h.callTool(context.Background(), tool, map[string]any{arg: value})
}

func (w *mcpEvalWorld) callWithTableArgs(tool string, table *godog.Table) error {
	args := map[string]any{}
	for _, row := range table.Rows {
		cells := row.Cells
		if len(cells) != 2 {
			return fmt.Errorf("arguments table needs exactly two columns, got %d", len(cells))
		}
		key, raw := cells[0].Value, cells[1].Value
		if n, err := strconv.Atoi(raw); err == nil {
			args[key] = n
			continue
		}
		args[key] = raw
	}
	return w.h.callTool(context.Background(), tool, args)
}

func (w *mcpEvalWorld) callCompleteWithSeededTask(stationId string) error {
	return w.h.callTool(context.Background(), "complete_task", map[string]any{
		"taskId":    w.h.claimedTaskID,
		"stationId": stationId,
	})
}

func (w *mcpEvalWorld) callSucceeded() error {
	if w.h.lastCallErr != nil {
		return fmt.Errorf("tool call failed: %w", w.h.lastCallErr)
	}
	if w.h.lastCallResult == nil || w.h.lastCallResult.IsError {
		return fmt.Errorf("tool call returned an error result: %s", w.h.lastCallContent)
	}
	return nil
}

func (w *mcpEvalWorld) callDidNotSucceedSilently() error {
	if w.h.lastCallErr != nil {
		return nil // protocol-level rejection
	}
	if w.h.lastCallResult != nil && w.h.lastCallResult.IsError {
		return nil // tool-level rejection
	}
	return fmt.Errorf("the call succeeded silently — wrong-typed arguments must not be coerced")
}

func (w *mcpEvalWorld) callErroredMentioning(fragment string) error {
	if w.h.lastCallErr == nil && (w.h.lastCallResult == nil || !w.h.lastCallResult.IsError) {
		return fmt.Errorf("expected a tool error, got success: %s", w.h.lastCallContent)
	}
	if !containsFold(w.h.lastCallContent, fragment) && w.h.lastCallErr != nil && !containsFold(w.h.lastCallErr.Error(), fragment) {
		return fmt.Errorf("expected the tool error to mention %q, got %q / %v", fragment, w.h.lastCallContent, w.h.lastCallErr)
	}
	return nil
}

func (w *mcpEvalWorld) fieldIsString(field, want string) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	if s, ok := got.(string); ok && s == want {
		return nil
	}
	return fmt.Errorf("structured result field %q = %v, want %q", field, got, want)
}

func (w *mcpEvalWorld) fieldIsNumber(field string, want int64) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	switch v := got.(type) {
	case float64:
		if int64(v) == want {
			return nil
		}
	case int:
		if int64(v) == want {
			return nil
		}
	case int64:
		if v == want {
			return nil
		}
	}
	return fmt.Errorf("structured result field %q = %v, want %d", field, got, want)
}

func (w *mcpEvalWorld) fieldIsDecimal(field string, want float64) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	if v, ok := got.(float64); ok && v == want {
		return nil
	}
	return fmt.Errorf("structured result field %q = %v, want %v", field, got, want)
}

func (w *mcpEvalWorld) fieldIsTrue(field string) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	if b, ok := got.(bool); ok && b {
		return nil
	}
	return fmt.Errorf("structured result field %q = %v, want true", field, got)
}

func (w *mcpEvalWorld) fieldIsNull(field string) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	if got != nil {
		return fmt.Errorf("structured result field %q = %v, want null", field, got)
	}
	return nil
}

func (w *mcpEvalWorld) arrayHasEntries(field string, want int64) error {
	arr, err := w.structuredArray(field)
	if err != nil {
		return err
	}
	if int64(len(arr)) != want {
		return fmt.Errorf("structured result array %q has %d entries, want %d", field, len(arr), want)
	}
	return nil
}

func (w *mcpEvalWorld) arrayEntryFieldIsString(field, key, want string) error {
	return w.arrayEntryMatches(field, key, func(v any) bool {
		s, ok := v.(string)
		return ok && s == want
	}, want)
}

func (w *mcpEvalWorld) arrayEntryFieldIsNumber(field, key string, want int64) error {
	return w.arrayEntryMatches(field, key, func(v any) bool {
		f, ok := v.(float64)
		return ok && int64(f) == want
	}, strconv.FormatInt(want, 10))
}

func (w *mcpEvalWorld) arrayEntryMatches(field, key string, match func(any) bool, want any) error {
	arr, err := w.structuredArray(field)
	if err != nil {
		return err
	}
	for _, entry := range arr {
		obj, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if v, present := obj[key]; present && match(v) {
			return nil
		}
	}
	return fmt.Errorf("no entry of structured result array %q has %q = %v", field, key, want)
}

// structuredField resolves a dotted path ("best.orderRef") against the
// structured content of the last tool result.
func (w *mcpEvalWorld) structuredField(field string) (any, error) {
	if w.h.lastCallResult == nil {
		return nil, fmt.Errorf("no tool result recorded")
	}
	current := w.h.lastCallResult.StructuredContent
	for _, part := range strings.Split(field, ".") {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("structured result path %q crosses a non-object at %q: %+v", field, part, current)
		}
		next, present := obj[part]
		if !present {
			return nil, fmt.Errorf("structured result has no field %q: %+v", field, w.h.lastCallResult.StructuredContent)
		}
		current = next
	}
	return current, nil
}

func (w *mcpEvalWorld) structuredArray(field string) ([]any, error) {
	got, err := w.structuredField(field)
	if err != nil {
		return nil, err
	}
	arr, ok := got.([]any)
	if !ok {
		return nil, fmt.Errorf("structured result field %q is not an array: %+v", field, got)
	}
	return arr, nil
}

func (w *mcpEvalWorld) eventPublished(name string) error {
	for _, e := range w.h.publisher.Events() {
		if e.EventName() == name {
			return nil
		}
	}
	return fmt.Errorf("expected domain event %q to be published", name)
}

func containsFold(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
