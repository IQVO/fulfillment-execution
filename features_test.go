// Package main_test holds the BDD acceptance suite: godog (Cucumber for Go)
// executes the Gherkin scenarios under features/ against the real chi router,
// wired to the in-memory adapters and a fixed Clock and served over a real
// httptest server. Every step drives the service through its REST API, so the
// suite is black-box with respect to the domain and application layers.
package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cucumber/godog"

	execmhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// scenarioStart is the fixed instant every scenario's Clock starts at, so
// CPTs and lease expiries are deterministic.
var scenarioStart = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// world is the per-scenario state: a freshly wired service plus whatever the
// previous steps observed through the API.
type world struct {
	server    *httptest.Server
	client    *http.Client
	clock     *memory.FixedClock
	publisher *events.BufferedPublisher

	// last response observed by a step that drives the API
	status int
	body   []byte
	header http.Header

	tasksByOrder  map[string]string
	claimedTaskId string
	claimBody     []byte
	packageId     string
	sealBody      []byte
}

// reset builds a brand-new service (fresh in-memory repos, fresh event
// buffer, Clock back at scenarioStart) so scenarios never share state.
func (w *world) reset() {
	if w.server != nil {
		w.server.Close()
	}

	tasks := memory.NewTaskRepo()
	stations := memory.NewStationRepo()
	packages := memory.NewPackageRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(scenarioStart)

	taskSeq, packageSeq := 0, 0
	newTaskId := func() shared.TaskId {
		taskSeq++
		return shared.TaskId(fmt.Sprintf("task-%d", taskSeq))
	}
	newPackageId := func() shared.PackageId {
		packageSeq++
		return shared.PackageId(fmt.Sprintf("pkg-%d", packageSeq))
	}

	h := &execmhttp.Handlers{
		CreateTask:           &usecases.CreateTask{Tasks: tasks, Publisher: publisher, Clock: clock, NewId: newTaskId},
		ClaimNext:            &usecases.ClaimNext{Tasks: tasks, Stations: stations, Publisher: publisher, Clock: clock},
		RenewLease:           &usecases.RenewLease{Tasks: tasks, Clock: clock},
		CompleteTask:         &usecases.CompleteTask{Tasks: tasks, Publisher: publisher, Clock: clock},
		SealPackage:          &usecases.SealPackage{Tasks: tasks, Packages: packages, Publisher: publisher, Clock: clock, NewId: newPackageId},
		RunSlam:              &usecases.RunSlam{Packages: packages, Publisher: publisher, Clock: clock},
		GetQueueDepth:        &usecases.GetQueueDepth{Tasks: tasks},
		ExpireLeases:         &usecases.ExpireLeases{Tasks: tasks, Publisher: publisher, Clock: clock},
		RegisterStation:      &usecases.RegisterStation{Stations: stations, Publisher: publisher},
		GetTasksByOrderRef:   &usecases.GetTasksByOrderRef{Tasks: tasks},
		CheckInStation:       &usecases.CheckInStation{Stations: stations},
		CheckOutStation:      &usecases.CheckOutStation{Stations: stations},
		GetInstalledCapacity: &usecases.GetInstalledCapacity{Stations: stations},
		SweepCPTMisses:       &usecases.SweepCPTMisses{Tasks: tasks, Publisher: publisher, Clock: clock},
	}

	w.server = httptest.NewServer(execmhttp.NewRouter(h, nil))
	w.client = w.server.Client()
	w.clock = clock
	w.publisher = publisher
	w.status = 0
	w.body = nil
	w.header = nil
	w.tasksByOrder = make(map[string]string)
	w.claimedTaskId = ""
	w.claimBody = nil
	w.packageId = ""
	w.sealBody = nil
}

// call issues a real HTTP request against the running server and returns the
// status, body and headers without touching the world's "last response".
func (w *world) call(method, path string, body any) (int, []byte, http.Header, error) {
	var payload io.Reader = http.NoBody
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("marshal request body: %w", err)
		}
		payload = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, w.server.URL+path, payload)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("read response body: %w", err)
	}
	return resp.StatusCode, respBody, resp.Header, nil
}

// do is call, recording the outcome as the "last response" the Then steps assert on.
func (w *world) do(method, path string, body any) error {
	status, respBody, header, err := w.call(method, path, body)
	if err != nil {
		return err
	}
	w.status, w.body, w.header = status, respBody, header
	return nil
}

func (w *world) decodeLast(v any) error {
	if err := json.Unmarshal(w.body, v); err != nil {
		return fmt.Errorf("decode response body %q: %w", string(w.body), err)
	}
	return nil
}

func splitList(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// --- Given steps -----------------------------------------------------------

func (w *world) aRunningService() error {
	if w.server == nil {
		return fmt.Errorf("no service running")
	}
	return w.do(http.MethodGet, "/healthz", nil)
}

func (w *world) aStationIsRegistered(stationId, capabilities string) error {
	if err := w.do(http.MethodPost, "/stations", map[string]any{
		"stationId":    stationId,
		"capabilities": splitList(capabilities),
	}); err != nil {
		return err
	}
	if w.status != http.StatusCreated {
		return fmt.Errorf("registering station %q: expected 201, got %d: %s", stationId, w.status, w.body)
	}
	return nil
}

func (w *world) aTaskForOrder(taskType, orderRef string, cptMinutes int, capabilities string) error {
	if err := w.do(http.MethodPost, "/tasks", map[string]any{
		"type":                 taskType,
		"cpt":                  w.clock.Now().Add(time.Duration(cptMinutes) * time.Minute),
		"orderRef":             orderRef,
		"requiredCapabilities": splitList(capabilities),
	}); err != nil {
		return err
	}
	if w.status != http.StatusCreated {
		return fmt.Errorf("creating %s task for %q: expected 201, got %d: %s", taskType, orderRef, w.status, w.body)
	}

	var created struct {
		Id string `json:"id"`
	}
	if err := w.decodeLast(&created); err != nil {
		return err
	}
	w.tasksByOrder[orderRef] = created.Id
	return nil
}

// --- When steps ------------------------------------------------------------

func (w *world) stationCallsClaimNext(stationId, taskType string) error {
	if err := w.do(http.MethodPost, "/stations/"+stationId+"/claim-next", map[string]any{
		"taskType": taskType,
	}); err != nil {
		return err
	}
	if w.status == http.StatusOK {
		var claimed struct {
			Id string `json:"id"`
		}
		if err := w.decodeLast(&claimed); err != nil {
			return err
		}
		w.claimedTaskId = claimed.Id
		w.claimBody = w.body
	}
	return nil
}

func (w *world) stationHasClaimedTheNextTask(stationId, taskType string) error {
	if err := w.stationCallsClaimNext(stationId, taskType); err != nil {
		return err
	}
	if w.status != http.StatusOK {
		return fmt.Errorf("station %q claiming next %s task: expected 200, got %d: %s", stationId, taskType, w.status, w.body)
	}
	return nil
}

func (w *world) theClockAdvancesBy(minutes int) error {
	w.clock.Advance(time.Duration(minutes) * time.Minute)
	return nil
}

func (w *world) stationRenewsTheLease(stationId string) error {
	return w.do(http.MethodPost, "/tasks/"+w.claimedTaskId+"/renew-lease", map[string]any{
		"stationId": stationId,
	})
}

func (w *world) theLeaseExpirySweepRuns() error {
	return w.do(http.MethodPost, "/tasks/expire-leases", nil)
}

func (w *world) theCPTMissedSweepRuns() error {
	return w.do(http.MethodPost, "/tasks/sweep-cpt-misses", nil)
}

func (w *world) stationCompletesTheClaimedTask(stationId string) error {
	return w.do(http.MethodPost, "/tasks/"+w.claimedTaskId+"/complete", map[string]any{
		"stationId": stationId,
	})
}

func (w *world) stationSealsAPackage(stationId, contents string) error {
	if err := w.do(http.MethodPost, "/tasks/"+w.claimedTaskId+"/seal-package", map[string]any{
		"stationId": stationId,
		"contents":  splitList(contents),
	}); err != nil {
		return err
	}
	if w.status == http.StatusCreated {
		var sealed struct {
			Id string `json:"id"`
		}
		if err := w.decodeLast(&sealed); err != nil {
			return err
		}
		w.packageId = sealed.Id
		w.sealBody = w.body
	}
	return nil
}

func (w *world) theSlamWeighCheckRuns(actual, expected float64) error {
	return w.do(http.MethodPost, "/packages/"+w.packageId+"/slam", map[string]any{
		"actualWeight":   actual,
		"expectedWeight": expected,
	})
}

func (w *world) theTasksForOrderAreLookedUp(orderRef string) error {
	return w.do(http.MethodGet, "/tasks?orderRef="+url.QueryEscape(orderRef), nil)
}

func (w *world) tasksAreLookedUpWithoutAnOrderRef() error {
	return w.do(http.MethodGet, "/tasks", nil)
}

func (w *world) workerChecksInAtStation(occupantId, stationId string) error {
	return w.do(http.MethodPost, "/stations/"+stationId+"/check-in", map[string]any{
		"occupantId": occupantId,
	})
}

func (w *world) workerHasCheckedInAtStation(occupantId, stationId string) error {
	if err := w.workerChecksInAtStation(occupantId, stationId); err != nil {
		return err
	}
	if w.status != http.StatusOK {
		return fmt.Errorf("worker %q checking in at station %q: expected 200, got %d: %s", occupantId, stationId, w.status, w.body)
	}
	return nil
}

func (w *world) stationChecksOut(stationId string) error {
	return w.do(http.MethodPost, "/stations/"+stationId+"/check-out", nil)
}

func (w *world) stationTriesToRenewTheLeaseOnTheTaskForOrder(stationId, orderRef string) error {
	taskId, ok := w.tasksByOrder[orderRef]
	if !ok {
		return fmt.Errorf("no task recorded for order %q", orderRef)
	}
	return w.do(http.MethodPost, "/tasks/"+taskId+"/renew-lease", map[string]any{
		"stationId": stationId,
	})
}

// --- Then steps ------------------------------------------------------------

func (w *world) theResponseStatusIs(expected int) error {
	if w.status != expected {
		return fmt.Errorf("expected status %d, got %d: %s", expected, w.status, w.body)
	}
	return nil
}

func (w *world) theClaimedTaskIsForOrder(orderRef string) error {
	var claimed struct {
		Id       string `json:"id"`
		OrderRef string `json:"orderRef"`
	}
	if err := json.Unmarshal(w.claimBody, &claimed); err != nil {
		return fmt.Errorf("decode claimed task %q: %w", string(w.claimBody), err)
	}
	if claimed.OrderRef != orderRef {
		return fmt.Errorf("expected the claimed task to be for order %q, got %q", orderRef, claimed.OrderRef)
	}
	if want := w.tasksByOrder[orderRef]; claimed.Id != want {
		return fmt.Errorf("expected claimed task id %q for order %q, got %q", want, orderRef, claimed.Id)
	}
	return nil
}

func (w *world) theClaimedTaskIsLeasedTo(stationId string) error {
	var claimed struct {
		Status         string  `json:"status"`
		LeaseStationId *string `json:"leaseStationId"`
		LeaseExpiry    *string `json:"leaseExpiry"`
	}
	if err := json.Unmarshal(w.claimBody, &claimed); err != nil {
		return fmt.Errorf("decode claimed task %q: %w", string(w.claimBody), err)
	}
	if claimed.Status != "CLAIMED" {
		return fmt.Errorf("expected task status CLAIMED, got %q", claimed.Status)
	}
	if claimed.LeaseStationId == nil || *claimed.LeaseStationId != stationId {
		return fmt.Errorf("expected the lease to be held by station %q, got %v", stationId, claimed.LeaseStationId)
	}
	if claimed.LeaseExpiry == nil {
		return fmt.Errorf("expected the lease to carry an expiry")
	}
	return nil
}

func (w *world) theResponseIsAProblemOfType(slug string) error {
	if ct := w.header.Get("Content-Type"); ct != "application/problem+json" {
		return fmt.Errorf("expected Content-Type application/problem+json, got %q", ct)
	}
	var body struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}
	if err := w.decodeLast(&body); err != nil {
		return err
	}
	if want := "https://errors.fulfillment-execution.warehouse-systems.dev/" + slug; body.Type != want {
		return fmt.Errorf("expected problem type %q, got %q", want, body.Type)
	}
	if body.Status != w.status {
		return fmt.Errorf("expected problem status %d to match the HTTP status %d", body.Status, w.status)
	}
	if body.Title == "" || body.Detail == "" {
		return fmt.Errorf("expected a non-empty problem title and detail, got %+v", body)
	}
	return nil
}

func (w *world) theQueueDepthIs(taskType string, expected int) error {
	status, body, _, err := w.call(http.MethodGet, "/queues/"+taskType+"/depth", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("reading %s queue depth: expected 200, got %d: %s", taskType, status, body)
	}
	var depth struct {
		Depth int `json:"depth"`
	}
	if err := json.Unmarshal(body, &depth); err != nil {
		return fmt.Errorf("decode queue depth %q: %w", string(body), err)
	}
	if depth.Depth != expected {
		return fmt.Errorf("expected %s queue depth %d, got %d", taskType, expected, depth.Depth)
	}
	return nil
}

func (w *world) leasesWereFreed(expected int) error {
	var freed struct {
		Freed int `json:"freed"`
	}
	if err := w.decodeLast(&freed); err != nil {
		return err
	}
	if freed.Freed != expected {
		return fmt.Errorf("expected %d freed lease(s), got %d", expected, freed.Freed)
	}
	return nil
}

func (w *world) tasksWereReportedAsCPTMissed(expected int) error {
	var reported struct {
		Reported int `json:"reported"`
	}
	if err := w.decodeLast(&reported); err != nil {
		return err
	}
	if reported.Reported != expected {
		return fmt.Errorf("expected %d task(s) reported as CPT-missed, got %d", expected, reported.Reported)
	}
	return nil
}

func (w *world) theSealedPackageHasStatus(expected string) error {
	var sealed struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(w.sealBody, &sealed); err != nil {
		return fmt.Errorf("decode sealed package %q: %w", string(w.sealBody), err)
	}
	if sealed.Status != expected {
		return fmt.Errorf("expected package status %q, got %q", expected, sealed.Status)
	}
	return nil
}

func (w *world) theSealedPackageHoldsContents(contents string) error {
	var sealed struct {
		ScannedContents []string `json:"scannedContents"`
	}
	if err := json.Unmarshal(w.sealBody, &sealed); err != nil {
		return fmt.Errorf("decode sealed package %q: %w", string(w.sealBody), err)
	}
	want := splitList(contents)
	if strings.Join(sealed.ScannedContents, ",") != strings.Join(want, ",") {
		return fmt.Errorf("expected scanned contents %v, got %v", want, sealed.ScannedContents)
	}
	return nil
}

func (w *world) theLookupReturnsTasksForOrder(expected int, orderRef string) error {
	var tasks []struct {
		Id       string `json:"id"`
		OrderRef string `json:"orderRef"`
	}
	if err := w.decodeLast(&tasks); err != nil {
		return err
	}
	if len(tasks) != expected {
		return fmt.Errorf("expected %d task(s) for order %q, got %d: %s", expected, orderRef, len(tasks), w.body)
	}
	seen := make(map[string]bool, len(tasks))
	for _, t := range tasks {
		if t.OrderRef != orderRef {
			return fmt.Errorf("expected task %q to reference order %q, got %q", t.Id, orderRef, t.OrderRef)
		}
		seen[t.Id] = true
	}
	for id := range w.tasksByOrder {
		if id == orderRef && !seen[w.tasksByOrder[id]] {
			return fmt.Errorf("expected the lookup to include task %q for order %q", w.tasksByOrder[id], orderRef)
		}
	}
	return nil
}

func (w *world) theInstalledCapacityForIs(capability string, expected int) error {
	status, body, _, err := w.call(http.MethodGet, "/capacity/"+url.PathEscape(capability), nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("reading %s installed capacity: expected 200, got %d: %s", capability, status, body)
	}
	var capacity struct {
		Capability string `json:"capability"`
		Installed  int    `json:"installed"`
	}
	if err := json.Unmarshal(body, &capacity); err != nil {
		return fmt.Errorf("decode installed capacity %q: %w", string(body), err)
	}
	if capacity.Capability != capability {
		return fmt.Errorf("expected capability %q in the response, got %q", capability, capacity.Capability)
	}
	if capacity.Installed != expected {
		return fmt.Errorf("expected %s installed capacity %d, got %d", capability, expected, capacity.Installed)
	}
	return nil
}

func (w *world) theStationResponseShowsOccupied() error {
	var resp struct {
		Occupied bool `json:"occupied"`
	}
	if err := w.decodeLast(&resp); err != nil {
		return err
	}
	if !resp.Occupied {
		return fmt.Errorf("expected the station to be occupied: %s", w.body)
	}
	return nil
}

func (w *world) theStationResponseShowsUnoccupied() error {
	var resp struct {
		Occupied bool `json:"occupied"`
	}
	if err := w.decodeLast(&resp); err != nil {
		return err
	}
	if resp.Occupied {
		return fmt.Errorf("expected the station to be unoccupied: %s", w.body)
	}
	return nil
}

func (w *world) noDomainEventsAreRecorded() error {
	if recorded := w.publisher.Events(); len(recorded) != 0 {
		return fmt.Errorf("expected no domain events, recorded: %v", w.recordedEventNames())
	}
	return nil
}

func (w *world) exactlyNDomainEventsAreRecorded(expected int, name string) error {
	if n := w.countEvents(name); n != expected {
		return fmt.Errorf("expected exactly %d %s domain event(s), got %d", expected, name, n)
	}
	return nil
}

func (w *world) countEvents(name string) int {
	count := 0
	for _, e := range w.publisher.Events() {
		if e.EventName() == name {
			count++
		}
	}
	return count
}

func (w *world) aDomainEventIsRecorded(name string) error {
	if w.countEvents(name) == 0 {
		return fmt.Errorf("expected a %s domain event, recorded: %v", name, w.recordedEventNames())
	}
	return nil
}

func (w *world) noDomainEventIsRecorded(name string) error {
	if n := w.countEvents(name); n != 0 {
		return fmt.Errorf("expected no %s domain event, got %d", name, n)
	}
	return nil
}

func (w *world) recordedEventNames() []string {
	published := w.publisher.Events()
	names := make([]string, 0, len(published))
	for _, e := range published {
		names = append(names, e.EventName())
	}
	return names
}

// InitializeScenario registers every step definition and the per-scenario
// reset hook. Each scenario gets its own server, repositories, event buffer
// and Clock, so scenarios are fully independent.
func InitializeScenario(sc *godog.ScenarioContext) {
	w := &world{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w.reset()
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		if w.server != nil {
			w.server.Close()
			w.server = nil
		}
		return ctx, nil
	})

	sc.Step(`^a running Fulfillment Execution service$`, w.aRunningService)
	sc.Step(`^a Station "([^"]*)" is registered with capabilities "([^"]*)"$`, w.aStationIsRegistered)
	sc.Step(`^a "([^"]*)" Task for order "([^"]*)" with a CPT (\d+) minutes from now requiring capabilities "([^"]*)"$`, w.aTaskForOrder)

	sc.Step(`^Station "([^"]*)" calls claimNext for task type "([^"]*)"$`, w.stationCallsClaimNext)
	sc.Step(`^Station "([^"]*)" has claimed the next "([^"]*)" Task$`, w.stationHasClaimedTheNextTask)
	sc.Step(`^the clock advances by (\d+) minutes$`, w.theClockAdvancesBy)
	sc.Step(`^Station "([^"]*)" renews the lease on the claimed Task$`, w.stationRenewsTheLease)
	sc.Step(`^the lease expiry sweep runs$`, w.theLeaseExpirySweepRuns)
	sc.Step(`^the CPT-missed sweep runs$`, w.theCPTMissedSweepRuns)
	sc.Step(`^Station "([^"]*)" completes the claimed Task$`, w.stationCompletesTheClaimedTask)
	sc.Step(`^Station "([^"]*)" seal(?:s|ed) a Package for the claimed Task with scanned contents "([^"]*)"$`, w.stationSealsAPackage)
	sc.Step(`^the SLAM weigh-check runs on the Package with an actual weight of ([0-9.]+) against an expected weight of ([0-9.]+)$`, w.theSlamWeighCheckRuns)
	sc.Step(`^the tasks for order "([^"]*)" are looked up$`, w.theTasksForOrderAreLookedUp)
	sc.Step(`^tasks are looked up without an orderRef$`, w.tasksAreLookedUpWithoutAnOrderRef)
	sc.Step(`^worker "([^"]*)" has checked in at Station "([^"]*)"$`, w.workerHasCheckedInAtStation)
	sc.Step(`^worker "([^"]*)" checks in at Station "([^"]*)"$`, w.workerChecksInAtStation)
	sc.Step(`^Station "([^"]*)" checks out$`, w.stationChecksOut)
	sc.Step(`^Station "([^"]*)" tries to renew the lease on the Task for order "([^"]*)"$`, w.stationTriesToRenewTheLeaseOnTheTaskForOrder)

	sc.Step(`^the response status is (\d+)$`, w.theResponseStatusIs)
	sc.Step(`^the claimed Task is the one for order "([^"]*)"$`, w.theClaimedTaskIsForOrder)
	sc.Step(`^the claimed Task is leased to Station "([^"]*)"$`, w.theClaimedTaskIsLeasedTo)
	sc.Step(`^the response is a Problem Details document of type "([^"]*)"$`, w.theResponseIsAProblemOfType)
	sc.Step(`^the queue depth for "([^"]*)" is (\d+)$`, w.theQueueDepthIs)
	sc.Step(`^the lookup returns (\d+) tasks? for order "([^"]*)"$`, w.theLookupReturnsTasksForOrder)
	sc.Step(`^the installed capacity for "([^"]*)" is (\d+)$`, w.theInstalledCapacityForIs)
	sc.Step(`^the Station response shows the station is occupied$`, w.theStationResponseShowsOccupied)
	sc.Step(`^the Station response shows the station is unoccupied$`, w.theStationResponseShowsUnoccupied)
	sc.Step(`^(\d+) leases? (?:were|was) freed$`, w.leasesWereFreed)
	sc.Step(`^(\d+) tasks? (?:were|was) reported as CPT-missed$`, w.tasksWereReportedAsCPTMissed)
	sc.Step(`^the sealed Package has status "([^"]*)"$`, w.theSealedPackageHasStatus)
	sc.Step(`^the sealed Package holds scanned contents "([^"]*)"$`, w.theSealedPackageHoldsContents)
	sc.Step(`^an? "([^"]*)" domain event is recorded$`, w.aDomainEventIsRecorded)
	sc.Step(`^no "([^"]*)" domain event is recorded$`, w.noDomainEventIsRecorded)
	sc.Step(`^no domain events are recorded$`, w.noDomainEventsAreRecorded)
	sc.Step(`^exactly (\d+) "([^"]*)" domain events? (?:is|are) recorded$`, w.exactlyNDomainEventsAreRecorded)
}

// TestFeatures runs every Gherkin scenario under features/ as a Go test.
func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run feature tests")
	}
}
