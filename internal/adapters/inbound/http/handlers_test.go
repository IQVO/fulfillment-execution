package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
)

// newTestServer builds the router with no identity layer: this service's
// REST and MCP surfaces are unauthenticated.
func newTestServer() (stdhttp.Handler, *memory.TaskRepo, *memory.StationRepo, *memory.PackageRepo, *memory.FixedClock) {
	h, tasks, stations, packages, clock := newTestHandlers()
	return http.NewRouter(h, nil), tasks, stations, packages, clock
}

func newTestHandlers() (*http.Handlers, *memory.TaskRepo, *memory.StationRepo, *memory.PackageRepo, *memory.FixedClock) {
	tasks := memory.NewTaskRepo()
	stations := memory.NewStationRepo()
	packages := memory.NewPackageRepo()
	consolidations := memory.NewOrderConsolidationRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))

	n := 0
	newTaskId := func() shared.TaskId {
		n++
		return shared.TaskId([]byte{'t', byte('0' + n)})
	}
	newPackageId := func() shared.PackageId { return shared.PackageId("pkg-1") }

	createTask := &usecases.CreateTask{Tasks: tasks, Publisher: publisher, Clock: clock, NewId: newTaskId}

	h := &http.Handlers{
		CreateTask:         createTask,
		ClaimNext:          &usecases.ClaimNext{Tasks: tasks, Stations: stations, Publisher: publisher, Clock: clock},
		RenewLease:         &usecases.RenewLease{Tasks: tasks, Clock: clock},
		CompleteTask:       &usecases.CompleteTask{Tasks: tasks, Publisher: publisher, Clock: clock},
		SealPackage:        &usecases.SealPackage{Tasks: tasks, Packages: packages, Publisher: publisher, Clock: clock, NewId: newPackageId},
		RunSlam:            &usecases.RunSlam{Packages: packages, Publisher: publisher, Clock: clock},
		GetQueueDepth:      &usecases.GetQueueDepth{Tasks: tasks},
		ExpireLeases:       &usecases.ExpireLeases{Tasks: tasks, Publisher: publisher, Clock: clock},
		RegisterStation:    &usecases.RegisterStation{Stations: stations, Publisher: publisher},
		GetTasksByOrderRef: &usecases.GetTasksByOrderRef{Tasks: tasks},
		CheckInStation:     &usecases.CheckInStation{Stations: stations},
		CheckOutStation:    &usecases.CheckOutStation{Stations: stations},
		ArriveAtRebin: &usecases.ArriveAtRebin{
			Consolidations: consolidations,
			CreateTask:     createTask,
			Publisher:      publisher,
			Clock:          clock,
		},
		GetInstalledCapacity:  &usecases.GetInstalledCapacity{Stations: stations},
		SweepCPTMisses:        &usecases.SweepCPTMisses{Tasks: tasks, Publisher: publisher, Clock: clock},
		GetPackage:            &usecases.GetPackage{Packages: packages},
		GetPackagesByOrderRef: &usecases.GetPackagesByOrderRef{Packages: packages},
	}
	return h, tasks, stations, packages, clock
}

func doJSON(t *testing.T, srv stdhttp.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func TestGetHealthz(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodGet, "/healthz", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestPostTask_CreatesTaskInPool(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type":                 "PICK",
		"cpt":                  time.Now().Add(time.Hour),
		"orderRef":             "order-1",
		"requiredCapabilities": []string{"pick"},
	})
	if rec.Code != stdhttp.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var created struct {
		Id      string `json:"id"`
		Fragile bool   `json:"fragile"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&created)
	if got, want := rec.Header().Get("Location"), "/tasks/"+created.Id; got != want {
		t.Fatalf("expected Location %q, got %q", want, got)
	}
	if created.Fragile {
		t.Fatalf("expected fragile to default false when omitted from the request")
	}
}

// The fragile packing hint round-trips through the create-task request and
// response DTOs untouched — it is stamped by wes-work-planning, not derived
// here.
func TestPostTask_FragileFlagRoundTripsThroughResponse(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type":                 "PACK",
		"cpt":                  time.Now().Add(time.Hour),
		"orderRef":             "order-1",
		"requiredCapabilities": []string{"pack"},
		"fragile":              true,
	})
	if rec.Code != stdhttp.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var created struct {
		Fragile bool `json:"fragile"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !created.Fragile {
		t.Fatalf("expected fragile: true to round-trip in the response body")
	}
}

// giftWrap on the task response is read-only: unlike fragile, it has
// exactly one ingestion path (WorkReleased.data.gift_wrap via the Kafka
// consumer — see ADR-0011), so POST /tasks always creates a task with
// GiftWrap() == false regardless of what the request body says, and the
// response reflects that.
func TestPostTask_GiftWrapDefaultsFalseAndIsNotSettableViaHTTP(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type":                 "PACK",
		"cpt":                  time.Now().Add(time.Hour),
		"orderRef":             "order-1",
		"requiredCapabilities": []string{"pack"},
		"giftWrap":             true,
	})
	if rec.Code != stdhttp.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var created struct {
		GiftWrap bool `json:"giftWrap"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if created.GiftWrap {
		t.Fatalf("expected giftWrap to remain false: it has no HTTP ingestion path, only Kafka")
	}
}

func TestPostTask_MissingRequiredField_Returns400(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"cpt":                  time.Now().Add(time.Hour),
		"orderRef":             "order-1",
		"requiredCapabilities": []string{"pick"},
	})
	if rec.Code != stdhttp.StatusBadRequest {
		t.Fatalf("expected 400 for missing type, got %d: %s", rec.Code, rec.Body.String())
	}
	// POST /tasks has no path segment identifying a resource, so instance
	// is omitted per REST_API_TASK.md's Stage 2 instance rule.
	assertProblemDetails(t, rec, stdhttp.StatusBadRequest,
		"https://errors.fulfillment-execution.warehouse-systems.dev/invalid-request", "")
}

// A task type outside the TaskType enum documented in apis/openapi.yaml is
// a malformed request (400), not a domain error: the HTTP surface must not
// mint tasks carrying free-form type strings the published contract
// rejects (exercised by Schemathesis contract testing,
// scripts/contract-test.sh).
func TestPostTask_UnknownTaskType_Returns400(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type":                 "SORT",
		"cpt":                  time.Now().Add(time.Hour),
		"orderRef":             "order-1",
		"requiredCapabilities": []string{"pick"},
	})
	if rec.Code != stdhttp.StatusBadRequest {
		t.Fatalf("expected 400 for a task type outside the enum, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, stdhttp.StatusBadRequest,
		"https://errors.fulfillment-execution.warehouse-systems.dev/invalid-request", "")
}

func TestPostClaimNext_LeasesTaskToStation(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PICK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pick"},
	})

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PICK"})
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostClaimNext_NoWorkReturns409(t *testing.T) {
	srv, _, stations, _, _ := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PICK"})
	if rec.Code != stdhttp.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, stdhttp.StatusConflict,
		"https://errors.fulfillment-execution.warehouse-systems.dev/no-claimable-task",
		"/stations/s1/claim-next")
}

func TestFullPickLifecycle_ClaimThenComplete(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PICK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pick"},
	})

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PICK"})
	var claimed struct {
		Id string `json:"id"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&claimed)

	completeRec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/complete", map[string]any{"stationId": "s1"})
	if completeRec.Code != stdhttp.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", completeRec.Code, completeRec.Body.String())
	}

	// No double-complete via the API.
	secondRec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/complete", map[string]any{"stationId": "s1"})
	if secondRec.Code != stdhttp.StatusConflict {
		t.Fatalf("expected 409 on double-complete, got %d", secondRec.Code)
	}
	assertProblemDetails(t, secondRec, stdhttp.StatusConflict,
		"https://errors.fulfillment-execution.warehouse-systems.dev/task-already-completed",
		"/tasks/"+claimed.Id+"/complete")
}

func TestSealPackage_ExpiredLeaseIsNotClaimedAndOtherStationIsNotOwner(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pack")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PACK", "cpt": clock.Now().Add(time.Hour * 24), "orderRef": "order-1", "requiredCapabilities": []string{"pack"},
	})
	claimRec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PACK"})
	var claimed struct {
		Id string `json:"id"`
	}
	_ = json.NewDecoder(claimRec.Body).Decode(&claimed)
	path := "/tasks/" + claimed.Id + "/seal-package"
	body := map[string]any{"stationId": "s1", "contents": []string{"sku-1"}}

	// Another station while the lease is ACTIVE: 409 task-not-owner.
	other := doJSON(t, srv, stdhttp.MethodPost, path, map[string]any{"stationId": "s2", "contents": []string{"sku-1"}})
	assertProblemDetails(t, other, stdhttp.StatusConflict,
		"https://errors.fulfillment-execution.warehouse-systems.dev/task-not-owner", path)

	// The former holder after its lease expired: 409 task-not-claimed — the
	// same problem type /complete and /renew-lease return for that condition.
	clock.Advance(usecases.DefaultLeaseDuration + time.Minute)
	expired := doJSON(t, srv, stdhttp.MethodPost, path, body)
	assertProblemDetails(t, expired, stdhttp.StatusConflict,
		"https://errors.fulfillment-execution.warehouse-systems.dev/task-not-claimed", path)
	complete := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/complete", map[string]any{"stationId": "s1"})
	assertProblemDetails(t, complete, stdhttp.StatusConflict,
		"https://errors.fulfillment-execution.warehouse-systems.dev/task-not-claimed", "/tasks/"+claimed.Id+"/complete")
}

func TestPackAndSlamLifecycle(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pack")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PACK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pack"},
	})

	claimRec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PACK"})
	var claimed struct {
		Id string `json:"id"`
	}
	_ = json.NewDecoder(claimRec.Body).Decode(&claimed)

	sealRec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/seal-package", map[string]any{
		"stationId": "s1", "contents": []string{"sku-1"},
	})
	if sealRec.Code != stdhttp.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", sealRec.Code, sealRec.Body.String())
	}
	var sealed struct {
		Id              string `json:"id"`
		FragileHandling bool   `json:"fragileHandling"`
	}
	_ = json.NewDecoder(sealRec.Body).Decode(&sealed)
	if got, want := sealRec.Header().Get("Location"), "/packages/"+sealed.Id; got != want {
		t.Fatalf("expected Location %q, got %q", want, got)
	}
	if sealed.FragileHandling {
		t.Fatalf("expected fragileHandling false: the owning task was not fragile")
	}

	slamRec := doJSON(t, srv, stdhttp.MethodPost, "/packages/"+sealed.Id+"/slam", map[string]any{
		"actualWeight": 2.0, "expectedWeight": 2.0,
	})
	if slamRec.Code != stdhttp.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", slamRec.Code, slamRec.Body.String())
	}
}

// fragileHandling on the sealed Package response is derived from the
// owning task's fragile flag, set at task creation, not from the
// seal-package request body (which has no fragile field of its own).
func TestPackLifecycle_FragileHandlingDerivedFromTask(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pack")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PACK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1",
		"requiredCapabilities": []string{"pack"}, "fragile": true,
	})

	claimRec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PACK"})
	var claimed struct {
		Id      string `json:"id"`
		Fragile bool   `json:"fragile"`
	}
	_ = json.NewDecoder(claimRec.Body).Decode(&claimed)
	if !claimed.Fragile {
		t.Fatalf("expected the claimed task response to carry fragile: true")
	}

	sealRec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/seal-package", map[string]any{
		"stationId": "s1", "contents": []string{"sku-1"},
	})
	if sealRec.Code != stdhttp.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", sealRec.Code, sealRec.Body.String())
	}
	var sealed struct {
		FragileHandling bool `json:"fragileHandling"`
	}
	if err := json.Unmarshal(sealRec.Body.Bytes(), &sealed); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !sealed.FragileHandling {
		t.Fatalf("expected fragileHandling: true, derived from the fragile task")
	}
}

// giftWrapRequested on the sealed Package response is derived from the
// owning task's GiftWrap flag, exactly like fragileHandling — and because
// GiftWrap has no HTTP ingestion path (see ADR-0011), a task created via
// POST /tasks always yields giftWrapRequested: false at seal time, even
// when the task was fragile.
func TestPackLifecycle_GiftWrapRequestedDefaultsFalseViaHTTP(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pack")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PACK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1",
		"requiredCapabilities": []string{"pack"}, "fragile": true,
	})

	claimRec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PACK"})
	var claimed struct {
		Id string `json:"id"`
	}
	_ = json.NewDecoder(claimRec.Body).Decode(&claimed)

	sealRec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/seal-package", map[string]any{
		"stationId": "s1", "contents": []string{"sku-1"},
	})
	if sealRec.Code != stdhttp.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", sealRec.Code, sealRec.Body.String())
	}
	var sealed struct {
		FragileHandling   bool `json:"fragileHandling"`
		GiftWrapRequested bool `json:"giftWrapRequested"`
	}
	if err := json.Unmarshal(sealRec.Body.Bytes(), &sealed); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !sealed.FragileHandling {
		t.Fatalf("expected fragileHandling: true, derived from the fragile task")
	}
	if sealed.GiftWrapRequested {
		t.Fatalf("expected giftWrapRequested: false — no HTTP path sets GiftWrap on the task")
	}
}

// sortLane on the sealed Package response reflects the priority-order
// derivation: a fragile task with no hazmat-classified scanned items (the
// only classification a station without a wired lookup can produce, since
// newTestServer's SealPackage has no ClassificationLookup) resolves to
// FRAGILE_NO_TILT — see Package.SortLane and ADR-0010.
func TestPackLifecycle_SortLaneFragileNoTiltWhenFragileAndNoHazmat(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pack")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PACK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1",
		"requiredCapabilities": []string{"pack"}, "fragile": true,
	})
	claimRec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PACK"})
	var claimed struct {
		Id string `json:"id"`
	}
	_ = json.NewDecoder(claimRec.Body).Decode(&claimed)

	sealRec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/seal-package", map[string]any{
		"stationId": "s1", "contents": []string{"sku-1"},
	})
	var sealed struct {
		SortLane string `json:"sortLane"`
	}
	if err := json.Unmarshal(sealRec.Body.Bytes(), &sealed); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if sealed.SortLane != "FRAGILE_NO_TILT" {
		t.Fatalf("expected sortLane FRAGILE_NO_TILT, got %q", sealed.SortLane)
	}
}

// sortLane is STANDARD when the sealed package is neither hazmat nor
// fragile.
func TestPackLifecycle_SortLaneStandardWhenNeitherHazmatNorFragile(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pack")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PACK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1",
		"requiredCapabilities": []string{"pack"},
	})
	claimRec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PACK"})
	var claimed struct {
		Id string `json:"id"`
	}
	_ = json.NewDecoder(claimRec.Body).Decode(&claimed)

	sealRec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/seal-package", map[string]any{
		"stationId": "s1", "contents": []string{"sku-1"},
	})
	var sealed struct {
		SortLane string `json:"sortLane"`
	}
	if err := json.Unmarshal(sealRec.Body.Bytes(), &sealed); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if sealed.SortLane != "STANDARD" {
		t.Fatalf("expected sortLane STANDARD, got %q", sealed.SortLane)
	}
}

func TestGetQueueDepth(t *testing.T) {
	srv, _, _, _, clock := newTestServer()
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PICK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pick"},
	})

	rec := doJSON(t, srv, stdhttp.MethodGet, "/queues/PICK/depth", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Depth int `json:"depth"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Depth != 1 {
		t.Fatalf("expected depth 1, got %d", resp.Depth)
	}
}

// TestGetInstalledCapacity is the read endpoint workforce-management's
// CommitShiftPlan calls to enforce plannedHeads against the real Station
// registry — see ADR-0018.
func TestGetInstalledCapacity(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	doJSON(t, srv, stdhttp.MethodPost, "/stations", map[string]any{
		"stationId": "s1", "capabilities": []string{"pick"},
	})
	doJSON(t, srv, stdhttp.MethodPost, "/stations", map[string]any{
		"stationId": "s2", "capabilities": []string{"pick", "pack"},
	})

	rec := doJSON(t, srv, stdhttp.MethodGet, "/capacity/pick", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Capability string `json:"capability"`
		Installed  int    `json:"installed"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Capability != "pick" || resp.Installed != 2 {
		t.Fatalf("expected capability pick, installed 2, got %+v", resp)
	}
}

// TestGetInstalledCapacity_UnregisteredCapability_ReturnsZeroNotError proves
// this endpoint mirrors GetQueueDepthHandler's own contract: an
// unrecognized capability is not a 404, it is a real answer of zero.
func TestGetInstalledCapacity_UnregisteredCapability_ReturnsZeroNotError(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodGet, "/capacity/rebin", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Installed int `json:"installed"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Installed != 0 {
		t.Fatalf("expected installed 0, got %d", resp.Installed)
	}
}

func TestGetTasksByOrderRef_ReturnsMatchingTasks(t *testing.T) {
	srv, _, _, _, clock := newTestServer()
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PICK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pick"},
	})
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PACK", "cpt": clock.Now().Add(2 * time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pack"},
	})
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "SLAM", "cpt": clock.Now().Add(3 * time.Hour), "orderRef": "order-2", "requiredCapabilities": []string{"slam"},
	})

	rec := doJSON(t, srv, stdhttp.MethodGet, "/tasks?orderRef=order-1", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp []struct {
		Id       string `json:"id"`
		Type     string `json:"type"`
		Status   string `json:"status"`
		OrderRef string `json:"orderRef"`
		Fragile  bool   `json:"fragile"`
		GiftWrap bool   `json:"giftWrap"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp) != 2 {
		t.Fatalf("expected 2 tasks for order-1, got %d: %+v", len(resp), resp)
	}
	for _, tk := range resp {
		if tk.OrderRef != "order-1" {
			t.Fatalf("expected only order-1 tasks, got %+v", tk)
		}
	}
}

func TestGetTasksByOrderRef_MissingParamReturns400(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodGet, "/tasks", nil)
	if rec.Code != stdhttp.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGetTasksByOrderRef_UnknownOrderRefReturnsEmptyArray(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodGet, "/tasks?orderRef=does-not-exist", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp []any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp) != 0 {
		t.Fatalf("expected empty array, got %v", resp)
	}
}

func TestGetTasksByOrderRef_SurfacesLeaseStationId(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PICK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pick"},
	})
	doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PICK"})

	rec := doJSON(t, srv, stdhttp.MethodGet, "/tasks?orderRef=order-1", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp []struct {
		LeaseStationId *string `json:"leaseStationId"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp) != 1 || resp[0].LeaseStationId == nil || *resp[0].LeaseStationId != "s1" {
		t.Fatalf("expected leaseStationId s1, got %+v", resp)
	}
}

func TestPostExpireLeases(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PICK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pick"},
	})
	doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PICK"})

	clock.Advance(usecases.DefaultLeaseDuration + time.Minute)

	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/expire-leases", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Freed int `json:"freed"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Freed != 1 {
		t.Fatalf("expected 1 freed task, got %d", resp.Freed)
	}
}

func TestPostSweepCPTMisses(t *testing.T) {
	srv, _, _, _, clock := newTestServer()
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PICK", "cpt": clock.Now().Add(time.Minute), "orderRef": "order-1", "requiredCapabilities": []string{"pick"},
	})

	clock.Advance(2 * time.Minute)

	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/sweep-cpt-misses", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Reported int `json:"reported"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Reported != 1 {
		t.Fatalf("expected 1 task reported, got %d", resp.Reported)
	}
}

func TestPostRenewLease_NotFoundOnUnknownTask(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/does-not-exist/renew-lease", map[string]any{"stationId": "s1"})
	if rec.Code != stdhttp.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, stdhttp.StatusNotFound,
		"https://errors.fulfillment-execution.warehouse-systems.dev/task-not-found",
		"/tasks/does-not-exist/renew-lease")
}

// assertProblemDetails checks rec's body against the RFC 7807 Problem
// Details shape (type, title, status, detail, instance) and confirms
// Content-Type is application/problem+json (not application/json).
func assertProblemDetails(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantType, wantInstance string) {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("expected Content-Type application/problem+json, got %q", ct)
	}
	var body struct {
		Type     string `json:"type"`
		Title    string `json:"title"`
		Status   int    `json:"status"`
		Detail   string `json:"detail"`
		Instance string `json:"instance"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal problem body: %v", err)
	}
	if body.Type != wantType {
		t.Fatalf("expected type %q, got %q", wantType, body.Type)
	}
	if body.Title == "" {
		t.Fatalf("expected a non-empty title")
	}
	if body.Status != wantStatus {
		t.Fatalf("expected status %d in body, got %d", wantStatus, body.Status)
	}
	if body.Detail == "" {
		t.Fatalf("expected a non-empty detail")
	}
	if body.Instance != wantInstance {
		t.Fatalf("expected instance %q, got %q", wantInstance, body.Instance)
	}
}

func TestPostRegisterStation_CreatesStation(t *testing.T) {
	srv, _, stations, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations", map[string]any{
		"stationId":    "s1",
		"capabilities": []string{"pick"},
	})
	if rec.Code != stdhttp.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Id           string   `json:"id"`
		Capabilities []string `json:"capabilities"`
		Occupied     bool     `json:"occupied"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Id != "s1" || resp.Occupied || len(resp.Capabilities) != 1 || resp.Capabilities[0] != "pick" {
		t.Fatalf("unexpected response body: %+v", resp)
	}
	if got, want := rec.Header().Get("Location"), "/stations/s1"; got != want {
		t.Fatalf("expected Location %q, got %q", want, got)
	}

	found, _ := stations.FindById(context.TODO(), "s1")
	if found == nil {
		t.Fatalf("expected station to be persisted")
	}
}

func TestPostRegisterStation_MissingStationId_Returns400(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations", map[string]any{
		"capabilities": []string{"pick"},
	})
	if rec.Code != stdhttp.StatusBadRequest {
		t.Fatalf("expected 400 for missing stationId, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostAdminExpireLeases_OldRoute_NoLongerExists(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/admin/expire-leases", nil)
	if rec.Code != stdhttp.StatusNotFound {
		t.Fatalf("expected the old /admin/expire-leases route to be gone (404), got %d", rec.Code)
	}
}

// Proves the gap this task closes: a freshly registered station can
// immediately be handed a task via claim-next over HTTP, where previously
// there was no way to create the station at all and claim-next always 404'd.
func TestPostRegisterStation_ThenClaimNextSucceeds(t *testing.T) {
	srv, _, _, _, clock := newTestServer()

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations", map[string]any{
		"stationId":    "s1",
		"capabilities": []string{"pick"},
	})
	if rec.Code != stdhttp.StatusCreated {
		t.Fatalf("expected 201 registering station, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PICK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pick"},
	})
	if rec.Code != stdhttp.StatusCreated {
		t.Fatalf("expected 201 creating task, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PICK"})
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200 claiming against freshly registered station, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostCheckInStation_Success(t *testing.T) {
	srv, _, stations, _, _ := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/check-in", map[string]any{"occupantId": "worker-1"})
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Id       string `json:"id"`
		Occupied bool   `json:"occupied"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Id != "s1" || !resp.Occupied {
		t.Fatalf("expected s1 occupied, got %+v", resp)
	}

	found, _ := stations.FindById(context.TODO(), "s1")
	if found == nil || !found.IsOccupied() {
		t.Fatalf("expected the check-in to be persisted")
	}
}

func TestPostCheckInStation_MissingOccupantId_Returns400(t *testing.T) {
	srv, _, stations, _, _ := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/check-in", map[string]any{})
	if rec.Code != stdhttp.StatusBadRequest {
		t.Fatalf("expected 400 for missing occupantId, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostCheckInStation_UnknownStation_Returns404(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/does-not-exist/check-in", map[string]any{"occupantId": "worker-1"})
	if rec.Code != stdhttp.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, stdhttp.StatusNotFound,
		"https://errors.fulfillment-execution.warehouse-systems.dev/station-not-found",
		"/stations/does-not-exist/check-in")
}

// Domain invariant surfaced through HTTP: one occupant at a time.
func TestPostCheckInStation_SecondOccupant_Returns409(t *testing.T) {
	srv, _, stations, _, _ := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))
	doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/check-in", map[string]any{"occupantId": "worker-1"})

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/check-in", map[string]any{"occupantId": "worker-2"})
	if rec.Code != stdhttp.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, stdhttp.StatusConflict,
		"https://errors.fulfillment-execution.warehouse-systems.dev/station-occupied",
		"/stations/s1/check-in")
}

func TestPostCheckOutStation_Success(t *testing.T) {
	srv, _, stations, _, _ := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))
	doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/check-in", map[string]any{"occupantId": "worker-1"})

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/check-out", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Occupied bool `json:"occupied"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Occupied {
		t.Fatalf("expected station vacant after check-out, got %+v", resp)
	}

	found, _ := stations.FindById(context.TODO(), "s1")
	if found == nil || found.IsOccupied() {
		t.Fatalf("expected the check-out to be persisted")
	}
}

func TestPostCheckOutStation_UnknownStation_Returns404(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/does-not-exist/check-out", nil)
	if rec.Code != stdhttp.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, stdhttp.StatusNotFound,
		"https://errors.fulfillment-execution.warehouse-systems.dev/station-not-found",
		"/stations/does-not-exist/check-out")
}

// Domain invariant surfaced through HTTP: check-out on an empty station.
func TestPostCheckOutStation_NotOccupied_Returns409(t *testing.T) {
	srv, _, stations, _, _ := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/check-out", nil)
	if rec.Code != stdhttp.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, stdhttp.StatusConflict,
		"https://errors.fulfillment-execution.warehouse-systems.dev/station-not-occupied",
		"/stations/s1/check-out")
}

func TestPostArriveAtRebin_SingleLineOrderCreatesPackTaskImmediately(t *testing.T) {
	srv, tasks, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/rebin/arrivals", map[string]any{
		"orderRef":                 "order-1",
		"lineId":                   "line-1",
		"requiredLineIds":          []string{"line-1"},
		"packCpt":                  time.Now().Add(time.Hour),
		"packRequiredCapabilities": []string{"pack"},
	})
	if rec.Code != stdhttp.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}

	packTasks, _ := tasks.FindByOrderRef(context.TODO(), "order-1")
	if len(packTasks) != 1 {
		t.Fatalf("expected exactly 1 PACK task for the single-line order, got %d", len(packTasks))
	}
}

func TestPostArriveAtRebin_MultiLineOrderWaitsForAllArrivals(t *testing.T) {
	srv, tasks, _, _, _ := newTestServer()
	body := func(lineId string) map[string]any {
		return map[string]any{
			"orderRef":                 "order-1",
			"lineId":                   lineId,
			"requiredLineIds":          []string{"line-1", "line-2"},
			"packCpt":                  time.Now().Add(time.Hour),
			"packRequiredCapabilities": []string{"pack"},
		}
	}

	rec := doJSON(t, srv, stdhttp.MethodPost, "/rebin/arrivals", body("line-1"))
	if rec.Code != stdhttp.StatusNoContent {
		t.Fatalf("expected 204 on first arrival, got %d: %s", rec.Code, rec.Body.String())
	}
	packTasks, _ := tasks.FindByOrderRef(context.TODO(), "order-1")
	if len(packTasks) != 0 {
		t.Fatalf("expected no PACK task before both lines arrive, got %d", len(packTasks))
	}

	rec = doJSON(t, srv, stdhttp.MethodPost, "/rebin/arrivals", body("line-2"))
	if rec.Code != stdhttp.StatusNoContent {
		t.Fatalf("expected 204 on second arrival, got %d: %s", rec.Code, rec.Body.String())
	}
	packTasks, _ = tasks.FindByOrderRef(context.TODO(), "order-1")
	if len(packTasks) != 1 {
		t.Fatalf("expected exactly 1 PACK task once both lines have arrived, got %d", len(packTasks))
	}
}

func TestPostArriveAtRebin_MissingOrderRef_Returns400(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/rebin/arrivals", map[string]any{
		"lineId":                   "line-1",
		"requiredLineIds":          []string{"line-1"},
		"packCpt":                  time.Now().Add(time.Hour),
		"packRequiredCapabilities": []string{"pack"},
	})
	if rec.Code != stdhttp.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

// Domain invariant surfaced through HTTP: an arrival for a line outside
// the order's established required set is rejected as 422, not silently
// accepted or treated as a data-integrity crash.
func TestPostArriveAtRebin_UnknownLine_Returns422(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	base := map[string]any{
		"orderRef":                 "order-1",
		"requiredLineIds":          []string{"line-1"},
		"packCpt":                  time.Now().Add(time.Hour),
		"packRequiredCapabilities": []string{"pack"},
	}

	firstArrival := map[string]any{}
	for k, v := range base {
		firstArrival[k] = v
	}
	firstArrival["lineId"] = "line-1"
	rec := doJSON(t, srv, stdhttp.MethodPost, "/rebin/arrivals", firstArrival)
	if rec.Code != stdhttp.StatusNoContent {
		t.Fatalf("expected 204 on first arrival, got %d: %s", rec.Code, rec.Body.String())
	}

	unknownArrival := map[string]any{}
	for k, v := range base {
		unknownArrival[k] = v
	}
	unknownArrival["lineId"] = "line-not-in-order"
	rec = doJSON(t, srv, stdhttp.MethodPost, "/rebin/arrivals", unknownArrival)
	if rec.Code != stdhttp.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, stdhttp.StatusUnprocessableEntity,
		"https://errors.fulfillment-execution.warehouse-systems.dev/rebin-unknown-line",
		"/rebin/arrivals")
}
