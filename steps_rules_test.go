package main_test

// Step definitions of the documentation-derived scenarios
// (features/*_rules.feature). They extend the shared world with:
//
//   - a catalogue that also declares the RELOCATE and REPLENISH paths, and
//     friendly steps that release those moves through the WorkReleased
//     consumer's message handler;
//   - a facility-layout and a product-classification double wired into the
//     use cases the REST handlers share (the harness leaves both nil);
//   - dotted-path assertions on the last REST response;
//   - the real integration encoder (Publisher.Encode) over the recorded
//     domain events, so the published wire contract of the documented
//     events can be asserted without a broker.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"
	kafkago "github.com/segmentio/kafka-go"

	kafkain "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	kafkaout "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/packview"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared/gs1"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// rulesState is the per-scenario state of the documentation-derived steps.
type rulesState struct {
	wire      []kafkaout.Encoded
	wireSeq   int
	locations *fakeLocations
	// packView is the local PackView double ConfirmPickScan resolves scans
	// against (ADR-0054); breaker is the inventory-storage double behind
	// POST /tasks/{id}/case-breaks.
	packView *packview.MemoryStore
	breaker  *fakeCaseBreaker
}

// fakeLocations is the facility-layout double behind RegisterStation's
// optional location-role check (ADR-0024).
type fakeLocations struct {
	roles map[string]string
	err   error
}

func (f *fakeLocations) GetRole(_ context.Context, code string) (ports.LocationRoleInfo, error) {
	if f.err != nil {
		return ports.LocationRoleInfo{}, f.err
	}
	role, known := f.roles[code]
	return ports.LocationRoleInfo{Role: role, Known: known}, nil
}

// flakyClassification wraps the classification copy and fails the lookup of
// the listed SKUs, like a storage error on the local copy (ADR-0039).
type flakyClassification struct {
	inner   ports.ProductClassificationLookup
	failing map[string]bool
}

func (f *flakyClassification) GetClassification(ctx context.Context, sku string) (ports.ClassificationInfo, error) {
	if f.failing[sku] {
		return ports.ClassificationInfo{}, errors.New("classification copy unavailable")
	}
	return f.inner.GetClassification(ctx, sku)
}

// fakeCaseBreaker is the inventory-storage double behind CompleteCaseBreak
// (ADR-0054): it records every break attempt — including the ones it fails,
// because the documented guarantee is that a retry reuses the SAME
// idempotency key, which is only observable on the failed attempt too.
type fakeCaseBreaker struct {
	fail     bool
	requests []ports.CaseBreakRequest
}

func (f *fakeCaseBreaker) BreakCase(_ context.Context, req ports.CaseBreakRequest) error {
	f.requests = append(f.requests, req)
	if f.fail {
		return ports.ErrCaseBreakUnavailable
	}
	return nil
}

// rulesCatalogue is warehouse-infra's declared process paths plus the two
// move paths the docs describe (RELOCATE, ADR-0047; REPLENISH, ADR-0050).
func rulesCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}},
		{Id: "PACK", MatchPrefix: "pack", Direct: true, RequiredCapabilities: []string{"pack"}},
		{Id: "REBIN", MatchPrefix: "rebin", Direct: true, RequiredCapabilities: []string{"rebin"}},
		{Id: "SLAM", MatchPrefix: "slam", Direct: true, RequiredCapabilities: []string{"slam"}},
		{Id: "DISPATCH", MatchPrefix: "dispatch", Direct: true, RequiredCapabilities: []string{"dispatch"}},
		{Id: "ARRIVAL", MatchPrefix: "arrival", Direct: true, RequiredCapabilities: []string{"arrival"}},
		{Id: "BREAK_CASE", MatchPrefix: "break", Direct: true, RequiredCapabilities: []string{"break"}},
		{Id: "RELOCATE", MatchPrefix: "relocate", Direct: true, RequiredCapabilities: []string{"relocate"}},
		{Id: "REPLENISH", MatchPrefix: "replenish", Direct: true, RequiredCapabilities: []string{"replenish"}},
	})
}

// --- clock -------------------------------------------------------------------

func (w *world) theClockAdvancesBySeconds(seconds int) error {
	w.clock.Advance(time.Duration(seconds) * time.Second)
	return nil
}

// --- WorkReleased ------------------------------------------------------------

func (w *world) releaseWithData(eventId string, data map[string]any) error {
	if _, ok := data["cpt"]; !ok {
		data["cpt"] = w.clock.Now().Add(time.Hour)
	}
	raw, err := cloudEventBytes(eventId, kafkain.TypeWorkReleased, data)
	if err != nil {
		return err
	}
	w.w2.deliveries[eventId] = raw
	return w.deliverWorkReleased(raw)
}

func (w *world) workPlanningPublishesJSONData(eventId, body string) error {
	var data map[string]any
	if err := json.Unmarshal([]byte(body), &data); err != nil {
		return fmt.Errorf("data %q is not a JSON object: %w", body, err)
	}
	return w.releaseWithData(eventId, data)
}

func (w *world) workPlanningPublishesWithObject(eventId, workUnit, pathId, key, body string) error {
	var object map[string]any
	if err := json.Unmarshal([]byte(body), &object); err != nil {
		return fmt.Errorf("%s %q is not a JSON object: %w", key, body, err)
	}
	return w.releaseWithData(eventId, map[string]any{
		"path_id": pathId, "work_unit_id": workUnit, "ref": "plan-" + workUnit, key: object,
	})
}

func (w *world) workPlanningPublishesWithoutObject(eventId, workUnit, pathId string) error {
	return w.releaseWithData(eventId, map[string]any{
		"path_id": pathId, "work_unit_id": workUnit, "ref": "plan-" + workUnit,
	})
}

func dueMinutes(raw string) time.Duration {
	if raw == "" {
		return time.Hour
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return time.Hour
	}
	return time.Duration(n) * time.Minute
}

func (w *world) relocateMoveReleased(kind, sku, planId, seq, workUnit, from, to, due string) error {
	n, err := strconv.Atoi(seq)
	if err != nil {
		return fmt.Errorf("move_seq %q: %w", seq, err)
	}
	move := map[string]any{"plan_id": planId, "move_seq": n, "kind": kind, "sku": sku}
	if from != "" {
		move["from_bin"] = from
	}
	if to != "" {
		move["to_bin"] = to
	}
	if err := w.releaseWithData("evt-"+workUnit, map[string]any{
		"path_id": "relocate-slotting", "work_unit_id": workUnit, "ref": planId,
		"cpt": w.clock.Now().Add(dueMinutes(due)), "relocation": move,
	}); err != nil {
		return err
	}
	return w.theConsumerAccepts()
}

func (w *world) replenishRequestReleased(requestId string, quantity int, sku, from, to, priority, workUnit, due string) error {
	if err := w.releaseWithData("evt-"+workUnit, map[string]any{
		"path_id": "replenish-pickface", "work_unit_id": workUnit, "ref": requestId,
		"cpt": w.clock.Now().Add(dueMinutes(due)),
		"replenishment": map[string]any{
			"request_id": requestId, "sku": sku, "from_bin": from, "to_bin": to,
			"quantity": quantity, "priority": priority,
		},
	}); err != nil {
		return err
	}
	return w.theConsumerAccepts()
}

func (w *world) replenishRequestOfIdLength(length int, verdictWorkUnit string) error {
	return w.releaseWithData("evt-"+verdictWorkUnit, map[string]any{
		"path_id": "replenish-pickface", "work_unit_id": verdictWorkUnit, "ref": "req",
		"replenishment": map[string]any{
			"request_id": strings.Repeat("R", length), "sku": "SKU-1", "from_bin": "RES-01", "to_bin": "FWD-01",
			"quantity": 10, "priority": "NORMAL",
		},
	})
}

func (w *world) workPlanningPublishesAnotherType(eventId, eventType string) error {
	raw, err := cloudEventBytes(eventId, eventType, map[string]any{"work_unit_id": "wu-x", "path_id": "pick-zone-a"})
	if err != nil {
		return err
	}
	return w.deliverWorkReleased(raw)
}

func (w *world) workPlanningPublishesRaw(payload string) error {
	return w.deliverWorkReleased([]byte(payload))
}

// --- completion with the operator's confirmation ------------------------------

func (w *world) stationCompletesWithConfirmation(stationId, body string) error {
	var in map[string]any
	if err := json.Unmarshal([]byte(body), &in); err != nil {
		return fmt.Errorf("confirmation %q is not a JSON object: %w", body, err)
	}
	in["stationId"] = stationId
	return w.do(http.MethodPost, "/tasks/"+w.claimedTaskId+"/completions", in)
}

func (w *world) responseIsClientError() error {
	if w.status < 400 || w.status > 499 {
		return fmt.Errorf("expected a 4xx client error, got %d: %s", w.status, w.body)
	}
	return nil
}

// --- dotted-path assertions ---------------------------------------------------

func jsonAt(body []byte, path string) (any, bool, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false, fmt.Errorf("body %q is not JSON: %w", string(body), err)
	}
	for _, seg := range strings.Split(path, ".") {
		switch cur := v.(type) {
		case map[string]any:
			next, ok := cur[seg]
			if !ok {
				return nil, false, nil
			}
			v = next
		case []any:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(cur) {
				return nil, false, nil
			}
			v = cur[idx]
		default:
			return nil, false, nil
		}
	}
	return v, true, nil
}

func renderJSON(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case nil:
		return "null"
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

func expectAt(body []byte, path, want string) error {
	v, ok, err := jsonAt(body, path)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("expected %q at %q, found nothing in %s", want, path, body)
	}
	if got := renderJSON(v); got != want {
		return fmt.Errorf("expected %q at %q, got %q", want, path, got)
	}
	return nil
}

func expectNothingAt(body []byte, path string) error {
	if v, ok, err := jsonAt(body, path); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("expected nothing at %q, found %v", path, v)
	}
	return nil
}

func (w *world) responseAtIs(path, want string) error { return expectAt(w.body, path, want) }

func (w *world) responseHasNothingAt(path string) error { return expectNothingAt(w.body, path) }

func (w *world) taskForOrderShows(orderRef, path, want string) error {
	status, body, _, err := w.call(http.MethodGet, "/tasks?orderRef="+url.QueryEscape(orderRef), nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("looking up tasks for %q: expected 200, got %d: %s", orderRef, status, body)
	}
	return expectAt(body, "items.0."+path, want)
}

func (w *world) taskForOrderShowsNothing(orderRef, path string) error {
	status, body, _, err := w.call(http.MethodGet, "/tasks?orderRef="+url.QueryEscape(orderRef), nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("looking up tasks for %q: expected 200, got %d: %s", orderRef, status, body)
	}
	return expectNothingAt(body, "items.0."+path)
}

// --- outbound doubles ----------------------------------------------------------

func (w *world) locationDouble() *fakeLocations {
	if w.rules.locations == nil {
		w.rules.locations = &fakeLocations{roles: map[string]string{}}
		w.handlers.RegisterStation.LocationLookup = w.rules.locations
	}
	return w.rules.locations
}

func (w *world) facilityLayoutSays(code, role string) error {
	w.locationDouble().roles[code] = role
	return nil
}

func (w *world) facilityLayoutKnowsNothing() error {
	w.locationDouble()
	return nil
}

func (w *world) facilityLayoutIsUnreachable() error {
	w.locationDouble().err = errors.New("facility-layout unreachable")
	return nil
}

func (w *world) classificationLookupFailsFor(sku string) error {
	flaky, ok := w.handlers.SealPackage.ClassificationLookup.(*flakyClassification)
	if !ok {
		flaky = &flakyClassification{inner: w.handlers.SealPackage.ClassificationLookup, failing: map[string]bool{}}
		w.handlers.SealPackage.ClassificationLookup = flaky
	}
	flaky.failing[sku] = true
	return nil
}

func (w *world) productMasterPublishes(eventId, sku string, tags []string, class, version int) error {
	data := map[string]any{"sku": sku, "handling_tags": tags, "version": version}
	if class > 0 {
		data["dot_hazard_class"] = class
	}
	raw, err := cloudEventBytes(eventId, kafkain.TypeProductClassified, data)
	if err != nil {
		return err
	}
	return w.w2.classifiedConsumer.Handle(context.Background(), kafkago.Message{Topic: kafkain.ProductMasterTopic, Value: raw})
}

func (w *world) productMasterReclassifies(sku string, class, version int, eventId string) error {
	return w.productMasterPublishes(eventId, sku, []string{"Hazmat"}, class, version)
}

func (w *world) productMasterHazmatNoClass(sku string) error {
	return w.productMasterPublishes("classified-"+sku, sku, []string{"Hazmat"}, 0, 1)
}

func (w *world) productMasterFragileOnly(sku string) error {
	return w.productMasterPublishes("classified-"+sku, sku, []string{"Fragile"}, 0, 1)
}

func (w *world) productMasterFragileWithStrayClass(sku string, class int) error {
	return w.productMasterPublishes("classified-"+sku, sku, []string{"Fragile"}, class, 1)
}

// --- integration encoder -------------------------------------------------------

func (w *world) recordedEventsAreEncoded() error {
	newId := func() string {
		w.rules.wireSeq++
		return fmt.Sprintf("00000000-0000-4000-8000-%012d", w.rules.wireSeq)
	}
	pub := kafkaout.NewPublisherWithWriter(nil, w.handlers.CompleteTask.Tasks, w.handlers.ClaimNext.Stations, newId)
	encoded, err := pub.Encode(context.Background(), w.publisher.Events()...)
	if err != nil {
		return fmt.Errorf("encode the recorded events: %w", err)
	}
	w.rules.wire = encoded
	return nil
}

func shortType(full string) string {
	return full[strings.LastIndex(full, ".")+1:]
}

func (w *world) wireMessages(name string) []kafkaout.Encoded {
	var out []kafkaout.Encoded
	for _, m := range w.rules.wire {
		if shortType(m.EventType) == name {
			out = append(out, m)
		}
	}
	return out
}

func (w *world) lastWireMessage(name string) (kafkaout.Encoded, error) {
	msgs := w.wireMessages(name)
	if len(msgs) == 0 {
		return kafkaout.Encoded{}, fmt.Errorf("no %s integration message was encoded; encoded: %v", name, w.wireTypes())
	}
	return msgs[len(msgs)-1], nil
}

func (w *world) wireTypes() []string {
	names := make([]string, 0, len(w.rules.wire))
	for _, m := range w.rules.wire {
		names = append(names, shortType(m.EventType))
	}
	return names
}

func (w *world) integrationTopicCarries(expected int, name string) error {
	if n := len(w.wireMessages(name)); n != expected {
		return fmt.Errorf("expected %d %s integration message(s), got %d; encoded: %v", expected, name, n, w.wireTypes())
	}
	return nil
}

func (w *world) integrationTopicCarriesNothing() error {
	if len(w.rules.wire) != 0 {
		return fmt.Errorf("expected no integration message, encoded: %v", w.wireTypes())
	}
	return nil
}

func (w *world) integrationMessageHas(name, path, want string) error {
	m, err := w.lastWireMessage(name)
	if err != nil {
		return err
	}
	return expectAt(m.Value, path, want)
}

func (w *world) integrationMessageHasNo(name, path string) error {
	m, err := w.lastWireMessage(name)
	if err != nil {
		return err
	}
	return expectNothingAt(m.Value, path)
}

func (w *world) integrationMessageKeyedBySubject(name string) error {
	m, err := w.lastWireMessage(name)
	if err != nil {
		return err
	}
	v, ok, err := jsonAt(m.Value, "subject")
	if err != nil {
		return err
	}
	if !ok || string(m.Key) != renderJSON(v) {
		return fmt.Errorf("expected the Kafka key %q to equal the subject %v", m.Key, v)
	}
	return nil
}

func (w *world) integrationMessagesHaveDistinctIds(name string) error {
	seen := map[string]bool{}
	msgs := w.wireMessages(name)
	if len(msgs) < 2 {
		return fmt.Errorf("expected at least two %s messages, got %d", name, len(msgs))
	}
	for _, m := range msgs {
		v, _, err := jsonAt(m.Value, "id")
		if err != nil {
			return err
		}
		id := renderJSON(v)
		if seen[id] {
			return fmt.Errorf("two %s messages share the CloudEvents id %q", name, id)
		}
		seen[id] = true
	}
	return nil
}

// --- pick scans and case breaks (ADR-0054, ADR-0055) --------------------------

// packLevelDeclared mirrors what product-master's ProductPackLevelDeclared
// writes into the local PackView: one GTIN, its SKU and its level.
func (w *world) packLevelDeclared(level, gtin, sku, unitsPerPack string) error {
	parsed, err := gs1.ParseGTIN(gtin)
	if err != nil {
		return fmt.Errorf("GTIN %q: %w", gtin, err)
	}
	units := 0
	if unitsPerPack != "" {
		units, err = strconv.Atoi(unitsPerPack)
		if err != nil {
			return fmt.Errorf("units per pack %q: %w", unitsPerPack, err)
		}
	}
	var packLevel task.PackLevel
	switch level {
	case "EACH":
		packLevel, units = task.PackLevelEach, 1
	case "CASE":
		packLevel = task.PackLevelCase
	default:
		return fmt.Errorf("unknown pack level %q (EACH or CASE)", level)
	}
	_, err = w.rules.packView.UpsertIfNewer(context.Background(), ports.PackRecord{
		GTIN: parsed.String(), SKU: sku, Level: packLevel, UnitsPerPack: units, Version: 1,
	})
	return err
}

func (w *world) stationScansClaimedTaskStep(stationId, gtin, count string) error {
	n := 1
	if count != "" {
		parsed, err := strconv.Atoi(count)
		if err != nil {
			return fmt.Errorf("scan count %q: %w", count, err)
		}
		n = parsed
	}
	return w.stationScansClaimedTask(stationId, gtin, n)
}

func (w *world) stationScansClaimedTask(stationId, gtin string, count int) error {
	return w.do(http.MethodPost, "/tasks/"+w.claimedTaskId+"/scans", map[string]any{
		"stationId": stationId, "gtin": gtin, "count": count,
	})
}

func (w *world) stationBreaksTheCase(stationId string) error {
	return w.do(http.MethodPost, "/tasks/"+w.claimedTaskId+"/case-breaks", map[string]any{
		"stationId": stationId,
	})
}

func (w *world) inventoryStorageIsUnreachable() error {
	w.rules.breaker.fail = true
	return nil
}

func (w *world) inventoryStorageIsReachableAgain() error {
	w.rules.breaker.fail = false
	return nil
}

// inventoryStorageReceived asserts every recorded break attempt — failed ones
// included — targeted the LPN, the loose bin and the stable per-task
// idempotency key (ADR-0054: a retry never breaks twice under a new key).
func (w *world) inventoryStorageReceived(expected int, lpn, bin, key string) error {
	if n := len(w.rules.breaker.requests); n != expected {
		return fmt.Errorf("expected %d case break request(s), inventory-storage received %d: %+v", expected, n, w.rules.breaker.requests)
	}
	for i, req := range w.rules.breaker.requests {
		if req.LPN != lpn || req.LooseBin != bin || req.IdempotencyKey != key {
			return fmt.Errorf("case break request %d was %+v, expected LPN %q bin %q key %q", i, req, lpn, bin, key)
		}
	}
	return nil
}

// registerRulesSteps registers the documentation-derived vocabulary on sc.
func (w *world) registerRulesSteps(sc *godog.ScenarioContext) {
	// Runs after the world's own reset hook: give the scenario its own state and
	// a consumer whose catalogue also declares the RELOCATE and REPLENISH paths.
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w.rules = rulesState{}
		w.w2.workConsumer.Apply.Catalogue = rulesCatalogue()
		// The harness leaves the pick-scan, case-break and work-request use
		// cases unwired; wire them the way cmd/execution/wire_handlers.go
		// does, against doubles, so the documented rules of those surfaces
		// are exercisable (ADR-0054, ADR-0055, ADR-0058).
		w.rules.packView = packview.NewMemoryStore()
		w.rules.breaker = &fakeCaseBreaker{}
		w.handlers.ConfirmPickScan = &usecases.ConfirmPickScan{
			Tasks: w.handlers.CompleteTask.Tasks, Packs: w.rules.packView, Clock: w.clock,
		}
		w.handlers.CompleteCaseBreak = &usecases.CompleteCaseBreak{
			Tasks: w.handlers.CompleteTask.Tasks, Breaker: w.rules.breaker,
			Publisher: w.publisher, Clock: w.clock,
		}
		// No WORK_PLANNING_A2A_URL: the delegation surface is explicitly off
		// (ADR-0058) — a nil Planning port answers 503, never a panic.
		w.handlers.RequestNextWork = &usecases.RequestNextWork{}
		return ctx, nil
	})

	sc.Step(`^the clock advances by (\d+) seconds$`, w.theClockAdvancesBySeconds)

	sc.Step(`^work planning publishes a WorkReleased event "([^"]*)" with the JSON data (.+)$`, w.workPlanningPublishesJSONData)
	sc.Step(`^work planning publishes a WorkReleased event "([^"]*)" for work unit "([^"]*)" on path "([^"]*)" with the "([^"]*)" object (.+)$`, w.workPlanningPublishesWithObject)
	sc.Step(`^work planning publishes a WorkReleased event "([^"]*)" for work unit "([^"]*)" on path "([^"]*)" without any move object$`, w.workPlanningPublishesWithoutObject)
	sc.Step(`^work planning publishes a CloudEvent "([^"]*)" of type "([^"]*)"$`, w.workPlanningPublishesAnotherType)
	sc.Step(`^work planning publishes the raw message (.+)$`, w.workPlanningPublishesRaw)
	sc.Step(`^work planning has released a RELOCATE move "([^"]*)" of SKU "([^"]*)" in plan "([^"]*)" step (\d+) as work unit "([^"]*)" from bin "([^"]*)" to bin "([^"]*)"(?: due in (\d+) minutes)?$`, w.relocateMoveReleased)
	sc.Step(`^work planning has released a REPLENISH request "([^"]*)" of (\d+) units of SKU "([^"]*)" from bin "([^"]*)" to bin "([^"]*)" with priority "([^"]*)" as work unit "([^"]*)"(?: due in (\d+) minutes)?$`, w.replenishRequestReleased)
	sc.Step(`^work planning publishes a REPLENISH request whose id has (\d+) characters as work unit "([^"]*)"$`, w.replenishRequestOfIdLength)

	sc.Step(`^Station "([^"]*)" completes the claimed Task with the confirmation (.+)$`, w.stationCompletesWithConfirmation)
	sc.Step(`^the response is a client error$`, w.responseIsClientError)

	sc.Step(`^the response at "([^"]*)" is "([^"]*)"$`, w.responseAtIs)
	sc.Step(`^the response has nothing at "([^"]*)"$`, w.responseHasNothingAt)
	sc.Step(`^the task for order "([^"]*)" shows "([^"]*)" as "([^"]*)"$`, w.taskForOrderShows)
	sc.Step(`^the task for order "([^"]*)" shows nothing at "([^"]*)"$`, w.taskForOrderShowsNothing)

	sc.Step(`^facility-layout says location "([^"]*)" is a "([^"]*)"$`, w.facilityLayoutSays)
	sc.Step(`^facility-layout does not know any location$`, w.facilityLayoutKnowsNothing)
	sc.Step(`^facility-layout is unreachable$`, w.facilityLayoutIsUnreachable)
	sc.Step(`^the classification lookup fails for SKU "([^"]*)"$`, w.classificationLookupFailsFor)
	sc.Step(`^product-master reclassifies SKU "([^"]*)" under DOT hazard class (\d+) as version (\d+) in event "([^"]*)"$`, w.productMasterReclassifies)
	sc.Step(`^product-master classifies SKU "([^"]*)" as Hazmat without a DOT hazard class$`, w.productMasterHazmatNoClass)
	sc.Step(`^product-master classifies SKU "([^"]*)" as Fragile only$`, w.productMasterFragileOnly)
	sc.Step(`^product-master classifies SKU "([^"]*)" as Fragile only with a stray DOT hazard class (\d+)$`, w.productMasterFragileWithStrayClass)

	sc.Step(`^the recorded events are encoded for the integration topic$`, w.recordedEventsAreEncoded)
	sc.Step(`^the integration topic carries (\d+) "([^"]*)" messages?$`, w.integrationTopicCarries)
	sc.Step(`^the integration topic carries nothing$`, w.integrationTopicCarriesNothing)
	sc.Step(`^the "([^"]*)" integration message has "([^"]*)" equal to "([^"]*)"$`, w.integrationMessageHas)
	sc.Step(`^the "([^"]*)" integration message has no "([^"]*)"$`, w.integrationMessageHasNo)
	sc.Step(`^the "([^"]*)" integration message is keyed by its subject$`, w.integrationMessageKeyedBySubject)
	sc.Step(`^the "([^"]*)" integration messages have distinct CloudEvents ids$`, w.integrationMessagesHaveDistinctIds)

	sc.Step(`^product-master has declared the (EACH|CASE) GTIN "([0-9]+)" of SKU "([^"]*)"(?: with (\d+) units per pack)?$`, w.packLevelDeclared)
	sc.Step(`^Station "([^"]*)" scans "([0-9]+)"(?: x (\d+))? against the claimed Task$`, w.stationScansClaimedTaskStep)
	sc.Step(`^Station "([^"]*)" breaks the case of the claimed Task$`, w.stationBreaksTheCase)
	sc.Step(`^inventory-storage is unreachable for case breaks$`, w.inventoryStorageIsUnreachable)
	sc.Step(`^inventory-storage is reachable for case breaks again$`, w.inventoryStorageIsReachableAgain)
	sc.Step(`^inventory-storage received (\d+) case breaks? for LPN "([^"]*)" into bin "([^"]*)" under the idempotency key "([^"]*)"$`, w.inventoryStorageReceived)
}
