package main_test

// Wave-2 step definitions for the godog suite: Rebin consolidation, the
// WorkReleased / ProductClassified Kafka consumer paths, domain-event
// payload assertions and a few raw-request helpers for validation
// boundaries. Facts that arrive from Kafka are not reachable over REST, so
// they are seeded through the real consumers' message handlers (which call
// the same use cases the production consumers call) against the same
// in-memory adapters the REST handlers use; everything is then asserted
// through REST or the recorded domain events.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cucumber/godog"
	kafkago "github.com/segmentio/kafka-go"

	execmhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	kafkain "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// wave2State is the per-scenario state the wave-2 steps add to the world.
type wave2State struct {
	workConsumer       *kafkain.Consumer
	classifiedConsumer *kafkain.ProductClassifiedConsumer
	consumerErr        error
	deliveries         map[string][]byte
}

// wave2Catalogue mirrors warehouse-infra's declared process paths, the same
// fixture the consumer package's own tests use.
func wave2Catalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}},
		{Id: "PACK", MatchPrefix: "pack", Direct: true, RequiredCapabilities: []string{"pack"}},
		{Id: "REBIN", MatchPrefix: "rebin", Direct: true, RequiredCapabilities: []string{"rebin"}},
		{Id: "SLAM", MatchPrefix: "slam", Direct: true, RequiredCapabilities: []string{"slam"}},
		{Id: "DISPATCH", MatchPrefix: "dispatch", Direct: true, RequiredCapabilities: []string{"dispatch"}},
		{Id: "ARRIVAL", MatchPrefix: "arrival", Direct: true, RequiredCapabilities: []string{"arrival"}},
		{Id: "BREAK_CASE", MatchPrefix: "break", Direct: true, RequiredCapabilities: []string{"break"}},
	})
}

// wireWave2 adds the collaborators the wave-2 scenarios need to the
// handlers reset() just built: the Rebin use case, the product
// classification copy the Pack path reads, and the two Kafka consumers.
func (w *world) wireWave2(h *execmhttp.Handlers) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	h.ArriveAtRebin = &usecases.ArriveAtRebin{
		Consolidations: memory.NewOrderConsolidationRepo(),
		CreateTask:     h.CreateTask,
		Publisher:      h.CreateTask.Publisher,
		Clock:          h.CreateTask.Clock,
	}

	classifications := productclassificationcopy.NewMemoryStore()
	h.SealPackage.ClassificationLookup = classifications

	applyWork := &usecases.ApplyWorkReleased{
		CreateTask: h.CreateTask,
		Processed:  memory.NewProcessedEventsRepo(),
		Catalogue:  wave2Catalogue(),
		Logger:     quiet,
	}
	applyClassified := &usecases.ApplyProductClassified{
		Processed: memory.NewProcessedEventsRepo(),
		Copy:      classifications,
	}
	w.w2 = wave2State{
		workConsumer:       &kafkain.Consumer{Apply: applyWork, Logger: quiet},
		classifiedConsumer: kafkain.NewProductClassifiedConsumerFromReader(nil, kafkain.ProductMasterTopic, applyClassified, quiet),
		deliveries:         make(map[string][]byte),
	}
}

// --- raw request helpers ---------------------------------------------------

func (w *world) sendRaw(method, path, body string) error {
	req, err := http.NewRequest(method, w.server.URL+path, strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	w.status, w.body, w.header = resp.StatusCode, payload, resp.Header
	return nil
}

func (w *world) aPostIsSentWithJSONBody(path, body string) error {
	return w.sendRaw(http.MethodPost, path, body)
}

// --- generic response assertions --------------------------------------------

func (w *world) responseObject() (map[string]any, error) {
	var obj map[string]any
	if err := w.decodeLast(&obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (w *world) responseFieldIsString(field, want string) error {
	obj, err := w.responseObject()
	if err != nil {
		return err
	}
	if got, ok := obj[field].(string); !ok || got != want {
		return fmt.Errorf("expected response field %q to be %q, got %v", field, want, obj[field])
	}
	return nil
}

func (w *world) responseFieldIsBool(field, want string) error {
	obj, err := w.responseObject()
	if err != nil {
		return err
	}
	if got, ok := obj[field].(bool); !ok || strconv.FormatBool(got) != want {
		return fmt.Errorf("expected response field %q to be %s, got %v", field, want, obj[field])
	}
	return nil
}

func (w *world) responseHasNoField(field string) error {
	obj, err := w.responseObject()
	if err != nil {
		return err
	}
	if v, present := obj[field]; present {
		return fmt.Errorf("expected response to omit %q, got %v", field, v)
	}
	return nil
}

func (w *world) responseHeaderIs(name, want string) error {
	if got := w.header.Get(name); got != want {
		return fmt.Errorf("expected response header %s to be %q, got %q", name, want, got)
	}
	return nil
}

func (w *world) problemInstanceIs(want string) error {
	obj, err := w.responseObject()
	if err != nil {
		return err
	}
	if got, _ := obj["instance"].(string); got != want {
		return fmt.Errorf("expected problem instance %q, got %q", want, got)
	}
	return nil
}

func (w *world) problemHasNoInstance() error {
	return w.responseHasNoField("instance")
}

// --- task read-model assertions ---------------------------------------------

type taskView struct {
	Id                   string     `json:"id"`
	Type                 string     `json:"type"`
	Status               string     `json:"status"`
	CPT                  time.Time  `json:"cpt"`
	OrderRef             string     `json:"orderRef"`
	RequiredCapabilities []string   `json:"requiredCapabilities"`
	Fragile              bool       `json:"fragile"`
	GiftWrap             bool       `json:"giftWrap"`
	LeaseStationId       *string    `json:"leaseStationId"`
	LeaseExpiry          *time.Time `json:"leaseExpiry"`
}

func (w *world) tasksFor(orderRef string) ([]taskView, error) {
	status, body, _, err := w.call(http.MethodGet, "/tasks?orderRef="+url.QueryEscape(orderRef), nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("looking up tasks for %q: expected 200, got %d: %s", orderRef, status, body)
	}
	var tasks []taskView
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, fmt.Errorf("decode tasks %q: %w", string(body), err)
	}
	return tasks, nil
}

func (w *world) onlyTask(orderRef, taskType string) (taskView, error) {
	tasks, err := w.tasksFor(orderRef)
	if err != nil {
		return taskView{}, err
	}
	var matches []taskView
	for _, t := range tasks {
		if t.Type == taskType {
			matches = append(matches, t)
		}
	}
	if len(matches) != 1 {
		return taskView{}, fmt.Errorf("expected exactly one %s task for order %q, got %d", taskType, orderRef, len(matches))
	}
	return matches[0], nil
}

func (w *world) orderHasTasksOfType(orderRef string, expected int, taskType string) error {
	tasks, err := w.tasksFor(orderRef)
	if err != nil {
		return err
	}
	count := 0
	for _, t := range tasks {
		if t.Type == taskType {
			count++
		}
	}
	if count != expected {
		return fmt.Errorf("expected %d %s task(s) for order %q, got %d", expected, taskType, orderRef, count)
	}
	return nil
}

func (w *world) taskHasStatus(taskType, orderRef, status string) error {
	t, err := w.onlyTask(orderRef, taskType)
	if err != nil {
		return err
	}
	if t.Status != status {
		return fmt.Errorf("expected the %s task for %q to be %s, got %s", taskType, orderRef, status, t.Status)
	}
	return nil
}

func (w *world) taskRequiresCapabilities(taskType, orderRef, capabilities string) error {
	t, err := w.onlyTask(orderRef, taskType)
	if err != nil {
		return err
	}
	got := append([]string(nil), t.RequiredCapabilities...)
	want := splitList(capabilities)
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		return fmt.Errorf("expected the %s task for %q to require %v, got %v", taskType, orderRef, want, got)
	}
	return nil
}

func (w *world) taskPackingFlags(taskType, orderRef, fragile, giftWrap string) error {
	t, err := w.onlyTask(orderRef, taskType)
	if err != nil {
		return err
	}
	if strconv.FormatBool(t.Fragile) != fragile || strconv.FormatBool(t.GiftWrap) != giftWrap {
		return fmt.Errorf("expected fragile=%s giftWrap=%s on the %s task for %q, got fragile=%v giftWrap=%v",
			fragile, giftWrap, taskType, orderRef, t.Fragile, t.GiftWrap)
	}
	return nil
}

func (w *world) taskIsDueAfterStart(taskType, orderRef string, minutes int) error {
	t, err := w.onlyTask(orderRef, taskType)
	if err != nil {
		return err
	}
	if want := scenarioStart.Add(time.Duration(minutes) * time.Minute); !t.CPT.Equal(want) {
		return fmt.Errorf("expected the %s task for %q to be due at %s, got %s", taskType, orderRef, want, t.CPT)
	}
	return nil
}

func (w *world) claimedTaskBelongsToOrder(taskType, orderRef string) error {
	var claimed struct {
		Type     string `json:"type"`
		OrderRef string `json:"orderRef"`
	}
	if err := json.Unmarshal(w.claimBody, &claimed); err != nil {
		return fmt.Errorf("decode claimed task %q: %w", string(w.claimBody), err)
	}
	if claimed.Type != taskType || claimed.OrderRef != orderRef {
		return fmt.Errorf("expected the claimed task to be a %s task for order %q, got %s for %q", taskType, orderRef, claimed.Type, claimed.OrderRef)
	}
	return nil
}

// --- domain-event payload assertions -----------------------------------------

func tablePairs(table *godog.Table) map[string]string {
	pairs := make(map[string]string, len(table.Rows))
	for _, row := range table.Rows {
		if len(row.Cells) >= 2 {
			pairs[row.Cells[0].Value] = row.Cells[1].Value
		}
	}
	return pairs
}

// eventFields renders a recorded domain event as field name -> text, using
// the event's own exported fields (promoted from its embedded base and
// transfer-detail structs by encoding/json).
func eventFields(e shared.DomainEvent) (map[string]string, error) {
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("marshal %s event: %w", e.EventName(), err)
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, fmt.Errorf("decode %s event: %w", e.EventName(), err)
	}
	fields := make(map[string]string, len(generic))
	for k, v := range generic {
		fields[k] = fmt.Sprint(v)
	}
	return fields, nil
}

func fieldsInclude(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func (w *world) domainEventCarries(name string, table *godog.Table) error {
	want := tablePairs(table)
	var seen []string
	for _, e := range w.publisher.Events() {
		if e.EventName() != name {
			continue
		}
		got, err := eventFields(e)
		if err != nil {
			return err
		}
		if fieldsInclude(got, want) {
			return nil
		}
		seen = append(seen, fmt.Sprint(got))
	}
	return fmt.Errorf("expected a %s domain event carrying %v, saw %v", name, want, seen)
}

func (w *world) noTransferFactIsRecorded() error {
	for _, name := range []string{"TransferPicked", "TransferDispatched", "TransferArrived"} {
		if n := w.countEvents(name); n != 0 {
			return fmt.Errorf("expected no transfer fact, got %d %s", n, name)
		}
	}
	return nil
}

// --- Rebin ---------------------------------------------------------------------

type rebinArrival struct {
	orderRef, lineId string
	required         []string
	cptMinutes       int
	capabilities     []string
	fragile          bool
	giftWrap         bool
}

func (w *world) arriveAtRebin(a rebinArrival) error {
	return w.do(http.MethodPost, "/rebin/arrivals", map[string]any{
		"orderRef":                 a.orderRef,
		"lineId":                   a.lineId,
		"requiredLineIds":          a.required,
		"packCpt":                  w.clock.Now().Add(time.Duration(a.cptMinutes) * time.Minute),
		"packRequiredCapabilities": a.capabilities,
		"packFragile":              a.fragile,
		"packGiftWrap":             a.giftWrap,
	})
}

func (w *world) lineArrivesAtRebin(lineId, orderRef, required string) error {
	return w.arriveAtRebin(rebinArrival{
		orderRef: orderRef, lineId: lineId, required: splitList(required),
		cptMinutes: 60, capabilities: []string{"pack"},
	})
}

func (w *world) lineArrivesAtRebinWithPackHints(lineId, orderRef, required string, cptMinutes int, capabilities, fragile, giftWrap string) error {
	return w.arriveAtRebin(rebinArrival{
		orderRef: orderRef, lineId: lineId, required: splitList(required),
		cptMinutes: cptMinutes, capabilities: splitList(capabilities),
		fragile: fragile == "true", giftWrap: giftWrap == "true",
	})
}

func (w *world) lineHasArrivedAtRebin(lineId, orderRef, required string) error {
	if err := w.lineArrivesAtRebin(lineId, orderRef, required); err != nil {
		return err
	}
	if w.status != http.StatusNoContent {
		return fmt.Errorf("line %q arriving for order %q: expected 204, got %d: %s", lineId, orderRef, w.status, w.body)
	}
	return nil
}

// --- Kafka consumers -------------------------------------------------------------

func cloudEventBytes(id, eventType string, data any) ([]byte, error) {
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/bdd")
	e.SetType(eventType)
	e.SetSubject(id)
	e.SetTime(scenarioStart)
	if err := e.SetData(ce.ApplicationJSON, data); err != nil {
		return nil, fmt.Errorf("set cloudevent data: %w", err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("marshal cloudevent: %w", err)
	}
	return raw, nil
}

// workReleasedData builds a WorkReleased payload from a field/value table.
// cpt_minutes is relative to the scenario clock; line_no and quantity are
// numbers, fragile and gift_wrap booleans, everything else a string.
func workReleasedData(now time.Time, table *godog.Table) (map[string]any, error) {
	data := make(map[string]any, len(table.Rows))
	for key, value := range tablePairs(table) {
		switch key {
		case "cpt_minutes":
			minutes, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("cpt_minutes %q: %w", value, err)
			}
			data["cpt"] = now.Add(time.Duration(minutes) * time.Minute)
		case "line_no", "quantity":
			n, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("%s %q: %w", key, value, err)
			}
			data[key] = n
		case "fragile", "gift_wrap":
			data[key] = value == "true"
		case "case_break.lpn", "case_break.sku", "case_break.loose_bin", "case_break.pick_task_id":
			// The nested WorkReleased.data.case_break object (ADR 0054).
			nested, _ := data["case_break"].(map[string]any)
			if nested == nil {
				nested = make(map[string]any)
				data["case_break"] = nested
			}
			nested[strings.TrimPrefix(key, "case_break.")] = value
		default:
			data[key] = value
		}
	}
	return data, nil
}

func (w *world) deliverWorkReleased(raw []byte) error {
	w.w2.consumerErr = w.w2.workConsumer.HandleMessage(context.Background(), raw)
	return nil
}

func (w *world) workPlanningPublishes(eventId string, table *godog.Table) error {
	data, err := workReleasedData(w.clock.Now(), table)
	if err != nil {
		return err
	}
	raw, err := cloudEventBytes(eventId, kafkain.TypeWorkReleased, data)
	if err != nil {
		return err
	}
	w.w2.deliveries[eventId] = raw
	return w.deliverWorkReleased(raw)
}

func (w *world) workPlanningPublishesAgain(eventId string) error {
	raw, ok := w.w2.deliveries[eventId]
	if !ok {
		return fmt.Errorf("no WorkReleased event %q was published earlier", eventId)
	}
	return w.deliverWorkReleased(raw)
}

func (w *world) theConsumerAccepts() error {
	if w.w2.consumerErr != nil {
		return fmt.Errorf("expected the consumer to accept the message, got: %w", w.w2.consumerErr)
	}
	return nil
}

func (w *world) theConsumerRejects() error {
	if w.w2.consumerErr == nil {
		return fmt.Errorf("expected the consumer to reject the message, but it was accepted")
	}
	return nil
}

func (w *world) productMasterClassifies(sku string, hazardClass int) error {
	raw, err := cloudEventBytes("classified-"+sku, kafkain.TypeProductClassified, map[string]any{
		"sku": sku, "handling_tags": []string{"Hazmat"}, "dot_hazard_class": hazardClass, "version": 1,
	})
	if err != nil {
		return err
	}
	return w.w2.classifiedConsumer.Handle(context.Background(), kafkago.Message{Topic: kafkain.ProductMasterTopic, Value: raw})
}

// --- sealed package assertions -------------------------------------------------------

func (w *world) theSealedPackageIsHandledAs(fragile, giftWrap, sortLane string) error {
	var sealed struct {
		FragileHandling   bool   `json:"fragileHandling"`
		GiftWrapRequested bool   `json:"giftWrapRequested"`
		SortLane          string `json:"sortLane"`
	}
	if err := json.Unmarshal(w.sealBody, &sealed); err != nil {
		return fmt.Errorf("decode sealed package %q: %w", string(w.sealBody), err)
	}
	if strconv.FormatBool(sealed.FragileHandling) != fragile ||
		strconv.FormatBool(sealed.GiftWrapRequested) != giftWrap ||
		sealed.SortLane != sortLane {
		return fmt.Errorf("expected fragile=%s giftWrap=%s lane=%s, got %+v", fragile, giftWrap, sortLane, sealed)
	}
	return nil
}

// registerWave2Steps registers the wave-2 vocabulary on sc.
func (w *world) registerWave2Steps(sc *godog.ScenarioContext) {
	sc.Step(`^a POST is sent to "([^"]*)" with the JSON body (.+)$`, w.aPostIsSentWithJSONBody)

	sc.Step(`^the response field "([^"]*)" is "([^"]*)"$`, w.responseFieldIsString)
	sc.Step(`^the response field "([^"]*)" is (true|false)$`, w.responseFieldIsBool)
	sc.Step(`^the response has no "([^"]*)" field$`, w.responseHasNoField)
	sc.Step(`^the response header "([^"]*)" is "([^"]*)"$`, w.responseHeaderIs)
	sc.Step(`^the Problem Details document has instance "([^"]*)"$`, w.problemInstanceIs)
	sc.Step(`^the Problem Details document has no instance$`, w.problemHasNoInstance)

	sc.Step(`^the order "([^"]*)" has (\d+) "([^"]*)" Tasks?$`, w.orderHasTasksOfType)
	sc.Step(`^the "([^"]*)" Task for order "([^"]*)" has status "([^"]*)"$`, w.taskHasStatus)
	sc.Step(`^the "([^"]*)" Task for order "([^"]*)" requires capabilities "([^"]*)"$`, w.taskRequiresCapabilities)
	sc.Step(`^the "([^"]*)" Task for order "([^"]*)" is flagged fragile (true|false) and gift wrap (true|false)$`, w.taskPackingFlags)
	sc.Step(`^the "([^"]*)" Task for order "([^"]*)" is due (\d+) minutes after the scenario start$`, w.taskIsDueAfterStart)
	sc.Step(`^the claimed Task is a "([^"]*)" Task for order "([^"]*)"$`, w.claimedTaskBelongsToOrder)

	sc.Step(`^the "([^"]*)" domain event carries:$`, w.domainEventCarries)
	sc.Step(`^no transfer fact is recorded$`, w.noTransferFactIsRecorded)

	sc.Step(`^line "([^"]*)" of order "([^"]*)" arrives at Rebin; the order needs lines "([^"]*)"$`, w.lineArrivesAtRebin)
	sc.Step(`^line "([^"]*)" of order "([^"]*)" has arrived at Rebin; the order needs lines "([^"]*)"$`, w.lineHasArrivedAtRebin)
	sc.Step(`^line "([^"]*)" of order "([^"]*)" arrives at Rebin; the order needs lines "([^"]*)" and its PACK task is due in (\d+) minutes, requires capabilities "([^"]*)", fragile (true|false), gift wrap (true|false)$`,
		w.lineArrivesAtRebinWithPackHints)

	sc.Step(`^work planning publishes a WorkReleased event "([^"]*)" with:$`, w.workPlanningPublishes)
	sc.Step(`^work planning publishes the WorkReleased event "([^"]*)" again$`, w.workPlanningPublishesAgain)
	sc.Step(`^the consumer accepts the message$`, w.theConsumerAccepts)
	sc.Step(`^the consumer rejects the message$`, w.theConsumerRejects)
	sc.Step(`^product-master classifies SKU "([^"]*)" under DOT hazard class (\d+)$`, w.productMasterClassifies)

	sc.Step(`^the sealed Package is fragile (true|false), gift wrap (true|false) and routed to the "([^"]*)" lane$`, w.theSealedPackageIsHandledAs)
}
