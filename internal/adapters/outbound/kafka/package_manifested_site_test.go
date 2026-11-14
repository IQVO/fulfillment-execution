package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// ADR 0065: PackageManifested publishes site_id, sourced from the order's
// PACK (or SLAM) task, and falls back to the PACK task's CPT when no SLAM
// task exists. These tests pin the exact wire bytes for every source
// combination, including the omit-when-unknown cases.

const manifestedEnvelopePrefix = `{
	"specversion":"1.0",
	"id":"` + goldenID + `",
	"source":"/warehouse/fulfillment-execution",
	"type":"com.warehouse.wes.fulfillment-execution.package.PackageManifested",
	"subject":"pkg-1029",
	"time":"2026-09-30T15:00:00Z",
	"datacontenttype":"application/json",
	"dataschema":"urn:warehouse:fulfillment-execution:events:PackageManifested:v1",
	"data":`

func manifestedWire(data string) string { return manifestedEnvelopePrefix + data + "}" }

// packAt builds a PACK task of orderRef at cpt, stamped with the order id and
// the order site ("" = no site), as ArriveAtRebin creates it.
func packAt(id, orderRef, site string, cpt time.Time) *task.Task {
	return task.New(shared.TaskId(id), task.Pack, shared.NewCPT(cpt), shared.OrderRef(orderRef), shared.NewCapabilitySet(), false, false).
		WithSourceOrderId("order-9").WithOrderSiteId(site)
}

// slamAt builds a claimed SLAM task of orderRef at cpt.
func slamAt(t *testing.T, id, orderRef, site string, cpt time.Time) *task.Task {
	t.Helper()
	slam := task.New(shared.TaskId(id), task.Slam, shared.NewCPT(cpt), shared.OrderRef(orderRef), shared.NewCapabilitySet(), false, false).
		WithSourceOrderId("order-9").WithOrderSiteId(site)
	if err := slam.Claim("station-09", shared.NewCapabilitySet(), goldenAt.Add(-time.Minute), time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	return slam
}

func publishManifested(t *testing.T, tasks *memory.TaskRepo, orderRef string) []byte {
	t.Helper()
	w := &fakeWriter{}
	p := outboundkafka.NewPublisherWithWriter(w, tasks, nil, goldenID1)
	if err := p.Publish(context.Background(), shared.NewPackageManifestedWithWeight("pkg-1029", shared.OrderRef(orderRef), 980, goldenAt)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(w.msgs))
	}
	return w.msgs[0].Value
}

func saveAll(t *testing.T, tasks *memory.TaskRepo, ts ...*task.Task) {
	t.Helper()
	for _, tk := range ts {
		if err := tasks.Save(context.Background(), tk); err != nil {
			t.Fatalf("save %s: %v", tk.Id(), err)
		}
	}
}

// The rebin path — the one every customer order takes: no SLAM task exists,
// so cpt is the PACK task's CPT (the order's promise date) and site_id is
// the PACK task's site (inherited from the PICK tasks).
func TestGolden_Integration_PackageManifested_PackTaskSourcesSiteAndCpt(t *testing.T) {
	tasks := memory.NewTaskRepo()
	saveAll(t, tasks, packAt("pack-9", "order-9", "WH1", goldenAt.Add(3*time.Hour)))

	assertJSONEqual(t, publishManifested(t, tasks, "order-9"), manifestedWire(
		`{"package_id":"pkg-1029","order_ref":"order-9","site_id":"WH1","source_order_id":"order-9","cpt":"2026-09-30T18:00:00Z","weight_g":980}`))
}

// A PACK task without a site (an order that named none, or a PACK created
// before migration 0028) keeps the key OUT of the payload — never "" — while
// the cpt fallback still applies.
func TestGolden_Integration_PackageManifested_NoSiteOmitsSiteId(t *testing.T) {
	tasks := memory.NewTaskRepo()
	saveAll(t, tasks, packAt("pack-9", "order-9", "", goldenAt.Add(3*time.Hour)))

	raw := publishManifested(t, tasks, "order-9")
	assertJSONEqual(t, raw, manifestedWire(
		`{"package_id":"pkg-1029","order_ref":"order-9","source_order_id":"order-9","cpt":"2026-09-30T18:00:00Z","weight_g":980}`))
	var env struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if _, present := env.Data["site_id"]; present {
		t.Fatalf("site_id must be omitted, not empty, when the order has no site: %s", raw)
	}
}

// Existing SLAM behaviour wins: when a SLAM task exists its CPT is the
// manifest's cpt (the PACK task's differs here, so the source is visible).
// The site is the first one found, SLAM before PACK.
func TestGolden_Integration_PackageManifested_SlamCptWinsOverPackCpt(t *testing.T) {
	tasks := memory.NewTaskRepo()
	saveAll(t, tasks,
		slamAt(t, "slam-9", "order-9", "", goldenAt.Add(30*time.Minute)),
		packAt("pack-9", "order-9", "WH1", goldenAt.Add(3*time.Hour)))

	assertJSONEqual(t, publishManifested(t, tasks, "order-9"), manifestedWire(
		`{"package_id":"pkg-1029","order_ref":"order-9","site_id":"WH1","source_order_id":"order-9","cpt":"2026-09-30T15:30:00Z","weight_g":980}`))
}

func TestGolden_Integration_PackageManifested_SlamSiteBeatsPackSite(t *testing.T) {
	tasks := memory.NewTaskRepo()
	saveAll(t, tasks,
		slamAt(t, "slam-9", "order-9", "WH2", goldenAt.Add(30*time.Minute)),
		packAt("pack-9", "order-9", "WH1", goldenAt.Add(3*time.Hour)))

	assertJSONEqual(t, publishManifested(t, tasks, "order-9"), manifestedWire(
		`{"package_id":"pkg-1029","order_ref":"order-9","site_id":"WH2","source_order_id":"order-9","cpt":"2026-09-30T15:30:00Z","weight_g":980}`))
}

// A retried PACK leg leaves several PACK tasks for one order: the one that
// was actually worked (the package was sealed from it) speaks for the
// manifest, not a pending leftover with a different CPT and site.
func TestGolden_Integration_PackageManifested_WorkedPackTaskWins(t *testing.T) {
	tasks := memory.NewTaskRepo()
	worked := packAt("pack-b", "order-9", "WH1", goldenAt.Add(3*time.Hour))
	if err := worked.Claim("station-05", shared.NewCapabilitySet(), goldenAt.Add(-time.Minute), time.Hour); err != nil {
		t.Fatal(err)
	}
	saveAll(t, tasks,
		packAt("pack-a", "order-9", "WH2", goldenAt.Add(5*time.Hour)), // pending leftover, lower id
		worked)

	assertJSONEqual(t, publishManifested(t, tasks, "order-9"), manifestedWire(
		`{"package_id":"pkg-1029","order_ref":"order-9","site_id":"WH1","source_order_id":"order-9","cpt":"2026-09-30T18:00:00Z","weight_g":980}`))
}

// Only the manifest's own PACK/SLAM legs speak for it: another task type
// sharing the orderRef contributes neither a site nor a cpt.
func TestGolden_Integration_PackageManifested_OtherTaskTypesDoNotSourceSiteOrCpt(t *testing.T) {
	tasks := memory.NewTaskRepo()
	pick := task.New("pick-9", task.Pick, shared.NewCPT(goldenAt.Add(time.Hour)), "order-9", shared.NewCapabilitySet(), false, false).
		WithSourceOrderId("order-9").WithOrderSiteId("WH1")
	saveAll(t, tasks, pick)

	assertJSONEqual(t, publishManifested(t, tasks, "order-9"), manifestedWire(
		`{"package_id":"pkg-1029","order_ref":"order-9","source_order_id":"order-9","weight_g":980}`))
}

// A PACK task with no CPT (zero instant) must not publish the zero time as
// the cpt; the site still publishes.
func TestGolden_Integration_PackageManifested_ZeroPackCptIsOmitted(t *testing.T) {
	tasks := memory.NewTaskRepo()
	saveAll(t, tasks, packAt("pack-9", "order-9", "WH1", time.Time{}))

	assertJSONEqual(t, publishManifested(t, tasks, "order-9"), manifestedWire(
		`{"package_id":"pkg-1029","order_ref":"order-9","site_id":"WH1","source_order_id":"order-9","weight_g":980}`))
}

// The cpt is published in UTC RFC3339 whatever zone the task's CPT carries.
func TestGolden_Integration_PackageManifested_PackCptIsUTC(t *testing.T) {
	tasks := memory.NewTaskRepo()
	brt := time.FixedZone("BRT", -3*3600)
	saveAll(t, tasks, packAt("pack-9", "order-9", "WH1", time.Date(2026, 9, 30, 18, 0, 0, 0, brt)))

	assertJSONEqual(t, publishManifested(t, tasks, "order-9"), manifestedWire(
		`{"package_id":"pkg-1029","order_ref":"order-9","site_id":"WH1","source_order_id":"order-9","cpt":"2026-09-30T21:00:00Z","weight_g":980}`))
}

// An invalid site never reaches a persisted task (the domain holds none), so
// nothing malformed can be published; a task rehydrated with one drops it.
func TestGolden_Integration_PackageManifested_InvalidSiteIsNeverPublished(t *testing.T) {
	tasks := memory.NewTaskRepo()
	saveAll(t, tasks, packAt("pack-9", "order-9", "wh-1", goldenAt.Add(3*time.Hour)))

	raw := publishManifested(t, tasks, "order-9")
	var env struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if _, present := env.Data["site_id"]; present {
		t.Fatalf("an invalid site must not be published: %s", raw)
	}
}
