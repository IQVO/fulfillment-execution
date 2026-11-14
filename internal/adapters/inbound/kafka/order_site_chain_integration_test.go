//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TestOrderSiteChain_WorkReleasedToPackageManifested walks the whole path of
// ADR 0065 on real Postgres: a WorkReleased that carries site_id for order
// work is consumed into PICK tasks (tasks.order_site_id, migration 0028),
// ArriveAtRebin gives the order's PACK task the PICK tasks' site through
// TaskRepo.FindBySourceOrderId, and the PackageManifested encoded for that
// order carries site_id and the PACK task's CPT (no SLAM task exists). An
// order whose release named no site publishes neither site_id nor a wrong one.
func TestOrderSiteChain_WorkReleasedToPackageManifested(t *testing.T) {
	ctx := context.Background()
	pool := transferDB(t)
	tasks := postgres.NewTaskRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	clock := fixedClockAt(time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC))

	n := 0
	newId := func() shared.TaskId { n++; return shared.TaskId(fmt.Sprintf("t-site-%d", n)) }
	ceId := 0
	pub := postgres.NewOutboxPublisher(pool, outboundkafka.NewPublisherWithWriter(nil, tasks, nil, func() string { ceId++; return fmt.Sprintf("ce-site-%d", ceId) }))
	create := &usecases.CreateTask{Tasks: tasks, Publisher: pub, Clock: clock, NewId: newId, UnitOfWork: uow}
	apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: postgres.NewProcessedEventsRepo(pool), Catalogue: testCatalogue(), UnitOfWork: uow}
	consumer := &inboundkafka.Consumer{Apply: apply, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	rebin := &usecases.ArriveAtRebin{Consolidations: postgres.NewOrderConsolidationRepo(pool), CreateTask: create, Publisher: pub, Clock: clock, UnitOfWork: uow}

	release := func(eventId, orderId string, line int, extra map[string]any) {
		t.Helper()
		unit := fmt.Sprintf("%s-line-%d", orderId, line)
		data := map[string]any{"path_id": "PICK", "work_unit_id": unit, "cpt": "2026-11-14T18:00:00Z", "ref": orderId, "line_no": line}
		for k, v := range extra {
			data[k] = v
		}
		if err := consumer.HandleMessage(ctx, workReleasedCE(eventId, inboundkafka.TypeWorkReleased, unit, data)); err != nil {
			t.Fatalf("consume %s: %v", eventId, err)
		}
	}

	// Order A named a site on its first line's release only (an order is
	// released to one site; the first non-empty one speaks for it). Order B
	// named none.
	release("e-a1", "ORD-A", 1, map[string]any{"site_id": "WH1"})
	release("e-a2", "ORD-A", 2, nil)
	release("e-b1", "ORD-B", 1, nil)
	release("e-bad", "ORD-C", 1, map[string]any{"site_id": "wh-1"}) // invalid: tolerated as no site

	picks, err := tasks.FindBySourceOrderId(ctx, "ORD-A")
	if err != nil || len(picks) != 2 {
		t.Fatalf("FindBySourceOrderId(ORD-A) = %d tasks, err %v, want 2", len(picks), err)
	}
	sites := []string{picks[0].OrderSiteId(), picks[1].OrderSiteId()}
	if !(sites[0] == "WH1" && sites[1] == "") && !(sites[0] == "" && sites[1] == "WH1") {
		t.Fatalf("ORD-A PICK order sites = %q, want one WH1 and one empty", sites)
	}
	if c, _ := tasks.FindBySourceOrderId(ctx, "ORD-C"); len(c) != 1 || c[0].OrderSiteId() != "" {
		t.Fatalf("ORD-C (invalid site_id) must still create its PICK task, without a site: %+v", c)
	}

	packCPT := shared.NewCPT(time.Date(2026, 11, 14, 21, 0, 0, 0, time.UTC))
	for _, order := range []string{"ORD-A", "ORD-B"} {
		if err := rebin.Execute(ctx, shared.OrderRef(order), "line-1", []string{"line-1"}, packCPT, shared.NewCapabilitySet("pack"), false, false); err != nil {
			t.Fatalf("rebin %s: %v", order, err)
		}
	}

	encoder := outboundkafka.NewPublisherWithWriter(nil, tasks, nil, func() string { return "ce-site-chain" })
	manifested := func(order string) map[string]any {
		t.Helper()
		encoded, err := encoder.Encode(ctx, shared.NewPackageManifestedWithWeight(shared.PackageId("pkg-"+order), shared.OrderRef(order), 980, time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)))
		if err != nil || len(encoded) != 1 {
			t.Fatalf("encode %s: %v (%d messages)", order, err, len(encoded))
		}
		env, err := cloudevents.Decode(encoded[0].Value)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		var data map[string]any
		if err := json.Unmarshal(env.Data(), &data); err != nil {
			t.Fatalf("data: %v", err)
		}
		return data
	}

	a := manifested("ORD-A")
	if a["site_id"] != "WH1" || a["cpt"] != "2026-11-14T21:00:00Z" || a["source_order_id"] != "ORD-A" {
		t.Fatalf("ORD-A manifest data = %v, want site_id WH1, the PACK task's cpt 2026-11-14T21:00:00Z and source_order_id ORD-A", a)
	}
	b := manifested("ORD-B")
	if _, present := b["site_id"]; present {
		t.Fatalf("ORD-B named no site: site_id must be omitted, got %v", b)
	}
	if b["cpt"] != "2026-11-14T21:00:00Z" {
		t.Fatalf("ORD-B cpt = %v, want the PACK task's 2026-11-14T21:00:00Z even without a site", b["cpt"])
	}

	// The persisted PACK task itself.
	packs, _ := tasks.FindByOrderRef(ctx, "ORD-A")
	if len(packs) != 1 || packs[0].Type() != task.Pack || packs[0].OrderSiteId() != "WH1" {
		t.Fatalf("ORD-A PACK task = %+v, want one PACK task at WH1", packs)
	}
}
