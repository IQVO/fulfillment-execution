package usecases_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// siteCatalogue is the process-path catalogue with the order paths and the
// RELOCATE path.
func siteCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}},
		{Id: "PACK", MatchPrefix: "pack", Direct: true, RequiredCapabilities: []string{"pack"}},
		{Id: "RELOCATE", MatchPrefix: "relocate", Direct: true, RequiredCapabilities: []string{"relocate"}},
	})
}

// siteApply builds an ApplyWorkReleased over a fresh in-memory repo whose
// catalogue knows the PICK, PACK and RELOCATE paths, logging into buf.
func siteApply(buf *bytes.Buffer) (*usecases.ApplyWorkReleased, *memory.TaskRepo) {
	tasks := memory.NewTaskRepo()
	create := &usecases.CreateTask{Tasks: tasks, Publisher: events.NewBufferedPublisher(), Clock: memory.NewFixedClock(atomicEpoch), NewId: fixedTaskId("s")}
	apply := &usecases.ApplyWorkReleased{
		CreateTask: create,
		Processed:  memory.NewProcessedEventsRepo(),
		Catalogue:  siteCatalogue(),
		Logger:     slog.New(slog.NewTextHandler(buf, nil)),
	}
	return apply, tasks
}

// TestApplyWorkReleased_StampsOrderSiteFromSiteId pins ADR 0065: the site
// the released ORDER named (WorkReleased.site_id) is kept on the order-work
// task as its order site — on every order path, not only PICK. An absent or
// malformed site is tolerated as "no site": the task is still created and
// the message never fails.
func TestApplyWorkReleased_StampsOrderSiteFromSiteId(t *testing.T) {
	cpt := atomicEpoch.Add(time.Hour)
	for _, tc := range []struct {
		name     string
		req      usecases.WorkReleasedRequest
		want     string
		wantWarn bool
	}{
		{"order PICK with a site", usecases.WorkReleasedRequest{EventId: "e1", PathId: "PICK", WorkUnitId: "ORD-9-line-1", Ref: "ORD-9", CPT: cpt, SiteId: "WH1"}, "WH1", false},
		{"order PACK with a site", usecases.WorkReleasedRequest{EventId: "e2", PathId: "PACK", WorkUnitId: "ORD-9", Ref: "ORD-9", CPT: cpt, SiteId: "WH1"}, "WH1", false},
		{"digits-only site", usecases.WorkReleasedRequest{EventId: "e3", PathId: "PICK", WorkUnitId: "ORD-9-line-1", Ref: "ORD-9", CPT: cpt, SiteId: "42"}, "42", false},
		{"no site stays unknown", usecases.WorkReleasedRequest{EventId: "e4", PathId: "PICK", WorkUnitId: "ORD-9-line-1", Ref: "ORD-9", CPT: cpt}, "", false},
		{"lower-case site is tolerated as no site, with a WARN", usecases.WorkReleasedRequest{EventId: "e5", PathId: "PICK", WorkUnitId: "ORD-9-line-1", Ref: "ORD-9", CPT: cpt, SiteId: "wh1"}, "", true},
		{"hyphenated site is tolerated as no site, with a WARN", usecases.WorkReleasedRequest{EventId: "e6", PathId: "PICK", WorkUnitId: "ORD-9-line-1", Ref: "ORD-9", CPT: cpt, SiteId: "WH-1"}, "", true},
		{"blank site is tolerated as no site, with a WARN", usecases.WorkReleasedRequest{EventId: "e7", PathId: "PICK", WorkUnitId: "ORD-9-line-1", Ref: "ORD-9", CPT: cpt, SiteId: " "}, "", true},
		{"transfer work never takes an order site", usecases.WorkReleasedRequest{EventId: "e8", PathId: "PICK", WorkUnitId: "demand-1", Ref: "demand-1", CPT: cpt, SiteId: "WH1",
			Transfer: &task.TransferDetails{TransferRef: "tr-1", DemandId: "demand-1", WorkKind: task.WorkKindTransferPick, SiteId: "WH1"}}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			apply, tasks := siteApply(&buf)
			if err := apply.Execute(context.Background(), tc.req); err != nil {
				t.Fatalf("apply: %v", err)
			}
			all, err := tasks.FindByOrderRef(context.Background(), shared.OrderRef(tc.req.WorkUnitId))
			if err != nil || len(all) != 1 {
				t.Fatalf("expected 1 task, got %d err=%v", len(all), err)
			}
			if got := all[0].OrderSiteId(); got != tc.want {
				t.Errorf("OrderSiteId = %q, want %q", got, tc.want)
			}
			out := buf.String()
			if tc.wantWarn && (!strings.Contains(out, "level=WARN") || !strings.Contains(out, tc.req.EventId) || !strings.Contains(out, "site")) {
				t.Errorf("want a WARN naming event %s and the site, got %q", tc.req.EventId, out)
			}
			if !tc.wantWarn && out != "" {
				t.Errorf("want no log output, got %q", out)
			}
		})
	}
}

// A transfer release keeps its site only inside the transfer block (the
// fact it publishes); the byte-identical transfer behaviour is untouched.
func TestApplyWorkReleased_TransferKeepsItsSiteInTheTransferBlockOnly(t *testing.T) {
	var buf bytes.Buffer
	apply, tasks := siteApply(&buf)
	req := usecases.WorkReleasedRequest{EventId: "e-tr", PathId: "PICK", WorkUnitId: "demand-1", Ref: "demand-1", CPT: atomicEpoch.Add(time.Hour), SiteId: "WH1",
		Transfer: &task.TransferDetails{TransferRef: "tr-1", DemandId: "demand-1", WorkKind: task.WorkKindTransferPick, SiteId: "ORIGIN"}}
	if err := apply.Execute(context.Background(), req); err != nil {
		t.Fatalf("apply: %v", err)
	}
	all, _ := tasks.FindByOrderRef(context.Background(), "demand-1")
	if len(all) != 1 {
		t.Fatalf("expected 1 task, got %d", len(all))
	}
	if all[0].OrderSiteId() != "" || all[0].Transfer() == nil || all[0].Transfer().SiteId != "ORIGIN" || all[0].SourceOrderId() != "" {
		t.Fatalf("transfer task: order site %q, transfer %+v, source order %q — want no order site, the transfer site ORIGIN, no source order", all[0].OrderSiteId(), all[0].Transfer(), all[0].SourceOrderId())
	}
}

// Relocation work is not order work: its release carries a site_id (the
// plan's site) but the task keeps no order site, and even a malformed one
// does not warn about an order site that was never going to be kept.
func TestApplyWorkReleased_RelocationIgnoresSiteId(t *testing.T) {
	for _, site := range []string{"WH1", "wh-1"} {
		t.Run(site, func(t *testing.T) {
			var buf bytes.Buffer
			apply, tasks := siteApply(&buf)
			req := usecases.WorkReleasedRequest{EventId: "e-rel", PathId: "RELOCATE", WorkUnitId: "wu-rel", CPT: atomicEpoch.Add(time.Hour), SiteId: site,
				Relocation: relocationMove(t, "Relocate", "A", "B")}
			if err := apply.Execute(context.Background(), req); err != nil {
				t.Fatalf("apply: %v", err)
			}
			all, _ := tasks.FindByOrderRef(context.Background(), "wu-rel")
			if len(all) != 1 || all[0].OrderSiteId() != "" {
				t.Fatalf("tasks = %d, order site %q, want one RELOCATE task with no order site", len(all), orderSiteOf(all))
			}
			if buf.Len() != 0 {
				t.Errorf("want no log output, got %q", buf.String())
			}
		})
	}
}

func orderSiteOf(ts []*task.Task) string {
	if len(ts) == 0 {
		return "<none>"
	}
	return ts[0].OrderSiteId()
}
