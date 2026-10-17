package task_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

func mustReplenishment(t *testing.T) task.ReplenishmentMove {
	t.Helper()
	m, err := task.NewReplenishmentMove("req-9", "SKU-1", binRsv, binFwd, 24, "NORMAL")
	if err != nil {
		t.Fatalf("NewReplenishmentMove: %v", err)
	}
	return m
}

// claimedReplenishment returns a REPLENISH task claimed by s1 until now+1m.
func claimedReplenishment(t *testing.T) *task.Task {
	t.Helper()
	tk := task.NewReplenishmentTask("p1", shared.NewCPT(now.Add(time.Hour)), "req-9", shared.NewCapabilitySet("replenish"), mustReplenishment(t))
	if err := tk.Claim("s1", shared.NewCapabilitySet("replenish"), now, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	return tk
}

func TestParseType_AcceptsReplenish(t *testing.T) {
	got, err := task.ParseType("REPLENISH")
	if err != nil || got != task.Replenish {
		t.Fatalf("ParseType(REPLENISH) = %q, %v", got, err)
	}
	if _, err := task.ParseType("replenish"); !errors.Is(err, task.ErrUnknownType) {
		t.Fatalf("ParseType(replenish) err = %v, want ErrUnknownType (exact match)", err)
	}
}

func TestParseReplenishmentPriority(t *testing.T) {
	for _, p := range []string{"NORMAL", "URGENT"} {
		got, err := task.ParseReplenishmentPriority(p)
		if err != nil || string(got) != p {
			t.Errorf("ParseReplenishmentPriority(%q) = %q, %v", p, got, err)
		}
	}
	for _, p := range []string{"", "urgent", "High", "Normal"} {
		if _, err := task.ParseReplenishmentPriority(p); !errors.Is(err, task.ErrUnknownReplenishmentPriority) {
			t.Errorf("ParseReplenishmentPriority(%q) err = %v, want ErrUnknownReplenishmentPriority", p, err)
		}
	}
}

func TestNewReplenishmentMove_TrimsAndBuildsRef(t *testing.T) {
	m, err := task.NewReplenishmentMove(" req-9 ", " SKU-1 ", " "+binRsv+" ", " "+binFwd+" ", 24, " URGENT ")
	if err != nil {
		t.Fatalf("NewReplenishmentMove: %v", err)
	}
	want := task.ReplenishmentMove{RequestId: "req-9", SKU: "SKU-1", FromBin: binRsv, ToBin: binFwd, Quantity: 24, Priority: task.ReplenishmentPriorityUrgent}
	if m != want {
		t.Fatalf("move = %+v, want %+v", m, want)
	}
	if m.Ref() != "req-9" {
		t.Fatalf("Ref() = %q, want the request id", m.Ref())
	}
}

func TestNewReplenishmentMove_Rejects(t *testing.T) {
	long := strings.Repeat("r", task.MaxReplenishmentRequestIdLen+1)
	cases := map[string]struct {
		req, sku, from, to string
		qty                int
		priority           string
		wantPriority       bool
	}{
		"blank request":    {" ", "SKU", "A", "B", 1, "NORMAL", false},
		"long request":     {long, "SKU", "A", "B", 1, "NORMAL", false},
		"blank sku":        {"r", " ", "A", "B", 1, "NORMAL", false},
		"blank from":       {"r", "SKU", "", "B", 1, "NORMAL", false},
		"blank to":         {"r", "SKU", "A", " ", 1, "NORMAL", false},
		"same bins":        {"r", "SKU", "A", "A", 1, "NORMAL", false},
		"same bins padded": {"r", "SKU", " A", "A ", 1, "NORMAL", false},
		"zero quantity":    {"r", "SKU", "A", "B", 0, "NORMAL", false},
		"negative":         {"r", "SKU", "A", "B", -3, "NORMAL", false},
		"no priority":      {"r", "SKU", "A", "B", 1, "", true},
		"lowercase":        {"r", "SKU", "A", "B", 1, "urgent", true},
	}
	for name, c := range cases {
		_, err := task.NewReplenishmentMove(c.req, c.sku, c.from, c.to, c.qty, c.priority)
		want := task.ErrInvalidReplenishmentMove
		if c.wantPriority {
			want = task.ErrUnknownReplenishmentPriority
		}
		if !errors.Is(err, want) {
			t.Errorf("%s: err = %v, want %v", name, err, want)
		}
	}
}

func TestNewReplenishmentMove_AcceptsTheLongestRequestId(t *testing.T) {
	id := strings.Repeat("r", task.MaxReplenishmentRequestIdLen)
	if _, err := task.NewReplenishmentMove(id, "SKU", "A", "B", 1, "NORMAL"); err != nil {
		t.Fatalf("a %d character request id must be accepted: %v", len(id), err)
	}
}

func TestNewReplenishmentTask_CarriesTheMove(t *testing.T) {
	m := mustReplenishment(t)
	tk := task.NewReplenishmentTask("p1", shared.NewCPT(now), "req-9", shared.NewCapabilitySet("replenish"), m)
	if tk.Type() != task.Replenish || tk.Status() != task.Pending {
		t.Fatalf("type/status = %s/%s", tk.Type(), tk.Status())
	}
	if got := tk.Replenishment(); got == nil || *got != m {
		t.Fatalf("Replenishment() = %+v, want %+v", got, m)
	}
	if tk.ReplenishmentResult() != nil || tk.Relocation() != nil {
		t.Fatal("a fresh REPLENISH task has no result and no relocation")
	}
	if tk.Fragile() || tk.GiftWrap() {
		t.Fatal("replenishment work carries no packing hints")
	}
}

func TestWithReplenishment_CopiesAndClears(t *testing.T) {
	m := mustReplenishment(t)
	r := task.ReplenishmentResult{Quantity: 5, FromBin: binRsv, ToBin: binFwd}
	tk := task.New("p1", task.Replenish, shared.NewCPT(now), "wu", shared.NewCapabilitySet(), false, false).WithReplenishment(&m, &r)
	m.Quantity, r.Quantity = 99, 99
	if tk.Replenishment().Quantity != 24 || tk.ReplenishmentResult().Quantity != 5 {
		t.Fatalf("the task must hold copies, got %+v / %+v", tk.Replenishment(), tk.ReplenishmentResult())
	}
	tk.WithReplenishment(nil, nil)
	if tk.Replenishment() != nil || tk.ReplenishmentResult() != nil {
		t.Fatal("WithReplenishment(nil, nil) must clear both")
	}
}

func TestComplete_ReplenishNeedsTheConfirmation(t *testing.T) {
	tk := claimedReplenishment(t)
	if err := tk.Complete("s1", now); !errors.Is(err, task.ErrReplenishmentConfirmationRequired) {
		t.Fatalf("Complete err = %v, want ErrReplenishmentConfirmationRequired", err)
	}
	if tk.Status() != task.Claimed {
		t.Fatalf("a rejected completion must leave the task claimed, got %s", tk.Status())
	}
}

func TestCompleteReplenishment_RecordsTheConfirmedMove(t *testing.T) {
	cases := map[string]task.ReplenishmentConfirmation{
		"quantity only":   {Quantity: 20},
		"matching scans":  {Quantity: 20, FromBin: binRsv, ToBin: binFwd},
		"padded scans":    {Quantity: 20, FromBin: " " + binRsv + " ", ToBin: " " + binFwd},
		"more than plan":  {Quantity: 30},
		"scan from only":  {Quantity: 20, FromBin: binRsv},
		"scan to only":    {Quantity: 20, ToBin: binFwd},
		"the plan itself": {Quantity: 24},
	}
	for name, c := range cases {
		tk := claimedReplenishment(t)
		if err := tk.CompleteReplenishment("s1", now, c); err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		got := tk.ReplenishmentResult()
		want := task.ReplenishmentResult{Quantity: c.Quantity, FromBin: binRsv, ToBin: binFwd}
		if tk.Status() != task.Completed || got == nil || *got != want {
			t.Errorf("%s: status %s result %+v, want completed with %+v", name, tk.Status(), got, want)
		}
	}
}

func TestCompleteReplenishment_RejectsBadConfirmations(t *testing.T) {
	cases := map[string]task.ReplenishmentConfirmation{
		"zero quantity":     {Quantity: 0, FromBin: binRsv},
		"negative quantity": {Quantity: -1},
		"other source":      {Quantity: 5, FromBin: "WH1-RSV-AMB-Z99-01-01-A"},
		"other destination": {Quantity: 5, ToBin: "WH1-FWD-AMB-Z99-01-01-A"},
	}
	for name, c := range cases {
		tk := claimedReplenishment(t)
		err := tk.CompleteReplenishment("s1", now, c)
		if !errors.Is(err, task.ErrInvalidReplenishmentConfirmation) {
			t.Errorf("%s: err = %v, want ErrInvalidReplenishmentConfirmation", name, err)
		}
		if tk.Status() != task.Claimed || tk.ReplenishmentResult() != nil {
			t.Errorf("%s: a rejected confirmation must change nothing (status %s, result %+v)", name, tk.Status(), tk.ReplenishmentResult())
		}
	}
}

func TestCompleteReplenishment_ClaimRulesApplyFirst(t *testing.T) {
	good := task.ReplenishmentConfirmation{Quantity: 5}
	tk := claimedReplenishment(t)
	if err := tk.CompleteReplenishment("s2", now, good); !errors.Is(err, task.ErrNotOwner) {
		t.Fatalf("another station: err = %v, want ErrNotOwner", err)
	}
	if err := tk.CompleteReplenishment("s1", now.Add(2*time.Minute), good); !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("expired lease: err = %v, want ErrNotClaimed", err)
	}
	if tk.Status() != task.Pending {
		t.Fatalf("an expired lease frees the task, got %s", tk.Status())
	}
	if err := tk.CompleteReplenishment("s1", now, good); !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("pending task: err = %v, want ErrNotClaimed", err)
	}
	done := claimedReplenishment(t)
	if err := done.CompleteReplenishment("s1", now, good); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := done.CompleteReplenishment("s1", now, good); !errors.Is(err, task.ErrAlreadyCompleted) {
		t.Fatalf("second completion: err = %v, want ErrAlreadyCompleted (replay)", err)
	}
}

func TestCompleteReplenishment_NotAReplenishTask(t *testing.T) {
	tk := task.New("t1", task.Pick, shared.NewCPT(now.Add(time.Hour)), "wu", shared.NewCapabilitySet("pick"), false, false)
	if err := tk.Claim("s1", shared.NewCapabilitySet("pick"), now, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := tk.CompleteReplenishment("s1", now, task.ReplenishmentConfirmation{Quantity: 1}); !errors.Is(err, task.ErrNotRelocationTask) {
		t.Fatalf("err = %v, want ErrNotRelocationTask", err)
	}
}

func TestCapabilityMatching_ReplenishNeedsTheReplenishCapability(t *testing.T) {
	tk := task.NewReplenishmentTask("p1", shared.NewCPT(now.Add(time.Hour)), "req-9", shared.NewCapabilitySet("replenish"), mustReplenishment(t))
	if err := tk.Claim("s1", shared.NewCapabilitySet("pick", "relocate"), now, time.Minute); !errors.Is(err, task.ErrCapabilityMismatch) {
		t.Fatalf("a station without replenish: err = %v, want ErrCapabilityMismatch", err)
	}
	if tk.Status() != task.Pending {
		t.Fatalf("a refused claim leaves the task pending, got %s", tk.Status())
	}
	if err := tk.Claim("s1", shared.NewCapabilitySet("replenish"), now, time.Minute); err != nil {
		t.Fatalf("a station with replenish: %v", err)
	}
}

func replenishmentWith(t *testing.T, from, to string) *task.Task {
	t.Helper()
	m, err := task.NewReplenishmentMove("req-9", "SKU-1", from, to, 24, "NORMAL")
	if err != nil {
		t.Fatalf("NewReplenishmentMove: %v", err)
	}
	return task.NewReplenishmentTask("p1", shared.NewCPT(now.Add(time.Hour)), "wu", shared.NewCapabilitySet("replenish"), m)
}

func TestPlacement_ReplenishUsesSourceThenDestination(t *testing.T) {
	p, ok := replenishmentWith(t, binRsv, binFwd).Placement()
	want := task.Placement{SiteId: "WH1", FromLocation: binRsv, ToLocation: binFwd}
	if !ok || p != want {
		t.Fatalf("Placement() = (%+v, %v), want (%+v, true)", p, ok, want)
	}
}

func TestPlacement_ReplenishSiteFallsBackToTheDestination(t *testing.T) {
	p, ok := replenishmentWith(t, "BIN-X", binFwd).Placement()
	if !ok || p.SiteId != "WH1" || p.FromLocation != "BIN-X" || p.ToLocation != binFwd {
		t.Fatalf("Placement() = (%+v, %v)", p, ok)
	}
	if p, ok := replenishmentWith(t, "BIN-A", "BIN-B").Placement(); ok {
		t.Fatalf("Placement() = (%+v, true), want none for bins that are not location codes", p)
	}
}

func TestPriorityBand_ReplenishIsTypeAndCPTOnly(t *testing.T) {
	urgent, err := task.NewReplenishmentMove("req-1", "SKU-1", binRsv, binFwd, 24, "URGENT")
	if err != nil {
		t.Fatalf("NewReplenishmentMove: %v", err)
	}
	at := time.Date(2026, 10, 17, 14, 0, 0, 0, time.UTC)
	normal := task.NewReplenishmentTask("p1", shared.NewCPT(at), "wu", shared.NewCapabilitySet("replenish"), mustReplenishment(t))
	hot := task.NewReplenishmentTask("p2", shared.NewCPT(at), "wu", shared.NewCapabilitySet("replenish"), urgent)
	if normal.PriorityBand() != hot.PriorityBand() {
		t.Fatalf("claimNext orders by CPT only, so NORMAL and URGENT at one CPT are peers: %q vs %q", normal.PriorityBand(), hot.PriorityBand())
	}
	if want := "REPLENISH:2026-10-17T14:00:00Z"; normal.PriorityBand() != want {
		t.Fatalf("PriorityBand() = %q, want %q", normal.PriorityBand(), want)
	}
}
