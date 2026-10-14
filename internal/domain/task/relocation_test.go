package task_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

func mustMove(t *testing.T, kind, from, to string) task.RelocationMove {
	t.Helper()
	m, err := task.NewRelocationMove("plan-7", 3, kind, "SKU-1", from, to)
	if err != nil {
		t.Fatalf("NewRelocationMove(%s, %q, %q): %v", kind, from, to, err)
	}
	return m
}

// claimedRelocation returns a RELOCATE task claimed by s1 until now+1m.
func claimedRelocation(t *testing.T, move task.RelocationMove) *task.Task {
	t.Helper()
	tk := task.NewRelocationTask("r1", shared.NewCPT(now.Add(time.Hour)), "slot-plan-7-3", shared.NewCapabilitySet("relocate"), move)
	if err := tk.Claim("s1", shared.NewCapabilitySet("relocate"), now, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	return tk
}

func TestParseRelocationKind(t *testing.T) {
	for _, k := range []string{"Relocate", "Assign", "Vacate"} {
		got, err := task.ParseRelocationKind(k)
		if err != nil || string(got) != k {
			t.Errorf("ParseRelocationKind(%q) = %q, %v", k, got, err)
		}
	}
	for _, k := range []string{"", "relocate", "ASSIGN", "Move"} {
		if _, err := task.ParseRelocationKind(k); !errors.Is(err, task.ErrUnknownRelocationKind) {
			t.Errorf("ParseRelocationKind(%q) err = %v, want ErrUnknownRelocationKind", k, err)
		}
	}
}

func TestParseType_AcceptsRelocate(t *testing.T) {
	got, err := task.ParseType("RELOCATE")
	if err != nil || got != task.Relocate {
		t.Fatalf("ParseType(RELOCATE) = %q, %v", got, err)
	}
}

func TestNewRelocationMove_ValidPerKind(t *testing.T) {
	cases := []struct{ kind, from, to string }{
		{"Relocate", "FWD-A", "FWD-B"},
		{"Assign", "", "FWD-B"},
		{"Assign", "RSV-1", "FWD-B"},
		{"Vacate", "FWD-A", ""},
		{"Vacate", "FWD-A", "RSV-1"},
	}
	for _, c := range cases {
		m := mustMove(t, c.kind, c.from, c.to)
		if m.FromBin != c.from || m.ToBin != c.to || string(m.Kind) != c.kind {
			t.Errorf("%+v: got %+v", c, m)
		}
	}
}

func TestNewRelocationMove_TrimsAndBuildsRef(t *testing.T) {
	m, err := task.NewRelocationMove("  plan-7 ", 0, " Relocate ", " SKU-1 ", " FWD-A ", " FWD-B ")
	if err != nil {
		t.Fatalf("NewRelocationMove: %v", err)
	}
	want := task.RelocationMove{PlanId: "plan-7", MoveSeq: 0, Kind: task.RelocationKindRelocate, SKU: "SKU-1", FromBin: "FWD-A", ToBin: "FWD-B"}
	if m != want {
		t.Fatalf("move = %+v, want %+v", m, want)
	}
	if m.Ref() != "plan-7:0" {
		t.Fatalf("Ref() = %q, want plan-7:0", m.Ref())
	}
	if mustMove(t, "Relocate", "A", "B").Ref() != "plan-7:3" {
		t.Fatal("Ref() must be <plan_id>:<move_seq>")
	}
}

func TestNewRelocationMove_Rejects(t *testing.T) {
	cases := map[string]struct {
		plan       string
		seq        int
		kind, sku  string
		from, to   string
		wantUnkind bool
	}{
		"blank plan":         {"  ", 1, "Relocate", "SKU", "A", "B", false},
		"negative seq":       {"p", -1, "Relocate", "SKU", "A", "B", false},
		"blank sku":          {"p", 1, "Relocate", " ", "A", "B", false},
		"relocate no from":   {"p", 1, "Relocate", "SKU", "", "B", false},
		"relocate no to":     {"p", 1, "Relocate", "SKU", "A", "", false},
		"assign no to":       {"p", 1, "Assign", "SKU", "A", "", false},
		"vacate no from":     {"p", 1, "Vacate", "SKU", "", "B", false},
		"same bins":          {"p", 1, "Relocate", "SKU", "A", "A", false},
		"same bins trimmed":  {"p", 1, "Assign", "SKU", " A", "A ", false},
		"unknown kind":       {"p", 1, "Swap", "SKU", "A", "B", true},
		"lowercase kind":     {"p", 1, "vacate", "SKU", "A", "", true},
		"empty kind":         {"p", 1, "", "SKU", "A", "B", true},
		"assign no bins":     {"p", 1, "Assign", "SKU", "", "", false},
		"vacate no bins":     {"p", 1, "Vacate", "SKU", "", "", false},
		"relocate no bins":   {"p", 1, "Relocate", "SKU", "", "", false},
		"vacate same bins":   {"p", 1, "Vacate", "SKU", "A", "A", false},
		"relocate blank sku": {"p", 0, "Relocate", "", "A", "B", false},
	}
	for name, c := range cases {
		_, err := task.NewRelocationMove(c.plan, c.seq, c.kind, c.sku, c.from, c.to)
		want := task.ErrInvalidRelocationMove
		if c.wantUnkind {
			want = task.ErrUnknownRelocationKind
		}
		if !errors.Is(err, want) {
			t.Errorf("%s: err = %v, want %v", name, err, want)
		}
	}
}

func TestNewRelocationTask_IsPendingRelocateWithMove(t *testing.T) {
	move := mustMove(t, "Assign", "", "FWD-B")
	tk := task.NewRelocationTask("r1", shared.NewCPT(now), "wu-1", shared.NewCapabilitySet("relocate"), move)
	if tk.Type() != task.Relocate || tk.Status() != task.Pending {
		t.Fatalf("type/status = %s/%s", tk.Type(), tk.Status())
	}
	if tk.Relocation() == nil || *tk.Relocation() != move {
		t.Fatalf("Relocation() = %+v, want %+v", tk.Relocation(), move)
	}
	if tk.RelocationResult() != nil {
		t.Fatal("a new task has no relocation result")
	}
	if tk.Fragile() || tk.GiftWrap() {
		t.Fatal("relocation work carries no packing hints")
	}
}

func TestComplete_RelocateTaskNeedsConfirmation(t *testing.T) {
	tk := claimedRelocation(t, mustMove(t, "Relocate", "FWD-A", "FWD-B"))
	if err := tk.Complete("s1", now.Add(time.Second)); !errors.Is(err, task.ErrRelocationConfirmationRequired) {
		t.Fatalf("Complete on RELOCATE = %v, want ErrRelocationConfirmationRequired", err)
	}
	if tk.Status() != task.Claimed {
		t.Fatalf("status = %s, the task must stay Claimed", tk.Status())
	}
}

func TestComplete_RelocateTypeWithoutMoveStillNeedsConfirmation(t *testing.T) {
	tk := task.New("r2", task.Relocate, shared.NewCPT(now), "wu", shared.NewCapabilitySet("relocate"), false, false)
	if err := tk.Claim("s1", shared.NewCapabilitySet("relocate"), now, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := tk.Complete("s1", now); !errors.Is(err, task.ErrRelocationConfirmationRequired) {
		t.Fatalf("Complete = %v, want ErrRelocationConfirmationRequired", err)
	}
	if err := tk.CompleteRelocation("s1", now, task.RelocationConfirmation{Quantity: 1, FromBin: "A", ToBin: "B"}); !errors.Is(err, task.ErrNotRelocationTask) {
		t.Fatalf("CompleteRelocation without a move = %v, want ErrNotRelocationTask", err)
	}
}

func TestComplete_ClaimErrorsWinOverTheConfirmationRule(t *testing.T) {
	tk := claimedRelocation(t, mustMove(t, "Relocate", "FWD-A", "FWD-B"))
	if err := tk.Complete("intruder", now); !errors.Is(err, task.ErrNotOwner) {
		t.Fatalf("Complete by another station = %v, want ErrNotOwner", err)
	}
}

func TestCompleteRelocation_ResolvesBinsPerKind(t *testing.T) {
	cases := []struct {
		name             string
		kind, from, to   string
		scanFrom, scanTo string
		wantFrom, wantTo string
	}{
		{"relocate both known, no scan", "Relocate", "FWD-A", "FWD-B", "", "", "FWD-A", "FWD-B"},
		{"relocate both known, confirming scans", "Relocate", "FWD-A", "FWD-B", "FWD-A", " FWD-B ", "FWD-A", "FWD-B"},
		{"assign scans the source", "Assign", "", "FWD-B", "RSV-9", "", "RSV-9", "FWD-B"},
		{"vacate scans the destination", "Vacate", "FWD-A", "", "", " RSV-2 ", "FWD-A", "RSV-2"},
	}
	for _, c := range cases {
		tk := claimedRelocation(t, mustMove(t, c.kind, c.from, c.to))
		err := tk.CompleteRelocation("s1", now.Add(time.Second), task.RelocationConfirmation{Quantity: 12, FromBin: c.scanFrom, ToBin: c.scanTo})
		if err != nil {
			t.Fatalf("%s: CompleteRelocation: %v", c.name, err)
		}
		if tk.Status() != task.Completed || tk.Lease() != nil {
			t.Fatalf("%s: status/lease = %s/%v", c.name, tk.Status(), tk.Lease())
		}
		want := task.RelocationResult{Quantity: 12, FromBin: c.wantFrom, ToBin: c.wantTo}
		if got := tk.RelocationResult(); got == nil || *got != want {
			t.Fatalf("%s: result = %+v, want %+v", c.name, got, want)
		}
	}
}

func TestCompleteRelocation_RejectsBadConfirmation(t *testing.T) {
	cases := []struct {
		name           string
		kind, from, to string
		c              task.RelocationConfirmation
	}{
		{"zero quantity", "Relocate", "A", "B", task.RelocationConfirmation{Quantity: 0}},
		{"negative quantity", "Relocate", "A", "B", task.RelocationConfirmation{Quantity: -1}},
		{"assign source not scanned", "Assign", "", "B", task.RelocationConfirmation{Quantity: 1}},
		{"assign source blank scan", "Assign", "", "B", task.RelocationConfirmation{Quantity: 1, FromBin: "  "}},
		{"vacate destination not scanned", "Vacate", "A", "", task.RelocationConfirmation{Quantity: 1}},
		{"scanned source differs from plan", "Relocate", "A", "B", task.RelocationConfirmation{Quantity: 1, FromBin: "X"}},
		{"scanned destination differs from plan", "Relocate", "A", "B", task.RelocationConfirmation{Quantity: 1, ToBin: "X"}},
		{"assign scanned source equals destination", "Assign", "", "B", task.RelocationConfirmation{Quantity: 1, FromBin: "B"}},
		{"vacate scanned destination equals source", "Vacate", "A", "", task.RelocationConfirmation{Quantity: 1, ToBin: "A"}},
	}
	for _, c := range cases {
		tk := claimedRelocation(t, mustMove(t, c.kind, c.from, c.to))
		err := tk.CompleteRelocation("s1", now, c.c)
		if !errors.Is(err, task.ErrInvalidRelocationConfirmation) {
			t.Errorf("%s: err = %v, want ErrInvalidRelocationConfirmation", c.name, err)
			continue
		}
		if tk.Status() != task.Claimed || tk.RelocationResult() != nil {
			t.Errorf("%s: a rejected confirmation must leave the task Claimed without a result", c.name)
		}
	}
}

func TestCompleteRelocation_QuantityOneIsTheMinimum(t *testing.T) {
	tk := claimedRelocation(t, mustMove(t, "Relocate", "A", "B"))
	if err := tk.CompleteRelocation("s1", now, task.RelocationConfirmation{Quantity: 1}); err != nil {
		t.Fatalf("quantity 1 must be accepted: %v", err)
	}
}

func TestCompleteRelocation_ClaimRules(t *testing.T) {
	move := mustMove(t, "Relocate", "A", "B")
	ok := task.RelocationConfirmation{Quantity: 1}

	tk := claimedRelocation(t, move)
	if err := tk.CompleteRelocation("intruder", now, ok); !errors.Is(err, task.ErrNotOwner) {
		t.Fatalf("other station: %v, want ErrNotOwner", err)
	}
	if err := tk.CompleteRelocation("s1", now.Add(time.Minute), ok); !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("expired lease: %v, want ErrNotClaimed", err)
	}
	if tk.Status() != task.Pending {
		t.Fatalf("an expired lease frees the task, status = %s", tk.Status())
	}

	done := claimedRelocation(t, move)
	if err := done.CompleteRelocation("s1", now, ok); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if err := done.CompleteRelocation("s1", now, ok); !errors.Is(err, task.ErrAlreadyCompleted) {
		t.Fatalf("double completion: %v, want ErrAlreadyCompleted", err)
	}

	pending := task.NewRelocationTask("r3", shared.NewCPT(now), "wu", shared.NewCapabilitySet("relocate"), move)
	if err := pending.CompleteRelocation("s1", now, ok); !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("pending: %v, want ErrNotClaimed", err)
	}
}

func TestCompleteRelocation_OnANonRelocationTask(t *testing.T) {
	tk := newPickTask()
	if err := tk.Claim("s1", shared.NewCapabilitySet("pick"), now, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := tk.CompleteRelocation("s1", now, task.RelocationConfirmation{Quantity: 1}); !errors.Is(err, task.ErrNotRelocationTask) {
		t.Fatalf("err = %v, want ErrNotRelocationTask", err)
	}
	if tk.Status() != task.Claimed {
		t.Fatalf("status = %s, want Claimed", tk.Status())
	}
}

func TestWithRelocation_CopiesAndClears(t *testing.T) {
	move := mustMove(t, "Vacate", "A", "")
	result := task.RelocationResult{Quantity: 4, FromBin: "A", ToBin: "R"}
	tk := task.Rehydrate("r1", task.Relocate, task.Completed, shared.NewCPT(now), "wu", shared.NewCapabilitySet("relocate"), nil, false, false, nil).
		WithRelocation(&move, &result)
	move.PlanId, result.Quantity = "mutated", 99
	if tk.Relocation().PlanId != "plan-7" || tk.RelocationResult().Quantity != 4 {
		t.Fatal("WithRelocation must copy the move and the result")
	}
	tk.WithRelocation(nil, nil)
	if tk.Relocation() != nil || tk.RelocationResult() != nil {
		t.Fatal("WithRelocation(nil, nil) must clear both")
	}
}

func TestRelocationConfirmation_HasInput(t *testing.T) {
	cases := map[task.RelocationConfirmation]bool{
		{}:                        false,
		{FromBin: "  "}:           false,
		{Quantity: 1}:             true,
		{Quantity: -1}:            true,
		{FromBin: "A"}:            true,
		{ToBin: "B"}:              true,
		{ToBin: " ", FromBin: ""}: false,
	}
	for c, want := range cases {
		if got := c.HasInput(); got != want {
			t.Errorf("%+v.HasInput() = %v, want %v", c, got, want)
		}
	}
}
