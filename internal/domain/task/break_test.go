package task_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

func mustCaseBreak(t *testing.T) task.CaseBreak {
	t.Helper()
	b, err := task.NewCaseBreak("003400000000000017", "SKU-1", binFwd, "k1")
	if err != nil {
		t.Fatalf("NewCaseBreak: %v", err)
	}
	return b
}

func claimedBreak(t *testing.T) *task.Task {
	t.Helper()
	tk := task.NewCaseBreakTask("b1", shared.NewCPT(now.Add(time.Hour)), "break-k1-003400000000000017", shared.NewCapabilitySet("pick"), mustCaseBreak(t))
	if err := tk.Claim("s1", shared.NewCapabilitySet("pick"), now, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	return tk
}

func TestParseType_AcceptsBreakCase(t *testing.T) {
	got, err := task.ParseType("BREAK_CASE")
	if err != nil || got != task.BreakCase {
		t.Fatalf("ParseType(BREAK_CASE) = %q, %v", got, err)
	}
}

func TestNewCaseBreak_Validates(t *testing.T) {
	b, err := task.NewCaseBreak(" 003400000000000017 ", " SKU-1 ", " "+binFwd+" ", " k1 ")
	if err != nil {
		t.Fatalf("NewCaseBreak: %v", err)
	}
	if b.LPN != "003400000000000017" || b.SKU != "SKU-1" || b.LooseBin != binFwd || b.PickTaskId != "k1" {
		t.Fatalf("break = %+v", b)
	}
	for name, args := range map[string][4]string{
		"blank lpn":       {" ", "SKU-1", binFwd, "k1"},
		"bad lpn chars":   {"LPN 1/2", "SKU-1", binFwd, "k1"},
		"lpn too long":    {strings.Repeat("A", 65), "SKU-1", binFwd, "k1"},
		"blank sku":       {"L1", " ", binFwd, "k1"},
		"blank loose bin": {"L1", "SKU-1", " ", "k1"},
		"blank pick task": {"L1", "SKU-1", binFwd, " "},
	} {
		if _, err := task.NewCaseBreak(args[0], args[1], args[2], args[3]); !errors.Is(err, task.ErrInvalidCaseBreak) {
			t.Errorf("%s: err = %v, want ErrInvalidCaseBreak", name, err)
		}
	}
}

func TestCaseBreakTask_GenericCompleteIsRefused(t *testing.T) {
	tk := claimedBreak(t)
	if err := tk.Complete("s1", now); !errors.Is(err, task.ErrCaseBreakCompletionRequired) {
		t.Fatalf("Complete on BREAK_CASE: err = %v, want ErrCaseBreakCompletionRequired", err)
	}
	if tk.Status() != task.Claimed {
		t.Fatalf("status = %s, want CLAIMED", tk.Status())
	}
}

func TestCaseBreakTask_AuthorizeThenComplete(t *testing.T) {
	tk := claimedBreak(t)
	b, err := tk.AuthorizeCaseBreak("s1", now)
	if err != nil || b.LPN != "003400000000000017" {
		t.Fatalf("AuthorizeCaseBreak = %+v, %v", b, err)
	}
	if tk.Status() != task.Claimed {
		t.Fatal("authorizing must not complete the task")
	}
	if err := tk.CompleteCaseBreak("s1", now); err != nil {
		t.Fatalf("CompleteCaseBreak: %v", err)
	}
	if tk.Status() != task.Completed || tk.Lease() != nil {
		t.Fatalf("status = %s lease = %v", tk.Status(), tk.Lease())
	}
	if _, err := tk.AuthorizeCaseBreak("s1", now); !errors.Is(err, task.ErrAlreadyCompleted) {
		t.Fatalf("authorize after complete: err = %v, want ErrAlreadyCompleted", err)
	}
	if err := tk.CompleteCaseBreak("s1", now); !errors.Is(err, task.ErrAlreadyCompleted) {
		t.Fatalf("second complete: err = %v, want ErrAlreadyCompleted", err)
	}
}

func TestCaseBreakTask_ClaimRules(t *testing.T) {
	tk := claimedBreak(t)
	if _, err := tk.AuthorizeCaseBreak("s2", now); !errors.Is(err, task.ErrNotOwner) {
		t.Errorf("other station: err = %v, want ErrNotOwner", err)
	}
	if _, err := tk.AuthorizeCaseBreak("s1", now.Add(2*time.Minute)); !errors.Is(err, task.ErrNotClaimed) {
		t.Errorf("expired lease: err = %v, want ErrNotClaimed", err)
	}
}

func TestAuthorizeCaseBreak_OnOtherTaskTypes(t *testing.T) {
	tk := claimedPick(t, "EACH", 1)
	if _, err := tk.AuthorizeCaseBreak("s1", now); !errors.Is(err, task.ErrNotCaseBreakTask) {
		t.Fatalf("err = %v, want ErrNotCaseBreakTask", err)
	}
	if err := tk.CompleteCaseBreak("s1", now); !errors.Is(err, task.ErrNotCaseBreakTask) {
		t.Fatalf("err = %v, want ErrNotCaseBreakTask", err)
	}
}

func TestWithCaseBreak_Copies(t *testing.T) {
	b := mustCaseBreak(t)
	tk := task.New("b2", task.BreakCase, shared.NewCPT(now), "o", shared.NewCapabilitySet(), false, false).WithCaseBreak(&b)
	b.LPN = "MUTATED"
	if got := tk.CaseBreak(); got == nil || got.LPN != "003400000000000017" {
		t.Fatalf("CaseBreak = %+v", got)
	}
	if tk.WithCaseBreak(nil).CaseBreak() != nil {
		t.Fatal("nil must clear")
	}
}

func TestPickPlanTask_PlacementIsNotAnnounced(t *testing.T) {
	// A pick plan names a source bin but only RELOCATE/REPLENISH are announced
	// (ADR 0049); BREAK_CASE is not announced either.
	if _, ok := claimedPick(t, "EACH", 1).Placement(); ok {
		t.Error("a PICK task must have no placement")
	}
	if _, ok := claimedBreak(t).Placement(); ok {
		t.Error("a BREAK_CASE task must have no placement")
	}
}
