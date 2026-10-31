package task_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

func eachPack(sku string) task.ScannedPack {
	return task.ScannedPack{GTIN: "04006381333931", SKU: sku, Level: task.PackLevelEach, UnitsPerPack: 1}
}

func casePack(sku string) task.ScannedPack {
	return task.ScannedPack{GTIN: "24006381333935", SKU: sku, Level: task.PackLevelCase, UnitsPerPack: 12}
}

// claimedPick returns a PICK task with a pick plan, claimed by s1 until now+1m.
func claimedPick(t *testing.T, unit string, quantity int) *task.Task {
	t.Helper()
	plan, err := task.NewPickPlan("SKU-1", quantity, unit, binFwd)
	if err != nil {
		t.Fatalf("NewPickPlan: %v", err)
	}
	tk := task.New("k1", task.Pick, shared.NewCPT(now.Add(time.Hour)), "wu-1", shared.NewCapabilitySet("pick"), false, false).WithPickPlan(&plan, 0)
	if err := tk.Claim("s1", shared.NewCapabilitySet("pick"), now, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	return tk
}

func TestNewPickPlan_ValidatesAndTrims(t *testing.T) {
	p, err := task.NewPickPlan(" SKU-1 ", 12, " CASE ", " "+binFwd+" ")
	if err != nil {
		t.Fatalf("NewPickPlan: %v", err)
	}
	if p.SKU != "SKU-1" || p.Quantity != 12 || p.Unit != task.PickUnitCase || p.FromBin != binFwd {
		t.Fatalf("plan = %+v", p)
	}
	if _, err := task.NewPickPlan("SKU-1", 1, "EACH", ""); err != nil {
		t.Fatalf("an absent from_bin is allowed: %v", err)
	}
	for name, c := range map[string]struct {
		sku  string
		qty  int
		unit string
	}{
		"blank sku": {" ", 1, "EACH"},
		"zero qty":  {"SKU-1", 0, "EACH"},
		"neg qty":   {"SKU-1", -3, "EACH"},
	} {
		if _, err := task.NewPickPlan(c.sku, c.qty, c.unit, ""); !errors.Is(err, task.ErrInvalidPickPlan) {
			t.Errorf("%s: err = %v, want ErrInvalidPickPlan", name, err)
		}
	}
	for _, u := range []string{"", "each", "PALLET", "INNER"} {
		if _, err := task.NewPickPlan("SKU-1", 1, u, ""); !errors.Is(err, task.ErrUnknownPickUnit) {
			t.Errorf("unit %q: err = %v, want ErrUnknownPickUnit", u, err)
		}
	}
}

func TestConfirmPickScan_EachUnit(t *testing.T) {
	tk := claimedPick(t, "EACH", 3)
	got, err := tk.ConfirmPickScan("s1", now, eachPack("SKU-1"), 1)
	if err != nil || got != 1 {
		t.Fatalf("ConfirmPickScan = %d, %v; want 1, nil", got, err)
	}
	got, err = tk.ConfirmPickScan("s1", now, eachPack("SKU-1"), 2)
	if err != nil || got != 2 {
		t.Fatalf("ConfirmPickScan(count 2) = %d, %v; want 2, nil", got, err)
	}
	if tk.PickedEaches() != 3 {
		t.Fatalf("PickedEaches = %d, want 3", tk.PickedEaches())
	}
}

func TestConfirmPickScan_CaseUnitMultipliesByUnitsPerPack(t *testing.T) {
	tk := claimedPick(t, "CASE", 36)
	got, err := tk.ConfirmPickScan("s1", now, casePack("SKU-1"), 2)
	if err != nil || got != 24 {
		t.Fatalf("ConfirmPickScan = %d, %v; want 24 eaches, nil", got, err)
	}
	if tk.PickedEaches() != 24 {
		t.Fatalf("PickedEaches = %d, want 24", tk.PickedEaches())
	}
}

func TestConfirmPickScan_Rejections(t *testing.T) {
	cases := map[string]struct {
		unit   string
		qty    int
		pack   task.ScannedPack
		count  int
		wantIs error
	}{
		"case scan on an EACH task":        {"EACH", 12, casePack("SKU-1"), 1, task.ErrScanLevelMismatch},
		"each scan on a CASE task":         {"CASE", 12, eachPack("SKU-1"), 1, task.ErrScanLevelMismatch},
		"inner level is not implemented":   {"EACH", 12, task.ScannedPack{GTIN: "g", SKU: "SKU-1", Level: task.PackLevelInner, UnitsPerPack: 6}, 1, task.ErrScanLevelMismatch},
		"each GTIN of another SKU":         {"EACH", 12, eachPack("SKU-2"), 1, task.ErrScanSkuMismatch},
		"case GTIN of another SKU":         {"CASE", 12, casePack("SKU-2"), 1, task.ErrScanSkuMismatch},
		"zero count":                       {"EACH", 12, eachPack("SKU-1"), 0, task.ErrInvalidScanCount},
		"negative count":                   {"EACH", 12, eachPack("SKU-1"), -1, task.ErrInvalidScanCount},
		"more eaches than planned":         {"EACH", 2, eachPack("SKU-1"), 3, task.ErrPickOverQuantity},
		"more cases than planned (eaches)": {"CASE", 12, casePack("SKU-1"), 2, task.ErrPickOverQuantity},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tk := claimedPick(t, c.unit, c.qty)
			if _, err := tk.ConfirmPickScan("s1", now, c.pack, c.count); !errors.Is(err, c.wantIs) {
				t.Fatalf("err = %v, want %v", err, c.wantIs)
			}
			if tk.PickedEaches() != 0 {
				t.Fatalf("a rejected scan must not change PickedEaches, got %d", tk.PickedEaches())
			}
		})
	}
}

func TestConfirmPickScan_ClaimRules(t *testing.T) {
	tk := claimedPick(t, "EACH", 3)
	if _, err := tk.ConfirmPickScan("s2", now, eachPack("SKU-1"), 1); !errors.Is(err, task.ErrNotOwner) {
		t.Errorf("other station: err = %v, want ErrNotOwner", err)
	}
	if _, err := tk.ConfirmPickScan("s1", now.Add(2*time.Minute), eachPack("SKU-1"), 1); !errors.Is(err, task.ErrNotClaimed) {
		t.Errorf("expired lease: err = %v, want ErrNotClaimed", err)
	}
	pending := task.New("k2", task.Pick, shared.NewCPT(now.Add(time.Hour)), "wu-2", shared.NewCapabilitySet("pick"), false, false)
	plan, _ := task.NewPickPlan("SKU-1", 3, "EACH", "")
	pending.WithPickPlan(&plan, 0)
	if _, err := pending.ConfirmPickScan("s1", now, eachPack("SKU-1"), 1); !errors.Is(err, task.ErrNotClaimed) {
		t.Errorf("pending: err = %v, want ErrNotClaimed", err)
	}
}

func TestConfirmPickScan_NoPlan(t *testing.T) {
	tk := task.New("k3", task.Pick, shared.NewCPT(now.Add(time.Hour)), "wu-3", shared.NewCapabilitySet("pick"), false, false)
	if err := tk.Claim("s1", shared.NewCapabilitySet("pick"), now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := tk.ConfirmPickScan("s1", now, eachPack("SKU-1"), 1); !errors.Is(err, task.ErrNoPickPlan) {
		t.Fatalf("err = %v, want ErrNoPickPlan", err)
	}
}

func TestComplete_PickWithPlanNeedsEveryEachScanned(t *testing.T) {
	tk := claimedPick(t, "EACH", 2)
	if _, err := tk.ConfirmPickScan("s1", now, eachPack("SKU-1"), 1); err != nil {
		t.Fatal(err)
	}
	if err := tk.Complete("s1", now); !errors.Is(err, task.ErrPickIncomplete) {
		t.Fatalf("Complete with 1 of 2 scanned: err = %v, want ErrPickIncomplete", err)
	}
	if tk.Status() != task.Claimed {
		t.Fatalf("a refused completion must leave the task Claimed, got %s", tk.Status())
	}
	if _, err := tk.ConfirmPickScan("s1", now, eachPack("SKU-1"), 1); err != nil {
		t.Fatal(err)
	}
	if err := tk.Complete("s1", now); err != nil {
		t.Fatalf("Complete with 2 of 2 scanned: %v", err)
	}
}

func TestComplete_PickWithoutPlanIsUnchanged(t *testing.T) {
	tk := task.New("k4", task.Pick, shared.NewCPT(now.Add(time.Hour)), "wu-4", shared.NewCapabilitySet("pick"), false, false)
	if err := tk.Claim("s1", shared.NewCapabilitySet("pick"), now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := tk.Complete("s1", now); err != nil {
		t.Fatalf("a plan-less PICK completes as before: %v", err)
	}
}

func TestWithPickPlan_CopiesAndClearsOnNil(t *testing.T) {
	plan, _ := task.NewPickPlan("SKU-1", 5, "EACH", "")
	tk := task.New("k5", task.Pick, shared.NewCPT(now), "wu-5", shared.NewCapabilitySet(), false, false).WithPickPlan(&plan, 4)
	plan.SKU = "MUTATED"
	if got := tk.PickPlan(); got == nil || got.SKU != "SKU-1" || tk.PickedEaches() != 4 {
		t.Fatalf("plan = %+v picked = %d", got, tk.PickedEaches())
	}
	tk.WithPickPlan(nil, 9)
	if tk.PickPlan() != nil || tk.PickedEaches() != 0 {
		t.Fatalf("nil plan must clear plan and picked, got %+v / %d", tk.PickPlan(), tk.PickedEaches())
	}
}
