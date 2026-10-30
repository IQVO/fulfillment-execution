package usecases

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// fakePlanning is the in-memory WorkPlanning double: it records the last
// path it was asked for and answers with the scripted outcome.
type fakePlanning struct {
	asked   []string
	release ports.ReleasedWork
	err     error
}

func (f *fakePlanning) ReleaseNextWork(_ context.Context, pathId string) (ports.ReleasedWork, error) {
	f.asked = append(f.asked, pathId)
	return f.release, f.err
}

func TestRequestNextWork_Delegates(t *testing.T) {
	planning := &fakePlanning{release: ports.ReleasedWork{WorkUnitId: "wu-1", CPT: "2026-12-31T18:00:00Z", Reference: "order-1"}}
	uc := &RequestNextWork{Planning: planning}

	released, err := uc.Execute(context.Background(), RequestNextWorkRequest{PathId: "pick-zone-a"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if released != planning.release {
		t.Errorf("released = %+v, want %+v", released, planning.release)
	}
	if len(planning.asked) != 1 || planning.asked[0] != "pick-zone-a" {
		t.Errorf("planning asked = %v, want [pick-zone-a]", planning.asked)
	}
}

func TestRequestNextWork_RejectionsPassThrough(t *testing.T) {
	cases := map[string]error{
		"no pending":  ports.ErrNoPendingWork,
		"unknown":     ports.ErrUnknownWorkPlanningPath,
		"unreachable": ports.ErrWorkPlanningUnreachable,
	}
	for name, scripted := range cases {
		t.Run(name, func(t *testing.T) {
			uc := &RequestNextWork{Planning: &fakePlanning{err: scripted}}
			if _, err := uc.Execute(context.Background(), RequestNextWorkRequest{PathId: "p"}); !errors.Is(err, scripted) {
				t.Fatalf("error = %v, want the typed delegation outcome %v", err, scripted)
			}
		})
	}
}

func TestRequestNextWork_EmptyPathIdIsRefusedLocally(t *testing.T) {
	planning := &fakePlanning{}
	uc := &RequestNextWork{Planning: planning}

	if _, err := uc.Execute(context.Background(), RequestNextWorkRequest{}); !errors.Is(err, ErrPathIdRequired) {
		t.Fatalf("error = %v, want ErrPathIdRequired", err)
	}
	if len(planning.asked) != 0 {
		t.Errorf("an empty path id must never reach the agent, asked = %v", planning.asked)
	}
}

func TestRequestNextWork_NotConfiguredFailsClosed(t *testing.T) {
	uc := &RequestNextWork{}

	if _, err := uc.Execute(context.Background(), RequestNextWorkRequest{PathId: "p"}); !errors.Is(err, ErrWorkPlanningNotConfigured) {
		t.Fatalf("error = %v, want ErrWorkPlanningNotConfigured", err)
	}
}
