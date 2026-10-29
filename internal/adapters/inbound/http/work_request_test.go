package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// newWorkRequestRouter builds the router with a RequestNextWork use case
// over the scripted planning double.
func newWorkRequestRouter(planning ports.WorkPlanning) http.Handler {
	h := &Handlers{
		RequestNextWork: &usecases.RequestNextWork{Planning: planning},
		Readiness:       &Readiness{},
	}
	return NewRouter(h, nil)
}

type scriptedPlanning struct {
	release ports.ReleasedWork
	err     error
}

func (s scriptedPlanning) ReleaseNextWork(context.Context, string) (ports.ReleasedWork, error) {
	return s.release, s.err
}

func TestPostWorkRequest_AcknowledgesReleasedUnit(t *testing.T) {
	router := newWorkRequestRouter(scriptedPlanning{release: ports.ReleasedWork{
		WorkUnitId: "wu-482910", CPT: "2026-08-21T12:00:00Z", Reference: "order-77213-line-1",
	}})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/paths/pick-zone-a/work-requests", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body %s", rec.Code, rec.Body.String())
	}
	var body struct {
		WorkUnitId string `json:"workUnitId"`
		Reference  string `json:"reference"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v (%s)", err, rec.Body.String())
	}
	if body.WorkUnitId != "wu-482910" || body.Reference != "order-77213-line-1" {
		t.Errorf("body = %+v, want the admitted unit acknowledgement", body)
	}
}

func TestPostWorkRequest_ProblemStatuses(t *testing.T) {
	cases := map[string]struct {
		err    error
		status int
		slug   string
	}{
		"no pending work":  {ports.ErrNoPendingWork, http.StatusConflict, "work-planning-no-pending-work"},
		"unknown path":     {ports.ErrUnknownWorkPlanningPath, http.StatusNotFound, "work-planning-unknown-path"},
		"peer unreachable": {ports.ErrWorkPlanningUnreachable, http.StatusBadGateway, "work-planning-unreachable"},
		"not configured":   {usecases.ErrWorkPlanningNotConfigured, http.StatusServiceUnavailable, "work-planning-not-configured"},
		"empty path id":    {usecases.ErrPathIdRequired, http.StatusBadRequest, "path-id-required"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			router := newWorkRequestRouter(scriptedPlanning{err: tc.err})

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/paths/p/work-requests", nil))

			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, tc.status, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
				t.Errorf("Content-Type = %q, want application/problem+json", ct)
			}
			if !strings.Contains(rec.Body.String(), tc.slug) {
				t.Errorf("problem type slug %q missing from %s", tc.slug, rec.Body.String())
			}
		})
	}
}
