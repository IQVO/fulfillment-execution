package http_test

import (
	"context"
	stdhttp "net/http"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
)

// apis/openapi.yaml claimNextTask: ClaimNextRequest.taskType is the TaskType
// enum and "400" is BadRequest. A value outside the enum is a malformed
// request, not "no claimable task" (409), which is what it used to answer.
func TestPostClaimNext_TaskTypeOutsideEnumReturns400(t *testing.T) {
	for _, taskType := range []string{"BOGUS", "pick", "Pick", " PICK", "PICK "} {
		t.Run(taskType, func(t *testing.T) {
			srv, _, stations, _, _ := newTestServer()
			_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))

			rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": taskType})

			if rec.Code != stdhttp.StatusBadRequest {
				t.Fatalf("expected 400 for taskType %q, got %d: %s", taskType, rec.Code, rec.Body.String())
			}
			assertProblemDetails(t, rec, stdhttp.StatusBadRequest,
				problemTypePrefix+"invalid-request", "/stations/s1/claim-next")
		})
	}
}

// The enum check must not over-reject: every published TaskType still reaches
// the use case (an empty queue answers 409 no-claimable-task, as before).
func TestPostClaimNext_EveryEnumTaskTypeReachesTheUseCase(t *testing.T) {
	for _, taskType := range []string{"PICK", "PACK", "SLAM", "REBIN", "DISPATCH", "ARRIVAL", "RELOCATE", "REPLENISH", "BREAK_CASE"} {
		t.Run(taskType, func(t *testing.T) {
			srv, _, stations, _, _ := newTestServer()
			_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))

			rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": taskType})

			assertProblemDetails(t, rec, stdhttp.StatusConflict,
				problemTypePrefix+"no-claimable-task", "/stations/s1/claim-next")
		})
	}
}

// An absent taskType stays a 400, as it always was.
func TestPostClaimNext_MissingTaskTypeStillReturns400(t *testing.T) {
	srv, _, stations, _, _ := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{})

	assertProblemDetails(t, rec, stdhttp.StatusBadRequest,
		problemTypePrefix+"invalid-request", "/stations/s1/claim-next")
}
