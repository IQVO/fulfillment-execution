package http_test

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
)

// sealedPackageServer returns a server holding one sealed package, "pkg-1"
// (the id newTestHandlers mints), ready for a SLAM weigh-check.
func sealedPackageServer(t *testing.T) stdhttp.Handler {
	t.Helper()
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pack")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PACK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pack"},
	})
	claimRec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PACK"})
	var claimed struct {
		Id string `json:"id"`
	}
	_ = json.NewDecoder(claimRec.Body).Decode(&claimed)
	sealRec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/seal-package", map[string]any{
		"stationId": "s1", "contents": []string{"sku-1"},
	})
	if sealRec.Code != stdhttp.StatusCreated {
		t.Fatalf("setup: expected 201 sealing the package, got %d: %s", sealRec.Code, sealRec.Body.String())
	}
	return srv
}

// apis/openapi.yaml runSlam: RunSlamRequest lists actualWeight and
// expectedWeight as required and "400" is BadRequest. A weigh-check with no
// weights must not be silently run as 0 against 0 (which labels the package).
func TestPostRunSlam_MissingWeightReturns400AndProcessesNothing(t *testing.T) {
	cases := map[string]string{
		"empty object":        `{}`,
		"only actualWeight":   `{"actualWeight":2.0}`,
		"only expectedWeight": `{"expectedWeight":2.0}`,
		"null body":           `null`,
		"unrelated field":     `{"weight":2.0}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			srv := sealedPackageServer(t)

			rec := doJSON(t, srv, stdhttp.MethodPost, "/packages/pkg-1/slam", json.RawMessage(body))

			if rec.Code != stdhttp.StatusBadRequest {
				t.Fatalf("expected 400 for %s, got %d: %s", body, rec.Code, rec.Body.String())
			}
			assertProblemDetails(t, rec, stdhttp.StatusBadRequest,
				problemTypePrefix+"invalid-request", "/packages/pkg-1/slam")

			// Nothing was processed: a correct weigh-check afterwards is the
			// first one (204), not a repeat (409 package-already-processed).
			retry := doJSON(t, srv, stdhttp.MethodPost, "/packages/pkg-1/slam", map[string]any{
				"actualWeight": 2.0, "expectedWeight": 2.0,
			})
			if retry.Code != stdhttp.StatusNoContent {
				t.Fatalf("expected the rejected request to leave the package unprocessed (204 on retry), got %d: %s",
					retry.Code, retry.Body.String())
			}
		})
	}
}

func TestPostRunSlam_BodyThatIsNotAnObjectReturns400(t *testing.T) {
	srv := sealedPackageServer(t)

	rec := doJSON(t, srv, stdhttp.MethodPost, "/packages/pkg-1/slam", json.RawMessage(`[1,2]`))

	assertProblemDetails(t, rec, stdhttp.StatusBadRequest,
		problemTypePrefix+"invalid-request", "/packages/pkg-1/slam")
}

// The spec documents that an explicit null is accepted and treated as 0 (Go's
// decoder cannot tell them apart), and an explicit 0 is a real weight. Only an
// ABSENT weight is rejected.
func TestPostRunSlam_ExplicitNullAndZeroWeightsAreStillAccepted(t *testing.T) {
	cases := map[string]string{
		"explicit nulls": `{"actualWeight":null,"expectedWeight":null}`,
		"explicit zeros": `{"actualWeight":0,"expectedWeight":0}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			srv := sealedPackageServer(t)

			rec := doJSON(t, srv, stdhttp.MethodPost, "/packages/pkg-1/slam", json.RawMessage(body))

			if rec.Code != stdhttp.StatusNoContent {
				t.Fatalf("expected 204 for %s, got %d: %s", body, rec.Code, rec.Body.String())
			}
		})
	}
}
