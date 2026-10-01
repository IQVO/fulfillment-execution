package http_test

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
)

const problemTypePrefix = "https://errors.fulfillment-execution.warehouse-systems.dev/"

type packageBody struct {
	Id                string   `json:"id"`
	OrderRef          string   `json:"orderRef"`
	Status            string   `json:"status"`
	ScannedContents   []string `json:"scannedContents"`
	FragileHandling   bool     `json:"fragileHandling"`
	GiftWrapRequested bool     `json:"giftWrapRequested"`
	SortLane          string   `json:"sortLane"`
}

type problemBody struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
}

// sealViaAPI drives create -> claim -> seal through the router and returns
// the sealed package id.
func sealViaAPI(t *testing.T, srv stdhttp.Handler, stations interface {
	Save(context.Context, *station.Station) error
}, now time.Time, orderRef string) string {
	t.Helper()
	_ = stations.Save(context.TODO(), station.New("pack-1", shared.NewCapabilitySet("pack")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PACK", "cpt": now.Add(time.Hour), "orderRef": orderRef, "requiredCapabilities": []string{"pack"},
	})
	claimRec := doJSON(t, srv, stdhttp.MethodPost, "/stations/pack-1/claim-next", map[string]any{"taskType": "PACK"})
	var claimed struct {
		Id string `json:"id"`
	}
	_ = json.NewDecoder(claimRec.Body).Decode(&claimed)
	sealRec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/seal-package", map[string]any{
		"stationId": "pack-1", "contents": []string{"sku-1", "sku-2"},
	})
	if sealRec.Code != stdhttp.StatusCreated {
		t.Fatalf("seal: expected 201, got %d: %s", sealRec.Code, sealRec.Body.String())
	}
	var sealed packageBody
	_ = json.NewDecoder(sealRec.Body).Decode(&sealed)
	return sealed.Id
}

func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder) problemBody {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("expected application/problem+json, got %q", ct)
	}
	var p problemBody
	if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	return p
}

func TestGetPackage_Returns200WithThePackageShape(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	id := sealViaAPI(t, srv, stations, clock.Now(), "order-1")

	rec := doJSON(t, srv, stdhttp.MethodGet, "/packages/"+id, nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %q", ct)
	}
	var got packageBody
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Id != id || got.OrderRef != "order-1" || got.Status != "SEALED" || got.SortLane != "STANDARD" {
		t.Fatalf("unexpected package: %+v", got)
	}
	if len(got.ScannedContents) != 2 || got.ScannedContents[0] != "sku-1" || got.ScannedContents[1] != "sku-2" {
		t.Fatalf("expected scannedContents [sku-1 sku-2], got %v", got.ScannedContents)
	}
}

// The reason this endpoint exists: SLAM returns 204 in both outcomes, and
// GET /packages/{id} is how the dock tells LABELED from DIVERTED.
func TestGetPackage_SurfacesSlamOutcomeAfter204(t *testing.T) {
	cases := []struct {
		name   string
		actual float64
		want   string
	}{
		{"label applied", 2.02, "LABELED"},
		{"weight discrepancy", 2.5, "DIVERTED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, stations, _, clock := newTestServer()
			id := sealViaAPI(t, srv, stations, clock.Now(), "order-1")
			slamRec := doJSON(t, srv, stdhttp.MethodPost, "/packages/"+id+"/slam", map[string]any{
				"actualWeight": tc.actual, "expectedWeight": 2.0,
			})
			if slamRec.Code != stdhttp.StatusNoContent {
				t.Fatalf("slam: expected 204 (contract unchanged), got %d", slamRec.Code)
			}

			rec := doJSON(t, srv, stdhttp.MethodGet, "/packages/"+id, nil)
			if rec.Code != stdhttp.StatusOK {
				t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
			}
			var got packageBody
			_ = json.NewDecoder(rec.Body).Decode(&got)
			if got.Status != tc.want {
				t.Fatalf("expected status %s, got %s", tc.want, got.Status)
			}
		})
	}
}

func TestGetPackage_UnknownIdReturns404PackageNotFound(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodGet, "/packages/does-not-exist", nil)
	if rec.Code != stdhttp.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
	p := decodeProblem(t, rec)
	if p.Type != problemTypePrefix+"package-not-found" {
		t.Fatalf("expected package-not-found problem type, got %q", p.Type)
	}
	if p.Title != "Package not found" || p.Status != stdhttp.StatusNotFound || p.Instance != "/packages/does-not-exist" {
		t.Fatalf("unexpected problem body: %+v", p)
	}
}

func TestGetPackagesByOrderRef_ReturnsMatchingPackages(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	id := sealViaAPI(t, srv, stations, clock.Now(), "order-1")

	rec := doJSON(t, srv, stdhttp.MethodGet, "/packages?orderRef=order-1", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var got []packageBody
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0].Id != id || got[0].OrderRef != "order-1" || got[0].Status != "SEALED" {
		t.Fatalf("expected the one order-1 package, got %+v", got)
	}

	other := doJSON(t, srv, stdhttp.MethodGet, "/packages?orderRef=order-2", nil)
	var none []packageBody
	_ = json.NewDecoder(other.Body).Decode(&none)
	if len(none) != 0 {
		t.Fatalf("expected no packages for order-2, got %+v", none)
	}
}

func TestGetPackagesByOrderRef_UnknownOrderRefReturnsEmptyArray(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodGet, "/packages?orderRef=does-not-exist", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); body != "[]\n" {
		t.Fatalf("expected a JSON empty array, got %q", body)
	}
}

func TestGetPackagesByOrderRef_MissingOrBlankOrderRefReturns400(t *testing.T) {
	for _, path := range []string{"/packages", "/packages?orderRef="} {
		t.Run(path, func(t *testing.T) {
			srv, _, _, _, _ := newTestServer()
			rec := doJSON(t, srv, stdhttp.MethodGet, path, nil)
			if rec.Code != stdhttp.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
			p := decodeProblem(t, rec)
			if p.Type != problemTypePrefix+"invalid-request" {
				t.Fatalf("expected invalid-request problem type, got %q", p.Type)
			}
			if p.Detail != "orderRef is required" || p.Instance != "/packages" {
				t.Fatalf("unexpected problem body: %+v", p)
			}
		})
	}
}
