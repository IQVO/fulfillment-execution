package http_test

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/packview"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

const (
	siocProblemBase = "https://errors.fulfillment-execution.warehouse-systems.dev/"
	siocCaseGTIN    = "24006381333935"
)

// siocServer is the router over in-memory adapters with a claimed PACK task
// "t1" held by s1, and a PackView holding the given CASE record of "sku-1"
// (none when rec is nil).
func siocServer(t *testing.T, rec *ports.PackRecord) (stdhttp.Handler, string) {
	t.Helper()
	h, _, stations, _, clock := newTestHandlers()
	view := packview.NewMemoryStore()
	if rec != nil {
		rec.SKU, rec.Level, rec.GTIN = "sku-1", task.PackLevelCase, siocCaseGTIN
		rec.Version, rec.EffectiveFrom = 1, clock.Now().Add(-time.Hour)
		to := rec.EffectiveTo
		rec.EffectiveTo = nil
		if _, err := view.UpsertIfNewer(context.Background(), *rec); err != nil {
			t.Fatal(err)
		}
		if to != nil {
			if _, err := view.RetireIfNewer(context.Background(), siocCaseGTIN, *to, 2); err != nil {
				t.Fatal(err)
			}
		}
	}
	h.SealPackage.CaseLevels = view
	srv := http.NewRouter(h, nil)

	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pack")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PACK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pack"},
	})
	claim := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/task-claims", map[string]any{"taskType": "PACK"})
	var claimed struct {
		Id string `json:"id"`
	}
	_ = json.NewDecoder(claim.Body).Decode(&claimed)
	return srv, "/tasks/" + claimed.Id + "/package-seals"
}

var fullCase = &ports.PackRecord{UnitsPerPack: 12, Dims: &ports.PackDims{LengthMM: 400, WidthMM: 300, HeightMM: 200, WeightG: 6500}}

func TestSealPackage_ShipInOwnContainer_ExactlyOneCase(t *testing.T) {
	rec := *fullCase
	srv, path := siocServer(t, &rec)

	got := doJSON(t, srv, stdhttp.MethodPost, path, map[string]any{
		"stationId": "s1", "contents": []string{"sku-1"}, "shipInOwnContainer": true, "lineQuantity": 12,
	})
	if got.Code != stdhttp.StatusCreated {
		t.Fatalf("status = %d (%s), want 201", got.Code, got.Body.String())
	}
	const want = `{"id":"pkg-1","orderRef":"order-1","status":"SEALED","scannedContents":["sku-1"],"fragileHandling":false,"giftWrapRequested":false,"sortLane":"STANDARD",` +
		`"shipInOwnContainer":true,"containerSource":"CASE","container":{"gtin":"` + siocCaseGTIN + `","lengthMm":400,"widthMm":300,"heightMm":200,"weightG":6500}}` + "\n"
	if got.Body.String() != want {
		t.Fatalf("body = %s\nwant   %s", got.Body.String(), want)
	}

	// The read model returns the same package, container included.
	read := doJSON(t, srv, stdhttp.MethodGet, "/packages/pkg-1", nil)
	if read.Code != stdhttp.StatusOK || read.Body.String() != want {
		t.Fatalf("GET /packages/pkg-1 = %d %s, want 200 %s", read.Code, read.Body.String(), want)
	}
}

// With the new fields absent the seal answers exactly as before the feature,
// whatever the PackView holds: the container fields are omitted, not null.
func TestSealPackage_WithoutOwnContainerFieldsResponseIsByteForByteUnchanged(t *testing.T) {
	const want = `{"id":"pkg-1","orderRef":"order-1","status":"SEALED","scannedContents":["sku-1"],"fragileHandling":false,"giftWrapRequested":false,"sortLane":"STANDARD"}` + "\n"
	rec := *fullCase
	for name, body := range map[string]map[string]any{
		"absent":                {"stationId": "s1", "contents": []string{"sku-1"}},
		"false":                 {"stationId": "s1", "contents": []string{"sku-1"}, "shipInOwnContainer": false},
		"false with a quantity": {"stationId": "s1", "contents": []string{"sku-1"}, "shipInOwnContainer": false, "lineQuantity": 12},
	} {
		t.Run(name, func(t *testing.T) {
			r := rec
			srv, path := siocServer(t, &r)
			got := doJSON(t, srv, stdhttp.MethodPost, path, body)
			if got.Code != stdhttp.StatusCreated || got.Body.String() != want {
				t.Fatalf("seal = %d %s, want 201 %s", got.Code, got.Body.String(), want)
			}
		})
	}
}

func TestSealPackage_ShipInOwnContainer_RequestValidation(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
		want string
	}{
		{"quantity missing", map[string]any{"stationId": "s1", "contents": []string{"sku-1"}, "shipInOwnContainer": true}, "lineQuantity is required when shipInOwnContainer is true"},
		{"quantity zero", map[string]any{"stationId": "s1", "contents": []string{"sku-1"}, "shipInOwnContainer": true, "lineQuantity": 0}, "lineQuantity must be at least 1"},
		{"quantity negative", map[string]any{"stationId": "s1", "contents": []string{"sku-1"}, "shipInOwnContainer": true, "lineQuantity": -3}, "lineQuantity must be at least 1"},
		{"station missing", map[string]any{"contents": []string{"sku-1"}, "shipInOwnContainer": true, "lineQuantity": 12}, "stationId is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := *fullCase
			srv, path := siocServer(t, &r)
			rec := doJSON(t, srv, stdhttp.MethodPost, path, tt.body)
			assertProblemDetails(t, rec, stdhttp.StatusBadRequest, siocProblemBase+"invalid-request", path)
			var p struct{ Detail string }
			_ = json.Unmarshal(rec.Body.Bytes(), &p)
			if p.Detail != tt.want {
				t.Fatalf("detail = %q, want %q", p.Detail, tt.want)
			}
		})
	}
}

func TestSealPackage_ShipInOwnContainer_RefusalsAreDocumentedProblems(t *testing.T) {
	ended := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		rec      *ports.PackRecord
		contents []string
		qty      int
		slug     string
	}{
		{"one each short", fullCase, []string{"sku-1"}, 11, "ship-in-own-container-quantity-mismatch"},
		{"one each over", fullCase, []string{"sku-1"}, 13, "ship-in-own-container-quantity-mismatch"},
		{"a single each", fullCase, []string{"sku-1"}, 1, "ship-in-own-container-quantity-mismatch"},
		{"two lines", fullCase, []string{"sku-1", "sku-2"}, 12, "ship-in-own-container-not-single-line"},
		{"retired case", &ports.PackRecord{UnitsPerPack: 12, Dims: fullCase.Dims, EffectiveTo: &ended}, []string{"sku-1"}, 12, "ship-in-own-container-case-retired"},
		{"no case level", nil, []string{"sku-1"}, 12, "ship-in-own-container-no-case-level"},
		{"case without dimensions", &ports.PackRecord{UnitsPerPack: 12}, []string{"sku-1"}, 12, "ship-in-own-container-case-dimensions-unknown"},
		{"empty contents", fullCase, nil, 12, "package-no-scanned-contents"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rec *ports.PackRecord
			if tt.rec != nil {
				c := *tt.rec
				rec = &c
			}
			srv, path := siocServer(t, rec)
			resp := doJSON(t, srv, stdhttp.MethodPost, path, map[string]any{
				"stationId": "s1", "contents": tt.contents, "shipInOwnContainer": true, "lineQuantity": tt.qty,
			})
			assertProblemDetails(t, resp, stdhttp.StatusUnprocessableEntity, siocProblemBase+tt.slug, path)

			// Nothing was sealed: the same station can still seal the ordinary way.
			ok := doJSON(t, srv, stdhttp.MethodPost, path, map[string]any{"stationId": "s1", "contents": []string{"sku-1"}})
			if ok.Code != stdhttp.StatusCreated {
				t.Fatalf("ordinary seal after a refusal = %d (%s), want 201", ok.Code, ok.Body.String())
			}
		})
	}
}

// The listing endpoint carries the container too, and only on that package.
func TestGetPackagesByOrderRef_CarriesOwnContainer(t *testing.T) {
	r := *fullCase
	srv, path := siocServer(t, &r)
	doJSON(t, srv, stdhttp.MethodPost, path, map[string]any{
		"stationId": "s1", "contents": []string{"sku-1"}, "shipInOwnContainer": true, "lineQuantity": 12,
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(stdhttp.MethodGet, "/packages?orderRef=order-1", nil)
	srv.ServeHTTP(rec, req)
	var listPage struct {
		Items []struct {
			ShipInOwnContainer bool   `json:"shipInOwnContainer"`
			ContainerSource    string `json:"containerSource"`
			Container          struct {
				WeightG int `json:"weightG"`
			} `json:"container"`
		} `json:"items"`
	}
	err := json.Unmarshal(rec.Body.Bytes(), &listPage)
	list := listPage.Items
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %s (%v)", rec.Body.String(), err)
	}
	if !list[0].ShipInOwnContainer || list[0].ContainerSource != "CASE" || list[0].Container.WeightG != 6500 {
		t.Fatalf("listed package = %+v, want the CASE container", list[0])
	}
}
