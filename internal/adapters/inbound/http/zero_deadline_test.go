package http_test

import (
	stdhttp "net/http"
	"testing"
	"time"
)

// zero_deadline_test.go: a present-but-zero deadline (`0001-01-01T00:00:00Z`) is a valid RFC 3339
// date-time, so the contract fuzz sends it and a 400 "is required" fails "API rejected
// schema-compliant request". An ABSENT deadline stays a malformed request (400); a zero one is a
// well-formed request the domain cannot accept (422, problem slug invalid-deadline).

const zeroDeadline = "0001-01-01T00:00:00Z"

func rebinBody(packCpt any) map[string]any {
	b := map[string]any{
		"orderRef":                 "order-1",
		"lineId":                   "line-1",
		"requiredLineIds":          []string{"line-1"},
		"packRequiredCapabilities": []string{"pack"},
	}
	if packCpt != nil {
		b["packCpt"] = packCpt
	}
	return b
}

func taskBody(cpt any) map[string]any {
	b := map[string]any{"type": "PICK", "orderRef": "order-1", "requiredCapabilities": []string{"pick"}}
	if cpt != nil {
		b["cpt"] = cpt
	}
	return b
}

func TestDeadlines_ZeroIs422AbsentIs400(t *testing.T) {
	cases := []struct {
		name, path string
		body       map[string]any
		want       int
	}{
		{"rebin zero packCpt", "/rebin/arrivals", rebinBody(zeroDeadline), stdhttp.StatusUnprocessableEntity},
		{"rebin absent packCpt", "/rebin/arrivals", rebinBody(nil), stdhttp.StatusBadRequest},
		{"rebin real packCpt", "/rebin/arrivals", rebinBody(time.Now().Add(time.Hour)), stdhttp.StatusNoContent},
		{"task zero cpt", "/tasks", taskBody(zeroDeadline), stdhttp.StatusUnprocessableEntity},
		{"task absent cpt", "/tasks", taskBody(nil), stdhttp.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, _, _, _ := newTestServer()
			rec := doJSON(t, srv, stdhttp.MethodPost, tc.path, tc.body)
			if rec.Code != tc.want {
				t.Fatalf("want %d, got %d: %s", tc.want, rec.Code, rec.Body.String())
			}
		})
	}
}
