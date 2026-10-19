package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// next records whether the guarded handler ran and what body it read.
func nulProbe(reached *bool, body *string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*reached = true
		if r.Body != nil {
			b := new(strings.Builder)
			buf := make([]byte, 512)
			for {
				n, err := r.Body.Read(buf)
				b.Write(buf[:n])
				if err != nil {
					break
				}
			}
			*body = b.String()
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func TestRejectNUL(t *testing.T) {
	cases := []struct {
		name     string
		target   string
		body     string
		wantCode int
		wantType string
		reaches  bool
	}{
		{"clean body passes", "/tasks", `{"orderRef":"a"}`, http.StatusNoContent, "", true},
		{"no body passes", "/tasks", "", http.StatusNoContent, "", true},
		{"raw NUL in the body", "/tasks", "{\"orderRef\":\"a\x00b\"}", http.StatusBadRequest, "malformed-request-body", false},
		{"escaped NUL in the body", "/tasks", `{"orderRef":"a\u0000b"}`, http.StatusBadRequest, "malformed-request-body", false},
		{"escaped backslash then u0000 is not a NUL", "/tasks", `{"orderRef":"a\\u0000b"}`, http.StatusNoContent, "", true},
		{"three backslashes then u0000 is a NUL", "/tasks", `{"orderRef":"a\\\u0000b"}`, http.StatusBadRequest, "malformed-request-body", false},
		{"a later escape after an escaped backslash", "/tasks", `{"a":"\\u0000","b":"\u0000"}`, http.StatusBadRequest, "malformed-request-body", false},
		{"encoded NUL in the path", "/tasks/a%00b/complete", "", http.StatusNotFound, "resource-not-found", false},
		{"encoded NUL in the query", "/tasks?orderRef=a%00b", "", http.StatusBadRequest, "invalid-request", false},
		{"clean query passes", "/tasks?orderRef=a%2500b", "", http.StatusNoContent, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var reached bool
			var seen string
			req := httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			rejectNUL(nulProbe(&reached, &seen)).ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if reached != tc.reaches {
				t.Fatalf("handler reached = %v, want %v", reached, tc.reaches)
			}
			if tc.wantType != "" && !strings.Contains(rec.Body.String(), "/"+tc.wantType) {
				t.Errorf("problem type lacks %q: %s", tc.wantType, rec.Body.String())
			}
			if tc.reaches && seen != tc.body {
				t.Errorf("the guarded handler read %q, want the body replayed intact: %q", seen, tc.body)
			}
		})
	}
}

func TestRejectNUL_RawNULInThePath(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/tasks", http.NoBody)
	req.URL.Path = "/tasks/a\x00b"
	var reached bool
	var seen string
	rec := httptest.NewRecorder()
	rejectNUL(nulProbe(&reached, &seen)).ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound || reached {
		t.Fatalf("status = %d reached = %v, want 404 and not reached", rec.Code, reached)
	}
}

// The guard is wired into the real router: a NUL never reaches a handler.
func TestRouter_RejectsNULBeforeAnyHandler(t *testing.T) {
	router := NewRouter(&Handlers{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader("{\"orderRef\":\"a\x00\"}"))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 from the NUL guard (a nil handler would have panicked): %s", rec.Code, rec.Body.String())
	}
}
