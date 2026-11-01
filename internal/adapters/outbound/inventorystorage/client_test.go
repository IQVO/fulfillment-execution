package inventorystorage_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/inventorystorage"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// BreakCase POSTs inventory-storage's noun route (REST guideline: a
// case-break is a thing the container records) with the task's stable key.
func TestBreakCasePostsTheCaseBreaksRouteWithTheIdempotencyKey(t *testing.T) {
	var gotMethod, gotPath, gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotKey = r.Method, r.URL.EscapedPath(), r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	err := inventorystorage.NewClient(srv.URL, nil).BreakCase(context.Background(),
		ports.CaseBreakRequest{LPN: "LPN/1", LooseBin: "BIN-1", IdempotencyKey: "task-1"})
	if err != nil {
		t.Fatalf("BreakCase: %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/containers/LPN%2F1/case-breaks" || gotKey != "task-1" {
		t.Fatalf("request = %s %s key=%q, want POST /containers/LPN%%2F1/case-breaks key=task-1", gotMethod, gotPath, gotKey)
	}
}
