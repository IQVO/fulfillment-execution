package inventorystorage_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/inventorystorage"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

const problemBase = "https://errors.inventory-storage.warehouse-systems.dev/"

func problemBody(slug string) string {
	return `{"type":"` + problemBase + slug + `","title":"t","status":409,"detail":"d"}`
}

// answer is one scripted response of a fake inventory-storage.
type answer struct {
	status int
	body   string
}

// fakeStorage replays the scripted answers (the last one repeats) and records the
// Idempotency-Key of every request.
func fakeStorage(t *testing.T, answers ...answer) (srv *httptest.Server, keys func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Idempotency-Key"))
		a := answers[min(len(seen), len(answers))-1]
		mu.Unlock()
		if a.body != "" {
			w.Header().Set("Content-Type", "application/problem+json")
		}
		w.WriteHeader(a.status)
		_, _ = w.Write([]byte(a.body))
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), seen...) }
}

var req = ports.CaseBreakRequest{LPN: "LPN-1", LooseBin: "BIN-1", IdempotencyKey: "break-case:tb-1"}

// A refusal the callee releases is retryable (and an unavailable); everything else
// stays the permanent rejection it was.
func TestBreakCaseReadsTheCalleesReplayClassOfARefusal(t *testing.T) {
	cases := []struct {
		name     string
		a        answer
		released bool
	}{
		{"RETRYABLE 404 bin-not-found", answer{404, problemBody("bin-not-found")}, true},
		{"RETRYABLE 404 container-not-found", answer{404, problemBody("container-not-found")}, true},
		{"RETRYABLE 404 product-classification-not-found", answer{404, problemBody("product-classification-not-found")}, true},
		{"RETRYABLE 409 concurrent-modification", answer{409, problemBody("concurrent-modification")}, true},
		{"RETRYABLE 409 container-not-sealed", answer{409, problemBody("container-not-sealed")}, true},
		{"RETRYABLE 409 held-unit-closed", answer{409, problemBody("held-unit-closed")}, true},
		{"RETRYABLE 409 transfer-receipt-conflict", answer{409, problemBody("transfer-receipt-conflict")}, true},
		{"a dependency outage answered 409 is retryable", answer{409, problemBody("location-classification-unavailable")}, true},
		{"FINAL 400 empty-bin-id", answer{400, problemBody("empty-bin-id")}, false},
		{"FINAL 409 idempotency-key-reused", answer{409, problemBody("idempotency-key-reused")}, false},
		{"an unknown slug is terminal", answer{409, problemBody("a-new-refusal-nobody-classified")}, false},
		{"a 4xx without a body is terminal", answer{409, ""}, false},
		{"a 4xx with a body that is not a problem is terminal", answer{404, "<html>nope</html>"}, false},
		{"a problem without a type is terminal", answer{409, `{"title":"bin-not-found"}`}, false},
		{"a type that is not an inventory-storage slug is terminal", answer{409, `{"type":"about:blank"}`}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := fakeStorage(t, tc.a)
			err := inventorystorage.NewClient(srv.URL, nil).BreakCase(context.Background(), req)
			if got := errors.Is(err, ports.ErrCaseBreakRefusalReleased); got != tc.released {
				t.Fatalf("released = %v, want %v (err %v)", got, tc.released, err)
			}
			if errors.Is(err, ports.ErrCaseBreakUnavailable) != tc.released || errors.Is(err, ports.ErrCaseBreakRejected) == tc.released {
				t.Fatalf("err = %v: a released refusal is retryable (unavailable), any other refusal is rejected", err)
			}
		})
	}
}

// Status ranges that are not refusals keep their reading: 408, 429 and 5xx are
// unavailable but not a released refusal, even with a slug of the table in the body.
func TestBreakCaseKeepsTheStatusReadingOfOutagesAndPermanentStatuses(t *testing.T) {
	for _, tc := range []struct {
		status    int
		body      string
		retryable bool
	}{
		{http.StatusRequestTimeout, "", true}, {http.StatusTooManyRequests, "", true},
		{http.StatusInternalServerError, problemBody("internal-error"), true}, {http.StatusServiceUnavailable, "", true},
		{http.StatusMethodNotAllowed, "", false},
	} {
		srv, _ := fakeStorage(t, answer{tc.status, tc.body})
		err := inventorystorage.NewClient(srv.URL, nil).BreakCase(context.Background(), req)
		if errors.Is(err, ports.ErrCaseBreakRefusalReleased) {
			t.Errorf("status %d: an outage is not a released refusal (%v)", tc.status, err)
		}
		if errors.Is(err, ports.ErrCaseBreakUnavailable) != tc.retryable {
			t.Errorf("status %d: unavailable = %v, want %v (%v)", tc.status, !tc.retryable, tc.retryable, err)
		}
	}
}

// The refusal's slug stays in the error, so the 503 detail of this service names why.
func TestBreakCaseNamesTheReleasedSlugInTheError(t *testing.T) {
	srv, _ := fakeStorage(t, answer{404, problemBody("bin-not-found")})
	err := inventorystorage.NewClient(srv.URL, nil).BreakCase(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "bin-not-found") {
		t.Fatalf("err = %v, want it to name bin-not-found", err)
	}
}

// Inside one call a released refusal is retried under the existing attempt budget
// (3, jittered), always with the SAME key, and a pass ends the call as a success.
func TestBreakerClientRetriesAReleasedRefusalUnderTheSameKeyAndASuccessEndsIt(t *testing.T) {
	srv, keys := fakeStorage(t, answer{409, problemBody("concurrent-modification")}, answer{http.StatusOK, ""})
	c := inventorystorage.NewBreakerClient(inventorystorage.NewClient(srv.URL, nil), nil)
	if err := c.BreakCase(context.Background(), req); err != nil {
		t.Fatalf("BreakCase: %v", err)
	}
	if got := keys(); len(got) != 2 || got[0] != "break-case:tb-1" || got[1] != "break-case:tb-1" {
		t.Fatalf("keys = %v, want the same key on both attempts", got)
	}
}

// A refusal that never passes ends after the same 3 attempts as an outage, still
// released (retryable), and it does not trip the circuit breaker: the callee
// answered, so the next call still reaches it.
func TestBreakerClientBoundsAForeverReleasedRefusalAndDoesNotTripTheBreaker(t *testing.T) {
	srv, keys := fakeStorage(t, answer{404, problemBody("bin-not-found")})
	c := inventorystorage.NewBreakerClient(inventorystorage.NewClient(srv.URL, nil), nil)
	for i := 1; i <= 8; i++ {
		before := len(keys())
		err := c.BreakCase(context.Background(), req)
		if !errors.Is(err, ports.ErrCaseBreakRefusalReleased) || !errors.Is(err, ports.ErrCaseBreakUnavailable) {
			t.Fatalf("call %d = %v, want the released refusal", i, err)
		}
		if n := len(keys()) - before; n != 3 {
			t.Fatalf("call %d made %d attempts, want 3 (the circuit must stay closed)", i, n)
		}
	}
}

// A final refusal is returned at once, once: no retry.
func TestBreakerClientDoesNotRetryAFinalRefusal(t *testing.T) {
	srv, keys := fakeStorage(t, answer{400, problemBody("empty-bin-id")})
	c := inventorystorage.NewBreakerClient(inventorystorage.NewClient(srv.URL, nil), nil)
	err := c.BreakCase(context.Background(), req)
	if !errors.Is(err, ports.ErrCaseBreakRejected) || len(keys()) != 1 {
		t.Fatalf("err = %v after %d attempts, want one attempt and a rejection", err, len(keys()))
	}
}
