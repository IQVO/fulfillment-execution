package http_test

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/url"
	"testing"
	"time"

	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// TestRetiredRoutesAreGone pins the hard cutover of the REST guideline
// (section 1): every pre-guideline verb route answers 404 or 405, there is no
// alias.
func TestRetiredRoutesAreGone(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	retired := []struct{ method, path string }{
		{stdhttp.MethodPost, "/stations/s1/claim-next"},
		{stdhttp.MethodPost, "/stations/s1/check-in"},
		{stdhttp.MethodPost, "/stations/s1/check-out"},
		{stdhttp.MethodPost, "/tasks/t1/renew-lease"},
		{stdhttp.MethodPost, "/tasks/t1/complete"},
		{stdhttp.MethodPost, "/tasks/t1/break-case"},
		{stdhttp.MethodPost, "/tasks/t1/seal-package"},
		{stdhttp.MethodPost, "/packages/p1/slam"},
		{stdhttp.MethodPost, "/tasks/expire-leases"},
		{stdhttp.MethodPost, "/tasks/sweep-cpt-misses"},
	}
	for _, rt := range retired {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			rec := doJSON(t, srv, rt.method, rt.path, map[string]any{})
			if rec.Code != stdhttp.StatusNotFound && rec.Code != stdhttp.StatusMethodNotAllowed {
				t.Fatalf("retired route answered %d, want 404 or 405: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

type taskPage struct {
	Items []struct {
		Id string `json:"id"`
	} `json:"items"`
	NextCursor string `json:"nextCursor"`
}

func getPage(t *testing.T, srv stdhttp.Handler, target string) taskPage {
	t.Helper()
	rec := doJSON(t, srv, stdhttp.MethodGet, target, nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("GET %s = %d: %s", target, rec.Code, rec.Body.String())
	}
	var p taskPage
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode %s: %v", rec.Body.String(), err)
	}
	return p
}

// walk follows nextCursor until it is absent and returns every id in order.
func walk(t *testing.T, srv stdhttp.Handler, base string, limit int) []string {
	t.Helper()
	var ids []string
	target := base + "&limit=" + itoaInt(limit)
	for pages := 0; pages < 50; pages++ {
		p := getPage(t, srv, target)
		if len(p.Items) > limit {
			t.Fatalf("page of %d items exceeds limit %d", len(p.Items), limit)
		}
		for _, it := range p.Items {
			ids = append(ids, it.Id)
		}
		if p.NextCursor == "" {
			return ids
		}
		target = base + "&limit=" + itoaInt(limit) + "&cursor=" + url.QueryEscape(p.NextCursor)
	}
	t.Fatal("pagination did not terminate")
	return nil
}

func itoaInt(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func assertWalkIsComplete(t *testing.T, got []string, want int) {
	t.Helper()
	if len(got) != want {
		t.Fatalf("walked %d rows %v, want %d", len(got), got, want)
	}
	seen := map[string]bool{}
	for i, id := range got {
		if seen[id] {
			t.Fatalf("row %q returned twice: %v", id, got)
		}
		seen[id] = true
		if i > 0 && got[i-1] >= id {
			t.Fatalf("rows not ordered by id: %v", got)
		}
	}
}

func TestGetTasks_PagesWalkEveryRowOnce(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	for i := 0; i < 7; i++ {
		rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
			"type": "PICK", "cpt": time.Now().Add(time.Hour), "orderRef": "order-p", "requiredCapabilities": []string{"pick"},
		})
		if rec.Code != stdhttp.StatusCreated {
			t.Fatalf("seed task: %d %s", rec.Code, rec.Body.String())
		}
	}
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PICK", "cpt": time.Now().Add(time.Hour), "orderRef": "other-order", "requiredCapabilities": []string{"pick"},
	})
	for _, limit := range []int{1, 2, 3, 7, 8} {
		assertWalkIsComplete(t, walk(t, srv, "/tasks?orderRef=order-p", limit), 7)
	}
	if p := getPage(t, srv, "/tasks?orderRef=order-p&limit=7"); p.NextCursor != "" {
		t.Fatalf("an exactly-full last page must not carry nextCursor, got %q", p.NextCursor)
	}
}

func TestGetPackages_PagesWalkEveryRowOnce(t *testing.T) {
	srv, _, _, packages, _ := newTestServer()
	for _, id := range []string{"pkg-a", "pkg-b", "pkg-c", "pkg-d", "pkg-e"} {
		if err := packages.Save(context.Background(), pack.New(shared.PackageId(id), "order-p", "", false, false)); err != nil {
			t.Fatalf("seed package: %v", err)
		}
	}
	for _, limit := range []int{1, 2, 5, 6} {
		assertWalkIsComplete(t, walk(t, srv, "/packages?orderRef=order-p", limit), 5)
	}
}

// TestCollections_RejectBadPagingQuery pins 400 invalid-limit, invalid-cursor
// and unknown-query-parameter (REST guideline 3.4, 4.2), including the
// retired paging parameters.
func TestCollections_RejectBadPagingQuery(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	cases := []struct{ name, query, slug string }{
		{"limit zero", "limit=0", "invalid-limit"},
		{"limit too big", "limit=501", "invalid-limit"},
		{"limit text", "limit=abc", "invalid-limit"},
		{"limit empty", "limit=", "invalid-limit"},
		{"limit repeated", "limit=1&limit=2", "invalid-limit"},
		{"cursor garbage", "cursor=%25%25", "invalid-cursor"},
		{"cursor not base64 json", "cursor=aGVsbG8", "invalid-cursor"},
		{"cursor repeated", "cursor=a&cursor=b", "invalid-cursor"},
		{"retired after", "after=x", "unknown-query-parameter"},
		{"retired offset", "offset=10", "unknown-query-parameter"},
		{"retired nextToken", "nextToken=x", "unknown-query-parameter"},
		{"undeclared filter", "status=PENDING", "unknown-query-parameter"},
	}
	for _, route := range []string{"/tasks", "/packages"} {
		for _, tc := range cases {
			t.Run(route+" "+tc.name, func(t *testing.T) {
				rec := doJSON(t, srv, stdhttp.MethodGet, route+"?orderRef=order-1&"+tc.query, nil)
				assertProblemDetails(t, rec, stdhttp.StatusBadRequest,
					"https://errors.fulfillment-execution.warehouse-systems.dev/"+tc.slug, route)
			})
		}
	}
}
