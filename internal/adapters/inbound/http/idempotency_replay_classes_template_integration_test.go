//go:build integration

// templates/idempotency/replay_classes_test.go.tmpl — the fleet reference test for
// idempotency replay classes (FINAL, OUTCOME, RETRYABLE).
//
// Copy to internal/adapters/inbound/http/idempotency_replay_classes_test.go (the package that owns
// the Idempotency-Key middleware) and substitute the three placeholders. THIS FILE IS VERBATIM: it
// imports the standard library only and names no database driver, so rendering it as an
// *_integration_test.go over a real Postgres does not trip a service's testcontainers sensor
// (TestPostgresIntegrationTestsUseTestcontainers flags pgx / database/sql in an
// *_integration_test.go without a testcontainers import in the package). The repo-specific half is
// ONE function, newReplayOps, in its own file (replay_classes_wiring_test.go.tmpl is the skeleton):
// it returns six closures, and that file is the only one that names a driver.
//
//	http           the package name of the middleware's directory (the test is http_test)
//	/tasks         one idempotent route of the service, e.g. /orders (a POST the middleware wraps)
//	idempotency-key-reused   the slug of the 409 the middleware answers for a key reused with another
//	                  request, e.g. idempotency-key-reused
//
// The tests drive the REAL middleware, built by the repo's own constructor, with a FAKE handler
// that answers what a handler of each class answers. They know nothing of the store: the repo's
// design (key row in the request transaction, a savepoint, a separate write) hides behind
// the replayOps closures. The tests must pass on every design; what differs is only how the
// closures count.
//
// Fleet rule: .claude/rules/fleet/idempotency-and-outbox.md. Standard and adoption steps:
// docs/idempotency-replay-classes.md (warehouse-harness-template). A real store (testcontainers)
// is the strongest wiring for key-in-transaction designs; an in-memory one is enough for a
// design whose store is behind a port. TestConcurrentDuplicateReplaysTheOriginalsStoredAnswer sends
// requests from several goroutines, so the store behind the closures must be safe for concurrent use.
package http_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Replay classes, as the catalogue entry declares them. The empty string is "unclassified".
const (
	classFinal     = "FINAL"
	classOutcome   = "OUTCOME"
	classRetryable = "RETRYABLE"
)

const (
	idempotencyKeyHeader      = "Idempotency-Key"
	idempotencyReplayedHeader = "Idempotency-Replayed"
	route                     = "/tasks"
	keyReusedSlug             = "idempotency-key-reused"
)

// problemEntry is one problem type of the service's catalogue, as the test needs it.
type problemEntry struct {
	Slug   string
	Status int
	Class  string // the class the catalogue entry declares; "" when it declares none
}

// replayAdapter is what the tests drive; closureAdapter (below) implements it from the replayOps a repo
// provides. One adapter value = one clean store.
//
//   - Catalogue: every problem type the service can answer on an idempotent route, read from the
//     production catalogue (NOT a copy of it), each with the class its entry declares.
//   - Middleware: the production Idempotency-Key middleware, from the repo's own constructor, over
//     this adapter's store, wrapping next.
//   - Refuse: answer e exactly as production does — through the production error writer, so the
//     class the catalogue declares is what marks the capturing writer (DoNotCache, KeepWrites, ...).
//   - Write: one observable write made THROUGH the request's context (r.Context()), i.e. in the
//     same unit of work / transaction the middleware opened for this request. It stands for the
//     aggregate and the outbox rows a real command commits.
//   - Committed: how many writes tagged tag are visible OUTSIDE any request (committed).
//   - KeyRows: how many rows the store holds for key (0 after the key was released).
type replayAdapter interface {
	Catalogue() []problemEntry
	Middleware(t *testing.T, next http.Handler) http.Handler
	Refuse(w http.ResponseWriter, r *http.Request, e problemEntry)
	Write(ctx context.Context, tag string) error
	Committed(t *testing.T, tag string) int
	KeyRows(t *testing.T, key string) int
}

// answerFn is what the fake handler does for one call.
type answerFn func(w http.ResponseWriter, r *http.Request)

// fakeHandler stands for the use case behind the middleware. It counts how often it RAN and
// answers from script in order (the last answer repeats). A write is made by the answer itself.
type fakeHandler struct {
	script []answerFn
	calls  int
}

func (h *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	i := h.calls
	h.calls++
	if i >= len(h.script) {
		i = len(h.script) - 1
	}
	h.script[i](w, r)
}

// refusing answers entry e; for an OUTCOME entry the command commits its write first.
func refusing(a replayAdapter, e problemEntry, tag string) answerFn {
	return func(w http.ResponseWriter, r *http.Request) {
		if e.Class == classOutcome {
			if err := a.Write(r.Context(), tag); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		a.Refuse(w, r, e)
	}
}

// succeeding commits one write and answers 201.
func succeeding(a replayAdapter, tag string) answerFn {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := a.Write(r.Context(), tag); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"created"}`)
	}
}

func post(t *testing.T, h http.Handler, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	return serve(h, key, body)
}

func firstOf(t *testing.T, a replayAdapter, class string) problemEntry {
	t.Helper()
	for _, e := range a.Catalogue() {
		if e.Class == class && e.Status < http.StatusInternalServerError {
			return e
		}
	}
	return problemEntry{}
}

func replayed(rec *httptest.ResponseRecorder) bool {
	return rec.Header().Get(idempotencyReplayedHeader) == "true"
}

// classDeclared reports whether c is one of the three classes.
func classDeclared(c string) bool {
	return c == classFinal || c == classOutcome || c == classRetryable
}

// (a) Every catalogue entry declares exactly one class; an unclassified entry fails. There is no
// runtime default: a new problem type is classified by the PR that adds it, or this test is red.
func TestEveryProblemTypeDeclaresAReplayClass(t *testing.T) {
	entries := newReplayAdapter(t).Catalogue()
	if len(entries) == 0 {
		t.Fatal("the catalogue accessor returned no problem types: the test would pass on nothing")
	}
	for _, e := range entries {
		t.Run(e.Slug, func(t *testing.T) {
			if !classDeclared(e.Class) {
				t.Errorf("problem type %q (%d) declares no replay class (%q): set FINAL, OUTCOME or RETRYABLE on its catalogue entry (.claude/rules/fleet/idempotency-and-outbox.md)", e.Slug, e.Status, e.Class)
			}
			if e.Status >= http.StatusInternalServerError && e.Class != classRetryable {
				t.Errorf("problem type %q answers %d: a 5xx is never stored, so its class is RETRYABLE, got %q", e.Slug, e.Status, e.Class)
			}
		})
	}
}

// The check above must bite: an unclassified entry and a misspelt class are refused.
func TestTheUnclassifiedEntryCheckBites(t *testing.T) {
	for _, c := range []string{"", "final", "STORED", "OUTCOME "} {
		if classDeclared(c) {
			t.Errorf("class %q is accepted; only FINAL, OUTCOME and RETRYABLE are classes", c)
		}
	}
	for _, c := range []string{classFinal, classOutcome, classRetryable} {
		if !classDeclared(c) {
			t.Errorf("class %q is refused", c)
		}
	}
}

// (b) For EVERY catalogue entry: the first request is answered with that problem, then the same
// request again under the same key. By class: FINAL and OUTCOME are stored and replayed (the
// handler ran once); RETRYABLE (and every 5xx) is not stored and runs again.
func TestEveryProblemTypeIsReplayedOrReExecutedByItsClass(t *testing.T) {
	for _, e := range newReplayAdapter(t).Catalogue() {
		t.Run(e.Slug+"/"+e.Class, func(t *testing.T) {
			if !classDeclared(e.Class) {
				t.Skip("unclassified: reported by TestEveryProblemTypeDeclaresAReplayClass")
			}
			a := newReplayAdapter(t)
			h := &fakeHandler{script: []answerFn{refusing(a, e, e.Slug)}}
			mw := a.Middleware(t, h)
			key := "key-" + e.Slug
			stored := e.Class != classRetryable && e.Status < http.StatusInternalServerError

			first := post(t, mw, key, `{"n":1}`)
			if first.Code != e.Status {
				t.Fatalf("first answer status = %d, want %d (%s)", first.Code, e.Status, first.Body.String())
			}
			if replayed(first) {
				t.Errorf("the first answer carries %s: true", idempotencyReplayedHeader)
			}
			wantRows := 0
			if stored {
				wantRows = 1
			}
			if got := a.KeyRows(t, key); got != wantRows {
				t.Errorf("key rows after the first answer = %d, want %d", got, wantRows)
			}
			wantWrites := 0
			if e.Class == classOutcome {
				wantWrites = 1
			}
			if got := a.Committed(t, e.Slug); got != wantWrites {
				t.Errorf("committed writes after the first answer = %d, want %d", got, wantWrites)
			}

			second := post(t, mw, key, `{"n":1}`)
			if second.Code != e.Status {
				t.Errorf("second answer status = %d, want %d", second.Code, e.Status)
			}
			if stored {
				if h.calls != 1 {
					t.Errorf("handler ran %d times, want 1: a %s answer is stored and replayed", h.calls, e.Class)
				}
				if !replayed(second) {
					t.Errorf("the replay does not carry %s: true", idempotencyReplayedHeader)
				}
				if second.Body.String() != first.Body.String() {
					t.Errorf("replayed body %q differs from the first answer %q", second.Body.String(), first.Body.String())
				}
			} else {
				if h.calls != 2 {
					t.Errorf("handler ran %d times, want 2: a %s answer is not stored, the retry executes", h.calls, e.Class)
				}
				if replayed(second) {
					t.Errorf("a %s answer was replayed", e.Class)
				}
			}
			if got := a.Committed(t, e.Slug); got != wantWrites {
				t.Errorf("committed writes after the second request = %d, want %d: a replay records nothing new", got, wantWrites)
			}
		})
	}
}

// FINAL: the answer is a function of the request alone. Same key + same body replays the same
// answer with Idempotency-Replayed: true and the handler ran once; the same key with another body
// is still the reuse conflict.
func TestFinalRefusalIsStoredAndReplayed(t *testing.T) {
	a := newReplayAdapter(t)
	e := firstOf(t, a, classFinal)
	if e.Slug == "" {
		t.Fatal("the catalogue has no FINAL entry below 500: classify the validation / malformed-body problem types")
	}
	h := &fakeHandler{script: []answerFn{refusing(a, e, e.Slug)}}
	mw := a.Middleware(t, h)

	first := post(t, mw, "k-final", `{"n":1}`)
	again := post(t, mw, "k-final", `{"n":1}`)
	if again.Code != first.Code || again.Body.String() != first.Body.String() {
		t.Errorf("replay = %d %q, want %d %q", again.Code, again.Body.String(), first.Code, first.Body.String())
	}
	if !replayed(again) || replayed(first) {
		t.Errorf("replayed header: first=%v replay=%v, want false/true", replayed(first), replayed(again))
	}
	if h.calls != 1 {
		t.Errorf("handler ran %d times, want 1", h.calls)
	}
	other := post(t, mw, "k-final", `{"n":2}`)
	if other.Code != http.StatusConflict || !strings.Contains(other.Body.String(), keyReusedSlug) {
		t.Errorf("another body under a stored key = %d %q, want 409 %s", other.Code, other.Body.String(), keyReusedSlug)
	}
	if h.calls != 1 {
		t.Errorf("handler ran %d times after the reuse conflict, want 1", h.calls)
	}
}

// OUTCOME: the command committed state and then refused. The writes commit WITH the stored
// answer (one transaction), the same key replays the answer, and a replay records nothing new.
func TestOutcomeRefusalCommitsItsWritesOnceAndIsReplayed(t *testing.T) {
	a := newReplayAdapter(t)
	e := firstOf(t, a, classOutcome)
	if e.Slug == "" {
		t.Skip("the catalogue has no OUTCOME entry: nothing to prove (add this test's case with the first one)")
	}
	h := &fakeHandler{script: []answerFn{refusing(a, e, "outcome")}}
	mw := a.Middleware(t, h)

	first := post(t, mw, "k-outcome", `{"n":1}`)
	if first.Code != e.Status {
		t.Fatalf("status = %d, want %d", first.Code, e.Status)
	}
	if got := a.Committed(t, "outcome"); got != 1 {
		t.Fatalf("committed writes after the refusal = %d, want 1: an OUTCOME refusal commits what the command did", got)
	}
	if got := a.KeyRows(t, "k-outcome"); got != 1 {
		t.Errorf("key rows = %d, want 1: the answer is stored in the same transaction as the writes", got)
	}
	for i := 0; i < 3; i++ {
		rep := post(t, mw, "k-outcome", `{"n":1}`)
		if rep.Code != e.Status || !replayed(rep) || rep.Body.String() != first.Body.String() {
			t.Errorf("replay %d = %d replayed=%v %q, want the stored %d answer", i, rep.Code, replayed(rep), rep.Body.String(), e.Status)
		}
	}
	if h.calls != 1 {
		t.Errorf("handler ran %d times, want 1", h.calls)
	}
	if got := a.Committed(t, "outcome"); got != 1 {
		t.Errorf("committed writes after 3 replays = %d, want 1: a replay records nothing new", got)
	}
}

// RETRYABLE: nothing committed, the answer depends on state that can change. The key is released
// (no stored row); the same request under the same key executes AGAIN and, once the state allows
// it, succeeds and is then stored; a different body under the same key is a NEW request.
func TestRetryableRefusalReleasesTheKeySoTheRetryExecutes(t *testing.T) {
	a := newReplayAdapter(t)
	e := firstOf(t, a, classRetryable)
	if e.Slug == "" {
		t.Fatal("the catalogue has no RETRYABLE entry below 500: classify the optimistic-lock conflict and the not-ready problem types")
	}
	h := &fakeHandler{script: []answerFn{refusing(a, e, "x"), succeeding(a, "ok")}}
	mw := a.Middleware(t, h)

	refused := post(t, mw, "k-retry", `{"n":1}`)
	if refused.Code != e.Status {
		t.Fatalf("first status = %d, want %d", refused.Code, e.Status)
	}
	if got := a.KeyRows(t, "k-retry"); got != 0 {
		t.Fatalf("key rows after a RETRYABLE refusal = %d, want 0: the key must be released", got)
	}
	retry := post(t, mw, "k-retry", `{"n":1}`)
	if h.calls != 2 {
		t.Fatalf("handler ran %d times, want 2: the retry under the same key executes again", h.calls)
	}
	if retry.Code != http.StatusCreated || replayed(retry) {
		t.Errorf("retry = %d replayed=%v, want a fresh 201 (the state changed)", retry.Code, replayed(retry))
	}
	if got := a.KeyRows(t, "k-retry"); got != 1 {
		t.Errorf("key rows after the success = %d, want 1: a 2xx is stored", got)
	}
	if rep := post(t, mw, "k-retry", `{"n":1}`); rep.Code != http.StatusCreated || !replayed(rep) || h.calls != 2 {
		t.Errorf("third request = %d replayed=%v calls=%d, want the stored 201 replayed", rep.Code, replayed(rep), h.calls)
	}
}

func TestRetryableRefusalLeavesTheKeyFreeForAnotherRequest(t *testing.T) {
	a := newReplayAdapter(t)
	e := firstOf(t, a, classRetryable)
	if e.Slug == "" {
		t.Fatal("the catalogue has no RETRYABLE entry below 500")
	}
	h := &fakeHandler{script: []answerFn{refusing(a, e, "x"), succeeding(a, "ok")}}
	mw := a.Middleware(t, h)

	post(t, mw, "k-free", `{"n":1}`)
	other := post(t, mw, "k-free", `{"n":2}`)
	if strings.Contains(other.Body.String(), keyReusedSlug) {
		t.Fatalf("a different body under a released key answered %s: the key must be free, the request is new", keyReusedSlug)
	}
	if h.calls != 2 || other.Code != http.StatusCreated {
		t.Errorf("different body under the released key = %d calls=%d, want a new request: 201 and 2 calls", other.Code, h.calls)
	}
}

// (c) A 2xx is stored and replayed.
func TestSuccessIsStoredAndReplayed(t *testing.T) {
	a := newReplayAdapter(t)
	h := &fakeHandler{script: []answerFn{succeeding(a, "ok")}}
	mw := a.Middleware(t, h)

	first := post(t, mw, "k-2xx", `{"n":1}`)
	rep := post(t, mw, "k-2xx", `{"n":1}`)
	if first.Code != http.StatusCreated || replayed(first) {
		t.Fatalf("first = %d replayed=%v, want a fresh 201", first.Code, replayed(first))
	}
	if rep.Code != http.StatusCreated || !replayed(rep) || rep.Body.String() != first.Body.String() {
		t.Errorf("replay = %d replayed=%v %q, want the stored 201", rep.Code, replayed(rep), rep.Body.String())
	}
	if h.calls != 1 || a.Committed(t, "ok") != 1 || a.KeyRows(t, "k-2xx") != 1 {
		t.Errorf("calls=%d committed=%d rows=%d, want 1/1/1", h.calls, a.Committed(t, "ok"), a.KeyRows(t, "k-2xx"))
	}
}

// (c) A 5xx is never stored: nothing is remembered under the key and the retry executes.
func TestServerErrorIsNotStored(t *testing.T) {
	a := newReplayAdapter(t)
	boom := func(w http.ResponseWriter, r *http.Request) {
		_ = a.Write(r.Context(), "boom")
		w.WriteHeader(http.StatusInternalServerError)
	}
	h := &fakeHandler{script: []answerFn{boom, succeeding(a, "ok")}}
	mw := a.Middleware(t, h)

	if first := post(t, mw, "k-5xx", `{"n":1}`); first.Code < http.StatusInternalServerError {
		t.Fatalf("first = %d, want a 5xx", first.Code)
	}
	if got := a.KeyRows(t, "k-5xx"); got != 0 {
		t.Errorf("key rows after a 5xx = %d, want 0", got)
	}
	retry := post(t, mw, "k-5xx", `{"n":1}`)
	if h.calls != 2 || retry.Code != http.StatusCreated || replayed(retry) {
		t.Errorf("retry = %d replayed=%v calls=%d, want a fresh 201 after 2 calls", retry.Code, replayed(retry), h.calls)
	}
}

// (c) A panic is never stored either. The middleware may re-panic (net/http recovers) or answer
// 500; both are fine, what is not fine is a stored row.
func TestPanicIsNotStored(t *testing.T) {
	a := newReplayAdapter(t)
	crash := func(http.ResponseWriter, *http.Request) { panic("handler panic") }
	h := &fakeHandler{script: []answerFn{crash, succeeding(a, "ok")}}
	mw := a.Middleware(t, h)

	func() {
		defer func() { _ = recover() }()
		post(t, mw, "k-panic", `{"n":1}`)
	}()
	if got := a.KeyRows(t, "k-panic"); got != 0 {
		t.Errorf("key rows after a panic = %d, want 0", got)
	}
	if retry := post(t, mw, "k-panic", `{"n":1}`); h.calls != 2 || retry.Code != http.StatusCreated {
		t.Errorf("retry = %d calls=%d, want a fresh 201 after 2 calls", retry.Code, h.calls)
	}
}

// (d) An IN-FLIGHT duplicate replays the original's answer (fleet carry P3-R15b / P4-R16). A copy of
// a keyed request (same key, same body) that passes the key lookup while the original is still in
// flight waits for it (on the key, or on an aggregate lock inside the use case) and, once the
// original committed, must answer the original's STORED answer: N copies are one run and N-1
// replays. The defect this kills: the copy re-runs the use case, the state the original committed
// makes that re-run a RETRYABLE refusal, which is not stored, and the copy answers that refusal
// instead of the original's 201. Fixes: re-read the key before answering an unstored refusal
// (material-flow-control cab4359, finishRefused), or take the key lock at the start of the unit and
// re-read there (amr-fleet-control b8d0026). A design that claims the key row inside the request
// transaction before the handler passes as it is: its copies wait on the key and replay.
//
// The original's handler holds the "lock" until every copy has entered the handler or
// inFlightGrace has passed (a design that blocks copies on the key never lets them in); each copy
// that does reach the handler waits until the original has answered, then refuses with the
// catalogue's first RETRYABLE entry, as a re-run against the original's committed state would. One
// copy carries another body: it must answer the reuse conflict, not its own refusal. The closures
// are called from several goroutines here, so the wiring's store must be safe for concurrent use
// (a real Postgres is; an in-memory fake needs its own mutex).
//
// Mutant it must kill: delete the re-read (before the unstored refusal, or at the start of the
// unit) and the copies answer the RETRYABLE status unreplayed; the harness selftest runs that
// mutant (TOY_MUTATION=no-reread) over both fix shapes.
func TestConcurrentDuplicateReplaysTheOriginalsStoredAnswer(t *testing.T) {
	a := newReplayAdapter(t)
	e := firstOf(t, a, classRetryable)
	if e.Slug == "" {
		t.Fatal("the catalogue has no RETRYABLE entry below 500")
	}
	h := newInFlightHandler(a, e, inFlightCopies)
	mw := a.Middleware(t, h)
	const key, body = "k-inflight", `{"n":1}`

	first, copies := sendInFlight(t, h, mw, key, body)
	if first.Code != http.StatusCreated || replayed(first) {
		t.Fatalf("original = %d replayed=%v %q, want a fresh 201", first.Code, replayed(first), first.Body.String())
	}
	for i, rec := range copies[:len(copies)-1] {
		if rec.Code != http.StatusCreated || !replayed(rec) || rec.Body.String() != first.Body.String() {
			t.Errorf("in-flight copy %d = %d replayed=%v %q, want the original's stored 201 replayed (not its own %d %s re-run)",
				i, rec.Code, replayed(rec), rec.Body.String(), e.Status, e.Slug)
		}
	}
	if other := copies[len(copies)-1]; other.Code != http.StatusConflict || !strings.Contains(other.Body.String(), keyReusedSlug) {
		t.Errorf("in-flight copy with another body = %d %q, want 409 %s", other.Code, other.Body.String(), keyReusedSlug)
	}
	if got := a.Committed(t, "inflight-original"); got != 1 {
		t.Errorf("committed writes of the original = %d, want 1: one run", got)
	}
	if got := a.KeyRows(t, key); got != 1 {
		t.Errorf("key rows = %d, want 1: the original's answer is stored once", got)
	}
	if rep := post(t, mw, key, body); rep.Code != http.StatusCreated || !replayed(rep) {
		t.Errorf("a later copy = %d replayed=%v, want the stored 201 replayed", rep.Code, replayed(rep))
	}
}

// sendInFlight sends the original, waits until it is inside the handler, sends inFlightCopies copies
// (the last with another body), lets the original finish and returns every answer.
func sendInFlight(t *testing.T, h *inFlightHandler, mw http.Handler, key, body string) (*httptest.ResponseRecorder, []*httptest.ResponseRecorder) {
	t.Helper()
	original := make(chan *httptest.ResponseRecorder, 1)
	go func() { original <- serve(mw, key, body) }()
	select {
	case <-h.originalEntered:
	case <-time.After(inFlightTimeout):
		t.Fatal("the original request never reached the handler")
	}
	copies := make([]*httptest.ResponseRecorder, h.copies)
	var wg sync.WaitGroup
	for i := range copies {
		b := body
		if i == len(copies)-1 {
			b = `{"n":2}` // the in-flight copy with another body
		}
		wg.Add(1)
		go func(i int, b string) {
			defer wg.Done()
			copies[i] = serve(mw, key, b)
		}(i, b)
	}
	h.awaitCopies(inFlightGrace)
	close(h.release)
	first := <-original
	close(h.originalAnswered)
	wg.Wait()
	return first, copies
}

const (
	inFlightCopies  = 3 // copies sent while the original is in flight; the last carries another body
	inFlightGrace   = 250 * time.Millisecond
	inFlightTimeout = 10 * time.Second // a bound so a broken middleware fails instead of hanging
)

// inFlightHandler: call 0 is the original (waits on release, writes, answers 201); every later call
// is a copy that reached the handler (waits until the original answered, then refuses with e).
type inFlightHandler struct {
	a                replayAdapter
	e                problemEntry
	mu               sync.Mutex
	calls            int
	originalEntered  chan struct{}
	copyEntered      chan struct{}
	copies           int
	release          chan struct{}
	originalAnswered chan struct{}
}

func newInFlightHandler(a replayAdapter, e problemEntry, copies int) *inFlightHandler {
	return &inFlightHandler{a: a, e: e, copies: copies, originalEntered: make(chan struct{}),
		copyEntered: make(chan struct{}, copies), release: make(chan struct{}), originalAnswered: make(chan struct{})}
}

func (h *inFlightHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	i := h.calls
	h.calls++
	h.mu.Unlock()
	if i == 0 {
		close(h.originalEntered)
		waitOrTimeout(h.release)
		succeeding(h.a, "inflight-original")(w, r)
		return
	}
	select {
	case h.copyEntered <- struct{}{}:
	default:
	}
	waitOrTimeout(h.originalAnswered)
	refusing(h.a, h.e, "inflight-copy")(w, r)
}

// awaitCopies returns once every copy entered the handler or grace has passed.
func (h *inFlightHandler) awaitCopies(grace time.Duration) {
	deadline := time.After(grace)
	for n := 0; n < h.copies; n++ {
		select {
		case <-h.copyEntered:
		case <-deadline:
			return
		}
	}
}

func waitOrTimeout(c <-chan struct{}) {
	select {
	case <-c:
	case <-time.After(inFlightTimeout):
	}
}

// serve sends one keyed POST; post is its *testing.T twin (the in-flight goroutines call serve).
func serve(h http.Handler, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, route, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(idempotencyKeyHeader, key)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// ---------------------------------------------------------------------------------------------
// replayOps is what a repo provides: the six operations of replayAdapter as FUNCTION FIELDS. Each
// closure may capture a pool, a store port or a fake; this file never names the type of any of
// them, so it needs no driver import. One replayOps value = one clean store. The repo returns it
// from newReplayOps (its own file, see replay_classes_wiring_test.go.tmpl):
//
//   - Catalogue: every problem type the service can answer on an idempotent route, read from the
//     production catalogue (NOT a copy of it), each with the class its entry declares.
//   - Middleware: the production Idempotency-Key middleware, from the repo's own constructor, over
//     this value's store, wrapping next.
//   - Refuse: answer e through the production error writer (writeError / WriteProblem) with the
//     entry's error or slug, so the production class marking runs. Do NOT write the response by hand.
//   - Write: one write made THROUGH the request's context, in the unit of work the middleware
//     opened for this request (the repo's TxBinder / unit-of-work join).
//   - Committed: count writes tagged tag from OUTSIDE any request (another connection).
//   - KeyRows: rows the idempotency store holds for key (0 after the key was released).
type replayOps struct {
	Catalogue  func() []problemEntry
	Middleware func(t *testing.T, next http.Handler) http.Handler
	Refuse     func(w http.ResponseWriter, r *http.Request, e problemEntry)
	Write      func(ctx context.Context, tag string) error
	Committed  func(t *testing.T, tag string) int
	KeyRows    func(t *testing.T, key string) int
}

// closureAdapter is the replayAdapter the tests above use: it forwards every method to its closure.
type closureAdapter struct{ ops replayOps }

func newReplayAdapter(t *testing.T) replayAdapter {
	t.Helper()
	return &closureAdapter{ops: newReplayOps(t)}
}

func (a *closureAdapter) Catalogue() []problemEntry { return a.ops.Catalogue() }

func (a *closureAdapter) Middleware(t *testing.T, next http.Handler) http.Handler {
	t.Helper()
	return a.ops.Middleware(t, next)
}

func (a *closureAdapter) Refuse(w http.ResponseWriter, r *http.Request, e problemEntry) {
	a.ops.Refuse(w, r, e)
}

func (a *closureAdapter) Write(ctx context.Context, tag string) error { return a.ops.Write(ctx, tag) }

func (a *closureAdapter) Committed(t *testing.T, tag string) int {
	t.Helper()
	return a.ops.Committed(t, tag)
}

func (a *closureAdapter) KeyRows(t *testing.T, key string) int {
	t.Helper()
	return a.ops.KeyRows(t, key)
}
