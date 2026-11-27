package http

import "net/http"

// ProblemTypeForTest is one problem type of the catalogue, for the external
// test package.
type ProblemTypeForTest struct {
	Slug   string
	Status int
	Class  string
}

// ProblemTypesForTest is the whole catalogue, one entry per slug and status.
// The class column is empty until the catalogue entries declare it — the
// replay-class tests go red on exactly that.
func ProblemTypesForTest() []ProblemTypeForTest {
	var out []ProblemTypeForTest
	seen := map[string]bool{}
	for _, e := range problemCatalog {
		slug, _ := problemTypeAndTitle(e.err)
		key := slug + http.StatusText(statusFor(e.err))
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, ProblemTypeForTest{Slug: slug, Status: statusFor(e.err), Class: ""})
	}
	return out
}

// RefuseForTest answers the problem type (slug, status) through the
// production error writer, so the production class marking (once it exists)
// is what the middleware's capture reads.
func RefuseForTest(w http.ResponseWriter, r *http.Request, slug string, status int) {
	for _, e := range problemCatalog {
		entrySlug, _ := problemTypeAndTitle(e.err)
		if entrySlug == slug && statusFor(e.err) == status {
			writeError(w, r, e.err)
			return
		}
	}
	panic("no problem type " + slug)
}
