package http

import (
	"net/http"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http/page"
)

// pageParams validates the keyset-paging query of a collection GET (REST
// guideline sections 3 and 4.2): an undeclared query parameter is 400
// unknown-query-parameter (so a consumer still sending a retired `after`,
// `offset`, `nextToken` fails loudly), a bad `limit` is 400 invalid-limit and
// an undecodable `cursor` is 400 invalid-cursor. filters are the collection's
// own declared parameters. ok is false when a problem was already written.
func pageParams(w http.ResponseWriter, r *http.Request, filters ...string) (limit int, after *page.Position, ok bool) {
	if name := page.Unknown(r, filters...); name != "" {
		writeProblem(w, http.StatusBadRequest, "unknown-query-parameter",
			"The query parameter is not declared by this route",
			"query parameter \""+sanitizeForLog(name)+"\" is not accepted; this collection takes limit, cursor and its documented filters",
			r.URL.Path)
		return 0, nil, false
	}
	limit, err := page.Limit(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-limit", "The limit is not valid", err.Error(), r.URL.Path)
		return 0, nil, false
	}
	after, err = page.After(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-cursor", "The cursor is not valid", err.Error(), r.URL.Path)
		return 0, nil, false
	}
	return limit, after, true
}

// afterID is the id the keyset resumes after ("" for the first page).
func afterID(after *page.Position) string {
	if after == nil {
		return ""
	}
	return after.ID
}

// idPosition is the cursor position of a row that is ordered by its id alone.
func idPosition(id string) page.Position { return page.Position{Sort: id, ID: id} }
