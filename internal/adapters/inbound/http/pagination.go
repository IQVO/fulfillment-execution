package http

import (
	"context"
	"net/http"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http/page"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
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

// writeOrderRefPage serves the collection-by-orderRef page shape shared by
// GET /tasks and GET /packages (ADR-0033): the keyset query is validated by
// pageParams (orderRef is the only declared filter), a missing/empty
// orderRef is 400, fetch loads one page (the caller narrows the keyset id
// type), rows are mapped through toResp and the page is ordered by id
// alone. An unknown orderRef is not an error: fetch returns an empty page.
func writeOrderRefPage[E any, R any](w http.ResponseWriter, r *http.Request,
	fetch func(context.Context, shared.OrderRef, string, int) ([]E, error),
	toResp func(E) R, position func(R) page.Position) {
	limit, after, ok := pageParams(w, r, "orderRef")
	if !ok {
		return
	}
	orderRef := r.URL.Query().Get("orderRef")
	if orderRef == "" {
		writeBadRequest(w, r, "orderRef is required")
		return
	}
	items, err := fetch(r.Context(), shared.OrderRef(orderRef), afterID(after), page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	resp := make([]R, 0, len(items))
	for _, e := range items {
		resp = append(resp, toResp(e))
	}
	writeJSON(w, http.StatusOK, page.Of(resp, limit, position))
}
