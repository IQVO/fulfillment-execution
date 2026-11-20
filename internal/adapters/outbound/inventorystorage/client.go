// Package inventorystorage is the outbound adapter of ports.CaseBreaker: an
// HTTP client of inventory-storage's POST /containers/{lpn}/case-breaks (ADR 0054),
// behind the fleet's retry + circuit-breaker pattern (see facilitylayout). A
// break is a write, so every attempt carries the same Idempotency-Key and a
// retry can never break a case twice.
package inventorystorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// DefaultTimeout bounds one break request.
const DefaultTimeout = 5 * time.Second

// maxErrorBody caps how much of an error response is quoted in an error;
// maxProblemBody caps how much of it is read to find the problem type.
const (
	maxErrorBody   = 512
	maxProblemBody = 4096
)

// HTTPDoer is the subset of *http.Client this adapter depends on.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client calls inventory-storage over plain net/http.
type Client struct {
	baseURL string
	doer    HTTPDoer
}

var _ ports.CaseBreaker = (*Client)(nil)

// NewClient builds a Client against baseURL (INVENTORY_STORAGE_BASE_URL). A
// nil doer defaults to an *http.Client with DefaultTimeout.
func NewClient(baseURL string, doer HTTPDoer) *Client {
	if doer == nil {
		doer = &http.Client{Timeout: DefaultTimeout}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), doer: doer}
}

type breakRequestBody struct {
	LooseBin string `json:"looseBin"`
}

// BreakCase posts the break. 2xx is success (including the replay of an
// earlier answer for the same Idempotency-Key). A refusal whose problem type
// inventory-storage declares RETRYABLE (caseBreakRefusals) was not stored: it
// is released and transient (ports.ErrCaseBreakRefusalReleased, which wraps
// ports.ErrCaseBreakUnavailable). Transport errors, 408, 429 and 5xx are
// transient (ports.ErrCaseBreakUnavailable); every other refusal (a FINAL or
// OUTCOME slug, an unknown slug, a 4xx without a problem body) is permanent
// (ports.ErrCaseBreakRejected).
func (c *Client) BreakCase(ctx context.Context, r ports.CaseBreakRequest) error {
	body, err := json.Marshal(breakRequestBody{LooseBin: r.LooseBin})
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/containers/%s/case-breaks", c.baseURL, url.PathEscape(r.LPN))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Idempotency-Key", r.IdempotencyKey)

	resp, err := c.doer.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", ports.ErrCaseBreakUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxProblemBody))
	detail := fmt.Sprintf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw[:min(len(raw), maxErrorBody)])))
	switch {
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("%w: %s", ports.ErrCaseBreakUnavailable, detail)
	case releasedRefusal(problemSlug(raw)):
		// The callee did not store this refusal: the key is free, so the same
		// request under the same key executes again (refusal_classes.go).
		return fmt.Errorf("%w: %w: %s", ports.ErrCaseBreakRefusalReleased, ports.ErrCaseBreakUnavailable, detail)
	default:
		return fmt.Errorf("%w: %s", ports.ErrCaseBreakRejected, detail)
	}
}

// problemSlug is the last path segment of the `type` of an RFC 7807 body, or ""
// when the body is not a problem document (an unknown refusal is terminal).
func problemSlug(body []byte) string {
	var p struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(body, &p) != nil {
		return ""
	}
	return p.Type[strings.LastIndex(p.Type, "/")+1:]
}
