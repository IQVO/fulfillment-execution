package ports

import (
	"context"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// InterleavingHintCandidate is one ranked candidate of an interleaving hint.
// Rank starts at 1 (the producer's preferred candidate).
type InterleavingHintCandidate struct {
	Rank   int
	TaskId string
}

// InterleavingHint is this context's local copy of one
// task-interleaving InterleavingHintIssued occurrence (ADR 0051): an
// ADVISORY recommendation of which open tasks a station could work next
// with the smallest detour. It is never an assignment: claimNext uses it
// only as a tie-break inside one priority band and ignores it entirely
// once ExpiresAt has passed.
type InterleavingHint struct {
	HintId     string
	StationId  string
	SiteId     string
	ExpiresAt  time.Time
	Candidates []InterleavingHintCandidate
	ReceivedAt time.Time
}

// InterleavingHintStore is the write side of the local hint copy: one
// ACTIVE hint per station, so storing a hint for a station supersedes
// whatever was stored for it before. Implementations must issue their write
// through the transaction bound to ctx by UnitOfWork (when there is one),
// so the processed-event claim and the hint upsert commit or roll back
// together.
type InterleavingHintStore interface {
	ReplaceForStation(ctx context.Context, hint InterleavingHint) error
}

// InterleavingHintLookup is the read side claimNext uses: the station's
// stored hint, or nil (not an error) when the station has none or the
// stored hint expired at or before now. An ignored hint costs nothing —
// every claimNext path that cannot use a hint behaves exactly as without
// one.
type InterleavingHintLookup interface {
	ActiveForStation(ctx context.Context, stationId shared.StationId, now time.Time) (*InterleavingHint, error)
}
