package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
)

// Sink is where OutboxRelay forwards drained rows — in production
// kafka.RelaySink over a topic-less writer; in tests a recorder. Each
// Encoded names its own topic.
type Sink interface {
	Send(ctx context.Context, msgs ...outboundkafka.Encoded) error
}

// OutboxRelay drains unpublished outbox_events rows onto a Sink, oldest
// first, marking each row published as it goes (ADR 0020).
//
// Delivery is at-least-once: a crash between a successful Send and the
// row's UPDATE republishes that event on the next pass. Every consumer of
// both topics already dedupes on the envelope event_id (the projector's
// processed_events table; wes-work-planning's own idempotent handling),
// and the event_id is minted at encode time and persisted with the row,
// so a redelivery carries the same id. Ordering per key is preserved
// because rows are drained in id order within one relay and FOR UPDATE
// SKIP LOCKED keeps two relays (a rolling deploy's overlapping old and
// new pod) from claiming the same row.
//
// With WithMaxAttempts(n), n > 0 (ADR 0042; cmd/execution passes
// OUTBOX_MAX_ATTEMPTS, default 20), a row that has failed n times is
// excluded from claims and logged at ERROR: later rows, including rows with
// the same key, then overtake it. That trade is deliberate (one poison row
// must not stop the whole relay forever), and the excluded row stays in
// outbox_events (attempts, last_error) until an operator re-arms it.
type OutboxRelay struct {
	pool        *pgxpool.Pool
	sink        Sink
	logger      *slog.Logger
	interval    time.Duration
	batchSize   int
	maxAttempts int
}

// DefaultOutboxMaxAttempts is the attempt cap a composition root uses when
// OUTBOX_MAX_ATTEMPTS is unset (fleet convention, ADR 0042). The relay's own
// zero value stays 0 (unlimited) so the option is always an explicit choice.
const DefaultOutboxMaxAttempts = 20

// RelayOption customises an OutboxRelay.
type RelayOption func(*OutboxRelay)

// WithInterval sets how long the relay sleeps between passes when the
// last pass found nothing to publish. Default 1s.
func WithInterval(d time.Duration) RelayOption {
	return func(r *OutboxRelay) { r.interval = d }
}

// WithBatchSize caps how many rows one pass claims. Default 100.
func WithBatchSize(n int) RelayOption {
	return func(r *OutboxRelay) { r.batchSize = n }
}

// WithMaxAttempts overrides the default of 0 (unlimited retries): a row
// that has failed this many times is excluded from future claims (still
// visible in outbox_events for manual inspection) instead of retried
// indefinitely, and the failure that reaches the cap is logged at ERROR
// with the row's id, topic, type and key (ADR 0042). 0 keeps retrying
// without bound. Every relay built by a composition root must pass it
// (architecture fitness test TestOutboxRelaysPassAttemptCap).
func WithMaxAttempts(n int) RelayOption {
	return func(r *OutboxRelay) { r.maxAttempts = n }
}

// MaxAttempts reports the configured attempt cap (0 = unlimited), so the
// composition root's wiring can be asserted without a database.
func (r *OutboxRelay) MaxAttempts() int { return r.maxAttempts }

// NewOutboxRelay constructs a relay draining pool into sink.
func NewOutboxRelay(pool *pgxpool.Pool, sink Sink, logger *slog.Logger, opts ...RelayOption) *OutboxRelay {
	if logger == nil {
		logger = slog.Default()
	}
	r := &OutboxRelay{pool: pool, sink: sink, logger: logger, interval: time.Second, batchSize: 100}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Run drains the outbox until ctx is cancelled. A pass that publishes a
// full batch is followed immediately by another pass (there is probably
// more waiting); an empty pass sleeps for the configured interval. A
// failing pass is logged and retried after the interval — the rows stay
// unpublished, so nothing is lost.
func (r *OutboxRelay) Run(ctx context.Context) error {
	for {
		n, err := r.RelayOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.ErrorContext(ctx, "outbox relay pass failed", "error", err)
		}
		if n == r.batchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.interval):
		}
	}
}

// pendingOutboxRow is one claimed outbox_events row awaiting publication.
type pendingOutboxRow struct {
	id  int64
	enc outboundkafka.Encoded
}

// RelayOnce performs a single pass: claim up to batchSize unpublished rows
// under a row lock, send each in id order, and mark it published. It
// returns how many rows were published. Rows are sent ONE AT A TIME so
// that on the first Send failure the pass stops exactly at the failed row
// (preserving per-key ordering — a later event for the same task must not
// overtake a failed earlier one), records the error on that row, commits
// what was already sent, and returns the error.
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("postgres: begin relay pass: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	batch, err := r.claim(ctx, tx)
	if err != nil {
		return 0, err
	}

	published, err := r.publishBatch(ctx, tx, batch)
	if err != nil {
		return published, err
	}

	if err := tx.Commit(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return published, fmt.Errorf("postgres: commit relay pass: %w", err)
	}
	if published > 0 {
		r.logger.DebugContext(ctx, "outbox relay published events", "count", published)
	}
	return published, nil
}

// claim locks up to batchSize unpublished outbox_events rows under tx,
// oldest first (FOR UPDATE SKIP LOCKED keeps a rolling deploy's overlapping
// old and new relay from claiming the same row), and decodes them for
// sending. With an attempt cap, rows that already failed maxAttempts times
// are skipped (ADR 0042).
func (r *OutboxRelay) claim(ctx context.Context, tx pgx.Tx) ([]pendingOutboxRow, error) {
	var attemptsFilter string
	args := []any{r.batchSize}
	if r.maxAttempts > 0 {
		attemptsFilter = "AND attempts < $2"
		args = append(args, r.maxAttempts)
	}
	rows, err := tx.Query(ctx, `
		SELECT id, topic, event_type, key, value, headers
		FROM outbox_events
		WHERE published_at IS NULL `+attemptsFilter+`
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim outbox rows: %w", err)
	}
	var batch []pendingOutboxRow
	for rows.Next() {
		var p pendingOutboxRow
		var rawHeaders []byte
		if err := rows.Scan(&p.id, &p.enc.Topic, &p.enc.EventType, &p.enc.Key, &p.enc.Value, &rawHeaders); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan outbox row: %w", err)
		}
		if p.enc.Headers, err = unmarshalHeaders(rawHeaders); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: decode outbox row %d headers: %w", p.id, err)
		}
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read outbox rows: %w", err)
	}
	return batch, nil
}

// publishBatch sends each claimed row to the sink in id order and marks it
// published, stopping exactly at the first Send failure per RelayOnce's
// at-least-once contract: the failed row's error is recorded and the pass
// is committed so the marks of the rows already sent survive.
func (r *OutboxRelay) publishBatch(ctx context.Context, tx pgx.Tx, batch []pendingOutboxRow) (int, error) {
	published := 0
	for _, p := range batch {
		if err := r.sink.Send(ctx, p.enc); err != nil {
			return published, r.failRow(ctx, tx, p, err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = $1
		`, p.id); err != nil {
			return published, fmt.Errorf("postgres: mark outbox row %d published: %w", p.id, err)
		}
		published++
	}
	return published, nil
}

// failRow records err on the failed row and commits the pass so the marks
// of the rows already sent survive, returning the send failure wrapped
// with any bookkeeping failures. The failure is logged only after a
// successful commit, so an ERROR saying a row reached the attempt cap is
// never emitted for an attempt that was rolled back.
func (r *OutboxRelay) failRow(ctx context.Context, tx pgx.Tx, p pendingOutboxRow, sendErr error) error {
	err := sendErr
	var attempts int
	recorded := true
	if uerr := tx.QueryRow(ctx, `
		UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1
		RETURNING attempts
	`, p.id, sendErr.Error()).Scan(&attempts); uerr != nil {
		recorded = false
		err = errors.Join(err, fmt.Errorf("postgres: record outbox failure: %w", uerr))
	}
	if cerr := tx.Commit(ctx); cerr != nil {
		recorded = false
		err = errors.Join(err, fmt.Errorf("postgres: commit relay pass: %w", cerr))
	}
	if recorded {
		r.logFailure(ctx, p, attempts, sendErr)
	}
	return fmt.Errorf("outbox relay: send %s to %s (row %d): %w", p.enc.EventType, p.enc.Topic, p.id, err)
}

// logFailure logs a failed send at WARN while the row will be retried, and
// at ERROR once the row has reached the attempt cap (ADR 0042): from then on
// claim excludes it, later rows overtake it (including rows with the same
// key) and it stays in outbox_events until an operator re-arms it
// (`UPDATE outbox_events SET attempts = 0 WHERE id = ...`).
func (r *OutboxRelay) logFailure(ctx context.Context, p pendingOutboxRow, attempts int, sendErr error) {
	if r.maxAttempts > 0 && attempts >= r.maxAttempts {
		r.logger.ErrorContext(ctx, "outbox relay: row reached the attempt cap and is excluded from future claims; re-arm it with attempts = 0 after fixing the cause",
			"outbox_id", p.id, "topic", p.enc.Topic, "event_type", p.enc.EventType,
			"key", string(p.enc.Key), "attempts", attempts, "max_attempts", r.maxAttempts, "error", sendErr)
		return
	}
	r.logger.WarnContext(ctx, "outbox relay: send failed, stopping this pass at the failed row",
		"outbox_id", p.id, "topic", p.enc.Topic, "attempts", attempts, "error", sendErr)
}
