// Package kafkacatalog is the outbound adapter that keeps a local,
// in-memory process-path catalogue up to date by consuming
// process-path-management's warehouse.process-path-management.events
// topic, replacing the static YAML file (filecatalog) this service used
// to boot-load. It satisfies BOTH ports.PathCatalogue (the read side
// every existing use case already depends on) and a Ready() readiness
// gate so this service's own /healthz can report "not ready" until the
// initial replay of the topic's full history completes.
//
// Why a readiness gate exists at all: the retired filecatalog.Load was a
// BOOT-TIME hard invariant — the process refused to start at all with a
// missing/malformed file, so every request was served against a
// complete, valid catalogue from the very first accepted connection. A
// fresh Kafka consumer group joining at the earliest offset needs time
// to replay the topic before its local cache is actually complete; if
// this service accepted traffic before that replay finished, an early
// request for a real, valid path could see a false ErrUnknownPath —
// a functional regression, not a cosmetic one. Ready() preserves the
// old guarantee's INTENT (never serve traffic against an incomplete
// catalogue) using a mechanism appropriate to an event-sourced cache
// rather than a one-shot file read: block readiness, not process
// startup, since a transient Kafka outage should not be fatal the way a
// missing config file was.
package kafkacatalog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
)

// replayCommitInterval makes this full-replay reader commit offsets periodically and asynchronously.
// With a GroupID set and CommitInterval left at zero, kafka-go's ReadMessage performs a SYNCHRONOUS
// commit round trip after EVERY message, turning boot replay into O(history) x RTT (order-management
// hit a CrashLoopBackOff from this). Async commits are safe ONLY because the group is unique to this
// process (uniqueConsumerGroup): its offsets are never resumed. Enforced by
// internal/architecture/events_fitness_test.go (TestReplayConsumersSetCommitInterval).
const replayCommitInterval = time.Second

// Topic is process-path-management's publish topic — this service has no
// business knowing anything else about that service beyond this topic
// name, the CloudEvents `type` strings, and the payload shape below,
// without importing the other repo's code.
const Topic = "warehouse.process-path-management.events"

// consumerGroupPrefix names this service's dedicated, PER-PROCESS
// consumer group on Topic. It is deliberately NOT a fixed, shared name
// (see NewConsumer's doc comment for the real bug this fixes): every
// call to NewConsumer generates a fresh, unique group id built on this
// prefix, so every process instance is guaranteed a genuine full replay
// of Topic into its own in-memory cache, every single start.
const consumerGroupPrefix = "fulfillment-execution-process-path-catalogue"

// Full CloudEvents `type` strings this consumer acts on —
// process-path-management's ProcessPath* events, byte-identical to the
// fleet cross-service contract (ADR-0032). Dispatch is on the exact string,
// never a short name or a suffix match.
const (
	TypeProcessPathCreated     = "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated"
	TypeProcessPathUpdated     = "com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated"
	TypeProcessPathDeactivated = "com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated"
)

// pathData is the payload shape for all three event types on Topic.
// RequiredCapabilities/MatchPrefix are absent (not empty-arrayed) on a
// ProcessPathDeactivated event — see process-path-management's own
// publisher doc comment. DestinationLocationRole mirrors that service's
// ProcessPathData.DestinationLocationRole wire field
// (`destination_location_role`, omitempty) — absent (not
// empty-stringed) for a path with no declared destination role, which
// unmarshal already leaves as pathData's own zero value "".
type pathData struct {
	PathId                  string   `json:"path_id"`
	MatchPrefix             string   `json:"match_prefix"`
	Direct                  bool     `json:"direct"`
	RequiredCapabilities    []string `json:"required_capabilities"`
	DestinationLocationRole string   `json:"destination_location_role"`
}

// Reader is the subset of *kafkago.Reader this Consumer needs, so tests
// can substitute a fake without a live broker.
type Reader interface {
	ReadMessage(ctx context.Context) (kafkago.Message, error)
	Close() error
}

// Consumer maintains a local pathcatalog.Catalogue by replaying Topic
// from its earliest offset (a fresh consumer group, same convention as
// labor-performance's analytics projector) and applying every
// ProcessPath* event as it arrives. It satisfies ports.PathCatalogue via
// Lookup, delegating to the current in-memory snapshot.
type Consumer struct {
	Reader Reader
	Logger *slog.Logger

	mu      sync.RWMutex
	paths   map[string]pathcatalog.PathDefinition
	ready   bool
	readyCh chan struct{}
	target  targetOffsets
}

// targetOffsets is the per-partition "caught up" watermark captured once
// at startup (see newTargetOffsets), so Ready() reflects "has this
// consumer seen everything that existed in the topic at the moment it
// started", not "will it ever catch up to a topic that keeps growing" —
// the latter would never settle to true on a live, actively-published
// topic.
type targetOffsets map[int]int64

// NewConsumer constructs a Consumer reading Topic from brokers under a
// fresh, PROCESS-UNIQUE consumer group, starting at the earliest offset.
// It dials the topic directly to capture each partition's current last
// offset as this instance's readiness target BEFORE starting to
// consume, so Ready() reports true exactly once THIS consumer has
// itself processed every message that existed in the topic at
// startup — not an instant before, not waiting for messages published
// after startup too, and — the critical property a fixed shared group
// name does NOT give you — never skipped because some OTHER process
// instance already committed past them.
//
// Why the group must be per-process, not a fixed shared name: this
// consumer's job is to rebuild a complete in-memory cache from Topic's
// full history on every single start (an event-sourced read model, not
// a work queue where each message should be processed exactly once
// fleet-wide). A FIXED group name defeats that: Kafka consumer group
// offsets are shared infrastructure state, not per-process state, so a
// brand-new process joining a group some earlier instance already
// consumed resumes from THAT instance's committed offset — meaning the
// new process's own (empty) in-memory map is reported ready without
// ever having actually replayed anything into it. This was caught by
// actually restarting a real instance against a live broker with an
// already-used group name and observing "ready" fire instantly with an
// empty path list — not by the unit tests alone, which only exercise a
// Reader directly and never touch group-offset semantics at all. An
// earlier attempted fix (checking the group's already-committed offset
// via OffsetFetch and treating "already covered" as ready) made this
// WORSE, not better: it papered over the symptom while making the
// same-empty-cache failure mode fire even more readily. The only
// correct fix is: never share a consumer group across process
// instances for this pattern.
func NewConsumer(ctx context.Context, brokers []string, logger *slog.Logger) (*Consumer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	target, err := newTargetOffsets(ctx, brokers, Topic)
	if err != nil {
		return nil, fmt.Errorf("kafkacatalog: determine readiness target: %w", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   Topic,
		GroupID: uniqueConsumerGroup(),
		// A fresh (guaranteed by GroupID's uniqueness, see above)
		// consumer group must see the topic's full history — see the
		// package doc comment and labor-performance's identical
		// AnalyticsConsumer precedent.
		StartOffset:    kafkago.FirstOffset,
		CommitInterval: replayCommitInterval,
	})

	c := &Consumer{
		Reader:  reader,
		Logger:  logger,
		paths:   make(map[string]pathcatalog.PathDefinition),
		readyCh: make(chan struct{}),
		target:  target,
	}
	if len(target) == 0 {
		// The topic has no partitions with any messages yet (a brand
		// new topic, or process-path-management has never published)
		// — there is nothing to catch up to, so this consumer is
		// trivially ready from the start.
		c.markReady()
	}
	return c, nil
}

// uniqueConsumerGroup builds a group id unique to this process instance
// (hostname + PID + a nanosecond timestamp, so even two processes on
// the same host started in the same second never collide) — see
// NewConsumer's doc comment for why per-process uniqueness is a
// correctness requirement here, not a cosmetic choice. This group is
// abandoned (never reused) on every restart; Kafka garbage-collects
// unused consumer group metadata on its own retention schedule, so this
// does not need explicit cleanup.
func uniqueConsumerGroup() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s-%s-%d-%d", consumerGroupPrefix, host, os.Getpid(), time.Now().UnixNano())
}

// newTargetOffsets dials Topic directly (no consumer group) and reads
// each partition's current last offset, returning only the partitions
// that actually have at least one message (an empty partition's last
// offset equals its first offset, meaning zero messages, so it
// contributes no readiness requirement).
func newTargetOffsets(ctx context.Context, brokers []string, topic string) (targetOffsets, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafkacatalog: no brokers configured")
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("kafkacatalog: dial %s: %w", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("kafkacatalog: read partitions for %s: %w", topic, err)
	}

	out := make(targetOffsets, len(partitions))
	for _, p := range partitions {
		pconn, err := kafkago.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			return nil, fmt.Errorf("kafkacatalog: dial leader for partition %d: %w", p.ID, err)
		}
		first, last, err := pconn.ReadOffsets()
		closeErr := pconn.Close()
		if err != nil {
			return nil, fmt.Errorf("kafkacatalog: read offsets for partition %d: %w", p.ID, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("kafkacatalog: close leader conn for partition %d: %w", p.ID, closeErr)
		}
		if last > first {
			// last is the offset of the NEXT message to be written,
			// i.e. exclusive — a consumer has caught up once it has
			// processed the message at offset last-1, so "ready" is
			// consumedOffset+1 >= last.
			out[p.ID] = last
		}
	}
	return out, nil
}

// Close releases the underlying Kafka reader.
func (c *Consumer) Close() error {
	return c.Reader.Close()
}

// Ready reports whether this consumer has processed every message that
// existed in Topic at the moment it started — see targetOffsets' doc
// comment. Safe to call concurrently (e.g. from an HTTP healthz
// handler).
func (c *Consumer) Ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready
}

// WaitReady blocks until Ready() would return true or ctx is done,
// whichever comes first. Used by the composition root to gate starting
// the WorkReleased consumer on catalogue completeness, mirroring the
// retired filecatalog.Load's "never process real work against an
// incomplete catalogue" guarantee.
func (c *Consumer) WaitReady(ctx context.Context) error {
	if c.Ready() {
		return nil
	}
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Consumer) markReady() {
	c.mu.Lock()
	alreadyReady := c.ready
	c.ready = true
	c.mu.Unlock()
	if !alreadyReady {
		close(c.readyCh)
	}
}

// Lookup satisfies ports.PathCatalogue against this consumer's current
// in-memory snapshot. Matching semantics are BYTE-FOR-BYTE identical to
// pathcatalog.Catalogue.Lookup (case-insensitive, id equals matchPrefix
// or starts with matchPrefix + "-", longest match wins) — this consumer
// builds a real pathcatalog.Catalogue on every update rather than
// reimplementing the match rule, so the two can never drift.
func (c *Consumer) Lookup(id string) (pathcatalog.PathDefinition, error) {
	c.mu.RLock()
	defs := make([]pathcatalog.PathDefinition, 0, len(c.paths))
	for _, d := range c.paths {
		defs = append(defs, d)
	}
	c.mu.RUnlock()
	return pathcatalog.New(defs).Lookup(id)
}

// Ids returns every currently-known path's canonical id (ACTIVE only —
// a Deactivated path is removed from the local cache entirely, see
// applyDeactivated, since a deactivated path is exactly as unusable as
// one that was never defined from a Lookup caller's perspective).
func (c *Consumer) Ids() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.paths))
	for id := range c.paths {
		out = append(out, id)
	}
	return out
}

// Run consumes Topic until ctx is cancelled or the reader returns a
// fatal error. A handling error is logged and the loop continues, so one
// malformed message cannot wedge this consumer (matching
// labor-performance's AnalyticsConsumer.Run convention). A message that is
// not a valid CloudEvents 1.0 event (including the retired flat envelope)
// is logged at WARN and committed past — never parsed any other way.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := c.handle(msg); err != nil {
			level := slog.LevelError
			if errors.Is(err, cloudevents.ErrNotCloudEvent) {
				level = slog.LevelWarn
			}
			c.Logger.Log(ctx, level, "process-path catalogue message handling failed",
				"topic", msg.Topic, "offset", msg.Offset, "partition", msg.Partition, "error", err)
		}
		c.checkReady(msg)
	}
}

func (c *Consumer) checkReady(msg kafkago.Message) {
	c.mu.RLock()
	target, tracked := c.target[msg.Partition]
	alreadyReady := c.ready
	c.mu.RUnlock()
	if alreadyReady || !tracked {
		return
	}
	if msg.Offset+1 < target {
		return
	}

	c.mu.Lock()
	delete(c.target, msg.Partition)
	allCaughtUp := len(c.target) == 0
	c.mu.Unlock()

	if allCaughtUp {
		c.markReady()
	}
}

func (c *Consumer) handle(msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		return fmt.Errorf("kafkacatalog: %w", err)
	}

	switch e.Type() {
	case TypeProcessPathCreated, TypeProcessPathUpdated:
		var data pathData
		if err := e.DataAs(&data); err != nil {
			return fmt.Errorf("kafkacatalog: decode %s data: %w", e.Type(), err)
		}
		c.applyUpsert(data)
	case TypeProcessPathDeactivated:
		var data pathData
		if err := e.DataAs(&data); err != nil {
			return fmt.Errorf("kafkacatalog: decode %s data: %w", e.Type(), err)
		}
		c.applyDeactivated(data.PathId)
	default:
		// An event type outside this consumer's contract — ignored
		// (forward compatibility), same convention as every other
		// consumer in this fleet that reads a shared topic.
	}
	return nil
}

func (c *Consumer) applyUpsert(data pathData) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paths[strings.ToUpper(data.PathId)] = pathcatalog.PathDefinition{
		Id:                      data.PathId,
		MatchPrefix:             data.MatchPrefix,
		Direct:                  data.Direct,
		RequiredCapabilities:    data.RequiredCapabilities,
		DestinationLocationRole: data.DestinationLocationRole,
	}
}

func (c *Consumer) applyDeactivated(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.paths, strings.ToUpper(id))
}

// waitReadyTimeout bounds how long the composition root waits for the
// initial replay before giving up and failing startup outright — a
// Kafka outage lasting longer than this is treated the same way the
// retired filecatalog.Load treated a missing file: fatal, not a silent
// partial-catalogue start.
const WaitReadyTimeout = 60 * time.Second
