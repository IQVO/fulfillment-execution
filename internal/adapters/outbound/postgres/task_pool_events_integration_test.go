//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// Pool events against a real Postgres (ADR 0049): TaskAvailable and
// TaskClaimed are outbox rows written in the SAME transaction as the task
// change, so the state change and its events commit or roll back together.

const (
	typeAvailable = "com.warehouse.wes.fulfillment-execution.task.TaskAvailable"
	typeClaimed   = "com.warehouse.wes.fulfillment-execution.task.TaskClaimed"
)

// insertThenFail writes the events' outbox rows through the real publisher and
// THEN fails, the worst case for atomicity: rows already inserted inside the
// transaction must disappear with the failed unit of work.
type insertThenFail struct{ inner *postgres.OutboxPublisher }

func (p insertThenFail) Publish(ctx context.Context, evts ...shared.DomainEvent) error {
	if err := p.inner.Publish(ctx, evts...); err != nil {
		return err
	}
	return errors.New("forced failure after the outbox rows were written")
}

func relocateMove(t *testing.T) task.RelocationMove {
	t.Helper()
	m, err := task.NewRelocationMove("plan-7", 3, "Relocate", "SKU-1", "WH1-RSV-AMB-B02-03-02-C", "WH1-FWD-AMB-A01-01-01-A")
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	return m
}

func createRelocate(t *testing.T, s *outboxStack, pub usecasesPublisher) (*task.Task, error) {
	t.Helper()
	uc := &usecases.CreateTask{Tasks: s.tasks, Publisher: pub, Clock: s.clock, NewId: taskIds("rl"), UnitOfWork: s.uow}
	return uc.ExecuteRelocation(context.Background(), shared.NewCPT(s.clock.t.Add(time.Hour)), "wu-rel", shared.NewCapabilitySet("relocate", "forklift"), relocateMove(t))
}

// usecasesPublisher is the ports.EventPublisher shape the use cases take.
type usecasesPublisher interface {
	Publish(ctx context.Context, evts ...shared.DomainEvent) error
}

func integrationRows(t *testing.T, s *outboxStack, eventType string) int {
	t.Helper()
	return countOutbox(t, s.pool, "topic = '"+outboundkafka.Topic+"' AND event_type = '"+eventType+"'")
}

// Release: the task row, its analytics TaskCreated and its integration
// TaskAvailable appear together, with the payload resolved from the row.
func TestPoolEvents_Release_CommitsTaskAndTaskAvailableTogether(t *testing.T) {
	s := newOutboxStack(t)
	tk, err := createRelocate(t, s, s.pub)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := integrationRows(t, s, typeAvailable); got != 1 {
		t.Fatalf("expected 1 integration TaskAvailable row, got %d", got)
	}
	var value []byte
	if err := s.pool.QueryRow(context.Background(), "SELECT value FROM outbox_events WHERE topic = $1 AND event_type = $2", outboundkafka.Topic, typeAvailable).Scan(&value); err != nil {
		t.Fatalf("read row: %v", err)
	}
	for _, want := range []string{
		`"task_id":"` + string(tk.Id()) + `"`, `"task_type":"RELOCATE"`, `"site_id":"WH1"`,
		`"from_location":"WH1-RSV-AMB-B02-03-02-C"`, `"to_location":"WH1-FWD-AMB-A01-01-01-A"`,
		`"required_capabilities":["forklift","relocate"]`, `"available_at":"`,
	} {
		if !bytes.Contains(value, []byte(want)) {
			t.Errorf("TaskAvailable payload lacks %s: %s", want, value)
		}
	}
}

// A PICK task has no location here: no TaskAvailable row, but the task and its
// analytics TaskCreated are written as before.
func TestPoolEvents_Release_PickIsNotAnnounced(t *testing.T) {
	s := newOutboxStack(t)
	create := &usecases.CreateTask{Tasks: s.tasks, Publisher: s.pub, Clock: s.clock, NewId: taskIds("pk"), UnitOfWork: s.uow}
	if _, err := create.Execute(context.Background(), task.Pick, shared.NewCPT(s.clock.t.Add(time.Hour)), "wu-pk", shared.NewCapabilitySet("pick"), false, false); err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := integrationRows(t, s, typeAvailable); got != 0 {
		t.Fatalf("a PICK task must not be announced, got %d TaskAvailable rows", got)
	}
	if got := countOutbox(t, s.pool, "topic = '"+outboundkafka.AnalyticsTopic+"'"); got != 1 {
		t.Fatalf("expected the analytics TaskCreated row, got %d analytics rows", got)
	}
}

// Atomicity of the release: a failure after the outbox rows were inserted
// leaves neither the task nor any row.
func TestPoolEvents_Release_RollsBackTaskAndEventsTogether(t *testing.T) {
	s := newOutboxStack(t)
	if _, err := createRelocate(t, s, insertThenFail{inner: s.pub}); err == nil {
		t.Fatal("expected the forced failure")
	}
	if found, err := s.tasks.FindById(context.Background(), "rl1"); err != nil || found != nil {
		t.Fatalf("the task row survived the rollback: %v err=%v", found, err)
	}
	if got := countOutbox(t, s.pool, "true"); got != 0 {
		t.Fatalf("outbox rows survived the rollback: %d", got)
	}
}

func claimRelocate(t *testing.T, s *outboxStack, pub usecasesPublisher, lease time.Duration) error {
	t.Helper()
	claim := &usecases.ClaimNext{Tasks: s.tasks, Stations: s.stations, Publisher: pub, Clock: s.clock, UnitOfWork: s.uow, LeaseDuration: lease}
	_, err := claim.Execute(context.Background(), "s1", task.Relocate)
	return err
}

func seedClaimable(t *testing.T, s *outboxStack) *task.Task {
	t.Helper()
	if err := s.stations.Save(context.Background(), station.New("s1", shared.NewCapabilitySet("relocate", "forklift"))); err != nil {
		t.Fatalf("station: %v", err)
	}
	tk, err := createRelocate(t, s, s.pub)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return tk
}

// Claim: the integration TaskClaimed row sits next to the unchanged analytics
// row, in the transaction of the claim.
func TestPoolEvents_Claim_WritesIntegrationRowNextToTheAnalyticsRow(t *testing.T) {
	s := newOutboxStack(t)
	tk := seedClaimable(t, s)
	if err := claimRelocate(t, s, s.pub, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if got := integrationRows(t, s, typeClaimed); got != 1 {
		t.Fatalf("expected 1 integration TaskClaimed row, got %d", got)
	}
	if got := countOutbox(t, s.pool, "topic = '"+outboundkafka.AnalyticsTopic+"' AND event_type = '"+typeClaimed+"'"); got != 1 {
		t.Fatalf("expected the analytics TaskClaimed row to remain, got %d", got)
	}
	var value []byte
	if err := s.pool.QueryRow(context.Background(), "SELECT value FROM outbox_events WHERE topic = $1 AND event_type = $2", outboundkafka.Topic, typeClaimed).Scan(&value); err != nil {
		t.Fatalf("read row: %v", err)
	}
	for _, want := range []string{`"task_id":"` + string(tk.Id()) + `"`, `"task_type":"RELOCATE"`, `"station_id":"s1"`, `"claimed_at":"`} {
		if !bytes.Contains(value, []byte(want)) {
			t.Errorf("TaskClaimed payload lacks %s: %s", want, value)
		}
	}
}

// Atomicity of the claim: when the publish fails after the rows were written,
// the task is still Pending and unclaimed, and no TaskClaimed row exists.
func TestPoolEvents_Claim_RollsBackStateAndEventsTogether(t *testing.T) {
	s := newOutboxStack(t)
	tk := seedClaimable(t, s)
	before := countOutbox(t, s.pool, "true")
	if err := claimRelocate(t, s, insertThenFail{inner: s.pub}, time.Minute); err == nil {
		t.Fatal("expected the forced failure")
	}
	found, err := s.tasks.FindById(context.Background(), tk.Id())
	if err != nil || found == nil || found.Status() != task.Pending || found.Lease() != nil {
		t.Fatalf("the claim survived the rollback: %+v err=%v", found, err)
	}
	if got := countOutbox(t, s.pool, "true"); got != before {
		t.Fatalf("outbox rows survived the rollback: %d, want %d", got, before)
	}
}

func sweepAt(s *outboxStack, pub usecasesPublisher, at time.Time) (int, error) {
	sweep := &usecases.ExpireLeases{Tasks: s.tasks, Publisher: pub, Clock: fixedClock{t: at}, UnitOfWork: s.uow}
	return sweep.Execute(context.Background())
}

// Lease expiry: the freed task re-enters the pool with a NEW available_at, and
// a second sweep adds nothing.
func TestPoolEvents_LeaseExpiry_ReEntersThePoolWithANewAvailableAt(t *testing.T) {
	s := newOutboxStack(t)
	seedClaimable(t, s)
	if err := claimRelocate(t, s, s.pub, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	sweepTime := s.clock.t.Add(5 * time.Minute)
	if n, err := sweepAt(s, s.pub, sweepTime); err != nil || n != 1 {
		t.Fatalf("sweep: n=%d err=%v", n, err)
	}
	if got := integrationRows(t, s, typeAvailable); got != 2 {
		t.Fatalf("expected the creation and the re-entry TaskAvailable rows, got %d", got)
	}
	var latest []byte
	if err := s.pool.QueryRow(context.Background(), "SELECT value FROM outbox_events WHERE topic = $1 AND event_type = $2 ORDER BY id DESC LIMIT 1", outboundkafka.Topic, typeAvailable).Scan(&latest); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if want := `"available_at":"` + sweepTime.UTC().Format(time.RFC3339Nano) + `"`; !bytes.Contains(latest, []byte(want)) {
		t.Fatalf("re-entry payload lacks %s: %s", want, latest)
	}
	if n, err := sweepAt(s, s.pub, sweepTime.Add(time.Minute)); err != nil || n != 0 {
		t.Fatalf("second sweep: n=%d err=%v", n, err)
	}
	if got := integrationRows(t, s, typeAvailable); got != 2 {
		t.Fatalf("a second sweep must add no TaskAvailable, got %d", got)
	}
}

// Atomicity of the sweep: a failure after the rows were written leaves the
// task Claimed with its lease and adds no row.
func TestPoolEvents_LeaseExpiry_RollsBackStateAndEventsTogether(t *testing.T) {
	s := newOutboxStack(t)
	tk := seedClaimable(t, s)
	if err := claimRelocate(t, s, s.pub, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	before := countOutbox(t, s.pool, "true")
	if _, err := sweepAt(s, insertThenFail{inner: s.pub}, s.clock.t.Add(5*time.Minute)); err == nil {
		t.Fatal("expected the forced failure")
	}
	found, err := s.tasks.FindById(context.Background(), tk.Id())
	if err != nil || found == nil || found.Status() != task.Claimed || found.Lease() == nil {
		t.Fatalf("the freed state survived the rollback: %+v err=%v", found, err)
	}
	if got := countOutbox(t, s.pool, "true"); got != before {
		t.Fatalf("outbox rows survived the rollback: %d, want %d", got, before)
	}
}

// The rows leave through the relay in commit order: TaskAvailable, then the
// claim's integration and analytics rows, keyed by the task id.
func TestPoolEvents_RelayDeliversPoolEventsInCommitOrder(t *testing.T) {
	s := newOutboxStack(t)
	tk := seedClaimable(t, s)
	if err := claimRelocate(t, s, s.pub, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	sink := &recordingSink{}
	if _, err := postgres.NewOutboxRelay(s.pool, sink, slog.Default()).RelayOnce(context.Background()); err != nil {
		t.Fatalf("relay: %v", err)
	}
	var order []string
	for _, m := range sink.sent {
		if m.Topic == outboundkafka.Topic {
			order = append(order, m.EventType)
			if string(m.Key) != string(tk.Id()) {
				t.Errorf("message %s keyed %q, want %q", m.EventType, m.Key, tk.Id())
			}
		}
	}
	if len(order) != 2 || order[0] != typeAvailable || order[1] != typeClaimed {
		t.Fatalf("integration order = %v, want [TaskAvailable TaskClaimed]", order)
	}
}
