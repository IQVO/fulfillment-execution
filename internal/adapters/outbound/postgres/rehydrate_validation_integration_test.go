//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// A persisted status / task_type string that no domain constant declares
// (corrupt row, manual edit, a writer from a newer release) must surface as
// a wrapped sentinel error on the read path — never rehydrate into an
// aggregate holding an invalid state. Runs against a throwaway Postgres
// (testcontainers, via outboxDB) with every migration applied.
func TestTaskRepo_ReadsRejectUnknownPersistedValues(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()

	insert := func(id, taskType, status string) {
		t.Helper()
		_, err := pool.Exec(ctx, `
			INSERT INTO tasks (id, task_type, status, cpt, order_ref)
			VALUES ($1, $2, $3, $4, $5)
		`, id, taskType, status, time.Now().Add(time.Hour), "order-"+id)
		if err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}
	insert("task-ok", "PICK", "PENDING")
	insert("task-bad-status", "PICK", "CANCELLED")
	insert("task-bad-type", "PUTAWAY", "PENDING")
	insert("task-empty-status", "PICK", "")

	got, err := repo.FindById(ctx, "task-ok")
	if err != nil || got == nil || got.Type() != task.Pick || got.Status() != task.Pending {
		t.Fatalf("valid row must still rehydrate: got %v, err %v", got, err)
	}

	cases := []struct {
		id      string
		wantErr error
	}{
		{"task-bad-status", task.ErrUnknownStatus},
		{"task-empty-status", task.ErrUnknownStatus},
		{"task-bad-type", task.ErrUnknownType},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			byId, err := repo.FindById(ctx, shared.TaskId(tc.id))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("FindById: expected %v, got %v", tc.wantErr, err)
			}
			if byId != nil {
				t.Fatalf("FindById: expected no aggregate on error, got %v", byId)
			}
			if !strings.Contains(err.Error(), "rehydrate task") || !strings.Contains(err.Error(), tc.id) {
				t.Fatalf("expected a wrapped 'rehydrate task %q' error, got %v", tc.id, err)
			}

			list, err := repo.FindByOrderRef(ctx, shared.OrderRef("order-"+tc.id))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("FindByOrderRef: expected %v, got %v", tc.wantErr, err)
			}
			if list != nil {
				t.Fatalf("FindByOrderRef: expected nil slice on error, got %v", list)
			}
		})
	}

	// The bulk scan path (scanTasks) also propagates the error.
	if _, err := repo.FindClaimableByType(ctx, "PUTAWAY", time.Now()); !errors.Is(err, task.ErrUnknownType) {
		t.Fatalf("FindClaimableByType: expected %v, got %v", task.ErrUnknownType, err)
	}
}

func TestPackageRepo_ReadsRejectUnknownPersistedStatus(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewPackageRepo(pool)
	ctx := context.Background()

	insert := func(id, status string) {
		t.Helper()
		_, err := pool.Exec(ctx, `
			INSERT INTO packages (id, order_ref, task_id, status)
			VALUES ($1, $2, $3, $4)
		`, id, "order-"+id, "task-"+id, status)
		if err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}
	insert("pkg-ok", "SEALED")
	insert("pkg-bad", "SHIPPED")
	insert("pkg-empty", "")

	got, err := repo.FindById(ctx, "pkg-ok")
	if err != nil || got == nil || got.Status() != pack.Sealed {
		t.Fatalf("valid row must still rehydrate: got %v, err %v", got, err)
	}

	for _, id := range []string{"pkg-bad", "pkg-empty"} {
		t.Run(id, func(t *testing.T) {
			byId, err := repo.FindById(ctx, shared.PackageId(id))
			if !errors.Is(err, pack.ErrUnknownStatus) {
				t.Fatalf("FindById: expected %v, got %v", pack.ErrUnknownStatus, err)
			}
			if byId != nil {
				t.Fatalf("FindById: expected no aggregate on error, got %v", byId)
			}
			if !strings.Contains(err.Error(), "rehydrate package") || !strings.Contains(err.Error(), id) {
				t.Fatalf("expected a wrapped 'rehydrate package %q' error, got %v", id, err)
			}

			if _, err := repo.FindByTaskId(ctx, shared.TaskId("task-"+id)); !errors.Is(err, pack.ErrUnknownStatus) {
				t.Fatalf("FindByTaskId: expected %v, got %v", pack.ErrUnknownStatus, err)
			}
			list, err := repo.FindByOrderRef(ctx, shared.OrderRef("order-"+id))
			if !errors.Is(err, pack.ErrUnknownStatus) {
				t.Fatalf("FindByOrderRef: expected %v, got %v", pack.ErrUnknownStatus, err)
			}
			if list != nil {
				t.Fatalf("FindByOrderRef: expected nil slice on error, got %v", list)
			}
		})
	}
}
