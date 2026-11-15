package postgres

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TestTaskInsert_ArgsMatchColumnsAndPlaceholders pins the upsert's shape:
// taskColumns, taskInsertSQL's placeholders and taskInsertArgs must agree
// one-to-one, in order. A drift here surfaces only at integration time as
// pgx's "mismatched param and argument count" (the statement describes N
// parameters while the call carries M arguments), so it is pinned cheaply
// here instead.
func TestTaskInsert_ArgsMatchColumnsAndPlaceholders(t *testing.T) {
	cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))
	tk := task.New(shared.TaskId("shape"), task.Pick, cpt, "order-1", shared.NewCapabilitySet("pick"), true, true)

	cols := strings.Split(taskColumns, ",")
	for i := range cols {
		cols[i] = strings.TrimSpace(cols[i])
	}

	args := taskInsertArgs(tk)
	if len(args) != len(cols) {
		t.Fatalf("taskInsertArgs gives %d args, taskColumns has %d columns", len(args), len(cols))
	}

	seen := map[int]bool{}
	for _, m := range regexp.MustCompile(`\$(\d+)`).FindAllStringSubmatch(taskInsertSQL, -1) {
		n, err := strconv.Atoi(m[1])
		if err != nil || n < 1 || n > len(cols) {
			t.Fatalf("taskInsertSQL references $%s, out of range 1..%d", m[1], len(cols))
		}
		seen[n] = true
	}
	if len(seen) != len(cols) {
		t.Fatalf("taskInsertSQL has %d distinct placeholders, want %d", len(seen), len(cols))
	}
}
