package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// WorkBatchRepo is the pgxpool-backed implementation of ports.WorkBatchRepo
// (ADR 0063). Every read and write goes through querierFrom, so a Save
// inside a UnitOfWork joins the use case's transaction — the batch row, its
// members and the outbox rows commit or roll back together.
type WorkBatchRepo struct {
	pool *pgxpool.Pool
}

// NewWorkBatchRepo constructs a WorkBatchRepo backed by pool.
func NewWorkBatchRepo(pool *pgxpool.Pool) *WorkBatchRepo {
	return &WorkBatchRepo{pool: pool}
}

// Save writes the whole aggregate snapshot: the batch row upserted on id,
// then every member upserted on (batch_id, task_id). The one-open-batch
// partial unique index (migration 0027) turns a lost formation race into a
// write error here, exactly like the per-task claim compare-and-set.
func (r *WorkBatchRepo) Save(ctx context.Context, b *workbatch.WorkBatch) error {
	q := querierFrom(ctx, r.pool)
	if _, err := q.Exec(ctx, `
		INSERT INTO work_batches (id, station_id, cpt, status, version, formed_at, completed_at, abandoned_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			version = EXCLUDED.version,
			completed_at = EXCLUDED.completed_at,
			abandoned_at = EXCLUDED.abandoned_at
	`, string(b.Id()), string(b.StationId()), b.CPT(), string(b.Status()), b.Version(), b.FormedAt(), b.CompletedAt(), b.AbandonedAt()); err != nil {
		return err
	}
	for _, m := range b.Members() {
		if _, err := q.Exec(ctx, `
			INSERT INTO work_batch_members (batch_id, task_id, cpt, from_bin, hazard_class, hint_rank, stop_seq, compartment, state)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (batch_id, task_id) DO UPDATE SET
				state = EXCLUDED.state
		`, string(b.Id()), string(m.TaskId), m.Cpt, m.FromBin, m.HazardClass, m.Rank, m.StopSeq, m.Compartment, string(m.State)); err != nil {
			return err
		}
	}
	return nil
}

func (r *WorkBatchRepo) FindById(ctx context.Context, id shared.BatchId) (*workbatch.WorkBatch, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT b.id, b.station_id, b.cpt, b.status, b.version, b.formed_at, b.completed_at, b.abandoned_at,
		       m.task_id, m.cpt, m.from_bin, m.hazard_class, m.hint_rank, m.stop_seq, m.compartment, m.state
		FROM work_batches b
		JOIN work_batch_members m ON m.batch_id = b.id
		WHERE b.id = $1
		ORDER BY m.stop_seq
	`, string(id))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanBatch(rows)
}

// FindOpenByStation returns the station's OPEN batch, or nil when it holds
// none. The partial unique index (migration 0027) guarantees at most one.
func (r *WorkBatchRepo) FindOpenByStation(ctx context.Context, stationId shared.StationId) (*workbatch.WorkBatch, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT b.id, b.station_id, b.cpt, b.status, b.version, b.formed_at, b.completed_at, b.abandoned_at,
		       m.task_id, m.cpt, m.from_bin, m.hazard_class, m.hint_rank, m.stop_seq, m.compartment, m.state
		FROM work_batches b
		JOIN work_batch_members m ON m.batch_id = b.id
		WHERE b.station_id = $1 AND b.status = 'OPEN'
		ORDER BY m.stop_seq
	`, string(stationId))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanBatch(rows)
}

func (r *WorkBatchRepo) FindOpenByTaskId(ctx context.Context, taskId shared.TaskId) (*workbatch.WorkBatch, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT b.id, b.station_id, b.cpt, b.status, b.version, b.formed_at, b.completed_at, b.abandoned_at,
		       m.task_id, m.cpt, m.from_bin, m.hazard_class, m.hint_rank, m.stop_seq, m.compartment, m.state
		FROM work_batches b
		JOIN work_batch_members m ON m.batch_id = b.id
		WHERE b.status = 'OPEN' AND m.task_id = $1 AND m.state = 'OPEN'
		ORDER BY m.stop_seq
	`, string(taskId))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanBatch(rows)
}

func (r *WorkBatchRepo) FindByTaskId(ctx context.Context, taskId shared.TaskId) (*workbatch.WorkBatch, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT b.id, b.station_id, b.cpt, b.status, b.version, b.formed_at, b.completed_at, b.abandoned_at,
		       m.task_id, m.cpt, m.from_bin, m.hazard_class, m.hint_rank, m.stop_seq, m.compartment, m.state
		FROM work_batches b
		JOIN work_batch_members m ON m.batch_id = b.id
		WHERE m.task_id = $1 AND m.state <> 'DETACHED' AND b.status <> 'ABANDONED'
		ORDER BY b.formed_at DESC
		LIMIT 1
	`, string(taskId))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// FindByTaskId matches at most one batch, but the join fans out to its
	// members; the member rows of that one batch are the whole result.
	return scanBatch(rows)
}

// scanBatch folds a b.*+m.* join into one Rehydrated aggregate. No rows
// means no batch (nil, nil); rows with a member that fails to parse is a
// hard error — every writer persists the canonical constants.
func scanBatch(rows pgx.Rows) (*workbatch.WorkBatch, error) {
	var (
		batchId     string
		stationId   string
		cpt         time.Time
		status      string
		version     int64
		formedAt    time.Time
		completedAt *time.Time
		abandonedAt *time.Time
		members     []workbatch.Member
	)
	for rows.Next() {
		var m workbatch.Member
		var memberCpt time.Time
		var mstate string
		if err := rows.Scan(&batchId, &stationId, &cpt, &status, &version, &formedAt, &completedAt, &abandonedAt,
			&m.TaskId, &memberCpt, &m.FromBin, &m.HazardClass, &m.Rank, &m.StopSeq, &m.Compartment, &mstate); err != nil {
			return nil, err
		}
		m.Cpt = memberCpt
		state, err := workbatch.ParseMemberState(mstate)
		if err != nil {
			return nil, err
		}
		m.State = state
		members = append(members, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if members == nil {
		return nil, nil
	}
	st, err := workbatch.ParseStatus(status)
	if err != nil {
		return nil, err
	}
	return workbatch.Rehydrate(shared.BatchId(batchId), shared.StationId(stationId), cpt, members, st, version, formedAt, completedAt, abandonedAt), nil
}
