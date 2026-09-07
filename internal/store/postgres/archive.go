package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Status possíveis de um job de arquivamento (seção 5.5).
const (
	JobPending   = "pending"
	JobExporting = "exporting"
	JobExported  = "exported"
	JobDropped   = "dropped"
	JobFailed    = "failed"
)

// Status possíveis de um lote dentro de um job.
const (
	BatchPending  = "pending"
	BatchUploaded = "uploaded"
	BatchVerified = "verified"
	BatchFailed   = "failed"
)

// Job é o registro de controle de uma partição em arquivamento.
type Job struct {
	ID             uuid.UUID
	PartitionName  string
	OccurredAtFrom time.Time
	OccurredAtTo   time.Time
	Status         string
	Prefix         string
	RowCount       *int64
	BytesTotal     *int64
	ErrorCode      *string
	Error          *string
}

// Batch é a unidade de progresso do job: um `batch_seq` → um objeto no frio.
type Batch struct {
	ID             uuid.UUID
	JobID          uuid.UUID
	BatchSeq       int
	Status         string
	Key            string
	RowCount       int64
	Bytes          int64
	SHA256         string
	OccurredAtFrom *time.Time
	OccurredAtTo   *time.Time
	// CursorID é o id da última linha do lote. Junto com OccurredAtTo forma
	// o cursor keyset exato para retomar sem reler lotes já verificados.
	CursorID *uuid.UUID
}

// Row é uma linha da camada quente pronta para virar linha Parquet (seção 6.2).
type Row struct {
	ID            string
	ProjectID     string
	SchemaVersion int32
	Action        string
	Outcome       string
	ActorType     string
	ActorID       string
	ResourceType  string
	ResourceID    string
	RequestID     string
	CorrelationID string
	IP            string
	UserAgent     string
	OccurredAt    time.Time
	ReceivedAt    time.Time
	Metadata      string
	Extensions    string
}

// Cursor identifica a última linha lida, para paginação keyset determinística.
type Cursor struct {
	OccurredAt time.Time
	ID         uuid.UUID
	Valid      bool
}

// ArchiveRepository concentra o estado e a leitura usados pelo modo `migrate`.
type ArchiveRepository struct {
	pool *pgxpool.Pool
}

// NewArchiveRepository constrói o repositório do cold path.
func NewArchiveRepository(pool *pgxpool.Pool) *ArchiveRepository {
	return &ArchiveRepository{pool: pool}
}

const listPartitionsSQL = `
SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
JOIN pg_namespace n ON n.oid = p.relnamespace
WHERE p.relname = 'events' AND n.nspname = ANY (current_schemas(false))
ORDER BY c.relname`

// EligiblePartitions devolve as partições fechadas cujo limite superior já
// saiu da janela quente, ordenadas da mais antiga para a mais nova
// (seção 5.4). Partições já `dropped` não voltam na lista.
func (r *ArchiveRepository) EligiblePartitions(ctx context.Context, cutoff time.Time, limit int) ([]Partition, error) {
	rows, err := r.pool.Query(ctx, listPartitionsSQL)
	if err != nil {
		return nil, fmt.Errorf("listar partições: %w", err)
	}
	defer rows.Close()

	var candidates []Partition
	for rows.Next() {
		var name, bound string
		if err := rows.Scan(&name, &bound); err != nil {
			return nil, fmt.Errorf("ler partição: %w", err)
		}
		if strings.Contains(bound, "DEFAULT") {
			continue
		}
		from, to, err := parsePartitionBound(bound)
		if err != nil {
			return nil, err
		}
		if to.After(cutoff) {
			continue
		}
		candidates = append(candidates, Partition{Name: name, From: from, To: to})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterar partições: %w", err)
	}

	dropped, err := r.droppedPartitions(ctx)
	if err != nil {
		return nil, err
	}

	eligible := make([]Partition, 0, len(candidates))
	for _, candidate := range candidates {
		if _, ok := dropped[candidate.Name]; ok {
			continue
		}
		eligible = append(eligible, candidate)
		if limit > 0 && len(eligible) == limit {
			break
		}
	}
	return eligible, nil
}

func (r *ArchiveRepository) droppedPartitions(ctx context.Context) (map[string]struct{}, error) {
	rows, err := r.pool.Query(ctx,
		"SELECT partition_name FROM cold_archive_jobs WHERE status = $1", JobDropped)
	if err != nil {
		return nil, fmt.Errorf("listar jobs dropped: %w", err)
	}
	defer rows.Close()

	out := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("ler job dropped: %w", err)
		}
		out[name] = struct{}{}
	}
	return out, rows.Err()
}

const upsertJobSQL = `
INSERT INTO cold_archive_jobs (
	id, partition_name, occurred_at_from, occurred_at_to, status, s3_prefix, started_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, now(), now())
ON CONFLICT (partition_name) DO UPDATE
SET s3_prefix  = EXCLUDED.s3_prefix,
	started_at = now(),
	updated_at = now()
RETURNING id, partition_name, occurred_at_from, occurred_at_to, status, s3_prefix, row_count, bytes_total, error_code, error`

// UpsertJob cria ou recupera o job da partição. Reexecuções retomam o mesmo
// registro, preservando o progresso anterior (seção 5.5, idempotência).
func (r *ArchiveRepository) UpsertJob(ctx context.Context, part Partition, prefix string) (Job, error) {
	var job Job
	err := r.pool.QueryRow(ctx, upsertJobSQL,
		uuid.New(), part.Name, part.From, part.To, JobPending, prefix,
	).Scan(
		&job.ID, &job.PartitionName, &job.OccurredAtFrom, &job.OccurredAtTo,
		&job.Status, &job.Prefix, &job.RowCount, &job.BytesTotal, &job.ErrorCode, &job.Error,
	)
	if err != nil {
		return Job{}, fmt.Errorf("upsert cold_archive_jobs: %w", err)
	}
	return job, nil
}

// SetJobStatus avança o estado do job, limpando o erro anterior.
func (r *ArchiveRepository) SetJobStatus(ctx context.Context, jobID uuid.UUID, status string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE cold_archive_jobs
		SET status      = $2,
			error       = NULL,
			error_code  = NULL,
			finished_at = CASE WHEN $2 IN ('exported', 'dropped') THEN now() ELSE finished_at END,
			updated_at  = now()
		WHERE id = $1`, jobID, status)
	if err != nil {
		return fmt.Errorf("atualizar status do job: %w", err)
	}
	return nil
}

// SetJobTotals registra os totais validados antes do DROP.
func (r *ArchiveRepository) SetJobTotals(ctx context.Context, jobID uuid.UUID, rows, bytes int64) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE cold_archive_jobs
		SET row_count = $2, bytes_total = $3, updated_at = now()
		WHERE id = $1`, jobID, rows, bytes)
	if err != nil {
		return fmt.Errorf("atualizar totais do job: %w", err)
	}
	return nil
}

// FailJob marca a partição como falha, preservando motivo e código para o
// runbook. A partição permanece no PostgreSQL.
func (r *ArchiveRepository) FailJob(ctx context.Context, jobID uuid.UUID, errorCode, message string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE cold_archive_jobs
		SET status = $2, error_code = $3, error = $4, finished_at = now(), updated_at = now()
		WHERE id = $1`, jobID, JobFailed, errorCode, message)
	if err != nil {
		return fmt.Errorf("marcar job como failed: %w", err)
	}
	return nil
}

// Batches devolve os lotes já conhecidos do job, indexados por batch_seq.
func (r *ArchiveRepository) Batches(ctx context.Context, jobID uuid.UUID) (map[int]Batch, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, job_id, batch_seq, status, s3_key,
			COALESCE(row_count, 0), COALESCE(bytes, 0), COALESCE(sha256, ''),
			occurred_at_from, occurred_at_to, cursor_id
		FROM cold_archive_batches
		WHERE job_id = $1
		ORDER BY batch_seq`, jobID)
	if err != nil {
		return nil, fmt.Errorf("listar lotes: %w", err)
	}
	defer rows.Close()

	out := map[int]Batch{}
	for rows.Next() {
		var b Batch
		if err := rows.Scan(&b.ID, &b.JobID, &b.BatchSeq, &b.Status, &b.Key,
			&b.RowCount, &b.Bytes, &b.SHA256, &b.OccurredAtFrom, &b.OccurredAtTo, &b.CursorID); err != nil {
			return nil, fmt.Errorf("ler lote: %w", err)
		}
		out[b.BatchSeq] = b
	}
	return out, rows.Err()
}

const saveBatchSQL = `
INSERT INTO cold_archive_batches (
	id, job_id, batch_seq, status, s3_key, row_count, bytes, sha256,
	occurred_at_from, occurred_at_to, cursor_id, error, error_code, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now())
ON CONFLICT (job_id, batch_seq) DO UPDATE
SET status           = EXCLUDED.status,
	s3_key           = EXCLUDED.s3_key,
	row_count        = EXCLUDED.row_count,
	bytes            = EXCLUDED.bytes,
	sha256           = EXCLUDED.sha256,
	occurred_at_from = EXCLUDED.occurred_at_from,
	occurred_at_to   = EXCLUDED.occurred_at_to,
	cursor_id        = EXCLUDED.cursor_id,
	error            = EXCLUDED.error,
	error_code       = EXCLUDED.error_code,
	updated_at       = now()`

// SaveBatch grava o estado de um lote de forma idempotente.
func (r *ArchiveRepository) SaveBatch(ctx context.Context, b Batch, errorCode, message string) error {
	var codePtr, msgPtr *string
	if errorCode != "" {
		codePtr = &errorCode
	}
	if message != "" {
		msgPtr = &message
	}
	if b.ID == uuid.Nil {
		b.ID = uuid.New()
	}
	_, err := r.pool.Exec(ctx, saveBatchSQL,
		b.ID, b.JobID, b.BatchSeq, b.Status, b.Key, b.RowCount, b.Bytes, b.SHA256,
		b.OccurredAtFrom, b.OccurredAtTo, b.CursorID, msgPtr, codePtr)
	if err != nil {
		return fmt.Errorf("salvar lote %d: %w", b.BatchSeq, err)
	}
	return nil
}

// CountPartition devolve o total de linhas da partição, usado na validação
// final antes do DROP.
func (r *ArchiveRepository) CountPartition(ctx context.Context, partition string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT count(*) FROM %s", quoteIdent(partition))
	if err := r.pool.QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("contar linhas de %s: %w", partition, err)
	}
	return count, nil
}

// Projects lista os projetos presentes na partição, em ordem estável.
// O layout do frio é prefixado por `project_id` (seção 5.3), então cada lote
// cobre um único projeto.
func (r *ArchiveRepository) Projects(ctx context.Context, partition string) ([]string, error) {
	query := fmt.Sprintf("SELECT DISTINCT project_id FROM %s ORDER BY project_id", quoteIdent(partition))
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("listar projetos de %s: %w", partition, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var projectID string
		if err := rows.Scan(&projectID); err != nil {
			return nil, fmt.Errorf("ler projeto de %s: %w", partition, err)
		}
		out = append(out, projectID)
	}
	return out, rows.Err()
}

const readRowsSQL = `
SELECT id::text, project_id, schema_version, action,
	COALESCE(outcome, ''), COALESCE(actor_type, ''), COALESCE(actor_id, ''),
	COALESCE(resource_type, ''), COALESCE(resource_id, ''),
	COALESCE(request_id, ''), COALESCE(correlation_id, ''),
	COALESCE(host(ip), ''), COALESCE(user_agent, ''),
	occurred_at, received_at, metadata::text, extensions::text
FROM %s
WHERE project_id = $1
  AND ($2::timestamptz IS NULL OR (occurred_at, id) > ($2::timestamptz, $3::uuid))
ORDER BY occurred_at, id
LIMIT $4`

// ReadRows lê o próximo lote de um projeto dentro da partição, em ordem
// (occurred_at, id). A partição está fechada, então a ordenação torna os
// lotes determinísticos: o mesmo `batch_seq` sempre cobre as mesmas linhas
// em um retry (seção 5.5).
func (r *ArchiveRepository) ReadRows(ctx context.Context, partition, projectID string, cursor Cursor, limit int) ([]Row, Cursor, error) {
	var fromTime *time.Time
	var fromID *uuid.UUID
	if cursor.Valid {
		fromTime = &cursor.OccurredAt
		fromID = &cursor.ID
	}

	query := fmt.Sprintf(readRowsSQL, quoteIdent(partition))
	pgRows, err := r.pool.Query(ctx, query, projectID, fromTime, fromID, limit)
	if err != nil {
		return nil, cursor, fmt.Errorf("ler linhas de %s: %w", partition, err)
	}
	defer pgRows.Close()

	out := make([]Row, 0, limit)
	next := cursor
	for pgRows.Next() {
		var row Row
		if err := pgRows.Scan(
			&row.ID, &row.ProjectID, &row.SchemaVersion, &row.Action,
			&row.Outcome, &row.ActorType, &row.ActorID,
			&row.ResourceType, &row.ResourceID,
			&row.RequestID, &row.CorrelationID,
			&row.IP, &row.UserAgent,
			&row.OccurredAt, &row.ReceivedAt, &row.Metadata, &row.Extensions,
		); err != nil {
			return nil, cursor, fmt.Errorf("scan linha de %s: %w", partition, err)
		}
		id, err := uuid.Parse(row.ID)
		if err != nil {
			return nil, cursor, fmt.Errorf("uuid inválido em %s: %w", partition, err)
		}
		next = Cursor{OccurredAt: row.OccurredAt, ID: id, Valid: true}
		out = append(out, row)
	}
	if err := pgRows.Err(); err != nil {
		return nil, cursor, fmt.Errorf("iterar linhas de %s: %w", partition, err)
	}
	return out, next, nil
}

// DropPartition destaca e remove a partição. O DETACH é CONCURRENTLY para não
// pegar ACCESS EXCLUSIVE no pai e travar a ingestão (SLO da seção 2).
func (r *ArchiveRepository) DropPartition(ctx context.Context, partition string) error {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SET lock_timeout = '30s'"); err != nil {
		return fmt.Errorf("set lock_timeout: %w", err)
	}

	detach := fmt.Sprintf("ALTER TABLE events DETACH PARTITION %s CONCURRENTLY", quoteIdent(partition))
	if _, err := conn.Exec(ctx, detach); err != nil {
		// Um DETACH CONCURRENTLY interrompido deixa a partição pendente;
		// nesse caso só o FINALIZE conclui a operação.
		finalize := fmt.Sprintf("ALTER TABLE events DETACH PARTITION %s FINALIZE", quoteIdent(partition))
		if _, finalizeErr := conn.Exec(ctx, finalize); finalizeErr != nil {
			return fmt.Errorf("detach %s: %w", partition, err)
		}
	}

	drop := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(partition))
	if _, err := conn.Exec(ctx, drop); err != nil {
		return fmt.Errorf("drop %s: %w", partition, err)
	}
	return nil
}
