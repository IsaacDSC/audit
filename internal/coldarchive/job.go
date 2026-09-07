// Package coldarchive implementa o modo `migrate`: exporta partições fora da
// janela quente para Parquet no object storage e só então remove a partição
// do PostgreSQL (spec 001, seção 5.5).
package coldarchive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/IsaacDSC/audit.git/internal/blobstore"
	"github.com/IsaacDSC/audit.git/internal/config"
	"github.com/IsaacDSC/audit.git/internal/obs"
	"github.com/IsaacDSC/audit.git/internal/store/postgres"
	"github.com/google/uuid"
)

// Códigos de erro registrados em `cold_archive_*` e nas métricas. São
// estáveis: o runbook e os alertas agregam por eles.
const (
	errCodePGRead        = "pg_read"
	errCodePGCount       = "pg_count"
	errCodePGDrop        = "pg_drop"
	errCodeStateWrite    = "state_write"
	errCodeParquetEncode = "parquet_encode"
	errCodeUpload        = "blob_upload"
	errCodeVerify        = "blob_verify"
	errCodeRowMismatch   = "row_count_mismatch"
)

// Estágios usados no atributo `stage` da métrica de falhas.
const (
	stageRead   = "read"
	stageUpload = "upload"
	stageVerify = "verify"
	stageDrop   = "drop"
)

// stageError carrega o estágio e o código de uma falha de lote até o nível da
// partição, para que o job registre o mesmo motivo em `cold_archive_jobs`.
type stageError struct {
	stage string
	code  string
	cause error
}

func (e *stageError) Error() string { return e.cause.Error() }
func (e *stageError) Unwrap() error { return e.cause }

// Store é o subconjunto do PostgreSQL usado pelo job.
type Store interface {
	EligiblePartitions(ctx context.Context, cutoff time.Time, limit int) ([]postgres.Partition, error)
	UpsertJob(ctx context.Context, part postgres.Partition, prefix string) (postgres.Job, error)
	SetJobStatus(ctx context.Context, jobID uuid.UUID, status string) error
	SetJobTotals(ctx context.Context, jobID uuid.UUID, rows, bytes int64) error
	FailJob(ctx context.Context, jobID uuid.UUID, errorCode, message string) error
	Batches(ctx context.Context, jobID uuid.UUID) (map[int]postgres.Batch, error)
	SaveBatch(ctx context.Context, b postgres.Batch, errorCode, message string) error
	CountPartition(ctx context.Context, partition string) (int64, error)
	Projects(ctx context.Context, partition string) ([]string, error)
	ReadRows(ctx context.Context, partition, projectID string, cursor postgres.Cursor, limit int) ([]postgres.Row, postgres.Cursor, error)
	DropPartition(ctx context.Context, partition string) error
}

// Result resume um run para o log `cold_archive_run_completed`.
type Result struct {
	PartitionsEligible int
	PartitionsExported int
	PartitionsDropped  int
	PartitionsFailed   int
	RowsTotal          int64
	BytesTotal         int64
}

// Job executa uma passada do arquivamento e encerra.
type Job struct {
	store   Store
	blobs   blobstore.BlobStore
	metrics *obs.ArchiveMetrics
	logger  *slog.Logger
	cfg     config.Config
	now     func() time.Time
}

// Deps agrupa as dependências do job.
type Deps struct {
	Config  config.Config
	Logger  *slog.Logger
	Metrics *obs.ArchiveMetrics
	Store   Store
	Blobs   blobstore.BlobStore
	Now     func() time.Time
}

// New constrói o job de arquivamento.
func New(deps Deps) *Job {
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Job{
		store:   deps.Store,
		blobs:   deps.Blobs,
		metrics: deps.Metrics,
		logger:  logger,
		cfg:     deps.Config,
		now:     now,
	}
}

// Run processa as partições elegíveis. Uma partição que falha não derruba o
// run: o erro fica registrado e as demais seguem (seção 5.5).
func (j *Job) Run(ctx context.Context) (Result, error) {
	runID := uuid.NewString()
	logger := j.logger.With(slog.String("run_id", runID))
	start := j.now()

	cutoff := start.UTC().Add(-j.cfg.Archive.HotRetention)
	partitions, err := j.store.EligiblePartitions(ctx, cutoff, j.cfg.Archive.MaxPartitionsPerRun)
	if err != nil {
		j.metrics.RecordFailure(ctx, errCodePGRead, stageRead)
		j.metrics.RecordRun(ctx, "failed", j.now().Sub(start))
		return Result{}, fmt.Errorf("listar partições elegíveis: %w", err)
	}

	result := Result{PartitionsEligible: len(partitions)}
	logger.Info("cold_archive_run_started",
		slog.Time("cutoff", cutoff),
		slog.Int("partitions_eligible", len(partitions)),
		slog.Bool("dry_run", j.cfg.Archive.DryRun),
	)

	for _, partition := range partitions {
		if ctx.Err() != nil {
			logger.Warn("cold_archive_run_interrupted",
				slog.String("partition_name", partition.Name),
				slog.String("error", ctx.Err().Error()),
			)
			break
		}

		outcome, err := j.archivePartition(ctx, logger, partition)
		result.RowsTotal += outcome.rows
		result.BytesTotal += outcome.bytes
		switch {
		case err != nil:
			result.PartitionsFailed++
		case outcome.dropped:
			result.PartitionsExported++
			result.PartitionsDropped++
		default:
			result.PartitionsExported++
		}
	}

	duration := j.now().Sub(start)
	status := "completed"
	if result.PartitionsFailed > 0 {
		status = "partial"
	}
	j.metrics.RecordRun(ctx, status, duration)

	logger.Info("cold_archive_run_completed",
		slog.String("status", status),
		slog.Int("partitions_eligible", result.PartitionsEligible),
		slog.Int("partitions_exported", result.PartitionsExported),
		slog.Int("partitions_dropped", result.PartitionsDropped),
		slog.Int("partitions_failed", result.PartitionsFailed),
		slog.Int64("rows_total", result.RowsTotal),
		slog.Int64("bytes_total", result.BytesTotal),
		slog.Int64("duration_ms", duration.Milliseconds()),
	)
	return result, nil
}

type partitionOutcome struct {
	rows    int64
	bytes   int64
	dropped bool
}

func (j *Job) archivePartition(
	ctx context.Context,
	runLogger *slog.Logger,
	partition postgres.Partition,
) (partitionOutcome, error) {
	var outcome partitionOutcome

	prefix := keyPrefix(j.cfg.ColdStorage.Prefix, j.cfg.Env)
	// O job guarda a URI completa (com bucket/conta), não só o prefixo: quem
	// abrir `cold_archive_jobs` no runbook consegue ir direto ao objeto.
	job, err := j.store.UpsertJob(ctx, partition, j.blobs.URI(prefix))
	if err != nil {
		j.metrics.RecordFailure(ctx, errCodeStateWrite, stageRead)
		runLogger.Error("cold_archive_partition_failed",
			slog.String("partition_name", partition.Name),
			slog.String("error_code", errCodeStateWrite),
			slog.String("error", err.Error()),
		)
		return outcome, err
	}

	logger := runLogger.With(
		slog.String("job_id", job.ID.String()),
		slog.String("partition_name", partition.Name),
	)
	fail := func(stage, code string, cause error, attrs ...any) (partitionOutcome, error) {
		j.metrics.RecordFailure(ctx, code, stage)
		if markErr := j.store.FailJob(ctx, job.ID, code, cause.Error()); markErr != nil {
			logger.Error("cold_archive_state_write_failed", slog.String("error", markErr.Error()))
		}
		logger.Error("cold_archive_partition_failed", append(attrs,
			slog.String("error_code", code),
			slog.String("stage", stage),
			slog.String("error", cause.Error()),
		)...)
		return outcome, cause
	}

	if err := j.store.SetJobStatus(ctx, job.ID, postgres.JobExporting); err != nil {
		return fail(stageRead, errCodeStateWrite, err)
	}

	existing, err := j.store.Batches(ctx, job.ID)
	if err != nil {
		return fail(stageRead, errCodeStateWrite, err)
	}

	projects, err := j.store.Projects(ctx, partition.Name)
	if err != nil {
		return fail(stageRead, errCodePGRead, err)
	}

	seq := 0
	for _, projectID := range projects {
		cursor := postgres.Cursor{}
		for {
			if ctx.Err() != nil {
				return fail(stageRead, errCodePGRead, ctx.Err())
			}

			key := objectKey(prefix, projectID, partition, seq)

			if done, next, batch := reuseBatch(ctx, j.blobs, existing, seq, key); done {
				if batch.Status == postgres.BatchUploaded {
					batch.Status = postgres.BatchVerified
					if err := j.store.SaveBatch(ctx, batch, "", ""); err != nil {
						return fail(stageVerify, errCodeStateWrite, err)
					}
					j.metrics.AddPendingBatches(ctx, partition.Name, -1)
					logger.Info("cold_archive_batch_reverified",
						slog.Int("batch_seq", seq),
						slog.String("object_key", key),
					)
				}
				outcome.rows += batch.RowCount
				outcome.bytes += batch.Bytes
				cursor = next
				seq++
				continue
			}

			rows, next, err := j.store.ReadRows(ctx, partition.Name, projectID, cursor, j.cfg.Archive.BatchRows)
			if err != nil {
				return fail(stageRead, errCodePGRead, err, slog.Int("batch_seq", seq))
			}
			if len(rows) == 0 {
				break
			}

			batch, err := j.exportBatch(ctx, logger, job.ID, partition, projectID, seq, key, rows)
			if err != nil {
				var staged *stageError
				if errors.As(err, &staged) {
					return fail(staged.stage, staged.code, staged.cause, slog.Int("batch_seq", seq))
				}
				return fail(stageUpload, errCodeStateWrite, err, slog.Int("batch_seq", seq))
			}

			outcome.rows += batch.RowCount
			outcome.bytes += batch.Bytes
			cursor = next
			seq++

			if len(rows) < j.cfg.Archive.BatchRows {
				break
			}
		}
	}

	expected, err := j.store.CountPartition(ctx, partition.Name)
	if err != nil {
		return fail(stageVerify, errCodePGCount, err)
	}
	// Só se todos os lotes estiverem verificados e a soma bater com o COUNT(*)
	// a partição pode sair do PostgreSQL (seção 5.5, garantias).
	if outcome.rows != expected {
		return fail(stageVerify, errCodeRowMismatch, fmt.Errorf(
			"soma dos lotes (%d) difere do COUNT(*) da partição (%d)", outcome.rows, expected))
	}

	if err := j.store.SetJobTotals(ctx, job.ID, outcome.rows, outcome.bytes); err != nil {
		return fail(stageVerify, errCodeStateWrite, err)
	}
	if err := j.store.SetJobStatus(ctx, job.ID, postgres.JobExported); err != nil {
		return fail(stageVerify, errCodeStateWrite, err)
	}

	if j.cfg.Archive.DryRun {
		logger.Info("cold_archive_partition_exported",
			slog.Int64("rows", outcome.rows),
			slog.Int64("bytes", outcome.bytes),
			slog.Bool("dry_run", true),
		)
		return outcome, nil
	}

	if err := j.store.DropPartition(ctx, partition.Name); err != nil {
		return fail(stageDrop, errCodePGDrop, err)
	}
	if err := j.store.SetJobStatus(ctx, job.ID, postgres.JobDropped); err != nil {
		return fail(stageDrop, errCodeStateWrite, err)
	}

	outcome.dropped = true
	j.metrics.RecordPartitionDropped(ctx)
	logger.Info("cold_archive_partition_dropped",
		slog.Int64("rows", outcome.rows),
		slog.Int64("bytes", outcome.bytes),
	)
	return outcome, nil
}

// exportBatch escreve o Parquet, sobe objeto e manifest e valida o resultado.
// A key é determinística: reexecutar sobrescreve o mesmo objeto em vez de
// criar um novo (seção 5.5, idempotência).
func (j *Job) exportBatch(
	ctx context.Context,
	logger *slog.Logger,
	jobID uuid.UUID,
	partition postgres.Partition,
	projectID string,
	seq int,
	key string,
	rows []postgres.Row,
) (postgres.Batch, error) {
	start := j.now()

	occurredFrom := rows[0].OccurredAt
	occurredTo := rows[len(rows)-1].OccurredAt
	cursorID, err := uuid.Parse(rows[len(rows)-1].ID)
	if err != nil {
		return postgres.Batch{}, fmt.Errorf("uuid inválido no lote %d: %w", seq, err)
	}

	batch := postgres.Batch{
		JobID:          jobID,
		BatchSeq:       seq,
		Status:         postgres.BatchPending,
		Key:            key,
		RowCount:       int64(len(rows)),
		OccurredAtFrom: &occurredFrom,
		OccurredAtTo:   &occurredTo,
		CursorID:       &cursorID,
	}
	if err := j.store.SaveBatch(ctx, batch, "", ""); err != nil {
		return postgres.Batch{}, &stageError{stage: stageUpload, code: errCodeStateWrite, cause: err}
	}
	j.metrics.AddPendingBatches(ctx, partition.Name, 1)

	// A métrica de falha é emitida no nível da partição, a partir do
	// stageError, para não contar a mesma falha duas vezes.
	failBatch := func(stage, code string, cause error) (postgres.Batch, error) {
		batch.Status = postgres.BatchFailed
		if saveErr := j.store.SaveBatch(ctx, batch, code, cause.Error()); saveErr != nil {
			logger.Error("cold_archive_state_write_failed", slog.String("error", saveErr.Error()))
		}
		j.metrics.RecordBatch(ctx, partition.Name, "failed", 0, 0, j.now().Sub(start))
		logger.Error("cold_archive_batch_failed",
			slog.Int("batch_seq", seq),
			slog.String("project_id", projectID),
			slog.String("object_key", key),
			slog.String("error_code", code),
			slog.String("stage", stage),
			slog.String("error", cause.Error()),
			slog.Int("rows", len(rows)),
			slog.Int64("duration_ms", j.now().Sub(start).Milliseconds()),
		)
		return postgres.Batch{}, &stageError{stage: stage, code: code, cause: cause}
	}

	payload, checksum, err := encodeParquet(rows)
	if err != nil {
		return failBatch(stageUpload, errCodeParquetEncode, err)
	}
	batch.Bytes = int64(len(payload))
	batch.SHA256 = checksum

	putOpts := blobstore.PutOptions{
		ContentType: "application/vnd.apache.parquet",
		SHA256Hex:   checksum,
		Metadata: map[string]string{
			"partition_name": partition.Name,
			"project_id":     projectID,
			"batch_seq":      fmt.Sprintf("%05d", seq),
		},
	}
	if err := j.blobs.Put(ctx, key, payload, putOpts); err != nil {
		return failBatch(stageUpload, errCodeUpload, err)
	}

	manifest := Manifest{
		PartitionName:        partition.Name,
		ProjectID:            projectID,
		BatchSeq:             seq,
		ObjectKey:            key,
		OccurredAtMin:        occurredFrom.UTC(),
		OccurredAtMax:        occurredTo.UTC(),
		RowCount:             int64(len(rows)),
		Bytes:                int64(len(payload)),
		SHA256:               checksum,
		ParquetSchemaVersion: ParquetSchemaVersion,
		Compression:          "zstd",
		ExportedAt:           j.now().UTC(),
	}
	manifestBody, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return failBatch(stageUpload, errCodeUpload, err)
	}
	if err := j.blobs.Put(ctx, manifestKey(key), manifestBody, blobstore.PutOptions{
		ContentType: "application/json",
	}); err != nil {
		return failBatch(stageUpload, errCodeUpload, err)
	}

	batch.Status = postgres.BatchUploaded
	if err := j.store.SaveBatch(ctx, batch, "", ""); err != nil {
		return postgres.Batch{}, &stageError{stage: stageUpload, code: errCodeStateWrite, cause: err}
	}

	info, ok, err := j.blobs.Head(ctx, key)
	if err != nil {
		return failBatch(stageVerify, errCodeVerify, err)
	}
	if !ok {
		return failBatch(stageVerify, errCodeVerify, fmt.Errorf("objeto %s ausente após upload", key))
	}
	if info.Size != batch.Bytes {
		return failBatch(stageVerify, errCodeVerify, fmt.Errorf(
			"tamanho divergente em %s: %d bytes no destino, %d esperados", key, info.Size, batch.Bytes))
	}

	batch.Status = postgres.BatchVerified
	if err := j.store.SaveBatch(ctx, batch, "", ""); err != nil {
		return postgres.Batch{}, &stageError{stage: stageVerify, code: errCodeStateWrite, cause: err}
	}
	j.metrics.AddPendingBatches(ctx, partition.Name, -1)

	duration := j.now().Sub(start)
	j.metrics.RecordBatch(ctx, partition.Name, "verified", batch.RowCount, batch.Bytes, duration)
	logger.Info("cold_archive_batch_verified",
		slog.Int("batch_seq", seq),
		slog.String("project_id", projectID),
		slog.String("object_key", key),
		slog.Int64("rows", batch.RowCount),
		slog.Int64("bytes", batch.Bytes),
		slog.Int64("duration_ms", duration.Milliseconds()),
	)
	return batch, nil
}

// reuseBatch decide se o lote `seq` já está pronto de um run anterior.
//
// A key esperada também identifica o projeto, então uma key divergente
// significa que o lote pertence a outro projeto — sinal de que a paginação do
// projeto corrente terminou.
func reuseBatch(
	ctx context.Context,
	blobs blobstore.BlobStore,
	existing map[int]postgres.Batch,
	seq int,
	key string,
) (bool, postgres.Cursor, postgres.Batch) {
	batch, ok := existing[seq]
	if !ok || batch.Key != key {
		return false, postgres.Cursor{}, postgres.Batch{}
	}
	if batch.Status != postgres.BatchVerified && batch.Status != postgres.BatchUploaded {
		return false, postgres.Cursor{}, postgres.Batch{}
	}
	if batch.OccurredAtTo == nil || batch.CursorID == nil {
		return false, postgres.Cursor{}, postgres.Batch{}
	}

	// `uploaded` sem verificação: revalida o objeto em vez de reler o PG.
	// Se o objeto sumiu ou diverge, o lote é reprocessado do zero.
	if batch.Status == postgres.BatchUploaded {
		info, exists, err := blobs.Head(ctx, key)
		if err != nil || !exists || info.Size != batch.Bytes {
			return false, postgres.Cursor{}, postgres.Batch{}
		}
	}

	cursor := postgres.Cursor{OccurredAt: *batch.OccurredAtTo, ID: *batch.CursorID, Valid: true}
	return true, cursor, batch
}
