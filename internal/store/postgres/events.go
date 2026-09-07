package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/IsaacDSC/audit.git/internal/event"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const insertEventSQL = `
INSERT INTO events (
	id, project_id, idempotency_key, schema_version, action, outcome,
	actor_type, actor_id, resource_type, resource_id,
	request_id, correlation_id, ip, user_agent,
	occurred_at, received_at, metadata, extensions
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9, $10,
	$11, $12, $13, $14,
	$15, $16, $17, $18
)
ON CONFLICT (project_id, idempotency_key, occurred_at) DO NOTHING
RETURNING id, received_at`

const selectExistingEventSQL = `
SELECT id, received_at
FROM events
WHERE project_id = $1 AND idempotency_key = $2 AND occurred_at = $3`

// EventRepository grava eventos na camada quente com INSERT síncrono:
// o 202 só é devolvido depois do commit (decisão 12.1).
type EventRepository struct {
	pool       *pgxpool.Pool
	partitions *PartitionManager
}

// NewEventRepository constrói o repositório de eventos.
func NewEventRepository(pool *pgxpool.Pool, partitions *PartitionManager) *EventRepository {
	return &EventRepository{pool: pool, partitions: partitions}
}

// Insert persiste o evento. Um retry com a mesma chave de idempotência não
// duplica: devolve o id original com Duplicate=true (seção 4.1).
func (r *EventRepository) Insert(ctx context.Context, rec *event.Record) (event.InsertResult, error) {
	result, err := r.insertOnce(ctx, rec)
	if err == nil {
		return result, nil
	}

	// A partição da semana pode não existir ainda (evento com occurred_at
	// fora da janela pré-criada). Cria sob demanda e tenta uma única vez mais.
	if !isMissingPartition(err) {
		return event.InsertResult{}, err
	}
	if ensureErr := r.partitions.Ensure(ctx, rec.OccurredAt); ensureErr != nil {
		return event.InsertResult{}, fmt.Errorf("%w (após falha de partição: %w)", ensureErr, err)
	}
	return r.insertOnce(ctx, rec)
}

func (r *EventRepository) insertOnce(ctx context.Context, rec *event.Record) (event.InsertResult, error) {
	var result event.InsertResult
	err := r.pool.QueryRow(ctx, insertEventSQL,
		rec.ID, rec.ProjectID, rec.IdempotencyKey, rec.SchemaVersion, rec.Action, rec.Outcome,
		rec.ActorType, rec.ActorID, rec.ResourceType, rec.ResourceID,
		rec.RequestID, rec.CorrelationID, rec.IP, rec.UserAgent,
		rec.OccurredAt, rec.ReceivedAt, rec.Metadata, rec.Extensions,
	).Scan(&result.ID, &result.ReceivedAt)

	if err == nil {
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return event.InsertResult{}, fmt.Errorf("insert event: %w", err)
	}

	// DO NOTHING não devolveu linha: o evento já existe.
	if err := r.pool.QueryRow(ctx, selectExistingEventSQL,
		rec.ProjectID, rec.IdempotencyKey, rec.OccurredAt,
	).Scan(&result.ID, &result.ReceivedAt); err != nil {
		return event.InsertResult{}, fmt.Errorf("ler evento existente: %w", err)
	}
	result.Duplicate = true
	return result, nil
}

// isMissingPartition detecta "no partition of relation ... found for row",
// reportado pelo PostgreSQL como check_violation.
func isMissingPartition(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514"
}
