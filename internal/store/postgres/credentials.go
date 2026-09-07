package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CredentialRepository lê a tabela `services` quando
// AUTH_CREDENTIALS_SOURCE=db (seção 3).
type CredentialRepository struct {
	pool *pgxpool.Pool
}

// NewCredentialRepository constrói o repositório de credenciais.
func NewCredentialRepository(pool *pgxpool.Pool) *CredentialRepository {
	return &CredentialRepository{pool: pool}
}

// Lookup devolve o hash do segredo do projeto. ok=false para projeto
// inexistente ou desabilitado — o handler responde 401 sem distinguir os dois.
func (r *CredentialRepository) Lookup(ctx context.Context, projectID string) (string, bool, error) {
	var hash string
	err := r.pool.QueryRow(ctx,
		"SELECT secret_hash FROM services WHERE project_id = $1 AND NOT disabled", projectID,
	).Scan(&hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("buscar credencial: %w", err)
	}
	return hash, true, nil
}
