// Package postgres implementa a camada quente (ingestão) e o controle do
// arquivamento frio descritos nas seções 5 e 6 da spec 001.
package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"time"

	"github.com/IsaacDSC/audit.git/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Open cria o pool de conexões usado pelos dois modos do binário.
func Open(ctx context.Context, cfg config.Database) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	// Timestamps sempre renderizados em UTC: o parsing dos bounds de partição
	// (partition.go) e os ranges dos lotes dependem disso.
	poolCfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	if cfg.StatementTimeout > 0 {
		ms := strconv.FormatInt(cfg.StatementTimeout.Milliseconds(), 10)
		poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = ms
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("abrir pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// Migrate aplica as migrations embutidas de forma idempotente, serializando
// execuções concorrentes (api e migrate podem subir ao mesmo tempo).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	const lockID = 8412734901283
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", lockID)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    text        PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("criar schema_migrations: %w", err)
	}

	entries, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("listar migrations: %w", err)
	}
	sort.Strings(entries)

	for _, entry := range entries {
		var exists bool
		if err := conn.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", entry,
		).Scan(&exists); err != nil {
			return fmt.Errorf("checar migration %s: %w", entry, err)
		}
		if exists {
			continue
		}

		body, err := migrationFS.ReadFile(entry)
		if err != nil {
			return fmt.Errorf("ler migration %s: %w", entry, err)
		}
		if _, err := conn.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("aplicar migration %s: %w", entry, err)
		}
		if _, err := conn.Exec(ctx,
			"INSERT INTO schema_migrations (version) VALUES ($1)", entry,
		); err != nil {
			return fmt.Errorf("registrar migration %s: %w", entry, err)
		}
	}
	return nil
}
