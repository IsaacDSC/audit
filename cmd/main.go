// Command audit é o binário único do serviço de auditoria (spec 001, seção 9).
//
// Dois modos, escolhidos por argumento:
//
//	audit api       sobe o servidor HTTP de ingestão (long-running)
//	audit migrate   executa uma passada do arquivamento quente → frio e encerra
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IsaacDSC/audit.git/internal/api"
	"github.com/IsaacDSC/audit.git/internal/auth"
	"github.com/IsaacDSC/audit.git/internal/blobstore"
	"github.com/IsaacDSC/audit.git/internal/coldarchive"
	"github.com/IsaacDSC/audit.git/internal/config"
	"github.com/IsaacDSC/audit.git/internal/event"
	"github.com/IsaacDSC/audit.git/internal/obs"
	"github.com/IsaacDSC/audit.git/internal/redact"
	"github.com/IsaacDSC/audit.git/internal/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

const usage = `audit — serviço de auditoria (spec 001)

uso:
  audit api        sobe o servidor HTTP de ingestão (POST /v1/events)
  audit migrate    exporta partições frias para o object storage e encerra

Configuração via ambiente; veja docs/specs/001-audit-service.md.
`

func main() {
	os.Exit(run())
}

func run() int {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	command := os.Args[1]
	if command != "api" && command != "migrate" {
		fmt.Fprintf(os.Stderr, "comando desconhecido: %q\n\n%s", command, usage)
		return 2
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}

	logger := obs.NewLogger(cfg)
	slog.SetDefault(logger)

	// SIGTERM encerra a API com drain e interrompe o job entre lotes; o job é
	// idempotente, então a próxima execução retoma o que faltou.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownMetrics, err := obs.SetupMetrics(ctx, cfg)
	if err != nil {
		logger.Error("otel_setup_failed", slog.String("error", err.Error()))
		return 1
	}
	defer func() {
		// O modo migrate é batch: sem flush explícito o último export OTLP se
		// perde junto com o processo (seção 11.2).
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if err := shutdownMetrics(flushCtx); err != nil {
			logger.Error("otel_shutdown_failed", slog.String("error", err.Error()))
		}
	}()

	switch command {
	case "api":
		err = runAPI(ctx, cfg, logger)
	case "migrate":
		err = runMigrate(ctx, cfg, logger)
	}
	if err != nil {
		logger.Error(command+"_failed", slog.String("error", err.Error()))
		return 1
	}
	return 0
}

func runAPI(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	if err := cfg.ValidateAPI(); err != nil {
		return err
	}

	pool, err := postgres.Open(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer pool.Close()

	if cfg.Database.AutoMigrate {
		if err := postgres.Migrate(ctx, pool); err != nil {
			return err
		}
	}

	partitions := postgres.NewPartitionManager(pool)
	// Pré-cria a semana corrente e as próximas para que o primeiro evento de
	// uma semana não pague o DDL dentro do orçamento de latência.
	if err := partitions.EnsureWindow(ctx, time.Now().UTC(), 2); err != nil {
		return err
	}

	credentials, err := credentialStore(cfg, pool)
	if err != nil {
		return err
	}

	metrics, err := obs.NewAPIMetrics()
	if err != nil {
		return err
	}

	server := api.NewServer(api.Deps{
		Config:  cfg,
		Logger:  logger,
		Metrics: metrics,
		Auth: auth.NewAuthenticator(credentials, auth.Options{
			TTL:         cfg.Auth.CacheTTL,
			NegativeTTL: cfg.Auth.NegativeCacheTTL,
			MaxEntries:  cfg.Auth.CacheSize,
		}),
		Parser: event.NewParser(cfg.Ingest, redact.New()),
		Events: postgres.NewEventRepository(pool, partitions),
		Health: pool,
	})

	go maintainPartitions(ctx, partitions, logger)

	return server.ListenAndServe(ctx)
}

func runMigrate(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	if err := cfg.ValidateMigrate(); err != nil {
		return err
	}

	runCtx := ctx
	if cfg.Archive.Timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, cfg.Archive.Timeout)
		defer cancel()
	}

	pool, err := postgres.Open(runCtx, cfg.Database)
	if err != nil {
		return err
	}
	defer pool.Close()

	if cfg.Database.AutoMigrate {
		if err := postgres.Migrate(runCtx, pool); err != nil {
			return err
		}
	}

	blobs, err := blobstore.New(runCtx, cfg.ColdStorage)
	if err != nil {
		return err
	}

	metrics, err := obs.NewArchiveMetrics()
	if err != nil {
		return err
	}

	job := coldarchive.New(coldarchive.Deps{
		Config:  cfg,
		Logger:  logger,
		Metrics: metrics,
		Store:   postgres.NewArchiveRepository(pool),
		Blobs:   blobs,
	})

	result, err := job.Run(runCtx)
	if err != nil {
		return err
	}
	// Partições que falharam mantêm os dados no PG e precisam de investigação;
	// o exit code diferente de zero faz o Container Apps Job sinalizar falha.
	if result.PartitionsFailed > 0 {
		return fmt.Errorf("%d partição(ões) falharam no arquivamento", result.PartitionsFailed)
	}
	return nil
}

func credentialStore(cfg config.Config, pool *pgxpool.Pool) (auth.Store, error) {
	if cfg.Auth.Source == config.CredentialsDB {
		return postgres.NewCredentialRepository(pool), nil
	}
	store, err := auth.NewMemoryStore(cfg.Auth.Raw)
	if err != nil {
		return nil, fmt.Errorf("AUDIT_CREDENTIALS: %w", err)
	}
	return store, nil
}

// maintainPartitions mantém a janela de partições futuras aberta enquanto a
// API roda, para que a virada de semana não caia no hot path.
func maintainPartitions(ctx context.Context, partitions *postgres.PartitionManager, logger *slog.Logger) {
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := partitions.EnsureWindow(ctx, time.Now().UTC(), 2); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				logger.Error("ensure_partitions_failed", slog.String("error", err.Error()))
			}
		}
	}
}
