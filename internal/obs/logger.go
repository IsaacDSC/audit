// Package obs concentra logs (log/slog) e métricas (OpenTelemetry → Elastic)
// conforme a seção 11 da spec 001.
package obs

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/IsaacDSC/audit.git/internal/config"
)

type loggerKey struct{}

// NewLogger devolve um logger JSON em stdout com os atributos base do serviço.
func NewLogger(cfg config.Config) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLevel(cfg.LogLevel),
	})
	return slog.New(handler).With(
		slog.String("service", cfg.ServiceName),
		slog.String("env", cfg.Env),
		slog.String("version", cfg.Version),
	)
}

// WithLogger anexa o logger ao contexto para que handlers e camadas
// internas herdem os atributos da request sem repeti-los.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// LoggerFrom recupera o logger do contexto; cai no default do slog se ausente.
func LoggerFrom(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

func parseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
