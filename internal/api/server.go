package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/IsaacDSC/audit.git/internal/config"
	"github.com/IsaacDSC/audit.git/internal/event"
	"github.com/IsaacDSC/audit.git/internal/obs"
)

// Authenticator valida o par (project_id, secret) do Basic Auth.
type Authenticator interface {
	Authenticate(ctx context.Context, projectID, secret string) error
}

// HealthChecker responde se as dependências estão utilizáveis.
type HealthChecker interface {
	Ping(ctx context.Context) error
}

// Deps agrupa as dependências do servidor HTTP.
type Deps struct {
	Config  config.Config
	Logger  *slog.Logger
	Metrics *obs.APIMetrics
	Auth    Authenticator
	Parser  *event.Parser
	Events  EventWriter
	Health  HealthChecker
	Now     func() time.Time
}

// Server é a API write-only de ingestão.
type Server struct {
	cfg          config.Config
	logger       *slog.Logger
	metrics      *obs.APIMetrics
	auth         Authenticator
	parser       *event.Parser
	events       EventWriter
	health       HealthChecker
	limiter      *ProjectLimiter
	maxBodyBytes int64
	now          func() time.Time
}

// NewServer constrói o servidor a partir das dependências já inicializadas.
func NewServer(deps Deps) *Server {
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		cfg:          deps.Config,
		logger:       logger,
		metrics:      deps.Metrics,
		auth:         deps.Auth,
		parser:       deps.Parser,
		events:       deps.Events,
		health:       deps.Health,
		limiter:      NewProjectLimiter(deps.Config.API.RateLimitRPS, deps.Config.API.RateLimitBurst),
		maxBodyBytes: deps.Config.Ingest.MaxBodyBytes,
		now:          now,
	}
}

// Handler monta as rotas. O serviço é write-only: não há endpoint de leitura
// de eventos (decisão 12.3).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("POST "+RouteEvents, chain(
		http.HandlerFunc(s.handleIngest),
		s.observe(RouteEvents),
		s.limitBody,
		s.authenticate,
		s.rateLimit,
	))
	mux.Handle("GET /healthz", chain(http.HandlerFunc(s.handleLive), s.observe("/healthz")))
	mux.Handle("GET /readyz", chain(http.HandlerFunc(s.handleReady), s.observe("/readyz")))

	return chain(mux, s.identify, s.recoverPanic)
}

func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.health == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.health.Ping(ctx); err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "not_ready", "dependências indisponíveis")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ListenAndServe sobe o servidor e o encerra graciosamente quando ctx é
// cancelado, drenando requests em voo.
func (s *Server) ListenAndServe(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.cfg.API.Addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: s.cfg.API.ReadHeaderTimeout,
		ReadTimeout:       s.cfg.API.ReadTimeout,
		WriteTimeout:      s.cfg.API.WriteTimeout,
		IdleTimeout:       s.cfg.API.IdleTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("api_listening", slog.String("addr", s.cfg.API.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.logger.Info("api_shutting_down")
		shutdownCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), s.cfg.API.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return <-errCh
	}
}
