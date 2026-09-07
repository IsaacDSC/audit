package api

import (
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/IsaacDSC/audit.git/internal/auth"
	"github.com/IsaacDSC/audit.git/internal/obs"
	"github.com/google/uuid"
)

type middleware func(http.Handler) http.Handler

func chain(h http.Handler, middlewares ...middleware) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

// recoverPanic transforma um panic em 500 sem derrubar o processo.
func (s *Server) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}
			obs.LoggerFrom(r.Context()).Error("panic_recovered",
				slog.Any("panic", recovered),
				slog.String("stack", string(debug.Stack())),
			)
			if rec, ok := w.(*statusRecorder); !ok || rec.status == 0 {
				writeError(w, r, http.StatusInternalServerError, "internal_error", "erro interno")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// identify garante `request_id` e `correlation_id` em toda request e anexa
// ambos ao logger do contexto (seção 11.1).
func (s *Server) identify(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := sanitizeID(r.Header.Get("X-Request-ID"))
		if requestID == "" {
			requestID = uuid.NewString()
		}
		correlationID := sanitizeID(r.Header.Get("X-Correlation-ID"))
		if correlationID == "" {
			correlationID = requestID
		}

		w.Header().Set("X-Request-ID", requestID)

		ctx, _ := withRequestInfo(r.Context())
		ctx = withRequestID(ctx, requestID)
		ctx = withCorrelationID(ctx, correlationID)
		ctx = obs.WithLogger(ctx, s.logger.With(
			slog.String("request_id", requestID),
			slog.String("correlation_id", correlationID),
		))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// observe registra log de conclusão e as métricas OTel da request.
// O `project_id` só existe depois do auth, então é lido do contexto no fim.
func (s *Server) observe(route string) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			recorder := &statusRecorder{ResponseWriter: w}

			next.ServeHTTP(recorder, r)

			duration := time.Since(start)
			status := recorder.Status()
			projectID := ""
			if info := requestInfoFrom(r.Context()); info != nil {
				projectID = info.projectID
			}
			outcome := "success"
			if status >= 400 {
				outcome = "failure"
			}

			s.metrics.RecordRequest(r.Context(), r.Method, route, projectID, outcome, status, duration)

			logger := obs.LoggerFrom(r.Context())
			attrs := []any{
				slog.String("project_id", projectID),
				slog.String("method", r.Method),
				slog.String("path", route),
				slog.Int("status", status),
				slog.Int64("duration_ms", duration.Milliseconds()),
				slog.String("outcome", outcome),
			}
			if status >= 500 {
				logger.Error("request_completed", attrs...)
			} else {
				logger.Info("request_completed", attrs...)
			}
		})
	}
}

// limitBody aplica o teto de payload da seção 8 antes de qualquer leitura.
func (s *Server) limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > s.maxBodyBytes {
			writeError(w, r, http.StatusRequestEntityTooLarge, "body_too_large",
				"body excede o limite configurado")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
		next.ServeHTTP(w, r)
	})
}

// authenticate valida o Basic Auth e injeta o `project_id` no contexto.
// A identidade do remetente vem daqui, nunca do body.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		projectID, secret, ok := r.BasicAuth()
		if !ok {
			unauthorized(w, r)
			return
		}

		if err := s.auth.Authenticate(r.Context(), projectID, secret); err != nil {
			if errors.Is(err, auth.ErrUnauthorized) {
				unauthorized(w, r)
				return
			}
			// Falha do store de credenciais não é culpa do cliente.
			obs.LoggerFrom(r.Context()).Error("auth_store_failed",
				slog.String("error_code", "auth_store"),
				slog.String("error", err.Error()),
			)
			writeError(w, r, http.StatusServiceUnavailable, "auth_unavailable",
				"não foi possível validar credenciais")
			return
		}

		if info := requestInfoFrom(r.Context()); info != nil {
			info.projectID = projectID
		}
		ctx := auth.WithProjectID(r.Context(), projectID)
		ctx = obs.WithLogger(ctx, obs.LoggerFrom(ctx).With(slog.String("project_id", projectID)))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// rateLimit aplica o teto por `project_id` (seção 8).
func (s *Server) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		projectID := auth.ProjectIDFrom(r.Context())
		if !s.limiter.Allow(projectID) {
			w.Header().Set("Retry-After", "1")
			writeError(w, r, http.StatusTooManyRequests, "rate_limited",
				"limite de requisições por projeto excedido")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func unauthorized(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("WWW-Authenticate", `Basic realm="audit", charset="UTF-8"`)
	writeError(w, r, http.StatusUnauthorized, "unauthorized", "credenciais inválidas")
}

// sanitizeID evita que um header hostil injete conteúdo arbitrário nos logs
// ou na resposta.
func sanitizeID(raw string) string {
	const maxLen = 128
	if len(raw) > maxLen {
		raw = raw[:maxLen]
	}
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			out = append(out, c)
		case c == '-', c == '_', c == '.', c == ':':
			out = append(out, c)
		}
	}
	return string(out)
}
