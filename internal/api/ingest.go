package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/IsaacDSC/audit.git/internal/auth"
	"github.com/IsaacDSC/audit.git/internal/event"
	"github.com/IsaacDSC/audit.git/internal/obs"
)

// RouteEvents é o único endpoint do serviço: ingestão write-only (seção 4.1).
const RouteEvents = "/v1/events"

// EventWriter persiste um evento na camada quente.
type EventWriter interface {
	Insert(ctx context.Context, rec *event.Record) (event.InsertResult, error)
}

// handleIngest recebe o evento, valida, mascara segredos e grava no
// PostgreSQL de forma síncrona antes de responder 202 (decisão 12.1).
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := obs.LoggerFrom(ctx)

	if contentType := r.Header.Get("Content-Type"); contentType != "" && !isJSON(contentType) {
		writeError(w, r, http.StatusUnsupportedMediaType, "unsupported_media_type",
			"Content-Type deve ser application/json")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeError(w, r, http.StatusRequestEntityTooLarge, "body_too_large",
				"body excede o limite configurado")
			return
		}
		writeError(w, r, http.StatusBadRequest, "unreadable_body", "não foi possível ler o body")
		return
	}

	projectID := auth.ProjectIDFrom(ctx)
	receivedAt := s.now().UTC()

	record, err := s.parser.Parse(projectID, body, r.Header.Get("Idempotency-Key"), receivedAt)
	if err != nil {
		var validationErr *event.Error
		if errors.As(err, &validationErr) {
			logger.Warn("event_rejected",
				slog.String("error_code", validationErr.Code),
				slog.String("error", validationErr.Message),
			)
			writeError(w, r, http.StatusBadRequest, validationErr.Code, validationErr.Message)
			return
		}
		logger.Error("event_parse_failed", slog.String("error", err.Error()))
		writeError(w, r, http.StatusInternalServerError, "internal_error", "erro interno")
		return
	}

	// Sem `correlation_id` no body, o da request serve de fallback (seção 11.1).
	if record.CorrelationID == nil {
		if correlationID := CorrelationIDFrom(ctx); correlationID != "" {
			record.CorrelationID = &correlationID
		}
	}
	if record.RequestID == nil {
		if requestID := RequestIDFrom(ctx); requestID != "" {
			record.RequestID = &requestID
		}
	}

	result, err := s.events.Insert(ctx, record)
	if err != nil {
		logger.Error("event_insert_failed",
			slog.String("error_code", "pg_insert"),
			slog.String("action", record.Action),
			slog.String("error", err.Error()),
		)
		writeError(w, r, http.StatusServiceUnavailable, "storage_unavailable",
			"não foi possível persistir o evento")
		return
	}

	logger.Info("event_ingested",
		slog.String("event_id", result.ID.String()),
		slog.String("action", record.Action),
		slog.Bool("duplicate", result.Duplicate),
		slog.Bool("derived_idempotency_key", record.DerivedKey),
		slog.Int("fields_redacted_count", record.RedactedFields),
	)

	// Retry seguro: o mesmo body devolve o id original em vez de 409
	// (seção 4.1, semântica escolhida).
	writeJSON(w, http.StatusAccepted, acceptedBody{
		ID:         result.ID.String(),
		ReceivedAt: result.ReceivedAt.UTC().Format(time.RFC3339Nano),
	})
}

func isJSON(contentType string) bool {
	mediaType, _, _ := strings.Cut(contentType, ";")
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	return mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
}
