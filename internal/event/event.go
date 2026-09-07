// Package event traduz o body recebido em `POST /v1/events` no registro
// persistido na camada quente (spec 001, seções 4.1, 6.1 e 7).
package event

import (
	"net/netip"
	"time"

	"github.com/google/uuid"
)

// Record é o evento pronto para o INSERT: colunas tipadas + dois JSONB.
type Record struct {
	ID             uuid.UUID
	ProjectID      string
	IdempotencyKey string
	SchemaVersion  int16

	Action        string
	Outcome       *string
	ActorType     *string
	ActorID       *string
	ResourceType  *string
	ResourceID    *string
	RequestID     *string
	CorrelationID *string
	IP            *netip.Addr
	UserAgent     *string

	OccurredAt time.Time
	ReceivedAt time.Time

	Metadata   []byte
	Extensions []byte

	// RedactedFields alimenta o log `fields_redacted_count` (decisão 12.7);
	// o valor original nunca é retido.
	RedactedFields int
	// DerivedKey indica que a chave veio do body, não do header.
	DerivedKey bool
}

// InsertResult descreve o desfecho do INSERT idempotente.
type InsertResult struct {
	ID         uuid.UUID
	ReceivedAt time.Time
	// Duplicate indica que o evento já existia: o retry devolve o id original
	// em vez de criar um segundo registro (seção 4.1).
	Duplicate bool
}

// Error é uma falha de validação com código estável para logs e para o corpo
// da resposta 400.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func newError(code, message string) *Error {
	return &Error{Code: code, Message: message}
}
