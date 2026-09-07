package api

import "context"

type (
	requestIDKey     struct{}
	correlationIDKey struct{}
	requestInfoKey   struct{}
)

// requestInfo carrega para cima o que só é descoberto no meio da cadeia.
// O middleware de observabilidade roda por fora do auth, então não enxerga o
// contexto derivado onde o `project_id` foi injetado; este holder é o canal
// de volta. É escrito e lido pela mesma goroutine da request.
type requestInfo struct {
	projectID string
}

func withRequestInfo(ctx context.Context) (context.Context, *requestInfo) {
	info := &requestInfo{}
	return context.WithValue(ctx, requestInfoKey{}, info), info
}

func requestInfoFrom(ctx context.Context) *requestInfo {
	info, _ := ctx.Value(requestInfoKey{}).(*requestInfo)
	return info
}

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestIDFrom devolve o `request_id` da request corrente.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

func withCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, id)
}

// CorrelationIDFrom devolve o `correlation_id` da request corrente.
func CorrelationIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(correlationIDKey{}).(string)
	return id
}
