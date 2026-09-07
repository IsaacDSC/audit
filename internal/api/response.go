package api

import (
	"encoding/json"
	"net/http"
)

// errorBody é o contrato de erro da API: código estável para automação,
// mensagem segura para humanos (nunca com segredo ou PII do body).
type errorBody struct {
	Error     errorDetail `json:"error"`
	RequestID string      `json:"request_id,omitempty"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// acceptedBody é a resposta de sucesso de `POST /v1/events` (seção 4.1).
type acceptedBody struct {
	ID         string `json:"id"`
	ReceivedAt string `json:"received_at"`
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	writeJSON(w, status, errorBody{
		Error:     errorDetail{Code: code, Message: message},
		RequestID: RequestIDFrom(r.Context()),
	})
}

// statusRecorder captura o status para logs e métricas, já que o
// ResponseWriter padrão não o expõe.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Status devolve o código efetivamente enviado (200 se o handler só escreveu).
func (w *statusRecorder) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}
