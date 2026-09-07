package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IsaacDSC/audit.git/internal/api"
	"github.com/IsaacDSC/audit.git/internal/auth"
	"github.com/IsaacDSC/audit.git/internal/config"
	"github.com/IsaacDSC/audit.git/internal/event"
	"github.com/IsaacDSC/audit.git/internal/obs"
	"github.com/IsaacDSC/audit.git/internal/redact"
	"github.com/google/uuid"
)

var receivedAt = time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

const eventBody = `{
	"action": "order.created",
	"actor": {"type": "user", "id": "usr_123"},
	"occurred_at": "2026-09-07T11:59:00Z",
	"metadata": {"amount_cents": 1500, "currency": "BRL"}
}`

// fakeWriter substitui o PostgreSQL: guarda o que foi inserido e aplica a
// mesma semântica de idempotência do UNIQUE (project_id, idempotency_key).
type fakeWriter struct {
	mu       sync.Mutex
	records  []*event.Record
	byKey    map[string]event.InsertResult
	failWith error
}

func newFakeWriter() *fakeWriter {
	return &fakeWriter{byKey: map[string]event.InsertResult{}}
}

func (f *fakeWriter) Insert(_ context.Context, rec *event.Record) (event.InsertResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failWith != nil {
		return event.InsertResult{}, f.failWith
	}

	key := rec.ProjectID + "\n" + rec.IdempotencyKey
	if existing, ok := f.byKey[key]; ok {
		existing.Duplicate = true
		return existing, nil
	}

	result := event.InsertResult{ID: rec.ID, ReceivedAt: rec.ReceivedAt}
	f.byKey[key] = result
	f.records = append(f.records, rec)
	return result, nil
}

func (f *fakeWriter) inserted() []*event.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*event.Record(nil), f.records...)
}

type fakeAuth struct {
	secret string
}

func (f fakeAuth) Authenticate(_ context.Context, projectID, secret string) error {
	if projectID == "" || secret != f.secret {
		return auth.ErrUnauthorized
	}
	return nil
}

type brokenAuth struct{}

func (brokenAuth) Authenticate(context.Context, string, string) error {
	return errors.New("store fora do ar")
}

// testConfig é explícita em vez de vir de config.Load(), para que o resultado
// não dependa do ambiente da máquina que roda o teste.
func testConfig() config.Config {
	return config.Config{
		Env:         "test",
		ServiceName: "audit",
		API: config.API{
			Addr:            ":0",
			ShutdownTimeout: time.Second,
			RateLimitRPS:    1000,
			RateLimitBurst:  1000,
		},
		Ingest: config.Ingest{
			MaxBodyBytes:      1024,
			MetadataMaxBytes:  4 * 1024,
			MetadataMaxKeys:   20,
			MetadataAllowlist: config.DefaultMetadataAllowlist,
			SchemaVersion:     1,
			MaxClockSkew:      24 * time.Hour,
			HotRetention:      90 * 24 * time.Hour,
		},
	}
}

func newTestServer(t *testing.T, writer api.EventWriter, authenticator api.Authenticator) http.Handler {
	t.Helper()

	cfg := testConfig()

	metrics, err := obs.NewAPIMetrics()
	if err != nil {
		t.Fatalf("NewAPIMetrics() error = %v", err)
	}

	if authenticator == nil {
		authenticator = fakeAuth{secret: "s3cr3t"}
	}

	return api.NewServer(api.Deps{
		Config:  cfg,
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Metrics: metrics,
		Auth:    authenticator,
		Parser:  event.NewParser(cfg.Ingest, redact.New()),
		Events:  writer,
		Now:     func() time.Time { return receivedAt },
	}).Handler()
}

func post(t *testing.T, handler http.Handler, body string, tweak func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, api.RouteEvents, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("billing", "s3cr3t")
	if tweak != nil {
		tweak(req)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

// O log e a métrica da request precisam do project_id, que só existe depois
// do auth — uma camada mais interna que a de observabilidade.
func TestObservabilitySeesAuthenticatedProject(t *testing.T) {
	cfg := testConfig()

	metrics, err := obs.NewAPIMetrics()
	if err != nil {
		t.Fatalf("NewAPIMetrics() error = %v", err)
	}

	var logs bytes.Buffer
	handler := api.NewServer(api.Deps{
		Config:  cfg,
		Logger:  slog.New(slog.NewJSONHandler(&logs, nil)),
		Metrics: metrics,
		Auth:    fakeAuth{secret: "s3cr3t"},
		Parser:  event.NewParser(cfg.Ingest, redact.New()),
		Events:  newFakeWriter(),
		Now:     func() time.Time { return receivedAt },
	}).Handler()

	if got := post(t, handler, eventBody, nil).Code; got != http.StatusAccepted {
		t.Fatalf("status = %d, quero 202", got)
	}

	var found bool
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var entry struct {
			Msg       string `json:"msg"`
			ProjectID string `json:"project_id"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log não é JSON: %v (%s)", err, line)
		}
		if entry.Msg != "request_completed" {
			continue
		}
		found = true
		if entry.ProjectID != "billing" {
			t.Errorf("request_completed.project_id = %q, quero billing", entry.ProjectID)
		}
	}
	if !found {
		t.Errorf("log request_completed ausente: %s", logs.String())
	}
}

func TestIngestAcceptsValidEvent(t *testing.T) {
	writer := newFakeWriter()
	response := post(t, newTestServer(t, writer, nil), eventBody, nil)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, quero 202: %s", response.Code, response.Body)
	}

	var body struct {
		ID         string `json:"id"`
		ReceivedAt string `json:"received_at"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("resposta não é JSON: %v", err)
	}
	if _, err := uuid.Parse(body.ID); err != nil {
		t.Errorf("id = %q, quero uuid", body.ID)
	}
	if body.ReceivedAt == "" {
		t.Error("received_at ausente na resposta")
	}
	if response.Header().Get("X-Request-ID") == "" {
		t.Error("X-Request-ID ausente na resposta")
	}

	records := writer.inserted()
	if len(records) != 1 {
		t.Fatalf("inseriu %d eventos, quero 1", len(records))
	}
	if records[0].ProjectID != "billing" {
		t.Errorf("project_id = %q, quero billing (do Basic Auth)", records[0].ProjectID)
	}
}

func TestIngestRejectsMissingRequiredFields(t *testing.T) {
	handler := newTestServer(t, newFakeWriter(), nil)

	tests := map[string]string{
		"sem action":      `{"occurred_at": "2026-09-07T11:59:00Z"}`,
		"sem occurred_at": `{"action": "order.created"}`,
		"json inválido":   `{`,
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			response := post(t, handler, body, nil)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, quero 400: %s", response.Code, response.Body)
			}
			if code := errorCode(t, response.Body.Bytes()); code == "" {
				t.Error("resposta de erro sem code")
			}
		})
	}
}

func TestIngestRequiresAuthentication(t *testing.T) {
	handler := newTestServer(t, newFakeWriter(), nil)

	tests := map[string]func(*http.Request){
		"sem header":     func(r *http.Request) { r.Header.Del("Authorization") },
		"segredo errado": func(r *http.Request) { r.SetBasicAuth("billing", "errado") },
		"username vazio": func(r *http.Request) { r.SetBasicAuth("", "s3cr3t") },
	}

	for name, tweak := range tests {
		t.Run(name, func(t *testing.T) {
			response := post(t, handler, eventBody, tweak)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, quero 401", response.Code)
			}
			if response.Header().Get("WWW-Authenticate") == "" {
				t.Error("WWW-Authenticate ausente no 401")
			}
			// O 401 não pode revelar se o projeto existe.
			if body := response.Body.String(); strings.Contains(body, "errado") {
				t.Errorf("resposta vaza detalhe da credencial: %s", body)
			}
		})
	}
}

// Falha do store de credenciais é do serviço, não do cliente.
func TestIngestReturns503WhenAuthStoreFails(t *testing.T) {
	response := post(t, newTestServer(t, newFakeWriter(), brokenAuth{}), eventBody, nil)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, quero 503", response.Code)
	}
}

func TestIngestRejectsOversizedBody(t *testing.T) {
	handler := newTestServer(t, newFakeWriter(), nil)
	oversized := `{"action":"a.b","occurred_at":"2026-09-07T11:59:00Z","pad":"` +
		strings.Repeat("x", 2048) + `"}`

	response := post(t, handler, oversized, nil)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, quero 413: %s", response.Code, response.Body)
	}
}

// O limite vale mesmo sem Content-Length confiável (body em chunks).
func TestIngestRejectsOversizedStreamedBody(t *testing.T) {
	handler := newTestServer(t, newFakeWriter(), nil)
	oversized := strings.Repeat("x", 4096)

	req := httptest.NewRequest(http.MethodPost, api.RouteEvents, strings.NewReader(oversized))
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("billing", "s3cr3t")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, quero 413: %s", response.Code, response.Body)
	}
}

// Decisão 12.4 + seção 4.1: retry devolve o id original, sem duplicar.
func TestIngestIsIdempotentWithDerivedKey(t *testing.T) {
	writer := newFakeWriter()
	handler := newTestServer(t, writer, nil)

	first := post(t, handler, eventBody, nil)
	second := post(t, handler, eventBody, nil)

	if first.Code != http.StatusAccepted || second.Code != http.StatusAccepted {
		t.Fatalf("status = %d e %d, quero 202 nos dois", first.Code, second.Code)
	}
	if got := len(writer.inserted()); got != 1 {
		t.Errorf("inseriu %d eventos, quero 1", got)
	}
	if idOf(t, first) != idOf(t, second) {
		t.Errorf("retry devolveu id diferente: %s != %s", idOf(t, first), idOf(t, second))
	}
}

func TestIngestIsIdempotentWithHeaderKey(t *testing.T) {
	writer := newFakeWriter()
	handler := newTestServer(t, writer, nil)

	withKey := func(r *http.Request) { r.Header.Set("Idempotency-Key", "pedido-42") }
	first := post(t, handler, eventBody, withKey)

	// Body diferente, mesma chave: continua sendo o mesmo evento.
	other := `{"action": "order.updated", "occurred_at": "2026-09-07T11:59:30Z"}`
	second := post(t, handler, other, withKey)

	if got := len(writer.inserted()); got != 1 {
		t.Errorf("inseriu %d eventos, quero 1", got)
	}
	if idOf(t, first) != idOf(t, second) {
		t.Error("mesma Idempotency-Key deveria devolver o id original")
	}
}

// Escopo da idempotência é o projeto: outro remetente não colide.
func TestIdempotencyIsScopedByProject(t *testing.T) {
	writer := newFakeWriter()
	handler := newTestServer(t, writer, nil)

	post(t, handler, eventBody, nil)
	post(t, handler, eventBody, func(r *http.Request) { r.SetBasicAuth("checkout", "s3cr3t") })

	if got := len(writer.inserted()); got != 2 {
		t.Errorf("inseriu %d eventos, quero 2", got)
	}
}

func TestIngestReturns503WhenStorageFails(t *testing.T) {
	writer := newFakeWriter()
	writer.failWith = errors.New("conexão perdida")

	response := post(t, newTestServer(t, writer, nil), eventBody, nil)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, quero 503", response.Code)
	}
	if strings.Contains(response.Body.String(), "conexão perdida") {
		t.Error("resposta expõe detalhe interno do erro")
	}
}

func TestIngestFallsBackToRequestCorrelationIDs(t *testing.T) {
	writer := newFakeWriter()
	handler := newTestServer(t, writer, nil)

	body := `{"action": "order.created", "occurred_at": "2026-09-07T11:59:00Z"}`
	post(t, handler, body, func(r *http.Request) {
		r.Header.Set("X-Correlation-ID", "corr-header")
	})

	records := writer.inserted()
	if len(records) != 1 {
		t.Fatalf("inseriu %d eventos", len(records))
	}
	if records[0].CorrelationID == nil || *records[0].CorrelationID != "corr-header" {
		t.Errorf("correlation_id = %v, quero corr-header", records[0].CorrelationID)
	}
	if records[0].RequestID == nil || *records[0].RequestID == "" {
		t.Error("request_id deveria cair no id da request")
	}
}

func TestIngestRejectsNonJSONContentType(t *testing.T) {
	response := post(t, newTestServer(t, newFakeWriter(), nil), eventBody, func(r *http.Request) {
		r.Header.Set("Content-Type", "text/plain")
	})
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, quero 415", response.Code)
	}
}

func TestRateLimitPerProject(t *testing.T) {
	cfg := testConfig()
	cfg.API.RateLimitRPS = 1
	cfg.API.RateLimitBurst = 2

	metrics, err := obs.NewAPIMetrics()
	if err != nil {
		t.Fatalf("NewAPIMetrics() error = %v", err)
	}

	handler := api.NewServer(api.Deps{
		Config:  cfg,
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Metrics: metrics,
		Auth:    fakeAuth{secret: "s3cr3t"},
		Parser:  event.NewParser(cfg.Ingest, redact.New()),
		Events:  newFakeWriter(),
		Now:     func() time.Time { return receivedAt },
	}).Handler()

	// Burst de 2 passa; o terceiro estoura o limite do projeto.
	uniqueBody := func(n int) string {
		return `{"action": "order.created", "occurred_at": "2026-09-07T11:59:0` +
			string(rune('0'+n)) + `Z"}`
	}
	for i := range 2 {
		if got := post(t, handler, uniqueBody(i), nil).Code; got != http.StatusAccepted {
			t.Fatalf("request %d: status = %d, quero 202", i, got)
		}
	}

	limited := post(t, handler, uniqueBody(2), nil)
	if limited.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, quero 429", limited.Code)
	}
	if limited.Header().Get("Retry-After") == "" {
		t.Error("Retry-After ausente no 429")
	}

	// Outro projeto tem o próprio bucket.
	other := post(t, handler, uniqueBody(3), func(r *http.Request) {
		r.SetBasicAuth("checkout", "s3cr3t")
	})
	if other.Code != http.StatusAccepted {
		t.Errorf("outro projeto: status = %d, quero 202", other.Code)
	}
}

// Decisão 12.3: o serviço é write-only.
func TestNoReadEndpoints(t *testing.T) {
	handler := newTestServer(t, newFakeWriter(), nil)

	for _, path := range []string{api.RouteEvents, "/v1/events/some-id", "/v1/query"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.SetBasicAuth("billing", "s3cr3t")

		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)

		if response.Code == http.StatusOK {
			t.Errorf("GET %s devolveu 200; o serviço não expõe leitura", path)
		}
	}
}

func TestHealthEndpoints(t *testing.T) {
	handler := newTestServer(t, newFakeWriter(), nil)

	for _, path := range []string{"/healthz", "/readyz"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Errorf("GET %s: status = %d, quero 200", path, response.Code)
		}
	}
}

func TestRequestIDIsEchoedAndSanitized(t *testing.T) {
	handler := newTestServer(t, newFakeWriter(), nil)

	response := post(t, handler, eventBody, func(r *http.Request) {
		r.Header.Set("X-Request-ID", "req_abc-123\r\nInjected: evil")
	})

	got := response.Header().Get("X-Request-ID")
	if strings.ContainsAny(got, "\r\n ") {
		t.Errorf("X-Request-ID não sanitizado: %q", got)
	}
	if !strings.HasPrefix(got, "req_abc-123") {
		t.Errorf("X-Request-ID = %q", got)
	}
}

func errorCode(t *testing.T, raw []byte) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("resposta de erro não é JSON: %v (%s)", err, raw)
	}
	return body.Error.Code
}

func idOf(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(bytes.NewReader(response.Body.Bytes())).Decode(&body); err != nil {
		t.Fatalf("resposta não é JSON: %v", err)
	}
	return body.ID
}
