package event_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/IsaacDSC/audit.git/internal/config"
	"github.com/IsaacDSC/audit.git/internal/event"
	"github.com/IsaacDSC/audit.git/internal/redact"
)

var now = time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

func newParser(t *testing.T) *event.Parser {
	t.Helper()
	return event.NewParser(config.Ingest{
		MetadataMaxBytes:  4 * 1024,
		MetadataMaxKeys:   20,
		MetadataAllowlist: config.DefaultMetadataAllowlist,
		SchemaVersion:     1,
		MaxClockSkew:      24 * time.Hour,
		HotRetention:      90 * 24 * time.Hour,
	}, redact.New())
}

const validBody = `{
	"action": "order.created",
	"actor": {"type": "user", "id": "usr_123"},
	"resource": {"type": "order", "id": "ord_456"},
	"outcome": "success",
	"occurred_at": "2026-09-07T12:00:00Z",
	"request_id": "req_abc",
	"correlation_id": "corr_xyz",
	"ip": "203.0.113.10",
	"user_agent": "svc-billing/1.2.0",
	"metadata": {"amount_cents": 1500, "currency": "BRL"}
}`

func TestParseMapsBodyToTypedColumns(t *testing.T) {
	record, err := newParser(t).Parse("billing", []byte(validBody), "", now)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if record.ProjectID != "billing" {
		t.Errorf("ProjectID = %q, quero billing", record.ProjectID)
	}
	if record.Action != "order.created" {
		t.Errorf("Action = %q", record.Action)
	}
	if got := deref(record.ActorID); got != "usr_123" {
		t.Errorf("ActorID = %q", got)
	}
	if got := deref(record.ResourceType); got != "order" {
		t.Errorf("ResourceType = %q", got)
	}
	if !record.OccurredAt.Equal(now) {
		t.Errorf("OccurredAt = %v, quero %v", record.OccurredAt, now)
	}
	if record.IP == nil || record.IP.String() != "203.0.113.10" {
		t.Errorf("IP = %v", record.IP)
	}
	if record.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d", record.SchemaVersion)
	}
}

// A identidade vem do Basic Auth: o body não pode sobrescrevê-la (seção 2).
func TestParseIgnoresServerDerivedFieldsInBody(t *testing.T) {
	body := `{
		"action": "order.created",
		"occurred_at": "2026-09-07T12:00:00Z",
		"project_id": "attacker",
		"id": "forged",
		"received_at": "1999-01-01T00:00:00Z",
		"schema_version": 99
	}`

	record, err := newParser(t).Parse("billing", []byte(body), "", now)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if record.ProjectID != "billing" {
		t.Errorf("ProjectID = %q, quero billing", record.ProjectID)
	}
	if record.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, quero 1", record.SchemaVersion)
	}
	if !record.ReceivedAt.Equal(now) {
		t.Errorf("ReceivedAt = %v, quero %v", record.ReceivedAt, now)
	}

	extensions := decode(t, record.Extensions)
	for _, field := range []string{"project_id", "id", "received_at", "schema_version"} {
		if _, ok := extensions[field]; ok {
			t.Errorf("campo reservado %q vazou para extensions", field)
		}
	}
}

// Allowlist global: o que não está nela é preservado em extensions.metadata.
func TestParseSplitsMetadataByAllowlist(t *testing.T) {
	body := `{
		"action": "order.created",
		"occurred_at": "2026-09-07T12:00:00Z",
		"metadata": {"amount_cents": 1500, "currency": "BRL", "warehouse": "sp-01"}
	}`

	record, err := newParser(t).Parse("billing", []byte(body), "", now)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	metadata := decode(t, record.Metadata)
	if _, ok := metadata["amount_cents"]; !ok {
		t.Error("amount_cents deveria estar em metadata")
	}
	if _, ok := metadata["warehouse"]; ok {
		t.Error("warehouse não está na allowlist e não deveria estar em metadata")
	}

	extensions := decode(t, record.Extensions)
	overflow, ok := extensions["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("extensions.metadata = %v, quero objeto", extensions["metadata"])
	}
	if overflow["warehouse"] != "sp-01" {
		t.Errorf("extensions.metadata.warehouse = %v", overflow["warehouse"])
	}
}

func TestParseKeepsUnknownTopLevelFieldsInExtensions(t *testing.T) {
	body := `{
		"action": "order.created",
		"occurred_at": "2026-09-07T12:00:00Z",
		"tenant": "acme",
		"nested": {"a": 1}
	}`

	record, err := newParser(t).Parse("billing", []byte(body), "", now)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	extensions := decode(t, record.Extensions)
	if extensions["tenant"] != "acme" {
		t.Errorf("extensions.tenant = %v", extensions["tenant"])
	}
	if _, ok := extensions["nested"]; !ok {
		t.Error("extensions.nested ausente")
	}
	if _, ok := extensions["action"]; ok {
		t.Error("action é coluna e não deveria duplicar em extensions")
	}
}

func TestParseRejectsInvalidBodies(t *testing.T) {
	tests := []struct {
		name string
		body string
		code string
	}{
		{"sem action", `{"occurred_at": "2026-09-07T12:00:00Z"}`, "missing_field"},
		{"action vazia", `{"action": "  ", "occurred_at": "2026-09-07T12:00:00Z"}`, "missing_field"},
		{"sem occurred_at", `{"action": "a.b"}`, "missing_field"},
		{"occurred_at não RFC3339", `{"action": "a.b", "occurred_at": "07/09/2026"}`, "invalid_field"},
		{"action não string", `{"action": 42, "occurred_at": "2026-09-07T12:00:00Z"}`, "invalid_field"},
		{"ip inválido", `{"action": "a.b", "occurred_at": "2026-09-07T12:00:00Z", "ip": "999.1.1.1"}`, "invalid_field"},
		{"metadata não objeto", `{"action": "a.b", "occurred_at": "2026-09-07T12:00:00Z", "metadata": []}`, "invalid_field"},
		{"body vazio", ``, "empty_body"},
		{"json inválido", `{`, "invalid_json"},
		{"array no topo", `[]`, "invalid_json"},
		{"futuro além do skew", `{"action": "a.b", "occurred_at": "2026-09-30T00:00:00Z"}`, "occurred_at_in_future"},
		{"anterior à janela quente", `{"action": "a.b", "occurred_at": "2020-01-01T00:00:00Z"}`, "occurred_at_too_old"},
	}

	parser := newParser(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.Parse("billing", []byte(tc.body), "", now)
			var validationErr *event.Error
			if !asError(err, &validationErr) {
				t.Fatalf("Parse() error = %v, quero *event.Error", err)
			}
			if validationErr.Code != tc.code {
				t.Errorf("code = %q, quero %q", validationErr.Code, tc.code)
			}
		})
	}
}

func TestParseEnforcesMetadataLimits(t *testing.T) {
	parser := event.NewParser(config.Ingest{
		MetadataMaxBytes:  64,
		MetadataMaxKeys:   2,
		MetadataAllowlist: config.DefaultMetadataAllowlist,
		SchemaVersion:     1,
		MaxClockSkew:      24 * time.Hour,
		HotRetention:      90 * 24 * time.Hour,
	}, redact.New())

	tooManyKeys := `{"action":"a.b","occurred_at":"2026-09-07T12:00:00Z","metadata":{"a":1,"b":2,"c":3}}`
	if _, err := parser.Parse("billing", []byte(tooManyKeys), "", now); !hasCode(err, "metadata_too_many_keys") {
		t.Errorf("erro = %v, quero metadata_too_many_keys", err)
	}

	tooLarge := `{"action":"a.b","occurred_at":"2026-09-07T12:00:00Z","metadata":{"currency":"` +
		strings.Repeat("x", 200) + `"}}`
	if _, err := parser.Parse("billing", []byte(tooLarge), "", now); !hasCode(err, "metadata_too_large") {
		t.Errorf("erro = %v, quero metadata_too_large", err)
	}
}

// Decisão 12.4: sem header, a chave sai de sha256(project_id + canonical_json).
func TestDerivedIdempotencyKeyIsStableAcrossKeyOrder(t *testing.T) {
	parser := newParser(t)

	first, err := parser.Parse("billing", []byte(validBody), "", now)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	reordered := `{
		"metadata": {"currency": "BRL", "amount_cents": 1500},
		"user_agent": "svc-billing/1.2.0",
		"ip": "203.0.113.10",
		"correlation_id": "corr_xyz",
		"request_id": "req_abc",
		"occurred_at": "2026-09-07T12:00:00Z",
		"outcome": "success",
		"resource": {"id": "ord_456", "type": "order"},
		"actor": {"id": "usr_123", "type": "user"},
		"action": "order.created"
	}`
	second, err := parser.Parse("billing", []byte(reordered), "", now)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if first.IdempotencyKey != second.IdempotencyKey {
		t.Errorf("chaves divergem para o mesmo body:\n%s\n%s", first.IdempotencyKey, second.IdempotencyKey)
	}
	if !first.DerivedKey {
		t.Error("DerivedKey deveria ser true sem header")
	}
}

func TestDerivedIdempotencyKeyIsScopedByProject(t *testing.T) {
	parser := newParser(t)

	billing, err := parser.Parse("billing", []byte(validBody), "", now)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	checkout, err := parser.Parse("checkout", []byte(validBody), "", now)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if billing.IdempotencyKey == checkout.IdempotencyKey {
		t.Error("projetos diferentes deveriam gerar chaves diferentes")
	}
}

func TestHeaderIdempotencyKeyWins(t *testing.T) {
	record, err := newParser(t).Parse("billing", []byte(validBody), "key-123", now)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if record.IdempotencyKey != "key-123" {
		t.Errorf("IdempotencyKey = %q, quero key-123", record.IdempotencyKey)
	}
	if record.DerivedKey {
		t.Error("DerivedKey deveria ser false com header")
	}
}

// Decisão 12.7: segredo não pode chegar ao PostgreSQL.
func TestParseRedactsSecretsBeforePersisting(t *testing.T) {
	body := `{
		"action": "auth.login",
		"occurred_at": "2026-09-07T12:00:00Z",
		"user_agent": "curl/8.0 Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abcd1234",
		"metadata": {"currency": "BRL"},
		"context": {
			"api_key": "super-secret-value",
			"aws": "AKIAIOSFODNN7EXAMPLE",
			"nested": {"note": "token=ghp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
		}
	}`

	record, err := newParser(t).Parse("billing", []byte(body), "", now)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if record.RedactedFields == 0 {
		t.Error("RedactedFields = 0, esperava campos mascarados")
	}

	serialized := string(record.Extensions) + deref(record.UserAgent)
	for _, secret := range []string{"super-secret-value", "AKIAIOSFODNN7EXAMPLE", "ghp_aaaa", "eyJhbGciOiJIUzI1NiJ9"} {
		if strings.Contains(serialized, secret) {
			t.Errorf("segredo %q sobreviveu à máscara: %s", secret, serialized)
		}
	}
}

func decode(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("json inválido: %v (%s)", err, raw)
	}
	return out
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func hasCode(err error, code string) bool {
	var validationErr *event.Error
	return asError(err, &validationErr) && validationErr.Code == code
}

func asError(err error, target **event.Error) bool {
	if err == nil {
		return false
	}
	casted, ok := err.(*event.Error)
	if !ok {
		return false
	}
	*target = casted
	return true
}
