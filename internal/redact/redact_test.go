package redact_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/IsaacDSC/audit.git/internal/redact"
)

func TestStringRedactsKnownSecretFormats(t *testing.T) {
	tests := []struct {
		name  string
		input string
		leak  string
	}{
		{"jwt", "token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.dBjftJeZ4CVPmB92K27uhbUJU1p1r", "eyJhbGciOiJIUzI1NiJ9"},
		{"bearer", "Authorization: Bearer abcdef0123456789abcdef", "abcdef0123456789abcdef"},
		{"aws access key", "usou AKIAIOSFODNN7EXAMPLE na chamada", "AKIAIOSFODNN7EXAMPLE"},
		{"github", "ghp_1234567890abcdefghijklmnopqrstuvwx", "ghp_1234567890abcdef"},
		{"slack", "xoxb-123456789012-abcdefghijkl", "xoxb-123456789012"},
		// Montado em runtime para o literal não disparar secret scanning no push.
		{"stripe", "sk_" + "live_" + strings.Repeat("x", 24), "sk_" + "live_"},
		{"google", "AIzaSyA1234567890abcdefghijklmnopqrstuv", "AIzaSyA1234567890abcdef"},
		{"pem", "-----BEGIN RSA PRIVATE KEY-----\nMIIEow==\n-----END RSA PRIVATE KEY-----", "MIIEow=="},
		{"url com credencial", "postgres://user:hunter2@db:5432/audit", "hunter2"},
	}

	redactor := redact.New()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, count := redactor.String(tc.input)
			if count != 1 {
				t.Errorf("count = %d, quero 1", count)
			}
			if strings.Contains(out, tc.leak) {
				t.Errorf("segredo vazou: %q", out)
			}
			if !strings.Contains(out, redact.Placeholder) {
				t.Errorf("faltou o placeholder em %q", out)
			}
		})
	}
}

func TestStringKeepsInnocuousText(t *testing.T) {
	input := "svc-billing/1.2.0 (linux; amd64)"
	out, count := redact.New().String(input)
	if count != 0 || out != input {
		t.Errorf("String(%q) = (%q, %d), quero inalterado", input, out, count)
	}
}

func TestMapRedactsBySensitiveKeyName(t *testing.T) {
	payload := map[string]any{
		"password":      "hunter2",
		"client_secret": "abc",
		"nested":        map[string]any{"api_key": "xyz", "plan_id": "gold"},
		"list":          []any{"Bearer abcdef0123456789abcdef", "ok"},
		"amount_cents":  json.Number("1500"),
	}

	count := redact.New().Map(payload)
	if count != 4 {
		t.Errorf("count = %d, quero 4", count)
	}

	if payload["password"] != redact.Placeholder {
		t.Errorf("password = %v", payload["password"])
	}
	if payload["client_secret"] != redact.Placeholder {
		t.Errorf("client_secret = %v", payload["client_secret"])
	}
	if payload["amount_cents"] != json.Number("1500") {
		t.Errorf("amount_cents foi alterado: %v", payload["amount_cents"])
	}

	nested := payload["nested"].(map[string]any)
	if nested["api_key"] != redact.Placeholder {
		t.Errorf("nested.api_key = %v", nested["api_key"])
	}
	if nested["plan_id"] != "gold" {
		t.Errorf("nested.plan_id foi alterado: %v", nested["plan_id"])
	}

	list := payload["list"].([]any)
	if strings.Contains(list[0].(string), "abcdef0123456789") {
		t.Errorf("segredo em lista sobreviveu: %v", list[0])
	}
	if list[1] != "ok" {
		t.Errorf("list[1] foi alterado: %v", list[1])
	}
}

// Chave sensível com valor numérico (ex.: "cvv": 123) também é segredo.
func TestMapRedactsNumericSecretByKey(t *testing.T) {
	payload := map[string]any{"cvv": json.Number("123")}
	if count := redact.New().Map(payload); count != 1 {
		t.Fatalf("count = %d, quero 1", count)
	}
	if payload["cvv"] != redact.Placeholder {
		t.Errorf("cvv = %v", payload["cvv"])
	}
}
