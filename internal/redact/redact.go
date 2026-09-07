// Package redact mascara tokens e segredos antes do INSERT, conforme a
// decisão 12.7 da spec 001. A máscara é irreversível: o valor original nunca
// chega ao PostgreSQL nem ao Parquet, e nunca é logado.
package redact

import (
	"encoding/json"
	"regexp"
)

// Placeholder substitui qualquer valor considerado sensível.
const Placeholder = "***REDACTED***"

// sensitiveKeyRe casa nomes de campo que carregam segredo por convenção.
// Vale a pena o falso positivo ocasional: gravar o segredo é irreversível.
var sensitiveKeyRe = regexp.MustCompile(`(?i)(pass(word|wd)?|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|client[_-]?secret|credential|authorization|session[_-]?id|cookie|signature|otp|cvv|card[_-]?number)`)

// secretValueRe casa formatos de segredo reconhecíveis pelo próprio valor,
// mesmo quando a chave é inocente (ex.: "note": "Bearer eyJ...").
var secretValueRe = regexp.MustCompile(
	`(?s)` +
		// JWT
		`eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}` +
		// Authorization: Bearer / Basic
		`|(?i:bearer|basic)\s+[A-Za-z0-9\-._~+/]{12,}={0,2}` +
		// Chaves de acesso AWS
		`|(?:AKIA|ASIA|AGPA|AIDA|AROA|ANPA|ANVA)[0-9A-Z]{16}` +
		// Blocos PEM de chave privada
		`|-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----` +
		// Tokens GitHub
		`|gh[pousr]_[A-Za-z0-9]{30,}` +
		// Tokens Slack
		`|xox[abprs]-[A-Za-z0-9-]{10,}` +
		// Chaves Stripe
		`|[sr]k_(?:live|test)_[A-Za-z0-9]{16,}` +
		// Chaves de API do Google
		`|AIza[0-9A-Za-z\-_]{35}`,
)

// urlCredRe casa credenciais embutidas em URL, preservando o esquema/host.
var urlCredRe = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.\-]*://)[^\s/:@]+:[^\s/@]+@`)

// Redactor aplica a máscara em valores JSON e em campos de texto conhecidos.
// É stateless e seguro para uso concorrente.
type Redactor struct{}

// New devolve um redactor com os padrões padrão da spec.
func New() *Redactor { return &Redactor{} }

// String mascara um valor de texto isolado (ex.: user_agent).
// Devolve o texto resultante e quantos campos foram mascarados (0 ou 1).
func (r *Redactor) String(s string) (string, int) {
	out := redactString(s)
	if out == s {
		return s, 0
	}
	return out, 1
}

// Map percorre um objeto JSON decodificado (map/slice/string aninhados) e
// mascara segredos in-place. Devolve o número de campos mascarados.
func (r *Redactor) Map(m map[string]any) int {
	if len(m) == 0 {
		return 0
	}
	count := 0
	for k, v := range m {
		redacted, n := r.value(k, v)
		m[k] = redacted
		count += n
	}
	return count
}

func (r *Redactor) value(key string, v any) (any, int) {
	switch typed := v.(type) {
	case string:
		if key != "" && sensitiveKeyRe.MatchString(key) {
			return Placeholder, 1
		}
		out := redactString(typed)
		if out != typed {
			return out, 1
		}
		return typed, 0

	case json.Number:
		// Um número não vaza token, mas uma chave sensível com valor numérico
		// (ex.: "pin": 1234) ainda é segredo.
		if key != "" && sensitiveKeyRe.MatchString(key) {
			return Placeholder, 1
		}
		return typed, 0

	case map[string]any:
		if key != "" && sensitiveKeyRe.MatchString(key) {
			return Placeholder, 1
		}
		return typed, r.Map(typed)

	case []any:
		if key != "" && sensitiveKeyRe.MatchString(key) {
			return Placeholder, 1
		}
		count := 0
		for i, item := range typed {
			redacted, n := r.value("", item)
			typed[i] = redacted
			count += n
		}
		return typed, count

	default:
		return v, 0
	}
}

func redactString(s string) string {
	if s == "" {
		return s
	}
	out := secretValueRe.ReplaceAllString(s, Placeholder)
	out = urlCredRe.ReplaceAllString(out, "${1}"+Placeholder+"@")
	return out
}
