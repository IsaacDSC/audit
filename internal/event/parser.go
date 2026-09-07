package event

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/IsaacDSC/audit.git/internal/config"
	"github.com/IsaacDSC/audit.git/internal/redact"
	"github.com/google/uuid"
)

// reservedFields são derivados pelo servidor. Se aparecerem no body são
// descartados, para que o remetente não consiga forjar sua própria
// identidade nem o instante de recebimento (seção 2, requisitos funcionais).
var reservedFields = map[string]struct{}{
	"id":             {},
	"project_id":     {},
	"received_at":    {},
	"schema_version": {},
}

// columnFields são consumidos por colunas tipadas e por isso não são
// repetidos em `extensions`.
var columnFields = map[string]struct{}{
	"action":         {},
	"actor":          {},
	"resource":       {},
	"outcome":        {},
	"occurred_at":    {},
	"request_id":     {},
	"correlation_id": {},
	"ip":             {},
	"user_agent":     {},
	"metadata":       {},
}

// maxLen limita o tamanho das colunas de texto; o excedente é truncado em vez
// de rejeitado, para não descartar auditoria por causa de um user agent longo.
const (
	maxLenAction      = 256
	maxLenOutcome     = 64
	maxLenType        = 128
	maxLenID          = 256
	maxLenUserAgent   = 512
	maxLenIdempotency = 256
)

// Parser converte o body cru em Record aplicando validação, máscara de
// segredos e a allowlist global de metadata.
type Parser struct {
	allowlist        map[string]struct{}
	metadataMaxBytes int
	metadataMaxKeys  int
	schemaVersion    int16
	maxClockSkew     time.Duration
	hotRetention     time.Duration
	redactor         *redact.Redactor
}

// NewParser constrói o parser a partir da configuração de ingestão.
func NewParser(cfg config.Ingest, redactor *redact.Redactor) *Parser {
	allowlist := make(map[string]struct{}, len(cfg.MetadataAllowlist))
	for _, key := range cfg.MetadataAllowlist {
		allowlist[key] = struct{}{}
	}
	if redactor == nil {
		redactor = redact.New()
	}
	return &Parser{
		allowlist:        allowlist,
		metadataMaxBytes: cfg.MetadataMaxBytes,
		metadataMaxKeys:  cfg.MetadataMaxKeys,
		schemaVersion:    cfg.SchemaVersion,
		maxClockSkew:     cfg.MaxClockSkew,
		hotRetention:     cfg.HotRetention,
		redactor:         redactor,
	}
}

// Parse valida o body e devolve o Record pronto para persistir.
// headerKey é o `Idempotency-Key` recebido (vazio → chave derivada).
func (p *Parser) Parse(projectID string, body []byte, headerKey string, receivedAt time.Time) (*Record, error) {
	root, err := decodeObject(body)
	if err != nil {
		return nil, err
	}

	// A chave derivada usa o body original, antes da máscara e do descarte
	// dos campos reservados: retries do mesmo payload colidem sempre.
	idempotencyKey := strings.TrimSpace(headerKey)
	derived := idempotencyKey == ""
	if derived {
		idempotencyKey, err = DeriveIdempotencyKey(projectID, root)
		if err != nil {
			return nil, newError("invalid_body", "body não é canonicalizável: "+err.Error())
		}
	} else if len(idempotencyKey) > maxLenIdempotency {
		return nil, newError("idempotency_key_too_long",
			fmt.Sprintf("Idempotency-Key excede %d caracteres", maxLenIdempotency))
	}

	rec := &Record{
		ID:             uuid.New(),
		ProjectID:      projectID,
		IdempotencyKey: idempotencyKey,
		SchemaVersion:  p.schemaVersion,
		ReceivedAt:     receivedAt,
		DerivedKey:     derived,
	}

	action, err := requiredString(root, "action", maxLenAction)
	if err != nil {
		return nil, err
	}
	rec.Action = action

	occurredAt, err := p.parseOccurredAt(root, receivedAt)
	if err != nil {
		return nil, err
	}
	rec.OccurredAt = occurredAt

	if rec.ActorType, rec.ActorID, err = nestedPair(root, "actor"); err != nil {
		return nil, err
	}
	if rec.ResourceType, rec.ResourceID, err = nestedPair(root, "resource"); err != nil {
		return nil, err
	}
	if rec.Outcome, err = optionalString(root, "outcome", maxLenOutcome); err != nil {
		return nil, err
	}
	if rec.RequestID, err = optionalString(root, "request_id", maxLenID); err != nil {
		return nil, err
	}
	if rec.CorrelationID, err = optionalString(root, "correlation_id", maxLenID); err != nil {
		return nil, err
	}
	if rec.UserAgent, err = optionalString(root, "user_agent", maxLenUserAgent); err != nil {
		return nil, err
	}
	if rec.IP, err = parseIP(root); err != nil {
		return nil, err
	}

	rawMetadata, err := p.extractMetadata(root)
	if err != nil {
		return nil, err
	}

	redacted := 0
	redacted += p.redactColumn(&rec.UserAgent)
	redacted += p.redactColumn(&rec.ActorID)
	redacted += p.redactColumn(&rec.ResourceID)
	redacted += p.redactColumn(&rec.RequestID)
	redacted += p.redactColumn(&rec.CorrelationID)
	redacted += p.redactor.Map(rawMetadata)

	extensions := map[string]any{}
	for key, value := range root {
		if _, isColumn := columnFields[key]; isColumn {
			continue
		}
		if _, isReserved := reservedFields[key]; isReserved {
			continue
		}
		extensions[key] = value
	}
	redacted += p.redactor.Map(extensions)

	// Allowlist global: o que não está nela é persistido em `extensions`,
	// sob a mesma chave `metadata`, preservando a origem (seção 6.1).
	metadata := map[string]any{}
	overflow := map[string]any{}
	for key, value := range rawMetadata {
		if _, ok := p.allowlist[key]; ok {
			metadata[key] = value
		} else {
			overflow[key] = value
		}
	}
	if len(overflow) > 0 {
		extensions["metadata"] = overflow
	}

	if rec.Metadata, err = encodeJSONB(metadata); err != nil {
		return nil, err
	}
	if rec.Extensions, err = encodeJSONB(extensions); err != nil {
		return nil, err
	}
	rec.RedactedFields = redacted

	return rec, nil
}

func (p *Parser) parseOccurredAt(root map[string]any, now time.Time) (time.Time, error) {
	raw, ok := root["occurred_at"]
	if !ok || raw == nil {
		return time.Time{}, newError("missing_field", "campo obrigatório ausente: occurred_at")
	}
	text, ok := raw.(string)
	if !ok {
		return time.Time{}, newError("invalid_field", "occurred_at deve ser string RFC3339")
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return time.Time{}, newError("invalid_field", "occurred_at deve ser RFC3339 (ex.: 2026-09-07T12:00:00Z)")
	}
	parsed = parsed.UTC()

	if p.maxClockSkew > 0 && parsed.After(now.Add(p.maxClockSkew)) {
		return time.Time{}, newError("occurred_at_in_future",
			fmt.Sprintf("occurred_at está mais de %s no futuro", p.maxClockSkew))
	}
	// Eventos anteriores à janela quente cairiam em uma partição já exportada
	// e removida pelo job `migrate`, então não podem ser aceitos.
	if p.hotRetention > 0 && parsed.Before(now.Add(-p.hotRetention)) {
		return time.Time{}, newError("occurred_at_too_old",
			fmt.Sprintf("occurred_at é anterior à janela quente de %s", p.hotRetention))
	}
	return parsed, nil
}

func (p *Parser) extractMetadata(root map[string]any) (map[string]any, error) {
	raw, ok := root["metadata"]
	if !ok || raw == nil {
		return map[string]any{}, nil
	}
	metadata, ok := raw.(map[string]any)
	if !ok {
		return nil, newError("invalid_field", "metadata deve ser um objeto JSON")
	}
	if p.metadataMaxKeys > 0 && len(metadata) > p.metadataMaxKeys {
		return nil, newError("metadata_too_many_keys",
			fmt.Sprintf("metadata tem %d chaves; o limite é %d", len(metadata), p.metadataMaxKeys))
	}
	if p.metadataMaxBytes > 0 {
		encoded, err := CanonicalJSON(metadata)
		if err != nil {
			return nil, newError("invalid_field", "metadata não é canonicalizável: "+err.Error())
		}
		if len(encoded) > p.metadataMaxBytes {
			return nil, newError("metadata_too_large",
				fmt.Sprintf("metadata tem %d bytes; o limite é %d", len(encoded), p.metadataMaxBytes))
		}
	}
	return metadata, nil
}

func (p *Parser) redactColumn(field **string) int {
	if *field == nil {
		return 0
	}
	value, n := p.redactor.String(**field)
	if n > 0 {
		*field = &value
	}
	return n
}

func decodeObject(body []byte) (map[string]any, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, newError("empty_body", "body vazio")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()

	var root any
	if err := decoder.Decode(&root); err != nil {
		return nil, newError("invalid_json", "body não é JSON válido")
	}
	if decoder.More() {
		return nil, newError("invalid_json", "body contém conteúdo após o objeto JSON")
	}
	obj, ok := root.(map[string]any)
	if !ok {
		return nil, newError("invalid_json", "body deve ser um objeto JSON")
	}
	return obj, nil
}

func requiredString(root map[string]any, key string, max int) (string, error) {
	raw, ok := root[key]
	if !ok || raw == nil {
		return "", newError("missing_field", "campo obrigatório ausente: "+key)
	}
	text, ok := raw.(string)
	if !ok {
		return "", newError("invalid_field", key+" deve ser string")
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return "", newError("missing_field", "campo obrigatório vazio: "+key)
	}
	return truncate(text, max), nil
}

func optionalString(root map[string]any, key string, max int) (*string, error) {
	raw, ok := root[key]
	if !ok || raw == nil {
		return nil, nil
	}
	text, ok := raw.(string)
	if !ok {
		return nil, newError("invalid_field", key+" deve ser string")
	}
	if text == "" {
		return nil, nil
	}
	out := truncate(text, max)
	return &out, nil
}

// nestedPair lê objetos no formato {"type": ..., "id": ...} (actor, resource).
func nestedPair(root map[string]any, key string) (*string, *string, error) {
	raw, ok := root[key]
	if !ok || raw == nil {
		return nil, nil, nil
	}
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, nil, newError("invalid_field", key+" deve ser um objeto com type e id")
	}
	typeValue, err := optionalString(obj, "type", maxLenType)
	if err != nil {
		return nil, nil, newError("invalid_field", key+".type deve ser string")
	}
	idValue, err := optionalString(obj, "id", maxLenID)
	if err != nil {
		return nil, nil, newError("invalid_field", key+".id deve ser string")
	}
	return typeValue, idValue, nil
}

func parseIP(root map[string]any) (*netip.Addr, error) {
	raw, ok := root["ip"]
	if !ok || raw == nil {
		return nil, nil
	}
	text, ok := raw.(string)
	if !ok {
		return nil, newError("invalid_field", "ip deve ser string")
	}
	if text == "" {
		return nil, nil
	}
	addr, err := netip.ParseAddr(text)
	if err != nil {
		return nil, newError("invalid_field", "ip não é um endereço IPv4/IPv6 válido")
	}
	return &addr, nil
}

func encodeJSONB(v map[string]any) ([]byte, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil, newError("invalid_body", "não foi possível serializar o evento: "+err.Error())
	}
	return encoded, nil
}

func truncate(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	// Corta em fronteira de rune para não gerar UTF-8 inválido no PG.
	cut := max
	for cut > 0 && !isRuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
