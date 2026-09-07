package event

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// CanonicalJSON serializa um valor JSON já decodificado de forma determinística:
// chaves de objeto em ordem lexicográfica e sem espaços supérfluos. É a base da
// chave de idempotência derivada (seção 4.1 / decisão 12.4).
//
// Números preservam o literal recebido (json.Number), então `1.50` e `1.5`
// produzem chaves diferentes — o cliente que quiser idempotência estável entre
// serializações distintas deve mandar o header `Idempotency-Key`.
func CanonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeCanonical(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DeriveIdempotencyKey implementa hex(sha256(project_id + "\n" + canonical_json(body))).
func DeriveIdempotencyKey(projectID string, body any) (string, error) {
	canonical, err := CanonicalJSON(body)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(projectID))
	h.Write([]byte("\n"))
	h.Write(canonical)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch typed := v.(type) {
	case nil:
		buf.WriteString("null")

	case bool:
		if typed {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}

	case json.Number:
		buf.WriteString(typed.String())

	case float64:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return err
		}
		buf.Write(encoded)

	case string:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return err
		}
		buf.Write(encoded)

	case []any:
		buf.WriteByte('[')
		for i, item := range typed {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')

	case map[string]any:
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			encoded, err := json.Marshal(k)
			if err != nil {
				return err
			}
			buf.Write(encoded)
			buf.WriteByte(':')
			if err := writeCanonical(buf, typed[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')

	default:
		return fmt.Errorf("tipo não canonicalizável: %T", v)
	}
	return nil
}
