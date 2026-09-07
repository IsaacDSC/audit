package event_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/IsaacDSC/audit.git/internal/event"
)

func decodeAny(t *testing.T, raw string) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.UseNumber()
	var out any
	if err := decoder.Decode(&out); err != nil {
		t.Fatalf("json inválido: %v", err)
	}
	return out
}

func TestCanonicalJSONSortsKeysRecursively(t *testing.T) {
	value := decodeAny(t, `{"b": 1, "a": {"z": [3, {"y": 1, "x": 2}], "c": null}}`)

	got, err := event.CanonicalJSON(value)
	if err != nil {
		t.Fatalf("CanonicalJSON() error = %v", err)
	}

	want := `{"a":{"c":null,"z":[3,{"x":2,"y":1}]},"b":1}`
	if string(got) != want {
		t.Errorf("CanonicalJSON() = %s\nquero               %s", got, want)
	}
}

func TestCanonicalJSONPreservesNumberLiterals(t *testing.T) {
	value := decodeAny(t, `{"amount": 1500, "rate": 0.10, "big": 123456789012345678901}`)

	got, err := event.CanonicalJSON(value)
	if err != nil {
		t.Fatalf("CanonicalJSON() error = %v", err)
	}

	want := `{"amount":1500,"big":123456789012345678901,"rate":0.10}`
	if string(got) != want {
		t.Errorf("CanonicalJSON() = %s\nquero               %s", got, want)
	}
}

func TestDeriveIdempotencyKeyIsDeterministic(t *testing.T) {
	first := decodeAny(t, `{"action":"a.b","metadata":{"x":1,"y":2}}`)
	second := decodeAny(t, `{"metadata":{"y":2,"x":1},"action":"a.b"}`)

	keyA, err := event.DeriveIdempotencyKey("billing", first)
	if err != nil {
		t.Fatalf("DeriveIdempotencyKey() error = %v", err)
	}
	keyB, err := event.DeriveIdempotencyKey("billing", second)
	if err != nil {
		t.Fatalf("DeriveIdempotencyKey() error = %v", err)
	}

	if keyA != keyB {
		t.Errorf("chaves divergem: %s != %s", keyA, keyB)
	}
	if len(keyA) != 64 {
		t.Errorf("len(chave) = %d, quero 64 (sha256 em hex)", len(keyA))
	}
}

func TestDeriveIdempotencyKeyChangesWithBody(t *testing.T) {
	base := decodeAny(t, `{"action":"a.b"}`)
	other := decodeAny(t, `{"action":"a.c"}`)

	keyA, _ := event.DeriveIdempotencyKey("billing", base)
	keyB, _ := event.DeriveIdempotencyKey("billing", other)

	if keyA == keyB {
		t.Error("bodies diferentes deveriam gerar chaves diferentes")
	}
}
