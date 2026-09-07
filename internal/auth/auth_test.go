package auth_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IsaacDSC/audit.git/internal/auth"
	"golang.org/x/crypto/bcrypt"
)

func hash(t *testing.T, secret string) string {
	t.Helper()
	// Custo mínimo: o teste valida a lógica, não a força do bcrypt.
	out, err := bcrypt.GenerateFromPassword([]byte(secret), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	return string(out)
}

type countingStore struct {
	mu     sync.Mutex
	hash   string
	calls  int
	err    error
	absent bool
}

func (s *countingStore) Lookup(context.Context, string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return "", false, s.err
	}
	if s.absent {
		return "", false, nil
	}
	return s.hash, true, nil
}

func (s *countingStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestNewMemoryStoreParsesCredentials(t *testing.T) {
	billing := hash(t, "s1")
	checkout := hash(t, "s2")

	store, err := auth.NewMemoryStore("billing:" + billing + ", checkout:" + checkout)
	if err != nil {
		t.Fatalf("NewMemoryStore() error = %v", err)
	}

	got, ok, err := store.Lookup(context.Background(), "checkout")
	if err != nil || !ok || got != checkout {
		t.Errorf("Lookup(checkout) = (%q, %v, %v)", got, ok, err)
	}
	if _, ok, _ := store.Lookup(context.Background(), "desconhecido"); ok {
		t.Error("projeto desconhecido não deveria existir")
	}
}

func TestNewMemoryStoreRejectsMalformedInput(t *testing.T) {
	tests := map[string]string{
		"vazio":            "",
		"sem hash":         "billing",
		"hash não bcrypt":  "billing:plaintext",
		"project_id vazio": ":" + hash(t, "s"),
	}

	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := auth.NewMemoryStore(raw); err == nil {
				t.Errorf("NewMemoryStore(%q) deveria falhar", raw)
			}
		})
	}
}

func TestAuthenticateAcceptsValidCredential(t *testing.T) {
	store := &countingStore{hash: hash(t, "s3cr3t")}
	authenticator := auth.NewAuthenticator(store, auth.Options{})

	if err := authenticator.Authenticate(context.Background(), "billing", "s3cr3t"); err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
}

func TestAuthenticateRejectsWrongSecretAndUnknownProject(t *testing.T) {
	valid := &countingStore{hash: hash(t, "s3cr3t")}
	if err := auth.NewAuthenticator(valid, auth.Options{}).
		Authenticate(context.Background(), "billing", "errado"); !errors.Is(err, auth.ErrUnauthorized) {
		t.Errorf("segredo errado: err = %v, quero ErrUnauthorized", err)
	}

	missing := &countingStore{absent: true}
	if err := auth.NewAuthenticator(missing, auth.Options{}).
		Authenticate(context.Background(), "fantasma", "x"); !errors.Is(err, auth.ErrUnauthorized) {
		t.Errorf("projeto inexistente: err = %v, quero ErrUnauthorized", err)
	}
}

func TestAuthenticateRejectsEmptyCredentialsWithoutHittingStore(t *testing.T) {
	store := &countingStore{hash: hash(t, "s")}
	authenticator := auth.NewAuthenticator(store, auth.Options{})

	if err := authenticator.Authenticate(context.Background(), "", "x"); !errors.Is(err, auth.ErrUnauthorized) {
		t.Errorf("err = %v, quero ErrUnauthorized", err)
	}
	if store.count() != 0 {
		t.Errorf("store consultado %d vez(es) para credencial vazia", store.count())
	}
}

// Falha do store não é culpa do cliente: vira 503, não 401.
func TestAuthenticateSurfacesStoreFailure(t *testing.T) {
	store := &countingStore{err: errors.New("conexão recusada")}
	err := auth.NewAuthenticator(store, auth.Options{}).
		Authenticate(context.Background(), "billing", "x")

	if err == nil || errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("err = %v, quero erro distinto de ErrUnauthorized", err)
	}
	if !strings.Contains(err.Error(), "conexão recusada") {
		t.Errorf("erro não preserva a causa: %v", err)
	}
}

// O cache existe para o bcrypt não consumir o orçamento de latência (10.1).
func TestAuthenticateCachesVerification(t *testing.T) {
	store := &countingStore{hash: hash(t, "s3cr3t")}
	authenticator := auth.NewAuthenticator(store, auth.Options{TTL: time.Minute})

	for range 5 {
		if err := authenticator.Authenticate(context.Background(), "billing", "s3cr3t"); err != nil {
			t.Fatalf("Authenticate() error = %v", err)
		}
	}

	if store.count() != 1 {
		t.Errorf("store consultado %d vezes, quero 1", store.count())
	}
}

func TestAuthenticateCacheExpires(t *testing.T) {
	store := &countingStore{hash: hash(t, "s3cr3t")}
	current := time.Now()
	authenticator := auth.NewAuthenticator(store, auth.Options{
		TTL: time.Minute,
		Now: func() time.Time { return current },
	})

	if err := authenticator.Authenticate(context.Background(), "billing", "s3cr3t"); err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	current = current.Add(2 * time.Minute)
	if err := authenticator.Authenticate(context.Background(), "billing", "s3cr3t"); err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}

	if store.count() != 2 {
		t.Errorf("store consultado %d vezes, quero 2 (cache expirado)", store.count())
	}
}

// Cache negativo com TTL curto evita que um cliente com credencial errada
// martele o store a cada request.
func TestAuthenticateCachesRejection(t *testing.T) {
	store := &countingStore{hash: hash(t, "s3cr3t")}
	authenticator := auth.NewAuthenticator(store, auth.Options{NegativeTTL: time.Minute})

	for range 3 {
		if err := authenticator.Authenticate(context.Background(), "billing", "errado"); !errors.Is(err, auth.ErrUnauthorized) {
			t.Fatalf("err = %v, quero ErrUnauthorized", err)
		}
	}

	if store.count() != 1 {
		t.Errorf("store consultado %d vezes, quero 1", store.count())
	}
}

func TestAuthenticateIsConcurrencySafe(t *testing.T) {
	store := &countingStore{hash: hash(t, "s3cr3t")}
	authenticator := auth.NewAuthenticator(store, auth.Options{TTL: time.Minute, MaxEntries: 4})

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			secret := "s3cr3t"
			if i%2 == 0 {
				secret = "errado"
			}
			_ = authenticator.Authenticate(context.Background(), "billing", secret)
		}(i)
	}
	wg.Wait()
}

func TestProjectIDContextRoundTrip(t *testing.T) {
	ctx := auth.WithProjectID(context.Background(), "billing")
	if got := auth.ProjectIDFrom(ctx); got != "billing" {
		t.Errorf("ProjectIDFrom() = %q", got)
	}
	if got := auth.ProjectIDFrom(context.Background()); got != "" {
		t.Errorf("ProjectIDFrom(vazio) = %q", got)
	}
}
