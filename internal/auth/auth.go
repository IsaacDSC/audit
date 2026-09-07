// Package auth implementa o Basic Auth da seção 3 da spec 001: o username é
// o `project_id` e o password é validado contra um hash bcrypt.
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// ErrUnauthorized é a única falha exposta ao cliente: credencial inválida,
// projeto inexistente ou desabilitado são indistinguíveis de fora.
var ErrUnauthorized = errors.New("credenciais inválidas")

// Store devolve o hash bcrypt do segredo de um projeto.
type Store interface {
	Lookup(ctx context.Context, projectID string) (secretHash string, ok bool, err error)
}

type projectKey struct{}

// WithProjectID anexa o projeto autenticado ao contexto.
func WithProjectID(ctx context.Context, projectID string) context.Context {
	return context.WithValue(ctx, projectKey{}, projectID)
}

// ProjectIDFrom recupera o projeto autenticado; vazio se não houver.
func ProjectIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(projectKey{}).(string)
	return id
}

// MemoryStore mantém credenciais vindas de env/config (seção 3, cadastro v1).
type MemoryStore struct {
	credentials map[string]string
}

// NewMemoryStore lê o formato "project_id:hash,project_id2:hash".
// O hash é bcrypt; o segredo em claro nunca é aceito nem armazenado.
func NewMemoryStore(raw string) (*MemoryStore, error) {
	credentials := map[string]string{}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		projectID, hash, ok := strings.Cut(entry, ":")
		projectID, hash = strings.TrimSpace(projectID), strings.TrimSpace(hash)
		if !ok || projectID == "" || hash == "" {
			return nil, fmt.Errorf("credencial malformada: esperado project_id:bcrypt_hash")
		}
		if _, err := bcrypt.Cost([]byte(hash)); err != nil {
			return nil, fmt.Errorf("credencial de %q não é um hash bcrypt válido", projectID)
		}
		credentials[projectID] = hash
	}
	if len(credentials) == 0 {
		return nil, errors.New("nenhuma credencial configurada")
	}
	return &MemoryStore{credentials: credentials}, nil
}

// Lookup implementa Store.
func (s *MemoryStore) Lookup(_ context.Context, projectID string) (string, bool, error) {
	hash, ok := s.credentials[projectID]
	return hash, ok, nil
}

// Authenticator valida credenciais com um cache de verificações.
//
// bcrypt custa dezenas de milissegundos por comparação, o que sozinho
// estouraria o orçamento de 10 ms de auth+parse (seção 10.1). O cache guarda
// apenas o resultado da verificação, indexado pelo hash do segredo — o
// segredo em claro nunca é retido.
type Authenticator struct {
	store       Store
	ttl         time.Duration
	negativeTTL time.Duration
	maxEntries  int
	now         func() time.Time

	mu      sync.Mutex
	entries map[string]cacheEntry
}

type cacheEntry struct {
	valid     bool
	expiresAt time.Time
}

// Options ajusta o cache de verificação.
type Options struct {
	TTL         time.Duration
	NegativeTTL time.Duration
	MaxEntries  int
	Now         func() time.Time
}

// NewAuthenticator constrói o autenticador sobre um Store.
func NewAuthenticator(store Store, opts Options) *Authenticator {
	if opts.TTL <= 0 {
		opts.TTL = 5 * time.Minute
	}
	if opts.NegativeTTL <= 0 {
		opts.NegativeTTL = 5 * time.Second
	}
	if opts.MaxEntries <= 0 {
		opts.MaxEntries = 4096
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Authenticator{
		store:       store,
		ttl:         opts.TTL,
		negativeTTL: opts.NegativeTTL,
		maxEntries:  opts.MaxEntries,
		now:         opts.Now,
		entries:     make(map[string]cacheEntry),
	}
}

// Authenticate valida o par (project_id, secret).
// Devolve ErrUnauthorized para qualquer falha de credencial e um erro
// distinto apenas quando o próprio store falha (que vira 503, não 401).
func (a *Authenticator) Authenticate(ctx context.Context, projectID, secret string) error {
	if projectID == "" || secret == "" {
		return ErrUnauthorized
	}

	key := cacheKey(projectID, secret)
	if valid, found := a.lookupCache(key); found {
		if !valid {
			return ErrUnauthorized
		}
		return nil
	}

	hash, ok, err := a.store.Lookup(ctx, projectID)
	if err != nil {
		return fmt.Errorf("consultar credenciais: %w", err)
	}
	if !ok {
		a.storeCache(key, false)
		return ErrUnauthorized
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(secret)); err != nil {
		a.storeCache(key, false)
		return ErrUnauthorized
	}

	a.storeCache(key, true)
	return nil
}

func (a *Authenticator) lookupCache(key string) (valid, found bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	entry, ok := a.entries[key]
	if !ok {
		return false, false
	}
	if a.now().After(entry.expiresAt) {
		delete(a.entries, key)
		return false, false
	}
	return entry.valid, true
}

func (a *Authenticator) storeCache(key string, valid bool) {
	ttl := a.ttl
	if !valid {
		ttl = a.negativeTTL
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.entries) >= a.maxEntries {
		a.evictExpiredLocked()
		// Ainda cheio: descarta tudo. O custo é revalidar bcrypt uma vez por
		// credencial, preferível a crescer sem limite.
		if len(a.entries) >= a.maxEntries {
			a.entries = make(map[string]cacheEntry, a.maxEntries)
		}
	}
	a.entries[key] = cacheEntry{valid: valid, expiresAt: a.now().Add(ttl)}
}

func (a *Authenticator) evictExpiredLocked() {
	now := a.now()
	for key, entry := range a.entries {
		if now.After(entry.expiresAt) {
			delete(a.entries, key)
		}
	}
}

// cacheKey deriva um índice do par credencial sem guardar o segredo.
func cacheKey(projectID, secret string) string {
	h := sha256.New()
	h.Write([]byte(projectID))
	h.Write([]byte{0})
	h.Write([]byte(secret))
	return hex.EncodeToString(h.Sum(nil))
}
