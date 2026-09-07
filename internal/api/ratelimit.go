package api

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ProjectLimiter é um token bucket por `project_id` (seção 8).
// Buckets ociosos são descartados para o mapa não crescer com o número de
// projetos que já pararam de enviar.
type ProjectLimiter struct {
	rps   rate.Limit
	burst int
	idle  time.Duration
	now   func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// NewProjectLimiter constrói o limitador. rps <= 0 desliga o limite.
func NewProjectLimiter(rps float64, burst int) *ProjectLimiter {
	if burst <= 0 {
		burst = 1
	}
	return &ProjectLimiter{
		rps:     rate.Limit(rps),
		burst:   burst,
		idle:    10 * time.Minute,
		now:     time.Now,
		buckets: map[string]*bucket{},
	}
}

// Allow consome um token do projeto.
func (l *ProjectLimiter) Allow(projectID string) bool {
	if l == nil || l.rps <= 0 {
		return true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b, ok := l.buckets[projectID]
	if !ok {
		// A limpeza acontece antes da inserção: um bucket recém-criado ainda
		// não tem lastSeen e seria descartado como ocioso.
		l.evictIdleLocked(now)
		b = &bucket{limiter: rate.NewLimiter(l.rps, l.burst)}
		l.buckets[projectID] = b
	}
	b.lastSeen = now

	return b.limiter.AllowN(now, 1)
}

func (l *ProjectLimiter) evictIdleLocked(now time.Time) {
	for id, b := range l.buckets {
		if now.Sub(b.lastSeen) > l.idle {
			delete(l.buckets, id)
		}
	}
}
