package api

import (
	"testing"
	"time"
)

func TestProjectLimiterEnforcesBurstAndRefill(t *testing.T) {
	current := time.Now()
	limiter := NewProjectLimiter(1, 2)
	limiter.now = func() time.Time { return current }

	if !limiter.Allow("billing") || !limiter.Allow("billing") {
		t.Fatal("o burst inicial de 2 deveria passar")
	}
	if limiter.Allow("billing") {
		t.Error("terceira request deveria estourar o limite")
	}

	current = current.Add(time.Second)
	if !limiter.Allow("billing") {
		t.Error("após 1s a 1 rps deveria haver um token novo")
	}
}

func TestProjectLimiterIsolatesProjects(t *testing.T) {
	current := time.Now()
	limiter := NewProjectLimiter(1, 1)
	limiter.now = func() time.Time { return current }

	if !limiter.Allow("billing") {
		t.Fatal("primeira request de billing deveria passar")
	}
	if limiter.Allow("billing") {
		t.Fatal("segunda request de billing deveria ser limitada")
	}
	if !limiter.Allow("checkout") {
		t.Error("checkout tem o próprio bucket e não deveria ser afetado")
	}
}

func TestProjectLimiterDisabledWhenRPSIsZero(t *testing.T) {
	limiter := NewProjectLimiter(0, 0)
	for range 100 {
		if !limiter.Allow("billing") {
			t.Fatal("rps=0 desliga o limite")
		}
	}
}

func TestProjectLimiterEvictsIdleBuckets(t *testing.T) {
	current := time.Now()
	limiter := NewProjectLimiter(1, 1)
	limiter.now = func() time.Time { return current }

	limiter.Allow("antigo")
	current = current.Add(2 * limiter.idle)
	limiter.Allow("novo")

	limiter.mu.Lock()
	_, stillThere := limiter.buckets["antigo"]
	total := len(limiter.buckets)
	limiter.mu.Unlock()

	if stillThere {
		t.Error("bucket ocioso deveria ter sido descartado")
	}
	if total != 1 {
		t.Errorf("len(buckets) = %d, quero 1", total)
	}
}
