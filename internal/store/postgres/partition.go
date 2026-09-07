package postgres

import (
	"context"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Partition descreve uma partição semanal de `events` (decisão 12.5).
type Partition struct {
	Name string
	From time.Time
	To   time.Time
}

// PartitionFor devolve a partição semanal ISO que contém t.
// O range é [segunda 00:00 UTC, segunda seguinte 00:00 UTC).
func PartitionFor(t time.Time) Partition {
	utc := t.UTC()
	day := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)

	// time.Weekday() é 0=domingo; a semana ISO começa na segunda.
	offset := (int(day.Weekday()) + 6) % 7
	from := day.AddDate(0, 0, -offset)
	to := from.AddDate(0, 0, 7)

	isoYear, isoWeek := from.ISOWeek()
	return Partition{
		Name: fmt.Sprintf("events_%04dw%02d", isoYear, isoWeek),
		From: from,
		To:   to,
	}
}

// PartitionManager cria partições sob demanda e lembra as que já existem,
// para manter o INSERT do hot path em um único roundtrip.
type PartitionManager struct {
	pool *pgxpool.Pool

	mu    sync.RWMutex
	known map[string]struct{}
}

// NewPartitionManager constrói o gerenciador de partições.
func NewPartitionManager(pool *pgxpool.Pool) *PartitionManager {
	return &PartitionManager{pool: pool, known: map[string]struct{}{}}
}

// Ensure garante que a partição que cobre t existe.
func (m *PartitionManager) Ensure(ctx context.Context, t time.Time) error {
	part := PartitionFor(t)

	m.mu.RLock()
	_, ok := m.known[part.Name]
	m.mu.RUnlock()
	if ok {
		return nil
	}

	if err := m.create(ctx, part); err != nil {
		return err
	}

	m.mu.Lock()
	m.known[part.Name] = struct{}{}
	m.mu.Unlock()
	return nil
}

// EnsureWindow pré-cria as partições da semana atual e das próximas,
// evitando que o primeiro evento de uma semana pague o DDL.
func (m *PartitionManager) EnsureWindow(ctx context.Context, now time.Time, weeksAhead int) error {
	for i := 0; i <= weeksAhead; i++ {
		if err := m.Ensure(ctx, now.AddDate(0, 0, 7*i)); err != nil {
			return err
		}
	}
	return nil
}

func (m *PartitionManager) create(ctx context.Context, part Partition) error {
	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	// CREATE TABLE ... PARTITION OF pega ACCESS EXCLUSIVE no pai; o advisory
	// lock evita que réplicas concorrentes se atropelem no mesmo DDL.
	lockID := advisoryLockID(part.Name)
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		return fmt.Errorf("advisory lock %s: %w", part.Name, err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", lockID)
	}()

	ddl := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s PARTITION OF events FOR VALUES FROM ('%s') TO ('%s')`,
		quoteIdent(part.Name),
		part.From.Format(time.RFC3339),
		part.To.Format(time.RFC3339),
	)
	if _, err := conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("criar partição %s: %w", part.Name, err)
	}
	return nil
}

var boundRe = regexp.MustCompile(`FOR VALUES FROM \('([^']+)'\) TO \('([^']+)'\)`)

var boundLayouts = []string{
	"2006-01-02 15:04:05.999999-07",
	"2006-01-02 15:04:05.999999-07:00",
	"2006-01-02 15:04:05-07",
	"2006-01-02 15:04:05",
}

// parsePartitionBound extrai [from, to) da expressão devolvida por
// pg_get_expr(relpartbound). A sessão roda em UTC (ver Open).
func parsePartitionBound(expr string) (time.Time, time.Time, error) {
	match := boundRe.FindStringSubmatch(expr)
	if match == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("bound de partição não reconhecido: %q", expr)
	}
	from, err := parseBoundTime(match[1])
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	to, err := parseBoundTime(match[2])
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return from, to, nil
}

func parseBoundTime(raw string) (time.Time, error) {
	for _, layout := range boundLayouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("timestamp de partição não reconhecido: %q", raw)
}

func advisoryLockID(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return int64(h.Sum64() >> 1)
}

// quoteIdent escapa um identificador para interpolação em DDL, que não
// aceita parâmetros vinculados.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
