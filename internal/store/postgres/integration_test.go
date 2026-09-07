package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/IsaacDSC/audit.git/internal/config"
	"github.com/IsaacDSC/audit.git/internal/event"
	"github.com/IsaacDSC/audit.git/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Estes testes exercitam o SQL de verdade (particionamento, jsonb, inet,
// idempotência, DETACH/DROP). Sem AUDIT_TEST_DATABASE_URL eles são pulados,
// para que `go test ./...` continue rodando em máquinas sem PostgreSQL.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("AUDIT_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AUDIT_TEST_DATABASE_URL não definido; pulando teste de integração")
	}

	ctx := context.Background()
	pool, err := postgres.Open(ctx, config.Database{
		URL:              url,
		MaxConns:         4,
		MinConns:         1,
		MaxConnLifetime:  time.Hour,
		ConnectTimeout:   5 * time.Second,
		StatementTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	// Cada teste começa de um schema limpo para não herdar partições nem
	// estado de arquivamento de outro caso.
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS cold_archive_batches CASCADE",
		"DROP TABLE IF EXISTS cold_archive_jobs CASCADE",
		"DROP TABLE IF EXISTS events CASCADE",
		"DROP TABLE IF EXISTS services CASCADE",
		"DROP TABLE IF EXISTS schema_migrations CASCADE",
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("limpar schema (%s): %v", stmt, err)
		}
	}

	if err := postgres.Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	return pool
}

func newRecord(t *testing.T, projectID, action string, occurredAt time.Time) *event.Record {
	t.Helper()

	ip := netip.MustParseAddr("203.0.113.10")
	outcome, actorType := "success", "user"
	return &event.Record{
		ID:             uuid.New(),
		ProjectID:      projectID,
		IdempotencyKey: fmt.Sprintf("key-%s-%s", projectID, action),
		SchemaVersion:  1,
		Action:         action,
		Outcome:        &outcome,
		ActorType:      &actorType,
		IP:             &ip,
		OccurredAt:     occurredAt,
		ReceivedAt:     occurredAt.Add(time.Second),
		Metadata:       []byte(`{"currency":"BRL","amount_cents":1500}`),
		Extensions:     []byte(`{"tenant":"acme"}`),
	}
}

func TestIntegrationMigrateIsIdempotent(t *testing.T) {
	pool := testPool(t)
	if err := postgres.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("Migrate() rodado duas vezes: %v", err)
	}
}

func TestIntegrationInsertPersistsTypedColumnsAndJSONB(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)

	partitions := postgres.NewPartitionManager(pool)
	repo := postgres.NewEventRepository(pool, partitions)

	occurredAt := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	record := newRecord(t, "billing", "order.created", occurredAt)
	if err := partitions.Ensure(ctx, occurredAt); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	result, err := repo.Insert(ctx, record)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if result.Duplicate {
		t.Error("primeiro insert não deveria ser duplicata")
	}

	var (
		projectID, action string
		ip                netip.Addr
		metadata          []byte
		extensions        []byte
		storedAt          time.Time
	)
	if err := pool.QueryRow(ctx, `
		SELECT project_id, action, ip, metadata, extensions, occurred_at
		FROM events WHERE id = $1`, result.ID,
	).Scan(&projectID, &action, &ip, &metadata, &extensions, &storedAt); err != nil {
		t.Fatalf("ler evento: %v", err)
	}

	if projectID != "billing" || action != "order.created" {
		t.Errorf("colunas = (%q, %q)", projectID, action)
	}
	if ip.String() != "203.0.113.10" {
		t.Errorf("ip = %v", ip)
	}
	if !storedAt.UTC().Equal(occurredAt) {
		t.Errorf("occurred_at = %v, quero %v", storedAt.UTC(), occurredAt)
	}

	var decoded map[string]any
	if err := json.Unmarshal(metadata, &decoded); err != nil {
		t.Fatalf("metadata não é jsonb válido: %v (%s)", err, metadata)
	}
	if decoded["currency"] != "BRL" {
		t.Errorf("metadata.currency = %v", decoded["currency"])
	}

	// jsonb precisa ser consultável, não um blob opaco.
	var currency string
	if err := pool.QueryRow(ctx,
		`SELECT metadata->>'currency' FROM events WHERE id = $1`, result.ID).Scan(&currency); err != nil {
		t.Fatalf("consultar metadata: %v", err)
	}
	if currency != "BRL" {
		t.Errorf("metadata->>'currency' = %q", currency)
	}
}

// Seção 4.1: retry com a mesma chave devolve o id original, sem duplicar.
func TestIntegrationInsertIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)

	partitions := postgres.NewPartitionManager(pool)
	repo := postgres.NewEventRepository(pool, partitions)

	occurredAt := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	first := newRecord(t, "billing", "order.created", occurredAt)

	original, err := repo.Insert(ctx, first)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	// Mesmo project_id + idempotency_key + occurred_at, id novo: é um retry.
	retry := newRecord(t, "billing", "order.created", occurredAt)
	second, err := repo.Insert(ctx, retry)
	if err != nil {
		t.Fatalf("Insert() (retry) error = %v", err)
	}

	if !second.Duplicate {
		t.Error("retry deveria ser marcado como duplicata")
	}
	if second.ID != original.ID {
		t.Errorf("retry devolveu id %s, quero o original %s", second.ID, original.ID)
	}

	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&count); err != nil {
		t.Fatalf("contar eventos: %v", err)
	}
	if count != 1 {
		t.Errorf("count(*) = %d, quero 1", count)
	}

	// Outro projeto com a mesma chave é um evento distinto.
	other := newRecord(t, "checkout", "order.created", occurredAt)
	other.IdempotencyKey = first.IdempotencyKey
	if _, err := repo.Insert(ctx, other); err != nil {
		t.Fatalf("Insert() (outro projeto) error = %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&count); err != nil {
		t.Fatalf("contar eventos: %v", err)
	}
	if count != 2 {
		t.Errorf("count(*) = %d, quero 2", count)
	}
}

// O INSERT precisa se recuperar sozinho quando a partição ainda não existe.
func TestIntegrationInsertCreatesMissingPartition(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)

	repo := postgres.NewEventRepository(pool, postgres.NewPartitionManager(pool))
	occurredAt := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

	if _, err := repo.Insert(ctx, newRecord(t, "billing", "order.created", occurredAt)); err != nil {
		t.Fatalf("Insert() sem partição pré-criada: %v", err)
	}

	expected := postgres.PartitionFor(occurredAt).Name
	var exists bool
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1)", expected).Scan(&exists); err != nil {
		t.Fatalf("checar partição: %v", err)
	}
	if !exists {
		t.Errorf("partição %s não foi criada", expected)
	}
}

func TestIntegrationEventsAreRoutedToWeeklyPartitions(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)

	repo := postgres.NewEventRepository(pool, postgres.NewPartitionManager(pool))

	week37 := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	week38 := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	if _, err := repo.Insert(ctx, newRecord(t, "billing", "a.b", week37)); err != nil {
		t.Fatalf("Insert() semana 37: %v", err)
	}
	if _, err := repo.Insert(ctx, newRecord(t, "billing", "c.d", week38)); err != nil {
		t.Fatalf("Insert() semana 38: %v", err)
	}

	for name, want := range map[string]int{"events_2026w37": 1, "events_2026w38": 1} {
		var count int
		if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %q", name)).Scan(&count); err != nil {
			t.Fatalf("contar %s: %v", name, err)
		}
		if count != want {
			t.Errorf("%s tem %d linhas, quero %d", name, count, want)
		}
	}
}

func TestIntegrationArchiveLifecycle(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)

	partitions := postgres.NewPartitionManager(pool)
	events := postgres.NewEventRepository(pool, partitions)
	archive := postgres.NewArchiveRepository(pool)

	old := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	for _, spec := range []struct {
		project string
		action  string
		at      time.Time
	}{
		{"billing", "a.b", old},
		{"billing", "c.d", old.Add(time.Hour)},
		{"checkout", "e.f", old.Add(2 * time.Hour)},
		{"billing", "g.h", recent},
	} {
		if _, err := events.Insert(ctx, newRecord(t, spec.project, spec.action, spec.at)); err != nil {
			t.Fatalf("Insert(): %v", err)
		}
	}

	oldPartition := postgres.PartitionFor(old)

	// Só a partição antiga saiu da janela quente.
	cutoff := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	eligible, err := archive.EligiblePartitions(ctx, cutoff, 10)
	if err != nil {
		t.Fatalf("EligiblePartitions() error = %v", err)
	}
	if len(eligible) != 1 || eligible[0].Name != oldPartition.Name {
		t.Fatalf("eligible = %+v, quero só %s", eligible, oldPartition.Name)
	}
	if !eligible[0].From.Equal(oldPartition.From) || !eligible[0].To.Equal(oldPartition.To) {
		t.Errorf("bounds lidos do catálogo = [%v, %v), quero [%v, %v)",
			eligible[0].From, eligible[0].To, oldPartition.From, oldPartition.To)
	}

	if got, err := archive.CountPartition(ctx, oldPartition.Name); err != nil || got != 3 {
		t.Fatalf("CountPartition() = (%d, %v), quero 3", got, err)
	}

	projects, err := archive.Projects(ctx, oldPartition.Name)
	if err != nil {
		t.Fatalf("Projects() error = %v", err)
	}
	if len(projects) != 2 || projects[0] != "billing" || projects[1] != "checkout" {
		t.Errorf("Projects() = %v, quero [billing checkout]", projects)
	}

	// Paginação keyset: dois lotes de 1 linha cobrem billing sem repetir.
	rows, cursor, err := archive.ReadRows(ctx, oldPartition.Name, "billing", postgres.Cursor{}, 1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("ReadRows() página 1 = (%d linhas, %v)", len(rows), err)
	}
	firstID := rows[0].ID
	if rows[0].Metadata == "" || rows[0].IP != "203.0.113.10" {
		t.Errorf("linha lida = %+v", rows[0])
	}

	rows, _, err = archive.ReadRows(ctx, oldPartition.Name, "billing", cursor, 10)
	if err != nil {
		t.Fatalf("ReadRows() página 2 error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("página 2 tem %d linhas, quero 1", len(rows))
	}
	if rows[0].ID == firstID {
		t.Error("a segunda página repetiu a linha da primeira")
	}

	// Estado do job e dos lotes.
	job, err := archive.UpsertJob(ctx, oldPartition, "audit/production")
	if err != nil {
		t.Fatalf("UpsertJob() error = %v", err)
	}
	again, err := archive.UpsertJob(ctx, oldPartition, "audit/production")
	if err != nil {
		t.Fatalf("UpsertJob() (retomada) error = %v", err)
	}
	if again.ID != job.ID {
		t.Errorf("UpsertJob() criou job novo (%s != %s) em vez de retomar", again.ID, job.ID)
	}

	cursorID := uuid.New()
	occurredTo := old.Add(time.Hour)
	batch := postgres.Batch{
		JobID:        job.ID,
		BatchSeq:     0,
		Status:       postgres.BatchVerified,
		Key:          "audit/production/project_id=billing/year=2026/week=02/events-x-00000.parquet",
		RowCount:     2,
		Bytes:        1024,
		SHA256:       "abc",
		OccurredAtTo: &occurredTo,
		CursorID:     &cursorID,
	}
	if err := archive.SaveBatch(ctx, batch, "", ""); err != nil {
		t.Fatalf("SaveBatch() error = %v", err)
	}
	// Regravar o mesmo batch_seq atualiza em vez de duplicar.
	batch.Bytes = 2048
	if err := archive.SaveBatch(ctx, batch, "", ""); err != nil {
		t.Fatalf("SaveBatch() (update) error = %v", err)
	}

	stored, err := archive.Batches(ctx, job.ID)
	if err != nil {
		t.Fatalf("Batches() error = %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("len(batches) = %d, quero 1", len(stored))
	}
	if got := stored[0]; got.Bytes != 2048 || got.CursorID == nil || *got.CursorID != cursorID {
		t.Errorf("lote persistido = %+v", got)
	}

	if err := archive.FailJob(ctx, job.ID, "blob_upload", "PutObject timeout"); err != nil {
		t.Fatalf("FailJob() error = %v", err)
	}
	if err := archive.SetJobTotals(ctx, job.ID, 3, 2048); err != nil {
		t.Fatalf("SetJobTotals() error = %v", err)
	}
	if err := archive.SetJobStatus(ctx, job.ID, postgres.JobExported); err != nil {
		t.Fatalf("SetJobStatus() error = %v", err)
	}

	// DROP remove a partição sem afetar a que ainda está quente.
	if err := archive.DropPartition(ctx, oldPartition.Name); err != nil {
		t.Fatalf("DropPartition() error = %v", err)
	}
	if err := archive.SetJobStatus(ctx, job.ID, postgres.JobDropped); err != nil {
		t.Fatalf("SetJobStatus(dropped) error = %v", err)
	}

	var remaining int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&remaining); err != nil {
		t.Fatalf("contar eventos: %v", err)
	}
	if remaining != 1 {
		t.Errorf("sobrou %d evento(s), quero 1 (só a partição quente)", remaining)
	}

	// Partição já dropped não volta como elegível.
	eligible, err = archive.EligiblePartitions(ctx, cutoff, 10)
	if err != nil {
		t.Fatalf("EligiblePartitions() error = %v", err)
	}
	if len(eligible) != 0 {
		t.Errorf("eligible = %+v, quero vazio", eligible)
	}
}

func TestIntegrationCredentialRepository(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)

	if _, err := pool.Exec(ctx, `
		INSERT INTO services (project_id, secret_hash, disabled) VALUES
			('billing', '$2a$04$hash', false),
			('legado',  '$2a$04$hash', true)`); err != nil {
		t.Fatalf("inserir credenciais: %v", err)
	}

	repo := postgres.NewCredentialRepository(pool)

	hash, ok, err := repo.Lookup(ctx, "billing")
	if err != nil || !ok || hash != "$2a$04$hash" {
		t.Errorf("Lookup(billing) = (%q, %v, %v)", hash, ok, err)
	}
	if _, ok, _ := repo.Lookup(ctx, "legado"); ok {
		t.Error("projeto desabilitado não deveria autenticar")
	}
	if _, ok, _ := repo.Lookup(ctx, "fantasma"); ok {
		t.Error("projeto inexistente não deveria autenticar")
	}
}
