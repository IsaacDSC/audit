package coldarchive_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IsaacDSC/audit.git/internal/blobstore"
	"github.com/IsaacDSC/audit.git/internal/coldarchive"
	"github.com/IsaacDSC/audit.git/internal/config"
	"github.com/IsaacDSC/audit.git/internal/obs"
	"github.com/IsaacDSC/audit.git/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

var (
	runAt     = time.Date(2026, 12, 10, 3, 0, 0, 0, time.UTC)
	partition = postgres.Partition{
		Name: "events_2026w37",
		From: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC),
	}
)

// fakeStore substitui o PostgreSQL preservando a semântica que o job depende:
// leitura ordenada por (occurred_at, id) e estado persistente entre runs.
type fakeStore struct {
	partitions []postgres.Partition
	rows       map[string][]postgres.Row // partição → linhas ordenadas

	jobs    map[string]postgres.Job
	batches map[uuid.UUID]map[int]postgres.Batch
	dropped map[string]bool

	readCalls     []string
	dropErr       error
	countOverride *int64
}

func newFakeStore(rows []postgres.Row) *fakeStore {
	sorted := append([]postgres.Row(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].OccurredAt.Equal(sorted[j].OccurredAt) {
			return sorted[i].OccurredAt.Before(sorted[j].OccurredAt)
		}
		return sorted[i].ID < sorted[j].ID
	})

	return &fakeStore{
		partitions: []postgres.Partition{partition},
		rows:       map[string][]postgres.Row{partition.Name: sorted},
		jobs:       map[string]postgres.Job{},
		batches:    map[uuid.UUID]map[int]postgres.Batch{},
		dropped:    map[string]bool{},
	}
}

func (s *fakeStore) EligiblePartitions(context.Context, time.Time, int) ([]postgres.Partition, error) {
	var out []postgres.Partition
	for _, p := range s.partitions {
		if !s.dropped[p.Name] {
			out = append(out, p)
		}
	}
	return out, nil
}

func (s *fakeStore) UpsertJob(_ context.Context, part postgres.Partition, prefix string) (postgres.Job, error) {
	if existing, ok := s.jobs[part.Name]; ok {
		return existing, nil
	}
	job := postgres.Job{
		ID:             uuid.New(),
		PartitionName:  part.Name,
		OccurredAtFrom: part.From,
		OccurredAtTo:   part.To,
		Status:         postgres.JobPending,
		Prefix:         prefix,
	}
	s.jobs[part.Name] = job
	s.batches[job.ID] = map[int]postgres.Batch{}
	return job, nil
}

func (s *fakeStore) SetJobStatus(_ context.Context, jobID uuid.UUID, status string) error {
	for name, job := range s.jobs {
		if job.ID == jobID {
			job.Status = status
			job.ErrorCode = nil
			s.jobs[name] = job
		}
	}
	return nil
}

func (s *fakeStore) SetJobTotals(_ context.Context, jobID uuid.UUID, rows, bytes int64) error {
	for name, job := range s.jobs {
		if job.ID == jobID {
			job.RowCount, job.BytesTotal = &rows, &bytes
			s.jobs[name] = job
		}
	}
	return nil
}

func (s *fakeStore) FailJob(_ context.Context, jobID uuid.UUID, errorCode, message string) error {
	for name, job := range s.jobs {
		if job.ID == jobID {
			job.Status = postgres.JobFailed
			job.ErrorCode, job.Error = &errorCode, &message
			s.jobs[name] = job
		}
	}
	return nil
}

func (s *fakeStore) Batches(_ context.Context, jobID uuid.UUID) (map[int]postgres.Batch, error) {
	out := map[int]postgres.Batch{}
	for seq, b := range s.batches[jobID] {
		out[seq] = b
	}
	return out, nil
}

func (s *fakeStore) SaveBatch(_ context.Context, b postgres.Batch, _, _ string) error {
	if s.batches[b.JobID] == nil {
		s.batches[b.JobID] = map[int]postgres.Batch{}
	}
	if b.ID == uuid.Nil {
		b.ID = uuid.New()
	}
	s.batches[b.JobID][b.BatchSeq] = b
	return nil
}

func (s *fakeStore) CountPartition(_ context.Context, name string) (int64, error) {
	if s.countOverride != nil {
		return *s.countOverride, nil
	}
	return int64(len(s.rows[name])), nil
}

func (s *fakeStore) Projects(_ context.Context, name string) ([]string, error) {
	seen := map[string]struct{}{}
	var out []string
	for _, row := range s.rows[name] {
		if _, ok := seen[row.ProjectID]; !ok {
			seen[row.ProjectID] = struct{}{}
			out = append(out, row.ProjectID)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *fakeStore) ReadRows(
	_ context.Context, name, projectID string, cursor postgres.Cursor, limit int,
) ([]postgres.Row, postgres.Cursor, error) {
	s.readCalls = append(s.readCalls, fmt.Sprintf("%s/%s", name, projectID))

	var out []postgres.Row
	for _, row := range s.rows[name] {
		if row.ProjectID != projectID {
			continue
		}
		if cursor.Valid && !after(row, cursor) {
			continue
		}
		out = append(out, row)
		if len(out) == limit {
			break
		}
	}
	if len(out) == 0 {
		return nil, cursor, nil
	}

	last := out[len(out)-1]
	return out, postgres.Cursor{
		OccurredAt: last.OccurredAt,
		ID:         uuid.MustParse(last.ID),
		Valid:      true,
	}, nil
}

func after(row postgres.Row, cursor postgres.Cursor) bool {
	if row.OccurredAt.After(cursor.OccurredAt) {
		return true
	}
	return row.OccurredAt.Equal(cursor.OccurredAt) && row.ID > cursor.ID.String()
}

func (s *fakeStore) DropPartition(_ context.Context, name string) error {
	if s.dropErr != nil {
		return s.dropErr
	}
	s.dropped[name] = true
	return nil
}

// fakeBlobs é um object storage em memória que registra cada Put.
type fakeBlobs struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    []string
	failOn  string
	failErr error
	// corruptSize simula um objeto que chegou truncado ao destino.
	corruptSize map[string]int64
}

func newFakeBlobs() *fakeBlobs {
	return &fakeBlobs{objects: map[string][]byte{}, corruptSize: map[string]int64{}}
}

func (b *fakeBlobs) Put(_ context.Context, key string, body []byte, _ blobstore.PutOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failOn != "" && strings.Contains(key, b.failOn) {
		return b.failErr
	}
	b.objects[key] = append([]byte(nil), body...)
	b.puts = append(b.puts, key)
	return nil
}

func (b *fakeBlobs) Head(_ context.Context, key string) (blobstore.ObjectInfo, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	body, ok := b.objects[key]
	if !ok {
		return blobstore.ObjectInfo{}, false, nil
	}
	size := int64(len(body))
	if override, ok := b.corruptSize[key]; ok {
		size = override
	}
	return blobstore.ObjectInfo{Size: size, ETag: "etag"}, true, nil
}

func (b *fakeBlobs) URI(key string) string { return "memory://" + key }

func (b *fakeBlobs) keys() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.objects))
	for key := range b.objects {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func makeRows(projectID string, count int, base time.Time) []postgres.Row {
	out := make([]postgres.Row, count)
	for i := range count {
		out[i] = postgres.Row{
			ID:            uuid.NewSHA1(uuid.Nil, fmt.Appendf(nil, "%s-%d", projectID, i)).String(),
			ProjectID:     projectID,
			SchemaVersion: 1,
			Action:        "order.created",
			Outcome:       "success",
			ActorType:     "user",
			ActorID:       fmt.Sprintf("usr_%d", i),
			OccurredAt:    base.Add(time.Duration(i) * time.Minute),
			ReceivedAt:    base.Add(time.Duration(i)*time.Minute + time.Second),
			Metadata:      `{"currency":"BRL"}`,
			Extensions:    `{}`,
		}
	}
	return out
}

func newJob(t *testing.T, store coldarchive.Store, blobs blobstore.BlobStore, tweak func(*config.Config)) *coldarchive.Job {
	t.Helper()

	cfg := config.Config{
		Env:         "production",
		ColdStorage: config.ColdStorage{Prefix: "audit"},
		Archive: config.Archive{
			HotRetention:        90 * 24 * time.Hour,
			BatchRows:           2,
			MaxPartitionsPerRun: 8,
		},
	}
	if tweak != nil {
		tweak(&cfg)
	}

	metrics, err := obs.NewArchiveMetrics()
	if err != nil {
		t.Fatalf("NewArchiveMetrics() error = %v", err)
	}

	return coldarchive.New(coldarchive.Deps{
		Config:  cfg,
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Metrics: metrics,
		Store:   store,
		Blobs:   blobs,
		Now:     func() time.Time { return runAt },
	})
}

func TestRunExportsPartitionAndDropsIt(t *testing.T) {
	rows := append(makeRows("billing", 3, partition.From), makeRows("checkout", 1, partition.From)...)
	store := newFakeStore(rows)
	blobs := newFakeBlobs()

	result, err := newJob(t, store, blobs, nil).Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.PartitionsDropped != 1 {
		t.Errorf("PartitionsDropped = %d, quero 1", result.PartitionsDropped)
	}
	if result.RowsTotal != 4 {
		t.Errorf("RowsTotal = %d, quero 4", result.RowsTotal)
	}
	if !store.dropped[partition.Name] {
		t.Error("a partição deveria ter sido removida do PostgreSQL")
	}
	if got := store.jobs[partition.Name].Status; got != postgres.JobDropped {
		t.Errorf("status do job = %q, quero dropped", got)
	}
}

// Seção 5.3: prefixo por project_id + ano/semana ISO, com manifest ao lado.
func TestObjectLayoutFollowsSpec(t *testing.T) {
	rows := append(makeRows("billing", 3, partition.From), makeRows("checkout", 1, partition.From)...)
	store := newFakeStore(rows)
	blobs := newFakeBlobs()

	if _, err := newJob(t, store, blobs, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	want := []string{
		"audit/production/project_id=billing/year=2026/week=37/events-events_2026w37-00000.manifest.json",
		"audit/production/project_id=billing/year=2026/week=37/events-events_2026w37-00000.parquet",
		"audit/production/project_id=billing/year=2026/week=37/events-events_2026w37-00001.manifest.json",
		"audit/production/project_id=billing/year=2026/week=37/events-events_2026w37-00001.parquet",
		"audit/production/project_id=checkout/year=2026/week=37/events-events_2026w37-00002.manifest.json",
		"audit/production/project_id=checkout/year=2026/week=37/events-events_2026w37-00002.parquet",
	}
	got := blobs.keys()
	if len(got) != len(want) {
		t.Fatalf("keys = %v\nquero %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("key[%d] = %q\nquero     %q", i, got[i], want[i])
		}
	}
}

func TestManifestDescribesBatch(t *testing.T) {
	store := newFakeStore(makeRows("billing", 2, partition.From))
	blobs := newFakeBlobs()

	if _, err := newJob(t, store, blobs, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	key := "audit/production/project_id=billing/year=2026/week=37/events-events_2026w37-00000.manifest.json"
	var manifest coldarchive.Manifest
	if err := json.Unmarshal(blobs.objects[key], &manifest); err != nil {
		t.Fatalf("manifest inválido: %v", err)
	}

	if manifest.PartitionName != partition.Name {
		t.Errorf("PartitionName = %q", manifest.PartitionName)
	}
	if manifest.ProjectID != "billing" {
		t.Errorf("ProjectID = %q", manifest.ProjectID)
	}
	if manifest.RowCount != 2 {
		t.Errorf("RowCount = %d, quero 2", manifest.RowCount)
	}
	if manifest.Compression != "zstd" {
		t.Errorf("Compression = %q, quero zstd", manifest.Compression)
	}
	if len(manifest.SHA256) != 64 {
		t.Errorf("SHA256 = %q, quero hex de 64 chars", manifest.SHA256)
	}
	if !manifest.OccurredAtMin.Equal(partition.From) {
		t.Errorf("OccurredAtMin = %v", manifest.OccurredAtMin)
	}
	if manifest.ParquetSchemaVersion != coldarchive.ParquetSchemaVersion {
		t.Errorf("ParquetSchemaVersion = %d", manifest.ParquetSchemaVersion)
	}
}

func TestParquetRoundTripsHotColumns(t *testing.T) {
	rows := makeRows("billing", 2, partition.From)
	store := newFakeStore(rows)
	blobs := newFakeBlobs()

	if _, err := newJob(t, store, blobs, func(c *config.Config) { c.Archive.BatchRows = 10 }).
		Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	key := "audit/production/project_id=billing/year=2026/week=37/events-events_2026w37-00000.parquet"
	payload := blobs.objects[key]

	type readRow struct {
		ID         string    `parquet:"id"`
		ProjectID  string    `parquet:"project_id"`
		Action     string    `parquet:"action"`
		ActorID    string    `parquet:"actor_id"`
		OccurredAt time.Time `parquet:"occurred_at,timestamp(microsecond)"`
		Metadata   string    `parquet:"metadata"`
	}

	decoded, err := parquet.Read[readRow](strings.NewReader(string(payload)), int64(len(payload)))
	if err != nil {
		t.Fatalf("parquet.Read() error = %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("len(rows) = %d, quero 2", len(decoded))
	}
	if decoded[0].ID != rows[0].ID {
		t.Errorf("id = %q, quero %q", decoded[0].ID, rows[0].ID)
	}
	if decoded[0].ProjectID != "billing" || decoded[0].Action != "order.created" {
		t.Errorf("colunas divergem: %+v", decoded[0])
	}
	if !decoded[0].OccurredAt.UTC().Equal(rows[0].OccurredAt) {
		t.Errorf("occurred_at = %v, quero %v", decoded[0].OccurredAt.UTC(), rows[0].OccurredAt)
	}
	if decoded[0].Metadata != `{"currency":"BRL"}` {
		t.Errorf("metadata = %q", decoded[0].Metadata)
	}
}

// Seção 5.5: retry não relê o PG nem reenvia lotes já verificados.
func TestRunResumesWithoutReprocessingVerifiedBatches(t *testing.T) {
	rows := makeRows("billing", 6, partition.From)
	store := newFakeStore(rows)
	blobs := newFakeBlobs()

	// Primeiro run falha no upload do terceiro lote.
	blobs.failOn = "00002"
	blobs.failErr = errors.New("PutObject timeout after 30s")

	if _, err := newJob(t, store, blobs, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if store.dropped[partition.Name] {
		t.Fatal("a partição não pode ser removida quando um lote falha")
	}
	job := store.jobs[partition.Name]
	if job.Status != postgres.JobFailed {
		t.Errorf("status = %q, quero failed", job.Status)
	}
	if job.ErrorCode == nil || *job.ErrorCode != "blob_upload" {
		t.Errorf("error_code = %v, quero blob_upload", job.ErrorCode)
	}

	putsBefore := len(blobs.puts)
	store.readCalls = nil

	// Segundo run: o obstáculo sumiu; só o que faltava é reprocessado.
	blobs.failOn = ""
	result, err := newJob(t, store, blobs, nil).Run(context.Background())
	if err != nil {
		t.Fatalf("Run() (retry) error = %v", err)
	}

	if !store.dropped[partition.Name] {
		t.Error("a partição deveria ter sido removida no retry")
	}
	if result.RowsTotal != 6 {
		t.Errorf("RowsTotal = %d, quero 6", result.RowsTotal)
	}

	// 3 lotes (seqs 2..4 não estavam prontos) + 1 leitura final vazia por
	// projeto; os dois primeiros lotes vieram do estado, sem tocar o PG.
	if len(store.readCalls) > 4 {
		t.Errorf("retry fez %d leituras no PG; lotes verified não deveriam ser relidos", len(store.readCalls))
	}

	newPuts := blobs.puts[putsBefore:]
	for _, key := range newPuts {
		if strings.Contains(key, "-00000.") || strings.Contains(key, "-00001.") {
			t.Errorf("lote já verificado foi reenviado: %s", key)
		}
	}
}

// Um objeto que sumiu do frio precisa ser reenviado, não assumido como bom.
func TestRunReuploadsWhenObjectDisappeared(t *testing.T) {
	rows := makeRows("billing", 2, partition.From)
	store := newFakeStore(rows)
	blobs := newFakeBlobs()

	if _, err := newJob(t, store, blobs, func(c *config.Config) { c.Archive.DryRun = true }).
		Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	key := "audit/production/project_id=billing/year=2026/week=37/events-events_2026w37-00000.parquet"
	// Marca o lote como `uploaded` (sem verificação) e apaga o objeto.
	job := store.jobs[partition.Name]
	batch := store.batches[job.ID][0]
	batch.Status = postgres.BatchUploaded
	store.batches[job.ID][0] = batch
	delete(blobs.objects, key)

	if _, err := newJob(t, store, blobs, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run() (retry) error = %v", err)
	}

	if _, ok := blobs.objects[key]; !ok {
		t.Error("o objeto ausente deveria ter sido reenviado")
	}
	if !store.dropped[partition.Name] {
		t.Error("a partição deveria ter sido removida após o reenvio")
	}
}

// Seção 5.5: DROP só depois que a soma dos lotes bate com o COUNT(*).
func TestRunKeepsPartitionWhenRowCountDiverges(t *testing.T) {
	store := newFakeStore(makeRows("billing", 4, partition.From))
	blobs := newFakeBlobs()

	inflated := int64(99)
	store.countOverride = &inflated

	result, err := newJob(t, store, blobs, nil).Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if store.dropped[partition.Name] {
		t.Error("a partição não pode ser removida com contagem divergente")
	}
	if result.PartitionsFailed != 1 {
		t.Errorf("PartitionsFailed = %d, quero 1", result.PartitionsFailed)
	}
	if code := store.jobs[partition.Name].ErrorCode; code == nil || *code != "row_count_mismatch" {
		t.Errorf("error_code = %v, quero row_count_mismatch", code)
	}
}

func TestRunKeepsPartitionWhenUploadedObjectIsTruncated(t *testing.T) {
	store := newFakeStore(makeRows("billing", 2, partition.From))
	blobs := newFakeBlobs()

	if _, err := newJob(t, store, blobs, func(c *config.Config) { c.Archive.DryRun = true }).
		Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	key := "audit/production/project_id=billing/year=2026/week=37/events-events_2026w37-00000.parquet"

	// Lote `uploaded` (sem verificação) cujo objeto no destino está truncado:
	// o job reprocessa e a validação de tamanho tem que barrar o DROP.
	job := store.jobs[partition.Name]
	batch := store.batches[job.ID][0]
	batch.Status = postgres.BatchUploaded
	store.batches[job.ID][0] = batch
	blobs.corruptSize[key] = 7

	if _, err := newJob(t, store, blobs, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if code := store.jobs[partition.Name].ErrorCode; code == nil || *code != "blob_verify" {
		t.Errorf("error_code = %v, quero blob_verify", code)
	}
	if store.dropped[partition.Name] {
		t.Error("a partição não pode ser removida com objeto divergente no destino")
	}
}

func TestRunKeepsPartitionWhenDropFails(t *testing.T) {
	store := newFakeStore(makeRows("billing", 2, partition.From))
	store.dropErr = errors.New("lock timeout")
	blobs := newFakeBlobs()

	result, err := newJob(t, store, blobs, nil).Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.PartitionsFailed != 1 {
		t.Errorf("PartitionsFailed = %d, quero 1", result.PartitionsFailed)
	}
	if code := store.jobs[partition.Name].ErrorCode; code == nil || *code != "pg_drop" {
		t.Errorf("error_code = %v, quero pg_drop", code)
	}
}

func TestDryRunExportsWithoutDropping(t *testing.T) {
	store := newFakeStore(makeRows("billing", 2, partition.From))
	blobs := newFakeBlobs()

	result, err := newJob(t, store, blobs, func(c *config.Config) { c.Archive.DryRun = true }).
		Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if store.dropped[partition.Name] {
		t.Error("dry run não pode remover a partição")
	}
	if result.PartitionsExported != 1 || result.PartitionsDropped != 0 {
		t.Errorf("exported = %d, dropped = %d", result.PartitionsExported, result.PartitionsDropped)
	}
	if got := store.jobs[partition.Name].Status; got != postgres.JobExported {
		t.Errorf("status = %q, quero exported", got)
	}
}

func TestRunOnEmptyEligibleSet(t *testing.T) {
	store := newFakeStore(nil)
	store.partitions = nil

	result, err := newJob(t, store, newFakeBlobs(), nil).Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.PartitionsEligible != 0 || result.PartitionsDropped != 0 {
		t.Errorf("resultado = %+v, quero tudo zerado", result)
	}
}
