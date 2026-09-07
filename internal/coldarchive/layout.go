package coldarchive

import (
	"fmt"
	"strings"
	"time"

	"github.com/IsaacDSC/audit.git/internal/store/postgres"
)

// Manifest acompanha cada Parquet no frio e é o que permite a um leitor
// externo validar o lote sem consultar o PostgreSQL (seção 5.3).
type Manifest struct {
	PartitionName        string    `json:"partition_name"`
	ProjectID            string    `json:"project_id"`
	BatchSeq             int       `json:"batch_seq"`
	ObjectKey            string    `json:"object_key"`
	OccurredAtMin        time.Time `json:"occurred_at_min"`
	OccurredAtMax        time.Time `json:"occurred_at_max"`
	RowCount             int64     `json:"row_count"`
	Bytes                int64     `json:"bytes"`
	SHA256               string    `json:"sha256"`
	ParquetSchemaVersion int       `json:"parquet_schema_version"`
	Compression          string    `json:"compression"`
	ExportedAt           time.Time `json:"exported_at"`
}

// keyPrefix devolve `{prefix}/{env}` — a raiz comum de todos os objetos.
func keyPrefix(prefix, env string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		prefix = "audit"
	}
	return prefix + "/" + env
}

// objectKey monta a key determinística de um lote:
//
//	{prefix}/{env}/project_id={p}/year={YYYY}/week={WW}/events-{partition}-{seq:05d}.parquet
//
// Um `batch_seq` sempre aponta para a mesma key, o que dá exactly-once lógico
// no frio: um retry sobrescreve o objeto em vez de criar outro.
func objectKey(prefix, projectID string, partition postgres.Partition, seq int) string {
	year, week := partition.From.UTC().ISOWeek()
	return fmt.Sprintf("%s/project_id=%s/year=%04d/week=%02d/events-%s-%05d.parquet",
		prefix, projectID, year, week, partition.Name, seq)
}

// manifestKey deriva a key do manifest a partir da key do Parquet.
func manifestKey(parquetKey string) string {
	return strings.TrimSuffix(parquetKey, ".parquet") + ".manifest.json"
}
