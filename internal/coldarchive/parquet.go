package coldarchive

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/IsaacDSC/audit.git/internal/store/postgres"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// ParquetSchemaVersion acompanha a evolução do schema frio; vai no manifest
// para que o sistema leitor saiba interpretar o arquivo (seção 6.4).
const ParquetSchemaVersion = 1

// parquetRow espelha a tabela quente (seção 6.2). `metadata` e `extensions`
// são strings JSON, evitando divergência de MAP tipado entre as camadas.
type parquetRow struct {
	ID            string    `parquet:"id"`
	ProjectID     string    `parquet:"project_id"`
	SchemaVersion int16     `parquet:"schema_version"`
	Action        string    `parquet:"action"`
	Outcome       string    `parquet:"outcome"`
	ActorType     string    `parquet:"actor_type"`
	ActorID       string    `parquet:"actor_id"`
	ResourceType  string    `parquet:"resource_type"`
	ResourceID    string    `parquet:"resource_id"`
	RequestID     string    `parquet:"request_id"`
	CorrelationID string    `parquet:"correlation_id"`
	IP            string    `parquet:"ip"`
	UserAgent     string    `parquet:"user_agent"`
	OccurredAt    time.Time `parquet:"occurred_at,timestamp(microsecond)"`
	ReceivedAt    time.Time `parquet:"received_at,timestamp(microsecond)"`
	Metadata      string    `parquet:"metadata"`
	Extensions    string    `parquet:"extensions"`
}

// encodeParquet serializa um lote em Parquet com compressão zstd
// (decisão 12.6) e devolve os bytes junto do sha256 hexadecimal.
func encodeParquet(rows []postgres.Row) ([]byte, string, error) {
	var buf bytes.Buffer

	writer := parquet.NewGenericWriter[parquetRow](&buf,
		parquet.Compression(&zstd.Codec{Level: zstd.DefaultLevel}),
	)

	batch := make([]parquetRow, len(rows))
	for i, row := range rows {
		batch[i] = parquetRow{
			ID:            row.ID,
			ProjectID:     row.ProjectID,
			SchemaVersion: int16(row.SchemaVersion),
			Action:        row.Action,
			Outcome:       row.Outcome,
			ActorType:     row.ActorType,
			ActorID:       row.ActorID,
			ResourceType:  row.ResourceType,
			ResourceID:    row.ResourceID,
			RequestID:     row.RequestID,
			CorrelationID: row.CorrelationID,
			IP:            row.IP,
			UserAgent:     row.UserAgent,
			OccurredAt:    row.OccurredAt.UTC(),
			ReceivedAt:    row.ReceivedAt.UTC(),
			Metadata:      row.Metadata,
			Extensions:    row.Extensions,
		}
	}

	if _, err := writer.Write(batch); err != nil {
		return nil, "", fmt.Errorf("escrever linhas parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, "", fmt.Errorf("fechar writer parquet: %w", err)
	}

	payload := buf.Bytes()
	digest := sha256.Sum256(payload)
	return payload, hex.EncodeToString(digest[:]), nil
}
