// Package blobstore abstrai o object storage da camada fria. O job `migrate`
// fala só com esta interface; S3 e Azure Blob ficam atrás dela (seção 5.3).
package blobstore

import (
	"context"
	"fmt"

	"github.com/IsaacDSC/audit.git/internal/config"
)

// PutOptions descreve o objeto a ser gravado.
type PutOptions struct {
	ContentType string
	// SHA256 em hexadecimal; enviado ao provedor como checksum de integridade
	// e replicado no manifest.
	SHA256Hex string
	Metadata  map[string]string
}

// ObjectInfo é o resultado de um HEAD.
type ObjectInfo struct {
	Size int64
	ETag string
}

// BlobStore é a interface única do frio. Objetos são imutáveis por
// convenção: um `batch_seq` sempre escreve a mesma key.
type BlobStore interface {
	// Put grava o objeto com criptografia em repouso (SSE-KMS ou CMK).
	Put(ctx context.Context, key string, body []byte, opts PutOptions) error
	// Head devolve os metadados do objeto; ok=false se ele não existe.
	Head(ctx context.Context, key string) (info ObjectInfo, ok bool, err error)
	// URI devolve a referência legível do objeto, para logs e manifest.
	URI(key string) string
}

// New escolhe a implementação pelo provider configurado.
func New(ctx context.Context, cfg config.ColdStorage) (BlobStore, error) {
	switch cfg.Provider {
	case config.ProviderS3:
		return NewS3(ctx, cfg)
	case config.ProviderAzure:
		return NewAzure(cfg)
	default:
		return nil, fmt.Errorf("provider de cold storage não suportado: %q", cfg.Provider)
	}
}
