package blobstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/IsaacDSC/audit.git/internal/config"
)

// AzureStore grava em um container do Azure Blob usando um encryption scope
// apoiado em CMK no Key Vault (decisão 12.7).
type AzureStore struct {
	client     *azblob.Client
	accountURL string
	container  string
	scope      string
}

// NewAzure monta o cliente com DefaultAzureCredential (managed identity em
// produção, credenciais locais em desenvolvimento).
func NewAzure(cfg config.ColdStorage) (*AzureStore, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("credencial Azure: %w", err)
	}

	client, err := azblob.NewClient(cfg.AccountURL, credential, nil)
	if err != nil {
		return nil, fmt.Errorf("cliente azblob: %w", err)
	}

	return &AzureStore{
		client:     client,
		accountURL: strings.TrimRight(cfg.AccountURL, "/"),
		container:  cfg.Bucket,
		scope:      cfg.EncryptionScope,
	}, nil
}

// Put grava o blob dentro do encryption scope configurado.
func (s *AzureStore) Put(ctx context.Context, key string, body []byte, opts PutOptions) error {
	uploadOpts := &azblob.UploadBufferOptions{}
	if s.scope != "" {
		uploadOpts.CPKScopeInfo = &blob.CPKScopeInfo{EncryptionScope: to.Ptr(s.scope)}
	}
	if opts.ContentType != "" {
		uploadOpts.HTTPHeaders = &blob.HTTPHeaders{BlobContentType: to.Ptr(opts.ContentType)}
	}
	if len(opts.Metadata) > 0 {
		metadata := make(map[string]*string, len(opts.Metadata))
		for k, v := range opts.Metadata {
			metadata[k] = to.Ptr(v)
		}
		uploadOpts.Metadata = metadata
	}

	if _, err := s.client.UploadBuffer(ctx, s.container, key, body, uploadOpts); err != nil {
		return fmt.Errorf("azblob UploadBuffer %s: %w", key, err)
	}
	return nil
}

// Head devolve tamanho e ETag do blob.
func (s *AzureStore) Head(ctx context.Context, key string) (ObjectInfo, bool, error) {
	blobClient := s.client.ServiceClient().NewContainerClient(s.container).NewBlobClient(key)

	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
			return ObjectInfo{}, false, nil
		}
		return ObjectInfo{}, false, fmt.Errorf("azblob GetProperties %s: %w", key, err)
	}

	info := ObjectInfo{}
	if props.ContentLength != nil {
		info.Size = *props.ContentLength
	}
	if props.ETag != nil {
		info.ETag = string(*props.ETag)
	}
	return info, true, nil
}

// URI devolve a URL do blob.
func (s *AzureStore) URI(key string) string {
	return s.accountURL + "/" + s.container + "/" + key
}
