package blobstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/IsaacDSC/audit.git/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3Store grava no S3 com SSE-KMS usando uma CMK (decisão 12.7).
type S3Store struct {
	client   *s3.Client
	bucket   string
	kmsKeyID string
}

// NewS3 monta o cliente a partir da cadeia de credenciais padrão da AWS.
func NewS3(ctx context.Context, cfg config.ColdStorage) (*S3Store, error) {
	opts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("carregar config AWS: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	})

	return &S3Store{client: client, bucket: cfg.Bucket, kmsKeyID: cfg.KMSKeyID}, nil
}

// Put grava o objeto criptografado com a CMK configurada.
func (s *S3Store) Put(ctx context.Context, key string, body []byte, opts PutOptions) error {
	input := &s3.PutObjectInput{
		Bucket:               aws.String(s.bucket),
		Key:                  aws.String(key),
		Body:                 bytes.NewReader(body),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		Metadata:             opts.Metadata,
	}
	if s.kmsKeyID != "" {
		input.SSEKMSKeyId = aws.String(s.kmsKeyID)
	}
	if opts.ContentType != "" {
		input.ContentType = aws.String(opts.ContentType)
	}
	// O S3 valida o checksum na ponta dele: um upload corrompido falha aqui
	// em vez de virar um Parquet ilegível no frio.
	if opts.SHA256Hex != "" {
		raw, err := hex.DecodeString(opts.SHA256Hex)
		if err != nil {
			return fmt.Errorf("sha256 inválido para %s: %w", key, err)
		}
		input.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(raw))
	}

	if _, err := s.client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("s3 PutObject %s: %w", key, err)
	}
	return nil
}

// Head devolve tamanho e ETag do objeto.
func (s *S3Store) Head(ctx context.Context, key string) (ObjectInfo, bool, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return ObjectInfo{}, false, nil
		}
		return ObjectInfo{}, false, fmt.Errorf("s3 HeadObject %s: %w", key, err)
	}

	info := ObjectInfo{}
	if out.ContentLength != nil {
		info.Size = *out.ContentLength
	}
	if out.ETag != nil {
		info.ETag = *out.ETag
	}
	return info, true, nil
}

// URI devolve a referência s3:// do objeto.
func (s *S3Store) URI(key string) string {
	return "s3://" + s.bucket + "/" + key
}

func isS3NotFound(err error) bool {
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NotFound" || code == "NoSuchKey" || code == "404"
	}
	return false
}
