// Package config carrega a configuração do serviço a partir do ambiente.
//
// Os dois modos (`api` e `migrate`) compartilham o mesmo Config; cada modo
// valida apenas o que precisa no boot (spec 001, seção 9.2).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Provider de object storage para a camada fria.
type Provider string

const (
	ProviderS3    Provider = "s3"
	ProviderAzure Provider = "azure"
)

// CredentialsSource define de onde vêm os pares project_id/secret.
type CredentialsSource string

const (
	CredentialsEnv CredentialsSource = "env"
	CredentialsDB  CredentialsSource = "db"
)

// DefaultMetadataAllowlist é a allowlist global de metadata (decisão 12.2).
var DefaultMetadataAllowlist = []string{"amount_cents", "currency", "plan_id", "actor_email"}

// Config agrega toda a configuração do binário.
type Config struct {
	Env         string
	ServiceName string
	Version     string
	LogLevel    string

	Database    Database
	API         API
	Auth        Auth
	Ingest      Ingest
	ColdStorage ColdStorage
	Archive     Archive
	Telemetry   Telemetry
}

// Database descreve a conexão com o PostgreSQL quente.
type Database struct {
	URL              string
	MaxConns         int32
	MinConns         int32
	MaxConnLifetime  time.Duration
	ConnectTimeout   time.Duration
	StatementTimeout time.Duration
	AutoMigrate      bool
}

// API descreve o servidor HTTP de ingestão.
type API struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
	RateLimitRPS      float64
	RateLimitBurst    int
}

// Auth descreve o store de credenciais Basic Auth.
type Auth struct {
	Source CredentialsSource
	// Raw no formato "project_id:bcrypt_hash,project_id2:bcrypt_hash".
	Raw string
	// CacheTTL evita pagar bcrypt a cada request (orçamento de latência 10.1).
	CacheTTL         time.Duration
	NegativeCacheTTL time.Duration
	CacheSize        int
}

// Ingest agrega os limites de payload da seção 8.
type Ingest struct {
	MaxBodyBytes      int64
	MetadataMaxBytes  int
	MetadataMaxKeys   int
	MetadataAllowlist []string
	SchemaVersion     int16
	// MaxClockSkew limita quão no futuro `occurred_at` pode estar.
	MaxClockSkew time.Duration
	// Eventos mais antigos que HotRetention cairiam em partição já arquivada.
	HotRetention time.Duration
}

// ColdStorage descreve o destino da camada fria (seção 5.3).
type ColdStorage struct {
	Provider Provider
	// Bucket S3 ou container Azure.
	Bucket string
	Prefix string

	// AWS
	Region       string
	KMSKeyID     string
	S3Endpoint   string
	UsePathStyle bool

	// Azure
	AccountURL      string
	EncryptionScope string
}

// Archive descreve o job `migrate` (seção 5.4/5.5).
type Archive struct {
	HotRetention        time.Duration
	BatchRows           int
	MaxPartitionsPerRun int
	Timeout             time.Duration
	DryRun              bool
}

// Telemetry descreve o export OTLP para o Elastic (decisão 12.9).
type Telemetry struct {
	Enabled  bool
	Endpoint string
	Protocol string // grpc | http
	Insecure bool
	Headers  map[string]string
	Interval time.Duration
}

// Load lê o ambiente e aplica os defaults da spec.
func Load() (Config, error) {
	var errs []string
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	cfg := Config{
		Env:         envStr("AUDIT_ENV", "development"),
		ServiceName: envStr("OTEL_SERVICE_NAME", "audit"),
		Version:     envStr("AUDIT_VERSION", "dev"),
		LogLevel:    envStr("LOG_LEVEL", "info"),
	}

	cfg.Database = Database{
		URL:              envStr("DATABASE_URL", ""),
		MaxConns:         int32(envInt("DB_MAX_CONNS", 20, fail)),
		MinConns:         int32(envInt("DB_MIN_CONNS", 2, fail)),
		MaxConnLifetime:  envDuration("DB_MAX_CONN_LIFETIME", time.Hour, fail),
		ConnectTimeout:   envDuration("DB_CONNECT_TIMEOUT", 5*time.Second, fail),
		StatementTimeout: envDuration("DB_STATEMENT_TIMEOUT", 3*time.Second, fail),
		AutoMigrate:      envBool("DB_AUTO_MIGRATE", true, fail),
	}

	cfg.API = API{
		Addr:              envStr("HTTP_ADDR", ":8080"),
		ReadHeaderTimeout: envDuration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second, fail),
		ReadTimeout:       envDuration("HTTP_READ_TIMEOUT", 10*time.Second, fail),
		WriteTimeout:      envDuration("HTTP_WRITE_TIMEOUT", 10*time.Second, fail),
		IdleTimeout:       envDuration("HTTP_IDLE_TIMEOUT", 60*time.Second, fail),
		ShutdownTimeout:   envDuration("HTTP_SHUTDOWN_TIMEOUT", 15*time.Second, fail),
		RateLimitRPS:      envFloat("RATE_LIMIT_RPS", 100, fail),
		RateLimitBurst:    envInt("RATE_LIMIT_BURST", 200, fail),
	}

	cfg.Auth = Auth{
		Source:           CredentialsSource(envStr("AUTH_CREDENTIALS_SOURCE", string(CredentialsEnv))),
		Raw:              os.Getenv("AUDIT_CREDENTIALS"),
		CacheTTL:         envDuration("AUTH_CACHE_TTL", 5*time.Minute, fail),
		NegativeCacheTTL: envDuration("AUTH_NEGATIVE_CACHE_TTL", 5*time.Second, fail),
		CacheSize:        envInt("AUTH_CACHE_SIZE", 4096, fail),
	}
	if cfg.Auth.Source != CredentialsEnv && cfg.Auth.Source != CredentialsDB {
		fail("AUTH_CREDENTIALS_SOURCE inválido: %q (use env|db)", cfg.Auth.Source)
	}

	hotRetention := envDuration("HOT_RETENTION", 90*24*time.Hour, fail)
	cfg.Ingest = Ingest{
		MaxBodyBytes:      int64(envInt("MAX_BODY_BYTES", 64*1024, fail)),
		MetadataMaxBytes:  envInt("METADATA_MAX_BYTES", 4*1024, fail),
		MetadataMaxKeys:   envInt("METADATA_MAX_KEYS", 20, fail),
		MetadataAllowlist: envList("METADATA_ALLOWLIST", DefaultMetadataAllowlist),
		SchemaVersion:     int16(envInt("SCHEMA_VERSION", 1, fail)),
		MaxClockSkew:      envDuration("MAX_CLOCK_SKEW", 24*time.Hour, fail),
		HotRetention:      hotRetention,
	}

	cfg.ColdStorage = ColdStorage{
		Provider:        Provider(envStr("COLD_STORAGE_PROVIDER", string(ProviderS3))),
		Bucket:          envStr("COLD_STORAGE_BUCKET", ""),
		Prefix:          strings.Trim(envStr("COLD_STORAGE_PREFIX", "audit"), "/"),
		Region:          envStr("AWS_REGION", ""),
		KMSKeyID:        envStr("COLD_STORAGE_KMS_KEY_ID", ""),
		S3Endpoint:      envStr("COLD_STORAGE_S3_ENDPOINT", ""),
		UsePathStyle:    envBool("COLD_STORAGE_S3_PATH_STYLE", false, fail),
		AccountURL:      envStr("COLD_STORAGE_AZURE_ACCOUNT_URL", ""),
		EncryptionScope: envStr("COLD_STORAGE_AZURE_ENCRYPTION_SCOPE", ""),
	}

	cfg.Archive = Archive{
		HotRetention:        hotRetention,
		BatchRows:           envInt("ARCHIVE_BATCH_ROWS", 200_000, fail),
		MaxPartitionsPerRun: envInt("ARCHIVE_MAX_PARTITIONS_PER_RUN", 8, fail),
		Timeout:             envDuration("ARCHIVE_TIMEOUT", 55*time.Minute, fail),
		DryRun:              envBool("ARCHIVE_DRY_RUN", false, fail),
	}

	endpoint := envStr("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", envStr("OTEL_EXPORTER_OTLP_ENDPOINT", ""))
	cfg.Telemetry = Telemetry{
		Enabled:  endpoint != "" && !envBool("OTEL_SDK_DISABLED", false, fail),
		Endpoint: endpoint,
		Protocol: envStr("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
		Insecure: envBool("OTEL_EXPORTER_OTLP_INSECURE", false, fail),
		Headers:  parseHeaders(os.Getenv("OTEL_EXPORTER_OTLP_HEADERS")),
		// A spec do OTel define esta variável em milissegundos inteiros, e o
		// próprio SDK a lê. Usar duração Go aqui faria o SDK reclamar.
		Interval: time.Duration(envInt("OTEL_METRIC_EXPORT_INTERVAL", 30_000, fail)) * time.Millisecond,
	}
	if p := cfg.Telemetry.Protocol; p != "grpc" && p != "http" && p != "http/protobuf" {
		fail("OTEL_EXPORTER_OTLP_PROTOCOL inválido: %q (use grpc|http)", p)
	}

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("config inválida:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return cfg, nil
}

// ValidateAPI checa o que o modo `api` precisa.
func (c Config) ValidateAPI() error {
	var errs []string
	if c.Database.URL == "" {
		errs = append(errs, "DATABASE_URL é obrigatório no modo api")
	}
	if c.Auth.Source == CredentialsEnv && strings.TrimSpace(c.Auth.Raw) == "" {
		errs = append(errs, "AUDIT_CREDENTIALS é obrigatório quando AUTH_CREDENTIALS_SOURCE=env")
	}
	if c.Ingest.MaxBodyBytes <= 0 {
		errs = append(errs, "MAX_BODY_BYTES deve ser > 0")
	}
	if len(errs) > 0 {
		return fmt.Errorf("config do modo api inválida:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// ValidateMigrate checa o que o modo `migrate` precisa.
func (c Config) ValidateMigrate() error {
	var errs []string
	if c.Database.URL == "" {
		errs = append(errs, "DATABASE_URL é obrigatório no modo migrate")
	}
	if c.ColdStorage.Bucket == "" {
		errs = append(errs, "COLD_STORAGE_BUCKET é obrigatório no modo migrate")
	}
	switch c.ColdStorage.Provider {
	case ProviderS3:
		if c.ColdStorage.KMSKeyID == "" {
			errs = append(errs, "COLD_STORAGE_KMS_KEY_ID é obrigatório para SSE-KMS (decisão 12.7)")
		}
	case ProviderAzure:
		if c.ColdStorage.AccountURL == "" {
			errs = append(errs, "COLD_STORAGE_AZURE_ACCOUNT_URL é obrigatório para o provider azure")
		}
		if c.ColdStorage.EncryptionScope == "" {
			errs = append(errs, "COLD_STORAGE_AZURE_ENCRYPTION_SCOPE (CMK) é obrigatório para o provider azure")
		}
	default:
		errs = append(errs, fmt.Sprintf("COLD_STORAGE_PROVIDER inválido: %q (use s3|azure)", c.ColdStorage.Provider))
	}
	if c.Archive.BatchRows <= 0 {
		errs = append(errs, "ARCHIVE_BATCH_ROWS deve ser > 0")
	}
	if len(errs) > 0 {
		return fmt.Errorf("config do modo migrate inválida:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

type failFunc func(format string, args ...any)

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int, fail failFunc) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fail("%s inválido: %q não é inteiro", key, v)
		return def
	}
	return n
}

func envFloat(key string, def float64, fail failFunc) float64 {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		fail("%s inválido: %q não é número", key, v)
		return def
	}
	return f
}

func envBool(key string, def bool, fail failFunc) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		fail("%s inválido: %q não é booleano", key, v)
		return def
	}
	return b
}

func envDuration(key string, def time.Duration, fail failFunc) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fail("%s inválido: %q não é duração (ex.: 90s, 5m, 24h)", key, v)
		return def
	}
	return d
}

func envList(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseHeaders(raw string) map[string]string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
