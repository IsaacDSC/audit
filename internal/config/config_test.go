package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/IsaacDSC/audit.git/internal/config"
)

// cleanEnv isola o teste do ambiente do desenvolvedor e do agente de CI.
// Load() trata string vazia como ausente, e t.Setenv restaura o valor
// original no fim do teste.
func cleanEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"AUDIT_ENV", "AUDIT_VERSION", "AUDIT_CREDENTIALS", "LOG_LEVEL", "OTEL_SERVICE_NAME",
		"DATABASE_URL", "DB_MAX_CONNS", "DB_MIN_CONNS", "DB_MAX_CONN_LIFETIME",
		"DB_CONNECT_TIMEOUT", "DB_STATEMENT_TIMEOUT", "DB_AUTO_MIGRATE",
		"HTTP_ADDR", "HTTP_READ_HEADER_TIMEOUT", "HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT",
		"HTTP_IDLE_TIMEOUT", "HTTP_SHUTDOWN_TIMEOUT", "RATE_LIMIT_RPS", "RATE_LIMIT_BURST",
		"AUTH_CREDENTIALS_SOURCE", "AUTH_CACHE_TTL", "AUTH_NEGATIVE_CACHE_TTL", "AUTH_CACHE_SIZE",
		"MAX_BODY_BYTES", "METADATA_MAX_BYTES", "METADATA_MAX_KEYS", "METADATA_ALLOWLIST",
		"SCHEMA_VERSION", "MAX_CLOCK_SKEW", "HOT_RETENTION",
		"COLD_STORAGE_PROVIDER", "COLD_STORAGE_BUCKET", "COLD_STORAGE_PREFIX",
		"COLD_STORAGE_KMS_KEY_ID", "COLD_STORAGE_S3_ENDPOINT", "COLD_STORAGE_S3_PATH_STYLE",
		"COLD_STORAGE_AZURE_ACCOUNT_URL", "COLD_STORAGE_AZURE_ENCRYPTION_SCOPE", "AWS_REGION",
		"ARCHIVE_BATCH_ROWS", "ARCHIVE_MAX_PARTITIONS_PER_RUN", "ARCHIVE_TIMEOUT", "ARCHIVE_DRY_RUN",
		"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_INSECURE", "OTEL_EXPORTER_OTLP_HEADERS",
		"OTEL_SDK_DISABLED", "OTEL_METRIC_EXPORT_INTERVAL",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadAppliesSpecDefaults(t *testing.T) {
	cleanEnv(t)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Limites da seção 8.
	if cfg.Ingest.MaxBodyBytes != 64*1024 {
		t.Errorf("MaxBodyBytes = %d, quero 65536", cfg.Ingest.MaxBodyBytes)
	}
	if cfg.Ingest.MetadataMaxBytes != 4*1024 {
		t.Errorf("MetadataMaxBytes = %d, quero 4096", cfg.Ingest.MetadataMaxBytes)
	}
	if cfg.Ingest.MetadataMaxKeys != 20 {
		t.Errorf("MetadataMaxKeys = %d, quero 20", cfg.Ingest.MetadataMaxKeys)
	}
	if cfg.API.RateLimitRPS != 100 {
		t.Errorf("RateLimitRPS = %v, quero 100", cfg.API.RateLimitRPS)
	}
	if cfg.Archive.HotRetention != 90*24*time.Hour {
		t.Errorf("HotRetention = %v, quero 2160h", cfg.Archive.HotRetention)
	}
	if cfg.ColdStorage.Provider != config.ProviderS3 {
		t.Errorf("Provider = %q, quero s3", cfg.ColdStorage.Provider)
	}
	if len(cfg.Ingest.MetadataAllowlist) == 0 {
		t.Error("allowlist de metadata vazia")
	}
}

func TestLoadReadsEnvOverrides(t *testing.T) {
	cleanEnv(t)

	t.Setenv("MAX_BODY_BYTES", "1024")
	t.Setenv("HOT_RETENTION", "48h")
	t.Setenv("METADATA_ALLOWLIST", " a , b ,, c ")
	t.Setenv("RATE_LIMIT_RPS", "42.5")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Ingest.MaxBodyBytes != 1024 {
		t.Errorf("MaxBodyBytes = %d", cfg.Ingest.MaxBodyBytes)
	}
	if cfg.Archive.HotRetention != 48*time.Hour {
		t.Errorf("HotRetention = %v", cfg.Archive.HotRetention)
	}
	if cfg.API.RateLimitRPS != 42.5 {
		t.Errorf("RateLimitRPS = %v", cfg.API.RateLimitRPS)
	}

	want := []string{"a", "b", "c"}
	if len(cfg.Ingest.MetadataAllowlist) != len(want) {
		t.Fatalf("allowlist = %v, quero %v", cfg.Ingest.MetadataAllowlist, want)
	}
	for i := range want {
		if cfg.Ingest.MetadataAllowlist[i] != want[i] {
			t.Errorf("allowlist[%d] = %q, quero %q", i, cfg.Ingest.MetadataAllowlist[i], want[i])
		}
	}
}

// Um valor mal digitado precisa falhar no boot, não virar default silencioso.
func TestLoadReportsAllInvalidValues(t *testing.T) {
	cleanEnv(t)

	t.Setenv("MAX_BODY_BYTES", "muito")
	t.Setenv("HOT_RETENTION", "noventa dias")

	_, err := config.Load()
	if err == nil {
		t.Fatal("Load() deveria falhar com valores inválidos")
	}
	for _, key := range []string{"MAX_BODY_BYTES", "HOT_RETENTION"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("erro não menciona %s: %v", key, err)
		}
	}
}

func TestValidateAPIRequiresDatabaseAndCredentials(t *testing.T) {
	cleanEnv(t)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	err = cfg.ValidateAPI()
	if err == nil {
		t.Fatal("ValidateAPI() deveria falhar sem DATABASE_URL nem credenciais")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL") ||
		!strings.Contains(err.Error(), "AUDIT_CREDENTIALS") {
		t.Errorf("erro incompleto: %v", err)
	}

	cfg.Database.URL = "postgres://localhost/audit"
	cfg.Auth.Raw = "billing:$2a$04$abcdefghijklmnopqrstuv"
	if err := cfg.ValidateAPI(); err != nil {
		t.Errorf("ValidateAPI() error = %v", err)
	}
}

// Decisão 12.7: sem chave de criptografia o job não pode subir nada.
func TestValidateMigrateRequiresEncryptionKey(t *testing.T) {
	cleanEnv(t)

	base, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	base.Database.URL = "postgres://localhost/audit"
	base.ColdStorage.Bucket = "audit-cold"

	t.Run("s3 sem KMS", func(t *testing.T) {
		cfg := base
		cfg.ColdStorage.Provider = config.ProviderS3
		if err := cfg.ValidateMigrate(); err == nil ||
			!strings.Contains(err.Error(), "COLD_STORAGE_KMS_KEY_ID") {
			t.Errorf("erro = %v, quero exigir KMS", err)
		}
	})

	t.Run("s3 completo", func(t *testing.T) {
		cfg := base
		cfg.ColdStorage.Provider = config.ProviderS3
		cfg.ColdStorage.KMSKeyID = "arn:aws:kms:us-east-1:1:key/abc"
		if err := cfg.ValidateMigrate(); err != nil {
			t.Errorf("ValidateMigrate() error = %v", err)
		}
	})

	t.Run("azure sem encryption scope", func(t *testing.T) {
		cfg := base
		cfg.ColdStorage.Provider = config.ProviderAzure
		cfg.ColdStorage.AccountURL = "https://acct.blob.core.windows.net"
		if err := cfg.ValidateMigrate(); err == nil ||
			!strings.Contains(err.Error(), "ENCRYPTION_SCOPE") {
			t.Errorf("erro = %v, quero exigir CMK", err)
		}
	})

	t.Run("provider desconhecido", func(t *testing.T) {
		cfg := base
		cfg.ColdStorage.Provider = "gcs"
		if err := cfg.ValidateMigrate(); err == nil {
			t.Error("provider desconhecido deveria falhar")
		}
	})
}

func TestTelemetryDisabledWithoutEndpoint(t *testing.T) {
	cleanEnv(t)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Telemetry.Enabled {
		t.Error("telemetria deveria ficar desligada sem OTEL_EXPORTER_OTLP_ENDPOINT")
	}

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://elastic.example:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=ApiKey abc, x-tenant=audit")

	cfg, err = config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.Telemetry.Enabled {
		t.Error("telemetria deveria estar ligada com endpoint configurado")
	}
	if cfg.Telemetry.Headers["Authorization"] != "ApiKey abc" {
		t.Errorf("headers = %v", cfg.Telemetry.Headers)
	}
	if cfg.Telemetry.Interval != 30*time.Second {
		t.Errorf("Interval = %v, quer 30s", cfg.Telemetry.Interval)
	}
}

// OTEL_METRIC_EXPORT_INTERVAL é lido também pelo SDK do OTel, que exige
// milissegundos inteiros. Aceitar duração Go aqui geraria log de erro do SDK.
func TestMetricExportIntervalIsMilliseconds(t *testing.T) {
	cleanEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://elastic.example:4317")
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "10000")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Telemetry.Interval != 10*time.Second {
		t.Errorf("Interval = %v, quer 10s", cfg.Telemetry.Interval)
	}

	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "10s")
	if _, err := config.Load(); err == nil {
		t.Error("duração Go deveria ser rejeitada, o SDK espera milissegundos")
	}
}
