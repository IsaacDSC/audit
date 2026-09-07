package obs

import (
	"context"
	"fmt"
	"time"

	"github.com/IsaacDSC/audit.git/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

const meterName = "github.com/IsaacDSC/audit"

// latencyBuckets é dimensionado para o SLO de p99 ≤ 100 ms (seção 11.1).
var latencyBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1, 2.5, 5, 10,
}

// archiveBuckets cobre lotes e runs do job, na casa de segundos a horas.
var archiveBuckets = []float64{0.5, 1, 5, 15, 30, 60, 300, 900, 1800, 3600, 7200}

// ShutdownFunc libera o MeterProvider, forçando o último export OTLP.
// O modo `migrate` depende disso para não perder métricas do run (seção 11.2).
type ShutdownFunc func(context.Context) error

// SetupMetrics configura o MeterProvider global. Sem endpoint OTLP
// configurado, o SDK fica inativo e os instrumentos viram no-op.
func SetupMetrics(ctx context.Context, cfg config.Config) (ShutdownFunc, error) {
	if !cfg.Telemetry.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := newExporter(ctx, cfg.Telemetry)
	if err != nil {
		return nil, fmt.Errorf("otlp metric exporter: %w", err)
	}

	// NewSchemaless em vez de NewWithAttributes: resource.Merge rejeita schemas
	// divergentes, e o schema de resource.Default() acompanha a versão do SDK.
	// Sem schema próprio, o merge herda o do Default e não quebra em upgrades.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.Version),
		semconv.DeploymentEnvironmentNameKey.String(cfg.Env),
	))
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
			exporter,
			sdkmetric.WithInterval(cfg.Telemetry.Interval),
		)),
		sdkmetric.WithView(
			bucketView("http.server.request.duration", latencyBuckets),
			bucketView("cold_archive.run.duration", archiveBuckets),
			bucketView("cold_archive.batch.duration", archiveBuckets),
		),
	)
	otel.SetMeterProvider(provider)

	return provider.Shutdown, nil
}

func bucketView(instrument string, boundaries []float64) sdkmetric.View {
	return sdkmetric.NewView(
		sdkmetric.Instrument{Name: instrument, Kind: sdkmetric.InstrumentKindHistogram},
		sdkmetric.Stream{
			Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: boundaries},
		},
	)
}

func newExporter(ctx context.Context, tel config.Telemetry) (sdkmetric.Exporter, error) {
	if tel.Protocol == "grpc" {
		opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpointURL(tel.Endpoint)}
		if tel.Insecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		if len(tel.Headers) > 0 {
			opts = append(opts, otlpmetricgrpc.WithHeaders(tel.Headers))
		}
		return otlpmetricgrpc.New(ctx, opts...)
	}

	opts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpointURL(tel.Endpoint)}
	if tel.Insecure {
		opts = append(opts, otlpmetrichttp.WithInsecure())
	}
	if len(tel.Headers) > 0 {
		opts = append(opts, otlpmetrichttp.WithHeaders(tel.Headers))
	}
	return otlpmetrichttp.New(ctx, opts...)
}

// APIMetrics são os instrumentos do endpoint de ingestão (seção 11.1).
type APIMetrics struct {
	requests metric.Int64Counter
	duration metric.Float64Histogram
}

// NewAPIMetrics cria os instrumentos da API.
func NewAPIMetrics() (*APIMetrics, error) {
	meter := otel.Meter(meterName)

	requests, err := meter.Int64Counter(
		"http.server.request.total",
		metric.WithDescription("Total de requests HTTP atendidas"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return nil, err
	}

	duration, err := meter.Float64Histogram(
		"http.server.request.duration",
		metric.WithDescription("Duração das requests HTTP medida no servidor"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	return &APIMetrics{requests: requests, duration: duration}, nil
}

// RecordRequest publica volume e latência de uma request concluída.
func (m *APIMetrics) RecordRequest(ctx context.Context, method, route, projectID, outcome string, status int, d time.Duration) {
	if m == nil {
		return
	}
	base := []attribute.KeyValue{
		attribute.String("http.method", method),
		attribute.String("http.route", route),
		attribute.String("project_id", projectID),
	}
	counterAttrs := append(append([]attribute.KeyValue{}, base...),
		attribute.Int("http.status_code", status),
		attribute.String("outcome", outcome),
	)
	m.requests.Add(ctx, 1, metric.WithAttributes(counterAttrs...))
	m.duration.Record(ctx, d.Seconds(), metric.WithAttributes(base...))
}

// ArchiveMetrics são os instrumentos do cold path (seção 11.2).
type ArchiveMetrics struct {
	runDuration       metric.Float64Histogram
	batchDuration     metric.Float64Histogram
	rowsMigrated      metric.Int64Counter
	bytesMigrated     metric.Int64Counter
	partitionsDropped metric.Int64Counter
	failures          metric.Int64Counter
	batchesPending    metric.Int64UpDownCounter
}

// NewArchiveMetrics cria os instrumentos do job de arquivamento.
func NewArchiveMetrics() (*ArchiveMetrics, error) {
	meter := otel.Meter(meterName)
	m := &ArchiveMetrics{}

	var err error
	if m.runDuration, err = meter.Float64Histogram(
		"cold_archive.run.duration",
		metric.WithDescription("Duração de um run completo do job de arquivamento"),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.batchDuration, err = meter.Float64Histogram(
		"cold_archive.batch.duration",
		metric.WithDescription("Duração de export+upload de um lote"),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.rowsMigrated, err = meter.Int64Counter(
		"cold_archive.rows.migrated",
		metric.WithDescription("Linhas migradas do quente para o frio"),
		metric.WithUnit("{row}"),
	); err != nil {
		return nil, err
	}
	if m.bytesMigrated, err = meter.Int64Counter(
		"cold_archive.bytes.migrated",
		metric.WithDescription("Bytes Parquet enviados ao object storage"),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if m.partitionsDropped, err = meter.Int64Counter(
		"cold_archive.partitions.dropped",
		metric.WithDescription("Partições removidas do PostgreSQL após validação"),
		metric.WithUnit("{partition}"),
	); err != nil {
		return nil, err
	}
	if m.failures, err = meter.Int64Counter(
		"cold_archive.failures",
		metric.WithDescription("Falhas do job agregáveis por motivo e estágio"),
		metric.WithUnit("{failure}"),
	); err != nil {
		return nil, err
	}
	if m.batchesPending, err = meter.Int64UpDownCounter(
		"cold_archive.batches.pending",
		metric.WithDescription("Lotes ainda não verificados (trabalho restante)"),
		metric.WithUnit("{batch}"),
	); err != nil {
		return nil, err
	}
	return m, nil
}

// RecordRun publica a duração de um run e seu status final.
func (m *ArchiveMetrics) RecordRun(ctx context.Context, status string, d time.Duration) {
	if m == nil {
		return
	}
	m.runDuration.Record(ctx, d.Seconds(), metric.WithAttributes(attribute.String("status", status)))
}

// RecordBatch publica duração, linhas e bytes de um lote.
func (m *ArchiveMetrics) RecordBatch(ctx context.Context, partition, status string, rows, bytes int64, d time.Duration) {
	if m == nil {
		return
	}
	m.batchDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("partition_name", partition),
		attribute.String("status", status),
	))
	partAttr := metric.WithAttributes(attribute.String("partition_name", partition))
	if rows > 0 {
		m.rowsMigrated.Add(ctx, rows, partAttr)
	}
	if bytes > 0 {
		m.bytesMigrated.Add(ctx, bytes, partAttr)
	}
}

// RecordPartitionDropped contabiliza uma partição efetivamente removida do PG.
func (m *ArchiveMetrics) RecordPartitionDropped(ctx context.Context) {
	if m == nil {
		return
	}
	m.partitionsDropped.Add(ctx, 1)
}

// RecordFailure agrega falhas por código e estágio (read|upload|verify|drop).
func (m *ArchiveMetrics) RecordFailure(ctx context.Context, errorCode, stage string) {
	if m == nil {
		return
	}
	m.failures.Add(ctx, 1, metric.WithAttributes(
		attribute.String("error_code", errorCode),
		attribute.String("stage", stage),
	))
}

// AddPendingBatches move o contador de trabalho restante da partição.
func (m *ArchiveMetrics) AddPendingBatches(ctx context.Context, partition string, delta int64) {
	if m == nil || delta == 0 {
		return
	}
	m.batchesPending.Add(ctx, delta, metric.WithAttributes(attribute.String("partition_name", partition)))
}
