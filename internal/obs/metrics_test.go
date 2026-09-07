package obs

import (
	"context"
	"testing"
	"time"

	"github.com/IsaacDSC/audit.git/internal/config"
)

func telemetryConfig(protocol string) config.Config {
	return config.Config{
		ServiceName: "audit",
		Version:     "test",
		Env:         "test",
		Telemetry: config.Telemetry{
			Enabled:  true,
			Endpoint: "http://127.0.0.1:4317",
			Protocol: protocol,
			Insecure: true,
			Interval: time.Minute,
		},
	}
}

// SetupMetrics montava o resource com um schema URL próprio, que passou a
// divergir do schema de resource.Default() e fazia o boot falhar com
// "conflicting Schema URL". O exporter OTLP não conecta na criação, então este
// teste roda offline e cobre a montagem do provider.
func TestSetupMetricsBuildsProvider(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		t.Run(protocol, func(t *testing.T) {
			shutdown, err := SetupMetrics(context.Background(), telemetryConfig(protocol))
			if err != nil {
				t.Fatalf("SetupMetrics: %v", err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = shutdown(ctx)
			})

			if _, err := NewAPIMetrics(); err != nil {
				t.Fatalf("NewAPIMetrics: %v", err)
			}
			if _, err := NewArchiveMetrics(); err != nil {
				t.Fatalf("NewArchiveMetrics: %v", err)
			}
		})
	}
}

func TestSetupMetricsDisabledIsNoop(t *testing.T) {
	shutdown, err := SetupMetrics(context.Background(), config.Config{})
	if err != nil {
		t.Fatalf("SetupMetrics: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
