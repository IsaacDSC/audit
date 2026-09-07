package postgres

import (
	"testing"
	"time"
)

func TestPartitionForAlignsToISOWeek(t *testing.T) {
	tests := []struct {
		name  string
		input time.Time
		want  Partition
	}{
		{
			name:  "segunda-feira",
			input: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
			want: Partition{
				Name: "events_2026w37",
				From: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name:  "domingo cai na semana que começou na segunda anterior",
			input: time.Date(2026, 9, 13, 23, 59, 59, 0, time.UTC),
			want: Partition{
				Name: "events_2026w37",
				From: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name:  "virada de ano usa o ano ISO",
			input: time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC),
			want: Partition{
				Name: "events_2026w53",
				From: time.Date(2026, 12, 28, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2027, 1, 4, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			// 01:00 em +03:00 é 22:00 UTC de domingo: ainda a semana anterior.
			name:  "instante fora de UTC é normalizado",
			input: time.Date(2026, 9, 7, 1, 0, 0, 0, time.FixedZone("EAT", 3*3600)),
			want: Partition{
				Name: "events_2026w36",
				From: time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := PartitionFor(tc.input)
			if got.Name != tc.want.Name {
				t.Errorf("Name = %q, quero %q", got.Name, tc.want.Name)
			}
			if !got.From.Equal(tc.want.From) {
				t.Errorf("From = %v, quero %v", got.From, tc.want.From)
			}
			if !got.To.Equal(tc.want.To) {
				t.Errorf("To = %v, quero %v", got.To, tc.want.To)
			}
		})
	}
}

func TestPartitionForCoversEveryInstantExactlyOnce(t *testing.T) {
	// Ranges consecutivos precisam encostar sem sobrepor, ou o PostgreSQL
	// recusa a partição (ou perde eventos na fronteira).
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	previous := PartitionFor(start)

	for day := 1; day <= 400; day++ {
		current := PartitionFor(start.AddDate(0, 0, day))
		if current.Name == previous.Name {
			continue
		}
		if !current.From.Equal(previous.To) {
			t.Fatalf("lacuna entre %s (até %v) e %s (de %v)",
				previous.Name, previous.To, current.Name, current.From)
		}
		previous = current
	}
}

func TestParsePartitionBound(t *testing.T) {
	tests := []struct {
		name string
		expr string
		from time.Time
		to   time.Time
	}{
		{
			name: "sem fração de segundo",
			expr: "FOR VALUES FROM ('2026-09-07 00:00:00+00') TO ('2026-09-14 00:00:00+00')",
			from: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "com fração de segundo",
			expr: "FOR VALUES FROM ('2026-09-07 00:00:00.5+00') TO ('2026-09-14 00:00:00+00')",
			from: time.Date(2026, 9, 7, 0, 0, 0, 500000000, time.UTC),
			to:   time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			from, to, err := parsePartitionBound(tc.expr)
			if err != nil {
				t.Fatalf("parsePartitionBound() error = %v", err)
			}
			if !from.Equal(tc.from) {
				t.Errorf("from = %v, quero %v", from, tc.from)
			}
			if !to.Equal(tc.to) {
				t.Errorf("to = %v, quero %v", to, tc.to)
			}
		})
	}
}

func TestParsePartitionBoundRejectsUnknownExpression(t *testing.T) {
	if _, _, err := parsePartitionBound("DEFAULT"); err == nil {
		t.Error("expressão DEFAULT deveria falhar")
	}
}

func TestQuoteIdentEscapesQuotes(t *testing.T) {
	if got := quoteIdent(`events_2026w37`); got != `"events_2026w37"` {
		t.Errorf("quoteIdent() = %s", got)
	}
	if got := quoteIdent(`ev"il`); got != `"ev""il"` {
		t.Errorf("quoteIdent() = %s", got)
	}
}
