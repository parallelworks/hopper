package hopper

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

type optimizationLogWriter struct{ io.Writer }

func (optimizationLogWriter) Close() error { return nil }

type optimizationLogStore struct {
	managedTestStore
	writer io.WriteCloser
}

func (s *optimizationLogStore) Writer(context.Context, *JobRow) (io.WriteCloser, error) {
	return s.writer, nil
}

func cappedLogAttempt(t testing.TB, base *slog.Logger) *attempt {
	t.Helper()
	output := make(retentionLogWriter, 1)
	store := &optimizationLogStore{writer: optimizationLogWriter{output}}
	a := &attempt{
		row: &JobRow{ID: JobID{15: 1}, Fence: 1, Attempt: 1, Kind: "logs"}, baseLogger: base,
		logs: JobLogsConfig{Store: store, Retention: &LogRetentionConfig{MaxRunBytes: 1}},
	}
	a.newLogger(t.Context())
	t.Cleanup(func() { a.closeLog(time.Second) })
	a.logger.InfoContext(t.Context(), "fills the external log")
	select {
	case line := <-output:
		if line != logStoreLimitMessage {
			t.Fatalf("limit marker = %q", line)
		}
	case <-time.After(time.Second):
		t.Fatal("store did not reach its byte limit")
	}
	return a
}

func BenchmarkCappedExternalLog(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "service-disabled"
		handler := slog.DiscardHandler
		if enabled {
			name = "service-enabled"
			handler = slog.NewTextHandler(io.Discard, nil) //nolint:sloglint // Measure service formatting even when its bytes are discarded.
		}
		b.Run(name, func(b *testing.B) {
			a := cappedLogAttempt(b, slog.New(handler))
			ctx := b.Context()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				a.logger.InfoContext(ctx, "later output", "count", 1)
			}
		})
	}
}
