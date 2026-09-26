package hopperotel_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/hopperotel"
	"github.com/parallelworks/hopper/internal/testdb"
)

type ping struct {
	Fail bool `json:"fail"`
}

func (ping) Kind() string { return "ping" }

func TestMiddlewareTracesAndMeasures(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	done := make(chan struct{}, 10)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[ping]) error {
		defer func() { done <- struct{}{} }()
		if job.Args.Fail {
			return errors.New("boom")
		}
		return nil
	})
	client, err := hopper.NewClient(hopperpgx.New(pool), &hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2}},
		Workers: workers,
		Logger:  slog.New(slog.DiscardHandler),
		Middleware: []hopper.Middleware{hopperotel.New(&hopperotel.Config{
			TracerProvider: tp, MeterProvider: mp, Propagator: propagation.TraceContext{},
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	unregister, err := hopperotel.RegisterStats(client, mp)
	if err != nil {
		t.Fatal(err)
	}
	defer unregister() //nolint:errcheck // cleanup
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Stop(ctx) //nolint:errcheck // cleanup

	// Insert inside a parent span, so the work span can be linked to it.
	pctx, parent := tp.Tracer("test").Start(ctx, "request")
	res, err := client.Insert(pctx, ping{}, &hopper.InsertOpts{Metadata: []byte(`{"tenant":"a"}`)})
	if err != nil {
		t.Fatal(err)
	}
	parent.End()
	if _, err := client.Insert(ctx, ping{Fail: true}, &hopper.InsertOpts{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("jobs did not run")
		}
	}
	// The finalize lands shortly after the worker returns.
	time.Sleep(200 * time.Millisecond)

	// Metadata carries the trace context alongside the caller's fields.
	job, err := client.JobGet(ctx, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if s := string(job.Metadata); !contains(s, `"tenant": "a"`) || !contains(s, "traceparent") {
		t.Errorf("metadata = %s", s)
	}

	// Spans: request -> hopper.insert; hopper.work with the request's trace.
	var insertSpan, workSpan, failedSpan tracetest.SpanStub
	for _, s := range exporter.GetSpans() {
		switch s.Name {
		case "hopper.insert":
			if s.Parent.SpanID() == parent.SpanContext().SpanID() {
				insertSpan = s
			}
		case "hopper.work ping":
			if attr(s.Attributes, "hopper.job.outcome") == "completed" {
				workSpan = s
			} else {
				failedSpan = s
			}
		}
	}
	if insertSpan.Name == "" || workSpan.Name == "" || failedSpan.Name == "" {
		t.Fatalf("spans = %v", names(exporter.GetSpans()))
	}
	if workSpan.SpanContext.TraceID() != parent.SpanContext().TraceID() {
		t.Errorf("work span is not in the request's trace")
	}
	if attr(workSpan.Attributes, "hopper.job.id") != res.Job.ID.String() || attr(workSpan.Attributes, "hopper.job.kind") != "ping" {
		t.Errorf("work span attributes = %v", workSpan.Attributes)
	}
	if failedSpan.Status.Code.String() != "Error" || len(failedSpan.Events) == 0 {
		t.Errorf("failed span = status %v, events %v", failedSpan.Status, failedSpan.Events)
	}

	// Metrics: attempts by outcome, durations, inserts, and the stats gauges.
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			got[m.Name] = true
			if m.Name == "hopper.job.attempts" {
				sum := m.Data.(metricdata.Sum[int64])
				outcomes := map[string]int64{}
				for _, dp := range sum.DataPoints {
					v, _ := dp.Attributes.Value("hopper.job.outcome")
					outcomes[v.AsString()] += dp.Value
				}
				if outcomes["completed"] != 1 || outcomes["failed"] != 1 {
					t.Errorf("attempts by outcome = %v", outcomes)
				}
			}
		}
	}
	for _, name := range []string{"hopper.job.attempts", "hopper.job.duration", "hopper.job.inserts", "hopper.queue.depth", "hopper.queue.oldest_available", "hopper.clients"} {
		if !got[name] {
			t.Errorf("metric %s not collected; got %v", name, got)
		}
	}
}

func attr(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.AsString()
		}
	}
	return ""
}

func names(spans tracetest.SpanStubs) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.Name
	}
	return out
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
