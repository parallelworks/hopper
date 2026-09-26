// Package hopperotel instruments hopper with OpenTelemetry.
//
//	client, err := hopper.NewClient(driver, &hopper.Config{
//		Middleware: []hopper.Middleware{hopperotel.Middleware(otel.GetTracerProvider())},
//	})
//	unregister, err := hopperotel.RegisterStats(client, otel.GetMeterProvider())
//
// The middleware propagates trace context from insert to work through the
// job's metadata, records a span per attempt, and counts attempts and their
// duration by kind, queue and outcome. RegisterStats adds observable gauges
// for queue depth and the age of the oldest claimable job.
package hopperotel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/parallelworks/hopper"
)

const scope = "github.com/parallelworks/hopper/hopperotel"

// Config selects the providers. Nil fields use the globals from the otel
// package.
type Config struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Propagator     propagation.TextMapPropagator
}

// Middleware returns tracing and metrics middleware on the given tracer
// provider and the global meter provider and propagator.
func Middleware(tp trace.TracerProvider) hopper.Middleware {
	return New(&Config{TracerProvider: tp})
}

// New returns tracing and metrics middleware.
func New(cfg *Config) hopper.Middleware {
	if cfg == nil {
		cfg = &Config{}
	}
	tp, mp, prop := cfg.TracerProvider, cfg.MeterProvider, cfg.Propagator
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	if prop == nil {
		prop = otel.GetTextMapPropagator()
	}
	meter := mp.Meter(scope)
	m := &middleware{tracer: tp.Tracer(scope), propagator: prop}
	// Instrument creation only fails on invalid names, which are constant.
	m.attempts, _ = meter.Int64Counter("hopper.job.attempts", metric.WithDescription("Job attempts by outcome."), metric.WithUnit("{attempt}"))
	m.duration, _ = meter.Float64Histogram("hopper.job.duration", metric.WithDescription("Job attempt duration."), metric.WithUnit("s"))
	m.inserts, _ = meter.Int64Counter("hopper.job.inserts", metric.WithDescription("Jobs inserted."), metric.WithUnit("{job}"))
	return m
}

type middleware struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	attempts   metric.Int64Counter
	duration   metric.Float64Histogram
	inserts    metric.Int64Counter
}

// Insert starts a producer span and injects its context into each job's
// metadata, so the worker's span joins the same trace.
func (m *middleware) Insert(ctx context.Context, params []hopper.InsertParams, next func(context.Context, []hopper.InsertParams) ([]*hopper.InsertResult, error)) ([]*hopper.InsertResult, error) {
	ctx, span := m.tracer.Start(ctx, "hopper.insert", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.Int("hopper.job.count", len(params))))
	defer span.End()

	carrier := propagation.MapCarrier{}
	m.propagator.Inject(ctx, carrier)
	tagged := make([]hopper.InsertParams, len(params))
	for i, p := range params {
		var opts hopper.InsertOpts
		if p.Opts != nil {
			opts = *p.Opts
		}
		if a, ok := p.Args.(hopper.JobArgsWithInsertOpts); ok && p.Opts == nil {
			// Keep kind-level metadata defaults when the call passed none.
			opts.Metadata = a.InsertOpts().Metadata
		}
		meta, err := mergeMetadata(opts.Metadata, carrier)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		opts.Metadata = meta
		tagged[i] = hopper.InsertParams{Args: p.Args, Opts: &opts}
	}

	results, err := next(ctx, tagged)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	for _, r := range results {
		if !r.Duplicate {
			m.inserts.Add(ctx, 1, metric.WithAttributes(attribute.String("hopper.job.kind", r.Job.Kind), attribute.String("hopper.queue", r.Job.Queue)))
		}
	}
	return results, nil
}

// Work extracts the trace context from the job's metadata, records a
// consumer span for the attempt and its outcome.
func (m *middleware) Work(ctx context.Context, job *hopper.JobRow, next func(context.Context) error) error {
	if carrier, err := carrierFromMetadata(job.Metadata); err == nil {
		ctx = m.propagator.Extract(ctx, carrier)
	}
	attrs := []attribute.KeyValue{
		attribute.String("hopper.job.id", job.ID.String()),
		attribute.String("hopper.job.kind", job.Kind),
		attribute.String("hopper.queue", job.Queue),
		attribute.Int("hopper.job.attempt", job.Attempt),
		attribute.Int("hopper.job.priority", job.Priority),
	}
	ctx, span := m.tracer.Start(ctx, "hopper.work "+job.Kind, trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	err := next(ctx)
	elapsed := time.Since(start)

	outcome := outcomeOf(err)
	span.SetAttributes(attribute.String("hopper.job.outcome", outcome))
	if err != nil && outcome != "snoozed" {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	metricAttrs := metric.WithAttributes(
		attribute.String("hopper.job.kind", job.Kind),
		attribute.String("hopper.queue", job.Queue),
		attribute.String("hopper.job.outcome", outcome),
	)
	m.attempts.Add(ctx, 1, metricAttrs)
	m.duration.Record(ctx, elapsed.Seconds(), metricAttrs)
	return err
}

func outcomeOf(err error) string {
	var (
		snooze *hopper.SnoozeError
		cancel *hopper.CancelError
	)
	switch {
	case err == nil:
		return "completed"
	case errors.As(err, &snooze):
		return "snoozed"
	case errors.As(err, &cancel):
		return "cancelled"
	default:
		return "failed"
	}
}

// mergeMetadata adds the carrier's keys to a JSON object.
func mergeMetadata(metadata json.RawMessage, carrier propagation.MapCarrier) (json.RawMessage, error) {
	if len(carrier) == 0 {
		return metadata, nil
	}
	obj := map[string]any{}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &obj); err != nil {
			return nil, fmt.Errorf("hopperotel: metadata is not a JSON object: %w", err)
		}
	}
	for k, v := range carrier {
		obj[k] = v
	}
	return json.Marshal(obj)
}

// carrierFromMetadata reads the string fields of a job's metadata.
func carrierFromMetadata(metadata json.RawMessage) (propagation.MapCarrier, error) {
	obj := map[string]any{}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &obj); err != nil {
			return nil, err
		}
	}
	carrier := propagation.MapCarrier{}
	for k, v := range obj {
		if s, ok := v.(string); ok {
			carrier[k] = s
		}
	}
	return carrier, nil
}

// RegisterStats registers observable gauges for queue depth by state
// ("hopper.queue.depth"), the age of the oldest claimable job
// ("hopper.queue.oldest_available") and the number of live clients
// ("hopper.clients"), read from client.Stats on each collection. Call the
// returned function to unregister.
func RegisterStats[TTx any](client *hopper.Client[TTx], mp metric.MeterProvider) (func() error, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(scope)
	depth, err := meter.Int64ObservableGauge("hopper.queue.depth", metric.WithDescription("Live jobs by queue and state."), metric.WithUnit("{job}"))
	if err != nil {
		return nil, err
	}
	oldest, err := meter.Float64ObservableGauge("hopper.queue.oldest_available", metric.WithDescription("Age of the oldest claimable job."), metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	clients, err := meter.Int64ObservableGauge("hopper.clients", metric.WithDescription("Clients with a live lease."), metric.WithUnit("{client}"))
	if err != nil {
		return nil, err
	}
	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		stats, err := client.Stats(ctx)
		if err != nil {
			return err
		}
		for name, q := range stats.Queues {
			queue := attribute.String("hopper.queue", name)
			for state, n := range map[string]int{"available": q.Available, "scheduled": q.Scheduled, "retryable": q.Retryable, "running": q.Running} {
				o.ObserveInt64(depth, int64(n), metric.WithAttributes(queue, attribute.String("hopper.job.state", state)))
			}
			o.ObserveFloat64(oldest, q.OldestAvailable.Seconds(), metric.WithAttributes(queue))
		}
		o.ObserveInt64(clients, int64(stats.LiveClients))
		return nil
	}, depth, oldest, clients)
	if err != nil {
		return nil, err
	}
	return reg.Unregister, nil
}
