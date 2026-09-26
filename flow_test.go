package hopper_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

// concurrencyMeter tracks how many jobs run at once, overall and per key.
type concurrencyMeter struct {
	mu      sync.Mutex
	running int
	peak    int
	perKey  map[string]int
	peakKey map[string]int
	total   atomic.Int64
}

func newMeter() *concurrencyMeter {
	return &concurrencyMeter{perKey: map[string]int{}, peakKey: map[string]int{}}
}

func (m *concurrencyMeter) enter(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running++
	m.perKey[key]++
	m.peak = max(m.peak, m.running)
	m.peakKey[key] = max(m.peakKey[key], m.perKey[key])
}

func (m *concurrencyMeter) leave(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running--
	m.perKey[key]--
	m.total.Add(1)
}

func meteredWorkers(m *concurrencyMeter, hold time.Duration) *hopper.Workers {
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[noop]) error {
		m.enter(job.PartitionKey)
		time.Sleep(hold)
		m.leave(job.PartitionKey)
		return nil
	})
	return workers
}

func TestGlobalLimitHoldsAcrossClients(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	m := newMeter()
	workers := meteredWorkers(m, 20*time.Millisecond)
	// Three clients, 8 workers each, but at most 5 running cluster-wide.
	for range 3 {
		c := h.client(&hopper.Config{
			Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 8, GlobalLimit: 5}},
			Workers: workers,
		})
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	inserter := h.client(&hopper.Config{})
	params := make([]hopper.InsertParams, 200)
	for i := range params {
		params[i] = hopper.InsertParams{Args: noop{N: i}}
	}
	if _, err := inserter.InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.total.Load() == 200 })
	if m.peak > 5 {
		t.Errorf("peak concurrency = %d, want at most the global limit 5", m.peak)
	}
	if m.peak < 3 {
		t.Errorf("peak concurrency = %d; the limit is not the bottleneck", m.peak)
	}
	queues, err := inserter.Queues().List(ctx)
	if err != nil || len(queues) != 1 || queues[0].Limits.GlobalLimit != 5 {
		t.Errorf("recorded limits = %v, %v", queues, err)
	}
}

func TestRateLimitHoldsAcrossClients(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	m := newMeter()
	workers := meteredWorkers(m, 0)
	for range 2 {
		c := h.client(&hopper.Config{
			Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 10, RateLimit: hopper.PerSecond(20), RateBurst: 5}},
			Workers: workers,
		})
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	inserter := h.client(&hopper.Config{})
	params := make([]hopper.InsertParams, 60)
	for i := range params {
		params[i] = hopper.InsertParams{Args: noop{N: i}}
	}
	start := time.Now()
	if _, err := inserter.InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	// 5 at once, then 20/s: 60 jobs take at least ~2.75s and not much more.
	waitFor(t, func() bool { return m.total.Load() == 60 })
	took := time.Since(start)
	if took < 2500*time.Millisecond {
		t.Errorf("60 jobs at 20/s (burst 5) finished in %s; the rate limit did not hold", took)
	}
	if took > 6*time.Second {
		t.Errorf("60 jobs at 20/s took %s; producers waited too long", took)
	}
}

func TestPartitionLimitAndPartitionKey(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	m := newMeter()
	workers := meteredWorkers(m, 15*time.Millisecond)
	for range 2 {
		c := h.client(&hopper.Config{
			Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 10, PartitionLimit: 2}},
			Workers: workers,
		})
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	inserter := h.client(&hopper.Config{})
	params := make([]hopper.InsertParams, 90)
	for i := range params {
		params[i] = hopper.InsertParams{Args: noop{N: i}, Opts: &hopper.InsertOpts{PartitionKey: []string{"tenant-a", "tenant-b", "tenant-c"}[i%3]}}
	}
	if _, err := inserter.InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.total.Load() == 90 })
	for key, peak := range m.peakKey {
		if peak > 2 {
			t.Errorf("partition %s ran %d at once, want at most 2", key, peak)
		}
	}
	if m.peak < 3 {
		t.Errorf("overall peak = %d; partitions did not run in parallel", m.peak)
	}
}

func TestSetLimitsAtRuntime(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	m := newMeter()
	workers := meteredWorkers(m, 20*time.Millisecond)
	c := h.started(workers, 8)
	other := h.client(&hopper.Config{})

	// Impose a global limit from another client; the worker picks it up
	// through the control notification.
	if err := other.Queues().SetLimits(ctx, hopper.QueueDefault, hopper.QueueLimits{GlobalLimit: 2}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s, err := c.Stats(ctx)
		return err == nil && s.Queues[hopper.QueueDefault] != nil
	})
	time.Sleep(fastTuning.LeaseRenew * 2) // the renewal path also applies it
	params := make([]hopper.InsertParams, 40)
	for i := range params {
		params[i] = hopper.InsertParams{Args: noop{N: i}}
	}
	if _, err := c.InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.total.Load() == 40 })
	if m.peak > 2 {
		t.Errorf("peak = %d after SetLimits(GlobalLimit: 2)", m.peak)
	}
	// Removing the limit lets the client use all its workers.
	if err := other.Queues().SetLimits(ctx, hopper.QueueDefault, hopper.QueueLimits{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(fastTuning.LeaseRenew * 2)
	m2 := newMeter()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[failing]) error {
		m2.enter("")
		time.Sleep(20 * time.Millisecond)
		m2.leave("")
		return nil
	})
	params = params[:0]
	for i := range 40 {
		params = append(params, hopper.InsertParams{Args: failing{Msg: "n"}, Opts: &hopper.InsertOpts{Priority: hopper.Priority(i%4 + 1)}})
	}
	if _, err := c.InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m2.total.Load() == 40 })
	if m2.peak < 4 {
		t.Errorf("peak = %d after removing the limit; want the client's workers in use", m2.peak)
	}
	if err := other.Queues().SetLimits(ctx, "", hopper.QueueLimits{}); err == nil {
		t.Error("empty queue name accepted")
	}
}

func TestPriorityAgingPromotesWaitingJobs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1, PriorityAging: time.Second}},
		Workers: workers,
	})
	// A low-priority job scheduled for later, so nothing claims it while it
	// ages; the leader promotes it one level per second.
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{Priority: hopper.PriorityLowest, ScheduledAt: time.Now().Add(-3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Queues().Pause(ctx, hopper.QueueDefault); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		job, err := c.JobGet(ctx, res.Job.ID)
		return err == nil && job.Priority < int(hopper.PriorityLowest)
	})
	job, _ := c.JobGet(ctx, res.Job.ID)
	if job.State != hopper.JobStateAvailable {
		t.Errorf("aged job = %+v", job)
	}
}

type shard struct {
	N int `json:"n"`
}

func (shard) Kind() string { return "shard" }

type reportDone struct {
	RunID int `json:"run_id"`
}

func (reportDone) Kind() string { return "report_done" }

type alertOps struct {
	RunID int `json:"run_id"`
}

func (alertOps) Kind() string { return "alert_ops" }

func TestBatchCallbacks(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var (
		mu        sync.Mutex
		callbacks []string
	)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[shard]) error {
		if job.Args.N == 2 {
			return hopper.Cancel(errors.New("shard 2 is bad"))
		}
		return nil
	})
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[reportDone]) error {
		mu.Lock()
		callbacks = append(callbacks, "done:"+string(job.Metadata))
		mu.Unlock()
		return nil
	})
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[alertOps]) error {
		var meta struct {
			BatchID     string `json:"batch_id"`
			BatchFailed int    `json:"batch_failed"`
		}
		if err := json.Unmarshal(job.Metadata, &meta); err != nil {
			return err
		}
		mu.Lock()
		callbacks = append(callbacks, "alert:"+meta.BatchID)
		mu.Unlock()
		if meta.BatchFailed != 1 || job.Args.RunID != 9 {
			t.Errorf("alert callback = %+v meta %+v", job.Args, meta)
		}
		return nil
	})
	c := h.started(workers, 4)

	// A healthy batch, inside a transaction.
	b := c.NewBatch(hopper.BatchOpts{OnSuccess: reportDone{RunID: 8}, OnFailure: alertOps{RunID: 8}})
	for i := range 3 {
		b.Add(shard{N: 10 + i}, nil)
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := b.InsertTx(ctx, tx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if len(ok.Jobs) != 3 || ok.Jobs[0].Job.BatchID != ok.ID {
		t.Fatalf("batch result = %+v", ok)
	}
	// A batch with a failing shard.
	b = c.NewBatch(hopper.BatchOpts{OnSuccess: reportDone{RunID: 9}, OnFailure: alertOps{RunID: 9}})
	b.Add(shard{N: 1}, nil)
	b.Add(shard{N: 2}, nil)
	bad, err := b.Insert(ctx)
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(callbacks) == 2 })
	mu.Lock()
	defer mu.Unlock()
	var sawDone, sawAlert bool
	for _, cb := range callbacks {
		switch {
		case len(cb) > 5 && cb[:5] == "done:":
			sawDone = true
		case cb == "alert:"+bad.ID.String():
			sawAlert = true
		}
	}
	if !sawDone || !sawAlert {
		t.Errorf("callbacks = %v", callbacks)
	}
	row, err := c.BatchGet(ctx, bad.ID)
	if err != nil || row.Pending != 0 || row.Failed != 1 || row.Total != 2 || row.CompletedAt.IsZero() {
		t.Errorf("batch = %+v, %v", row, err)
	}
	if _, err := c.NewBatch(hopper.BatchOpts{}).Insert(ctx); err == nil {
		t.Error("empty batch accepted")
	}
}
