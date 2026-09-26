package hopper_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

type wfStep struct {
	Name string `json:"name"`
	Fail bool   `json:"fail,omitempty"`
}

func (wfStep) Kind() string { return "wf_step" }

// stepLog records when each step ran.
type stepLog struct {
	mu    sync.Mutex
	order []string
	at    map[string]time.Time
}

func (l *stepLog) record(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.at == nil {
		l.at = map[string]time.Time{}
	}
	l.order = append(l.order, name)
	l.at[name] = time.Now()
}

func (l *stepLog) before(a, b string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	ta, oka := l.at[a]
	tb, okb := l.at[b]
	return oka && okb && ta.Before(tb)
}

func (l *stepLog) ran(name string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.at[name]
	return ok
}

func TestWorkflow(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var (
		log       stepLog
		mu        sync.Mutex
		callbacks []string
	)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[wfStep]) error {
		log.record(job.Args.Name)
		if job.Args.Fail {
			return hopper.Cancel(errors.New("step failed"))
		}
		return nil
	})
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[reportDone]) error {
		mu.Lock()
		callbacks = append(callbacks, "done")
		mu.Unlock()
		return nil
	})
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[alertOps]) error {
		mu.Lock()
		callbacks = append(callbacks, "alert")
		mu.Unlock()
		return nil
	})
	c := h.started(workers, 4)

	// fetch -> parse -> {index, notify}, inside a transaction.
	wf := hopper.NewWorkflow("ingest-9", &hopper.WorkflowOpts{OnSuccess: reportDone{RunID: 9}, OnFailure: alertOps{RunID: 9}})
	fetch := wf.Add(wfStep{Name: "fetch"}, nil)
	parse := wf.Add(wfStep{Name: "parse"}, hopper.After(fetch))
	wf.Add(wfStep{Name: "index"}, hopper.After(parse))
	wf.Add(wfStep{Name: "notify"}, &hopper.StepOpts{After: []hopper.Step{parse}, Insert: &hopper.InsertOpts{Priority: hopper.PriorityHigh}})
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.InsertWorkflowTx(ctx, tx, wf)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if len(res.Jobs) != 4 || res.Jobs[0].Job.State != hopper.JobStateAvailable || res.Jobs[1].Job.State != hopper.JobStatePending ||
		res.Jobs[3].Job.Priority != int(hopper.PriorityHigh) || res.Jobs[1].Job.BatchID != res.ID {
		t.Fatalf("workflow result = %+v", res)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(callbacks) == 1 })
	if !log.before("fetch", "parse") || !log.before("parse", "index") || !log.before("parse", "notify") {
		t.Errorf("steps ran out of order: %v", log.order)
	}
	row, err := c.WorkflowGet(ctx, res.ID)
	if err != nil || row.Batch.Name != "ingest-9" || row.Batch.Pending != 0 || row.Batch.Failed != 0 || len(row.Jobs) != 4 || len(row.Edges) != 3 {
		t.Errorf("workflow = %+v, %v", row, err)
	}
	mu.Lock()
	if callbacks[0] != "done" {
		t.Errorf("callbacks = %v", callbacks)
	}
	callbacks = nil
	mu.Unlock()

	// A failing step cancels what depends on it and runs the failure
	// callback; an independent branch still runs.
	wf = hopper.NewWorkflow("ingest-10", &hopper.WorkflowOpts{OnSuccess: reportDone{RunID: 10}, OnFailure: alertOps{RunID: 10}})
	a := wf.Add(wfStep{Name: "a"}, nil)
	b := wf.Add(wfStep{Name: "b", Fail: true}, hopper.After(a))
	wf.Add(wfStep{Name: "c"}, hopper.After(b))
	wf.Add(wfStep{Name: "d"}, hopper.After(a))
	res, err = c.InsertWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(callbacks) == 1 })
	cj := waitForJob(t, c, res.Jobs[2].Job.ID, hopper.JobStateCancelled)
	if len(cj.Errors) != 1 || cj.Errors[0].Error != "hopper: dependency failed" {
		t.Errorf("cancelled step = %+v", cj.Errors)
	}
	if log.ran("c") || !log.ran("d") {
		t.Errorf("ran %v; c must not run, d must", log.order)
	}
	row, err = c.WorkflowGet(ctx, res.ID)
	if err != nil || row.Batch.Failed != 2 || row.Batch.CompletedAt.IsZero() {
		t.Errorf("failed workflow = %+v, %v", row.Batch, err)
	}
	mu.Lock()
	if callbacks[0] != "alert" {
		t.Errorf("callbacks = %v", callbacks)
	}
	mu.Unlock()

	// Validation.
	if _, err := c.InsertWorkflow(ctx, hopper.NewWorkflow("empty", nil)); err == nil {
		t.Error("empty workflow accepted")
	}
	other := hopper.NewWorkflow("other", nil)
	other.Add(wfStep{Name: "x"}, hopper.After(a))
	if _, err := c.InsertWorkflow(ctx, other); err == nil {
		t.Error("step from another workflow accepted")
	}
	wf = hopper.NewWorkflow("unique", nil)
	wf.Add(wfStep{Name: "u"}, &hopper.StepOpts{Insert: &hopper.InsertOpts{Unique: &hopper.UniqueOpts{}}})
	if _, err := c.InsertWorkflow(ctx, wf); err == nil {
		t.Error("unique step accepted")
	}
}
