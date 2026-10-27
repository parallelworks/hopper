package hopper_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/hopper"
)

type depStep struct {
	Workflow int    `json:"workflow"`
	Name     string `json:"name"`
	Fail     bool   `json:"fail,omitempty"`
}

func (depStep) Kind() string { return "dep_step" }

type depOutput struct {
	Workflow int    `json:"workflow"`
	Name     string `json:"name"`
}

// depRecorder keeps the dependencies each step's claims carried.
type depRecorder struct {
	mu   sync.Mutex
	seen map[string][][]hopper.Dependency
}

func (r *depRecorder) record(key string, deps []hopper.Dependency) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen == nil {
		r.seen = map[string][][]hopper.Dependency{}
	}
	r.seen[key] = append(r.seen[key], deps)
}

func (r *depRecorder) get(key string) [][]hopper.Dependency {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[key]
}

func depWorker(rec *depRecorder) func(context.Context, *hopper.Job[depStep]) error {
	return func(ctx context.Context, job *hopper.Job[depStep]) error {
		rec.record(fmt.Sprintf("%d/%s", job.Args.Workflow, job.Args.Name), job.Dependencies)
		if err := hopper.SetOutput(ctx, depOutput{Workflow: job.Args.Workflow, Name: job.Args.Name}); err != nil {
			return err
		}
		if job.Args.Fail {
			return hopper.Cancel(errors.New("step failed"))
		}
		return nil
	}
}

func TestWorkflowDependencies(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var rec depRecorder
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, depWorker(&rec))
	c := h.started(workers, 4)

	wf := hopper.NewWorkflow("deps", nil)
	build := wf.Add(depStep{Name: "build"}, &hopper.StepOpts{Name: "build"})
	lint := wf.Add(depStep{Name: "lint", Fail: true}, &hopper.StepOpts{Name: "lint"})
	report := wf.Add(depStep{Name: "report"}, &hopper.StepOpts{Name: "report", After: []hopper.Step{build, lint},
		OnDependencyFailure: hopper.DependencyIgnore})
	wf.Add(depStep{Name: "last"}, hopper.After(report))
	res, err := c.InsertWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, res.Jobs[3].Job.ID, hopper.JobStateCompleted)

	if got := rec.get("0/build"); len(got) != 1 || got[0] != nil {
		t.Errorf("build ran with dependencies %+v", got)
	}
	got := rec.get("0/report")
	if len(got) != 1 || len(got[0]) != 2 {
		t.Fatalf("report ran with dependencies %+v", got)
	}
	job := &hopper.JobRow{Dependencies: got[0]}
	for name, state := range map[string]hopper.JobState{"build": hopper.JobStateCompleted, "lint": hopper.JobStateDiscarded} {
		d, ok := job.Dependency(name)
		var out depOutput
		if !ok || d.State != state || json.Unmarshal(d.Output, &out) != nil || out.Name != name {
			t.Errorf("report's dependency %q = %+v, %v", name, d, ok)
		}
	}
	if got := rec.get("0/last"); len(got) != 1 || len(got[0]) != 1 || got[0][0].Step != "report" || got[0][0].State != hopper.JobStateCompleted {
		t.Errorf("last ran with dependencies %+v", got)
	}
	row, err := c.WorkflowGet(ctx, res.ID)
	if err != nil || len(row.Steps) != 3 || row.Steps[res.Jobs[2].Job.ID] != "report" {
		t.Errorf("workflow steps = %v, %v", row.Steps, err)
	}
	waitFor(t, func() bool { return h.count("SELECT count(*) FROM hopper_job_results") == 0 })
}

func TestWorkflowStepNamesAreChecked(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	c := h.client(&hopper.Config{})
	ctx := context.Background()
	for _, tc := range []struct {
		names []string
		err   string
	}{
		{[]string{"a", "a"}, "both named"},
		{[]string{"a\x00b"}, "not valid UTF-8 text"},
		{[]string{"\xff"}, "not valid UTF-8 text"},
	} {
		wf := hopper.NewWorkflow("names", nil)
		for _, n := range tc.names {
			wf.Add(depStep{Name: n}, &hopper.StepOpts{Name: n})
		}
		if _, err := c.InsertWorkflow(ctx, wf); err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("names %q: err = %v, want %q", tc.names, err, tc.err)
		}
	}
	if n := h.count("SELECT count(*) FROM hopper_jobs"); n != 0 {
		t.Errorf("%d jobs inserted by rejected workflows", n)
	}
}

// TestWorkflowDependenciesConcurrent works forty workflows (a root, four
// named steps after it behind ignore edges, of which some fail, and a sink
// after those) on four clients at once. Every step's claim carries exactly
// its dependencies, with the state and output each finished with, and once
// every workflow completes no result is left.
func TestWorkflowDependenciesConcurrent(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var rec depRecorder
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, depWorker(&rec))
	clients := make([]*hopper.Client[pgxTx], 4)
	for i := range clients {
		clients[i] = h.started(workers, 8)
	}
	const workflows, middles = 40, 4
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a fixed sequence for a test
	failed := map[string]bool{}
	var sinks []hopper.JobID
	for w := range workflows {
		wf := hopper.NewWorkflow(fmt.Sprintf("wf-%d", w), nil)
		root := wf.Add(depStep{Workflow: w, Name: "root"}, &hopper.StepOpts{Name: "root"})
		mids := make([]hopper.Step, middles)
		for m := range mids {
			name := fmt.Sprintf("m%d", m)
			fail := rng.IntN(4) == 0
			failed[fmt.Sprintf("%d/%s", w, name)] = fail
			mids[m] = wf.Add(depStep{Workflow: w, Name: name, Fail: fail}, &hopper.StepOpts{Name: name, After: []hopper.Step{root}})
		}
		wf.Add(depStep{Workflow: w, Name: "sink"}, &hopper.StepOpts{After: mids, OnDependencyFailure: hopper.DependencyIgnore})
		res, err := clients[w%len(clients)].InsertWorkflow(ctx, wf)
		if err != nil {
			t.Fatal(err)
		}
		sinks = append(sinks, res.Jobs[middles+1].Job.ID)
	}
	for _, id := range sinks {
		waitForJob(t, clients[0], id, hopper.JobStateCompleted)
	}
	for w := range workflows {
		for m := range middles {
			got := rec.get(fmt.Sprintf("%d/m%d", w, m))
			if len(got) != 1 || len(got[0]) != 1 || got[0][0].Step != "root" || got[0][0].State != hopper.JobStateCompleted {
				t.Errorf("workflow %d's m%d ran with %+v", w, m, got)
			}
		}
		got := rec.get(fmt.Sprintf("%d/sink", w))
		if len(got) != 1 || len(got[0]) != middles {
			t.Errorf("workflow %d's sink ran with %+v", w, got)
			continue
		}
		job := &hopper.JobRow{Dependencies: got[0]}
		for m := range middles {
			name := fmt.Sprintf("m%d", m)
			want := hopper.JobStateCompleted
			if failed[fmt.Sprintf("%d/%s", w, name)] {
				want = hopper.JobStateDiscarded
			}
			d, ok := job.Dependency(name)
			var out depOutput
			if !ok || d.State != want || json.Unmarshal(d.Output, &out) != nil || out.Workflow != w || out.Name != name {
				t.Errorf("workflow %d's sink saw %s as %+v, want %s with its output", w, name, d, want)
			}
		}
	}
	waitFor(t, func() bool { return h.count("SELECT count(*) FROM hopper_job_results") == 0 })
}

type crashStep struct {
	Name string `json:"name"`
}

func (crashStep) Kind() string { return "crash_step" }

// TestWorkflowDependencyOfRescuedJob crashes a client while it runs two
// dependencies. The one with an attempt left runs again elsewhere, and its
// dependent gets the output of the attempt that completed; the one on its
// last attempt is discarded by the rescue, and its dependent, behind an
// ignore edge, gets that state and no output.
func TestWorkflowDependencyOfRescuedJob(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var rec depRecorder
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[crashStep]) error {
		if err := hopper.SetOutput(ctx, fmt.Sprintf("attempt %d", job.Attempt)); err != nil {
			return err
		}
		if job.Attempt == 1 {
			started <- struct{}{}
			<-release
		}
		return nil
	})
	hopper.AddWorkFunc(workers, depWorker(&rec))
	victim := h.started(workers, 4)

	wf := hopper.NewWorkflow("rescued", nil)
	again := wf.Add(crashStep{Name: "again"}, &hopper.StepOpts{Name: "again", Insert: &hopper.InsertOpts{MaxAttempts: 2}})
	lost := wf.Add(crashStep{Name: "lost"}, &hopper.StepOpts{Name: "lost", Insert: &hopper.InsertOpts{MaxAttempts: 1}})
	wf.Add(depStep{Name: "after-again"}, hopper.After(again))
	wf.Add(depStep{Name: "after-lost"}, &hopper.StepOpts{After: []hopper.Step{lost}, OnDependencyFailure: hopper.DependencyIgnore})
	res, err := victim.InsertWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	<-started
	victim.Abandon()
	close(release)
	survivor := h.started(workers, 4)
	waitForJob(t, survivor, res.Jobs[2].Job.ID, hopper.JobStateCompleted)
	waitForJob(t, survivor, res.Jobs[3].Job.ID, hopper.JobStateCompleted)

	got := rec.get("0/after-again")
	if len(got) != 1 || len(got[0]) != 1 || got[0][0].State != hopper.JobStateCompleted || string(got[0][0].Output) != `"attempt 2"` {
		t.Errorf("after-again ran with %+v, want again's second attempt's output", got)
	}
	got = rec.get("0/after-lost")
	if len(got) != 1 || len(got[0]) != 1 || got[0][0].State != hopper.JobStateDiscarded || got[0][0].Output != nil {
		t.Errorf("after-lost ran with %+v, want lost discarded with no output", got)
	}
}
