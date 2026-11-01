package hopper_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

// TestWorkflowAfterAny runs a step that waits for any one of a matrix of
// steps, one of which completes, and another that waits for a matrix whose
// steps all fail, which is canceled. The first is claimed with the steps of
// its group that had finished, and the workflow completes once every step,
// those of the group still running among them, has finished.
func TestWorkflowAfterAny(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var rec depRecorder
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, depWorker(&rec))
	c := h.started(workers, 4)

	wf := hopper.NewWorkflow("any", nil)
	setup := wf.Add(depStep{Name: "setup"}, &hopper.StepOpts{Name: "setup"})
	var matrix, failing []hopper.Step
	for i, name := range []string{"m0", "m1", "m2"} {
		matrix = append(matrix, wf.Add(depStep{Name: name, Fail: i != 1}, &hopper.StepOpts{Name: name, After: []hopper.Step{setup}}))
		failing = append(failing, wf.Add(depStep{Name: "f" + name, Fail: true}, nil))
	}
	opts := hopper.AfterAny(matrix...)
	opts.After, opts.Name, opts.Dependencies = []hopper.Step{setup}, "report", true
	wf.Add(depStep{Name: "report"}, opts)                       // step 7
	wf.Add(depStep{Name: "never"}, hopper.AfterAny(failing...)) // step 8
	res, err := c.InsertWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, res.Jobs[7].Job.ID, hopper.JobStateCompleted)
	waitForJob(t, c, res.Jobs[8].Job.ID, hopper.JobStateCanceled)
	got := rec.get("0/report")
	if len(got) != 1 {
		t.Fatalf("report ran %d times", len(got))
	}
	job := &hopper.JobRow{Dependencies: got[0]}
	if d, ok := job.Dependency("m1"); !ok || d.State != hopper.JobStateCompleted {
		t.Errorf("report's dependency m1 = %+v, %v; want it completed", d, ok)
	}
	if d, ok := job.Dependency("setup"); !ok || d.State != hopper.JobStateCompleted {
		t.Errorf("report's dependency setup = %+v, %v", d, ok)
	}
	if rec.get("0/never") != nil {
		t.Error("the step after a matrix that failed ran")
	}
	row, err := c.WorkflowGet(ctx, res.ID)
	if err != nil {
		t.Fatal(err)
	}
	groups := 0
	for _, e := range row.Edges {
		if e.AnyOf != 0 {
			groups++
		}
	}
	if groups != 6 {
		t.Errorf("%d edges in groups, want 6", groups)
	}
	waitFor(t, func() bool {
		b, err := c.BatchGet(ctx, res.ID)
		return err == nil && !b.CompletedAt.IsZero()
	})
	waitFor(t, func() bool { return h.count("SELECT count(*) FROM hopper_job_results") == 0 })
}

func TestWorkflowAfterAnyIsChecked(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	c := h.client(&hopper.Config{})
	ctx := context.Background()
	other := hopper.NewWorkflow("other", nil)
	foreign := other.Add(depStep{}, nil)
	for _, tc := range []struct {
		name  string
		build func(wf *hopper.Workflow)
		err   string
	}{
		{"empty group", func(wf *hopper.Workflow) {
			wf.Add(depStep{}, &hopper.StepOpts{AfterAny: [][]hopper.Step{{}}})
		}, "empty any-of group"},
		{"in two groups", func(wf *hopper.Workflow) {
			a, b := wf.Add(depStep{}, nil), wf.Add(depStep{}, nil)
			wf.Add(depStep{}, &hopper.StepOpts{AfterAny: [][]hopper.Step{{a, b}, {b}}})
		}, "two places"},
		{"in a group and After", func(wf *hopper.Workflow) {
			a, b := wf.Add(depStep{}, nil), wf.Add(depStep{}, nil)
			wf.Add(depStep{}, &hopper.StepOpts{After: []hopper.Step{a}, AfterAny: [][]hopper.Step{{a, b}}})
		}, "two places"},
		{"another workflow's step", func(wf *hopper.Workflow) {
			wf.Add(depStep{}, hopper.AfterAny(foreign))
		}, "another workflow"},
	} {
		wf := hopper.NewWorkflow("bad", nil)
		tc.build(wf)
		if _, err := c.InsertWorkflow(ctx, wf); err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("%s: err = %v, want %q", tc.name, err, tc.err)
		}
	}
	if n := h.count("SELECT count(*) FROM hopper_jobs"); n != 0 {
		t.Errorf("%d jobs inserted by rejected workflows", n)
	}
}

// TestWorkflowAfterAnyExhaustedByCascade cancels a step whose any-of group
// a cascade exhausted when the finalizing client follows up, not at the
// leader's next sweep: s1 waits for s0, and s2 for any of s1, so s0's
// failure cancels s1 by cascade, which the finalize returns to the client
// for its follow-up. The leader passes once an hour, after its first.
func TestWorkflowAfterAnyExhaustedByCascade(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var rec depRecorder
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, depWorker(&rec))
	tun := fastTuning
	tun.LeaderInterval, tun.LeaderTTL = time.Hour, time.Hour
	c := h.startedWith(workers, 4, tun)
	waitFor(t, c.IsLeader)
	time.Sleep(300 * time.Millisecond) // the leader's first pass, on an empty schema

	wf := hopper.NewWorkflow("cascade", nil)
	s0 := wf.Add(depStep{Name: "s0", Fail: true}, nil)
	s1 := wf.Add(depStep{Name: "s1"}, hopper.After(s0))
	wf.Add(depStep{Name: "s2"}, hopper.AfterAny(s1))
	res, err := c.InsertWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, res.Jobs[0].Job.ID, hopper.JobStateDiscarded, hopper.JobStateCanceled)
	deadline := time.Now().Add(time.Second)
	for {
		j, err := c.JobGet(ctx, res.Jobs[2].Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if j.State == hopper.JobStateCanceled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("s2 is %s a second after s0 failed; want it canceled by the finalizing client's follow-up", j.State)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rec.get("0/s1") != nil || rec.get("0/s2") != nil {
		t.Error("a step after the failed step ran")
	}
}
