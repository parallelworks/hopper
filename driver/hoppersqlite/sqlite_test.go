package hoppersqlite_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hoppersqlite"
)

func TestOpenRefuses(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, c := range []struct {
		path string
		cfg  *hoppersqlite.Config
	}{
		{"", nil},
		{":memory:", nil},
		{"file:" + filepath.Join(dir, "x.db"), nil},
		{filepath.Join(dir, "x.db"), &hoppersqlite.Config{Synchronous: "OFF"}},
		{filepath.Join(dir, "x.db"), &hoppersqlite.Config{ListenInterval: -time.Second}},
		{filepath.Join(dir, "missing", "x.db"), nil},
	} {
		if d, err := hoppersqlite.Open(c.path, c.cfg); err == nil {
			_ = d.Close()
			t.Errorf("Open(%q, %+v) succeeded", c.path, c.cfg)
		}
	}
}

// TestOpen opens one file twice in a process, as two clients of one
// application could: both see each other's writes, share one write lock,
// and keep the database in write-ahead log mode.
func TestOpen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "hopper db?.sqlite")
	first, err := hoppersqlite.Open(path, &hoppersqlite.Config{Synchronous: "normal"})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := hoppersqlite.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var mode string
	if err := second.DB().QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
		t.Errorf("journal mode = %q, %v", mode, err)
	}
	if _, err := first.DB().ExecContext(ctx, "CREATE TABLE t (x INTEGER)"); err != nil {
		t.Fatal(err)
	}
	tx, err := first.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := second.DB().ExecContext(wctx, "INSERT INTO t VALUES (2)"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a write through the other Driver while the first's transaction is open = %v, want it to wait", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.DB().ExecContext(ctx, "INSERT INTO t VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := first.DB().QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil || n != 2 {
		t.Errorf("rows = %d, %v", n, err)
	}
}

// TestTransactions checks how the driver's transactions take the write
// lock: not when they begin but on their first statement, which therefore
// never reads what another transaction changes before it writes; and a
// read-only one not at all.
func TestTransactions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := open(t)
	db := d.DB()
	if _, err := db.ExecContext(ctx, "CREATE TABLE counter (n INTEGER); INSERT INTO counter VALUES (0)"); err != nil {
		t.Fatal(err)
	}
	holder, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.ExecContext(ctx, "UPDATE counter SET n = n + 1"); err != nil {
		t.Fatal(err)
	}

	// Begin returns at once while another transaction writes.
	start := time.Now()
	waiter, err := db.BeginTx(ctx, nil)
	if err != nil || time.Since(start) > time.Second {
		t.Fatalf("BeginTx while another transaction writes = %v after %s", err, time.Since(start))
	}
	// A read-only transaction reads the last committed state meanwhile, and
	// may not write.
	ro, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := ro.QueryRowContext(ctx, "SELECT n FROM counter").Scan(&n); err != nil || n != 0 {
		t.Errorf("read-only read = %d, %v", n, err)
	}
	if _, err := ro.ExecContext(ctx, "UPDATE counter SET n = 100"); err == nil {
		t.Error("a read-only transaction wrote")
	}
	if err := ro.Rollback(); err != nil {
		t.Fatal(err)
	}
	// The waiter's first statement, a read, waits for the lock as long as
	// its context allows.
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := waiter.QueryRowContext(short, "SELECT n FROM counter").Scan(&n); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("first statement while another transaction writes = %v, want it to wait", err)
	}
	// Once the holder commits, the read sees its write, and a write after
	// it succeeds: what it read cannot have gone stale.
	done := make(chan error, 1)
	go func() {
		var seen int
		if err := waiter.QueryRowContext(ctx, "SELECT n FROM counter").Scan(&seen); err != nil {
			done <- err
			return
		}
		if seen != 1 {
			done <- fmt.Errorf("read %d after the holder committed, want 1", seen)
			return
		}
		_, err := waiter.ExecContext(ctx, "UPDATE counter SET n = ?", seen+1)
		done <- err
	}()
	for hoppersqlite.LockWaiters(d) == 0 {
		time.Sleep(time.Millisecond)
	}
	if err := holder.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := waiter.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT n FROM counter").Scan(&n); err != nil || n != 2 {
		t.Errorf("counter = %d, %v; want 2", n, err)
	}
}

type countArgs struct {
	N int `json:"n"`
}

func (countArgs) Kind() string { return "count" }

// TestClient runs hopper's client on the driver: two clients work jobs,
// one with an awaited result and one completed in the worker's own
// transaction, and a workflow, whose steps run after their dependencies.
func TestClient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := open(t)
	var (
		mu   sync.Mutex
		done = map[int]int{}
	)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[countArgs]) error {
		mu.Lock()
		done[job.Args.N]++
		mu.Unlock()
		if job.Args.N == -1 {
			return hopper.SetOutput(ctx, map[string]int{"answer": 42})
		}
		if job.Args.N == -2 {
			client, ok := hopper.ClientFromContext[*sql.Tx](ctx)
			if !ok {
				return errors.New("no client in the worker's context")
			}
			tx, err := d.DB().BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback() //nolint:errcheck // committed below
			if _, err := client.InsertTx(ctx, tx, countArgs{N: -3}, nil); err != nil {
				return err
			}
			if err := hopper.CompleteTx(ctx, tx, job); err != nil {
				return err
			}
			return tx.Commit()
		}
		return nil
	})
	clients := make([]*hopper.Client[*sql.Tx], 2)
	for i := range clients {
		c, err := hopper.NewClient(d, &hopper.Config{
			Queues:       map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 10}},
			Workers:      workers,
			PollInterval: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		defer c.Stop(ctx) //nolint:errcheck // cleanup
		clients[i] = c
	}
	params := make([]hopper.InsertParams, 500)
	for i := range params {
		params[i] = hopper.InsertParams{Args: countArgs{N: i}}
	}
	if _, err := clients[0].InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	awaited, err := clients[1].Insert(ctx, countArgs{N: -1}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clients[1].Insert(ctx, countArgs{N: -2}, nil); err != nil {
		t.Fatal(err)
	}
	wf := hopper.NewWorkflow("chain", nil)
	first := wf.Add(countArgs{N: 1000}, nil)
	second := wf.Add(countArgs{N: 1001}, hopper.After(first))
	wf.Add(countArgs{N: 1002}, hopper.After(second))
	flow, err := clients[0].InsertWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}

	actx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	answer, err := hopper.Await[map[string]int](actx, clients[0], awaited.Job.ID)
	if err != nil || answer["answer"] != 42 {
		t.Fatalf("awaited job = %+v, %v", answer, err)
	}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		mu.Lock()
		finished := len(done)
		mu.Unlock()
		if finished >= 506 { // 500, -1, -2, -3 and the workflow's three
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d jobs worked, want %d", finished, 506)
		}
	}
	mu.Lock()
	if len(done) != 506 {
		t.Errorf("%d jobs worked, want 506", len(done))
	}
	for n, times := range done {
		if times != 1 {
			t.Errorf("job %d worked %d times", n, times)
		}
	}
	mu.Unlock()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		w, err := clients[1].WorkflowGet(ctx, flow.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !w.Batch.CompletedAt.IsZero() {
			var order []int
			for _, j := range w.Jobs {
				var args countArgs
				if err := json.Unmarshal(j.Args, &args); err != nil {
					t.Fatal(err)
				}
				order = append(order, args.N)
				if j.State != hopper.JobStateCompleted {
					t.Errorf("workflow job %d is %s", args.N, j.State)
				}
			}
			if fmt.Sprint(order) != "[1000 1001 1002]" {
				t.Errorf("workflow jobs = %v", order)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow not completed: %+v", w)
		}
	}
}

// helperEnv names the role a re-executed test binary plays: one process of
// several that share a database.
const helperEnv = "HOPPERSQLITE_TEST_HELPER"

// TestHelperProcess is not a test: TestAcrossProcesses runs the test binary
// again with helperEnv set, and this plays the role it names.
func TestHelperProcess(t *testing.T) {
	role := os.Getenv(helperEnv)
	if role == "" {
		t.Skip("run by TestAcrossProcesses")
	}
	ctx := context.Background()
	d, err := hoppersqlite.Open(os.Getenv("HOPPERSQLITE_TEST_DB"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	x := d.Executor()
	switch role {
	case "claim":
		if err := claimAll(ctx, x, os.Stdout); err != nil {
			t.Fatal(err)
		}
	case "notify":
		if _, err := x.JobInsertMany(ctx, []driver.JobInsertParams{{Kind: "k", Queue: "from-another-process", Priority: 2, MaxAttempts: 1}},
			driver.JobInsertOpts{Notify: true}); err != nil {
			t.Fatal(err)
		}
	case "worker":
		chaosWorker(ctx, t, d)
	case "hog":
		hog(ctx, t, d)
	case "hold":
		tx, err := d.DB().BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "UPDATE hopper_queues SET updated_at = updated_at"); err != nil {
			t.Fatal(err)
		}
		fmt.Println("holding")
		time.Sleep(time.Second)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

// claimAll claims and completes jobs of the default queue until none is
// left, writing the ID and fence of each claim to out.
func claimAll(ctx context.Context, exec driver.Executor, out *os.File) error {
	clientID, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "test", TTL: time.Minute})
	if err != nil {
		return err
	}
	w := bufio.NewWriter(out)
	defer w.Flush()
	for {
		res, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 7})
		if err != nil {
			return err
		}
		if len(res.Jobs) == 0 {
			return nil
		}
		fin := make([]driver.JobFinalize, len(res.Jobs))
		for i, j := range res.Jobs {
			fmt.Fprintf(w, "claimed %s %d\n", j.ID, j.Fence)
			fin[i] = driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, Fence: j.Fence, State: driver.JobStateCompleted, Archive: true}
		}
		applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: fin})
		if err != nil {
			return err
		}
		if len(applied) != len(fin) {
			return fmt.Errorf("applied %d of %d results", len(applied), len(fin))
		}
	}
}

// output collects a process's output, which the test reads while the
// process writes it.
type output struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (o *output) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.Write(p)
}

func (o *output) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.String()
}

// helper starts the test binary as a process playing role on the database
// at path.
func helper(t *testing.T, path, role string) (*exec.Cmd, *output) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$", "-test.v")
	cmd.Env = append(os.Environ(), helperEnv+"="+role, "HOPPERSQLITE_TEST_DB="+path)
	out := &output{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd, out
}

// TestAcrossProcesses shares one database among processes: claims by four
// processes and by four goroutines of this one take every job exactly
// once; a notification another process commits reaches this one's
// listener; and a write waits for another process's transaction.
func TestAcrossProcesses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "hopper.db")
	d := openAt(t, path)
	x := d.Executor()

	const jobs = 1200
	p := make([]driver.JobInsertParams, jobs)
	for i := range p {
		p[i] = driver.JobInsertParams{Kind: "k", Queue: "default", Priority: 1 + i%4, MaxAttempts: 1}
	}
	if _, err := x.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	type proc struct {
		cmd *exec.Cmd
		out *output
	}
	var procs []proc
	for range 4 {
		cmd, out := helper(t, path, "claim")
		procs = append(procs, proc{cmd, out})
	}
	outs := make([]*os.File, 4)
	var wg sync.WaitGroup
	for i := range outs {
		f, err := os.Create(filepath.Join(t.TempDir(), "claims"))
		if err != nil {
			t.Fatal(err)
		}
		outs[i] = f
		wg.Go(func() {
			if err := claimAll(ctx, x, f); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	claims := map[string]int{}
	count := func(text string) {
		for _, line := range strings.Split(text, "\n") {
			if id, ok := strings.CutPrefix(line, "claimed "); ok {
				claims[id]++
			}
		}
	}
	for _, p := range procs {
		if err := p.cmd.Wait(); err != nil {
			t.Fatalf("claiming process: %v\n%s", err, p.out)
		}
		count(p.out.String())
	}
	for _, f := range outs {
		b, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
		count(string(b))
	}
	twice := 0
	for _, n := range claims {
		if n != 1 {
			twice++
		}
	}
	if len(claims) != jobs || twice != 0 {
		t.Errorf("%d jobs claimed, %d of them more than once; want %d, once each", len(claims), twice, jobs)
	}
	var history, fenced int
	if err := d.DB().QueryRowContext(ctx, `SELECT count(*), count(*) FILTER (WHERE fence <> 1 OR attempt <> 1 OR state <> 'completed') FROM hopper_job_history`).
		Scan(&history, &fenced); err != nil {
		t.Fatal(err)
	}
	if history != jobs || fenced != 0 {
		t.Errorf("history has %d jobs, %d not completed in one claim; want %d, 0", history, fenced, jobs)
	}

	// A notification committed by another process.
	l, err := d.Listener(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(ctx) //nolint:errcheck // cleanup
	if err := l.Listen(ctx, driver.ChannelInsert); err != nil {
		t.Fatal(err)
	}
	cmd, out := helper(t, path, "notify")
	if err := cmd.Wait(); err != nil {
		t.Fatalf("notifying process: %v\n%s", err, out)
	}
	nctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if n, err := l.Next(nctx); err != nil || n.Payload != "from-another-process" {
		t.Errorf("notification from another process = %+v, %v", n, err)
	}

	// A write waits for another process's transaction, as long as its
	// context allows.
	cmd, out = helper(t, path, "hold")
	for !strings.Contains(out.String(), "holding") {
		time.Sleep(5 * time.Millisecond)
	}
	short, cancelShort := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelShort()
	if err := x.QueueEnsure(short, []string{"q"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a write while another process's transaction is open = %v, want it to wait", err)
	}
	if err := x.QueueEnsure(ctx, []string{"q"}); err != nil {
		t.Errorf("a write once it ends = %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("holding process: %v\n%s", err, out)
	}
}
