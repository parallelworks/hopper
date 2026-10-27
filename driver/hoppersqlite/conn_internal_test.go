package hoppersqlite

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/hoppermigrate"
)

// TestMayWrite checks which statements run on their own take the write
// lock.
func TestMayWrite(t *testing.T) {
	for query, want := range map[string]bool{
		"SELECT 1":                                           false,
		"select * from t where x = 'INSERT'":                 false,
		"INSERT INTO t VALUES (1) RETURNING x":               true,
		"  -- note\n/* more */ update t SET x = 1":           true,
		"DELETE FROM t":                                      true,
		"REPLACE INTO t VALUES (1)":                          true,
		"WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c": true,
		"WITH c AS (SELECT 1) SELECT * FROM c":               false,
		"PRAGMA journal_mode":                                false,
		"PRAGMA journal_mode = WAL":                          true,
		"CREATE TABLE t (x)":                                 true,
		"ANALYZE t":                                          true,
		"-- only a comment":                                  false,
		"INSERT\nINTO t VALUES (1)":                          true,
		"delete\tFROM t":                                     true,
		"WITH q AS (SELECT name, updated_at FROM hopper_queues) SELECT * FROM q":           false,
		"WITH j AS (SELECT replace(kind, 'a', 'b') AS k FROM hopper_jobs) SELECT k FROM j": false,
		"WITH d AS (SELECT count(*) FROM h WHERE state = 'deleted') SELECT * FROM d":       false,
		"WITH d AS (SELECT 1 AS \"update\") SELECT * FROM d":                               false,
		"WITH d AS (SELECT 1) SELECT * FROM d -- then DELETE":                              false,
		"WITH d AS (SELECT 'it''s; DELETE') SELECT * FROM d":                               false,
		"WITH d AS (SELECT 1) REPLACE INTO t SELECT * FROM d":                              true,
		"SELECT * FROM t WHERE x = 1; DELETE FROM t":                                       true,
		"SELECT ';DELETE FROM t'":                                                          false,
		"PRAGMA wal_checkpoint(TRUNCATE)":                                                  false,
		"PRAGMA main.table_info(t)":                                                        false,
		"PRAGMA user_version(5)":                                                           true,
		"PRAGMA main.journal_mode = WAL":                                                   true,
		"PRAGMA optimize":                                                                  true,
		"PRAGMA data_version":                                                              false,
		"(SELECT 1)":                                                                       false,
		"VALUES (1)":                                                                       false,
		"EXPLAIN DELETE FROM t":                                                            false,
		"SELECT :update, ?1, $delete":                                                      false,
		"\fDELETE FROM t":                                                                  true,
		"SELECT 1;\f\fUPDATE t SET x = 1":                                                  true,
		"WITH d AS (SELECT 1)\fINSERT INTO t SELECT * FROM d":                              true,
		"\f-- note\n\fREPLACE INTO t VALUES (1)":                                           true,
	} {
		if got := mayWrite(query); got != want {
			t.Errorf("mayWrite(%q) = %v, want %v", query, got, want)
		}
	}
}

// TestClock checks that the database's clock doesn't go back when the
// host's goes back by up to maxClockLead, and follows the host's when it
// goes back further, as a clock set far ahead and then corrected does,
// saying so.
func TestClock(t *testing.T) {
	var logged bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logged, nil))
	db := &database{}
	first := db.clock(log)
	if second := db.clock(log); second.Before(first) {
		t.Errorf("the clock went back from %s to %s", first, second)
	}
	// As if the host's clock was stepped back by less than the lead.
	ahead := time.Now().Add(maxClockLead / 2).UnixMicro()
	db.last.Store(ahead)
	if got := db.clock(log); got.UnixMicro() != ahead {
		t.Errorf("clock = %s, want the latest time it read, %s", got, time.UnixMicro(ahead))
	}
	if logged.Len() != 0 {
		t.Errorf("the clock logged %q while it held the time", logged.String())
	}
	// As if the host's clock was set a year ahead, read, and corrected.
	db.last.Store(time.Now().AddDate(1, 0, 0).UnixMicro())
	before := time.Now()
	got := db.clock(log)
	if got.Before(before.Add(-time.Millisecond)) || got.After(time.Now()) {
		t.Errorf("clock = %s after the host's clock came back a year, want the host's, %s", got, before)
	}
	if !strings.Contains(logged.String(), "following the host's clock") {
		t.Errorf("the clock didn't say it followed the host's back: %q", logged.String())
	}
	if again := db.clock(log); again.Before(got) {
		t.Errorf("the clock went back from %s to %s once it followed the host's", got, again)
	}
}

// TestClockLeadKeepsShortLeases checks the clock's lead at the shortest
// lease TTL hopper allows, 1s: a lease renewed a third of its TTL ago, when
// its next renewal is due, isn't expired to a clock that leads the host's
// by up to maxClockLead, so a leader that reads that clock rescues none of
// its client's jobs.
func TestClockLeadKeepsShortLeases(t *testing.T) {
	const ttl = time.Second
	ctx := context.Background()
	d, err := Open(filepath.Join(t.TempDir(), "hopper.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}); err != nil {
		t.Fatal(err)
	}
	x := d.Executor()
	id, err := x.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "test", TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(ttl / 3)
	// As if the host's clock read maxClockLead ahead and was stepped back.
	d.db.last.Store(time.Now().Add(maxClockLead - time.Millisecond).UnixMicro())
	if n, err := x.ClientPruneExpired(ctx); err != nil || n != 0 {
		t.Fatalf("pruned %d clients, %v; the lease was renewed on time", n, err)
	}
	if res, err := x.ClientRenew(ctx, driver.ClientRenewParams{ClientID: id, TTL: ttl}); err != nil || !res.Renewed {
		t.Errorf("the lease wasn't renewed: %+v, %v", res, err)
	}
}

type owner struct{ name string }

func (o *owner) String() string { return o.name }

type value []int

func (value) String() string { return "value" }

// TestOwnerWaits checks that a write fails at once, naming the owner, when
// a transaction of its own owner holds the lock, and that one of another
// owner waits, saying who holds the lock when its context ends.
func TestOwnerWaits(t *testing.T) {
	db := &database{lock: make(chan struct{}, 1)}
	a, b := &owner{"job a"}, &owner{"job b"}
	if err := db.acquire(context.Background(), a, true); err != nil {
		t.Fatal(err)
	}
	err := db.acquire(context.Background(), a, true)
	if !errors.Is(err, ErrLockHeldByOwner) || !strings.Contains(err.Error(), "job a") {
		t.Errorf("acquire by the holder's owner = %v, want ErrLockHeldByOwner", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = db.acquire(ctx, b, true)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "job a") {
		t.Errorf("acquire by another owner = %v, want it to wait and name the holder", err)
	}
	if sameOwner(value{1}, value{1}) || sameOwner(nil, nil) || !sameOwner(a, a) || sameOwner(a, b) {
		t.Error("sameOwner compares owners other than by identity, or panics")
	}
	db.release()
	if err := db.acquire(context.Background(), b, true); err != nil {
		t.Fatal(err)
	}
	db.release()

	// A lock held by a transaction the driver began for one operation, or
	// by a statement on its own, is waited for by its owner's other writes.
	if err := db.acquire(context.Background(), a, false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := db.acquire(ctx, a, false); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("acquire by the owner of a lock no caller's transaction holds = %v, want it to wait", err)
	}
	db.release()
}
