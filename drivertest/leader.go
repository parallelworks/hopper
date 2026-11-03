package drivertest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// leaderAttempt takes or renews the leader lease for id.
func leaderAttempt(ctx context.Context, t *testing.T, exec driver.Executor, id int64) driver.LeaderResult {
	t.Helper()
	res, err := exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: id, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func requestResign(ctx context.Context, t *testing.T, exec driver.Executor) int64 {
	t.Helper()
	id, err := exec.LeaderRequestResign(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// testLeaderResignRequest checks a request that the leader resign: it
// names the holder of a live lease, whose renewals report it until the
// holder resigns; it asks no one with no live lease; and a client that
// takes the lease over is not asked.
func testLeaderResignRequest[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	a, b := register(ctx, t, exec), register(ctx, t, exec)

	if id := requestResign(ctx, t, exec); id != 0 {
		t.Fatalf("request with no leader asked client %d", id)
	}
	if res := leaderAttempt(ctx, t, exec, a); !res.Leader || res.ResignRequested {
		t.Fatalf("first attempt = %+v, want the lease and no request", res)
	}
	if id := requestResign(ctx, t, exec); id != a {
		t.Fatalf("request asked client %d, want the leader %d", id, a)
	}
	if res := leaderAttempt(ctx, t, exec, b); res.Leader || res.ResignRequested {
		t.Fatalf("other client's attempt = %+v, want neither the lease nor the request", res)
	}
	for range 2 {
		if res := leaderAttempt(ctx, t, exec, a); !res.Leader || !res.ResignRequested {
			t.Fatalf("leader's renewal after the request = %+v, want the lease with the request, until it resigns", res)
		}
	}
	if err := exec.LeaderResign(ctx, a); err != nil {
		t.Fatal(err)
	}
	if res := leaderAttempt(ctx, t, exec, b); !res.Leader || res.ResignRequested {
		t.Fatalf("election after the resign = %+v, want the lease and no request", res)
	}

	// A request of a lease that expired asks no one; one made of a live
	// lease does not pass to the client that takes it over once it expires.
	f.ExpireLeader(ctx, t, d)
	if id := requestResign(ctx, t, exec); id != 0 {
		t.Fatalf("request of an expired lease asked client %d", id)
	}
	if res := leaderAttempt(ctx, t, exec, b); !res.Leader || res.ResignRequested {
		t.Fatalf("renewal of an expired lease = %+v, want the lease and no request", res)
	}
	if id := requestResign(ctx, t, exec); id != b {
		t.Fatalf("request asked client %d, want %d", id, b)
	}
	f.ExpireLeader(ctx, t, d)
	if res := leaderAttempt(ctx, t, exec, a); !res.Leader || res.ResignRequested {
		t.Fatalf("takeover of a lease its holder was asked to give up = %+v, want the lease and no request", res)
	}
	if res := leaderAttempt(ctx, t, exec, b); res.Leader || res.ResignRequested {
		t.Fatalf("former holder's attempt = %+v, want neither", res)
	}
}

// testLeaderResignRacesRenewal makes requests while the leader renews,
// again and again: a request that names the leader is never lost to a
// renewal it races, so the leader's next renewal reports it.
func testLeaderResignRacesRenewal[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	a := register(ctx, t, exec)
	for round := range 20 {
		if res := leaderAttempt(ctx, t, exec, a); !res.Leader || res.ResignRequested {
			t.Fatalf("round %d: election = %+v, want the lease and no request", round, res)
		}
		var (
			wg    sync.WaitGroup
			asked int64
		)
		wg.Go(func() {
			for range 5 {
				if _, err := exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: a, TTL: time.Hour}); err != nil {
					t.Error(err)
				}
			}
		})
		wg.Go(func() {
			id, err := exec.LeaderRequestResign(ctx)
			if err != nil {
				t.Error(err)
			}
			asked = id
		})
		wg.Wait()
		if asked != a {
			t.Fatalf("round %d: request asked client %d, want the leader %d", round, asked, a)
		}
		if res := leaderAttempt(ctx, t, exec, a); !res.Leader || !res.ResignRequested {
			t.Fatalf("round %d: renewal after a request that raced renewals = %+v, want the request kept", round, res)
		}
		if err := exec.LeaderResign(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
}

// testLeaderResignRacesElection makes a request while another client takes
// over an expired lease, again and again: the request asks either no one,
// before the takeover, or the new leader, after it, never the expired
// holder; and the new leader is asked if and only if the request named it.
func testLeaderResignRacesElection[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	a, b := register(ctx, t, exec), register(ctx, t, exec)
	for round := range 20 {
		if res := leaderAttempt(ctx, t, exec, a); !res.Leader {
			t.Fatalf("round %d: election = %+v", round, res)
		}
		f.ExpireLeader(ctx, t, d)
		var (
			wg      sync.WaitGroup
			asked   int64
			elected driver.LeaderResult
		)
		wg.Go(func() {
			res, err := exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: b, TTL: time.Hour})
			if err != nil {
				t.Error(err)
			}
			elected = res
		})
		wg.Go(func() {
			id, err := exec.LeaderRequestResign(ctx)
			if err != nil {
				t.Error(err)
			}
			asked = id
		})
		wg.Wait()
		if !elected.Leader || elected.ResignRequested {
			t.Fatalf("round %d: takeover = %+v, want the lease and no request", round, elected)
		}
		if asked != 0 && asked != b {
			t.Fatalf("round %d: request asked client %d, want no one or the new leader %d", round, asked, b)
		}
		if res := leaderAttempt(ctx, t, exec, b); !res.Leader || res.ResignRequested != (asked == b) {
			t.Fatalf("round %d: new leader's renewal = %+v after a request that asked client %d", round, res, asked)
		}
		if err := exec.LeaderResign(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
}
