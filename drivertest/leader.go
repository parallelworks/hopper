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

// requestResign asks the leader leaderID names (0 for any) to resign.
func requestResign(ctx context.Context, t *testing.T, exec driver.Executor, leaderID int64) int64 {
	t.Helper()
	id, err := exec.LeaderRequestResign(ctx, leaderID)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// testLeaderResignRequest checks a request that the leader resign: it
// names the holder of a live lease, whose renewals report it until the
// holder resigns; it asks no one with no live lease, or when it names a
// client that does not hold it; and a client that takes the lease over is
// not asked.
func testLeaderResignRequest[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	a, b := register(ctx, t, exec), register(ctx, t, exec)

	if id := requestResign(ctx, t, exec, 0); id != 0 {
		t.Fatalf("request with no leader asked client %d", id)
	}
	if id := requestResign(ctx, t, exec, a); id != 0 {
		t.Fatalf("request of client %d with no leader asked client %d", a, id)
	}
	if res := leaderAttempt(ctx, t, exec, a); !res.Leader || res.ResignRequested {
		t.Fatalf("first attempt = %+v, want the lease and no request", res)
	}
	clients, err := exec.ClientList(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range clients {
		if c.Leader != (c.ID == a) {
			t.Errorf("client %d listed as leader: %v, want %v", c.ID, c.Leader, c.ID == a)
		}
	}
	// A request that names a client that does not lead asks no one.
	if id := requestResign(ctx, t, exec, b); id != 0 {
		t.Fatalf("request of the non-leader %d asked client %d", b, id)
	}
	if res := leaderAttempt(ctx, t, exec, a); !res.Leader || res.ResignRequested {
		t.Fatalf("leader's renewal after a request of another client = %+v, want the lease and no request", res)
	}
	if id := requestResign(ctx, t, exec, a); id != a {
		t.Fatalf("request of the leader asked client %d, want %d", id, a)
	}
	if _, err := exec.LeaderResign(ctx, a); err != nil {
		t.Fatal(err)
	}
	if res := leaderAttempt(ctx, t, exec, a); !res.Leader || res.ResignRequested {
		t.Fatalf("election after the resign = %+v, want the lease and no request", res)
	}
	if id := requestResign(ctx, t, exec, 0); id != a {
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
	if _, err := exec.LeaderResign(ctx, a); err != nil {
		t.Fatal(err)
	}
	if res := leaderAttempt(ctx, t, exec, b); !res.Leader || res.ResignRequested {
		t.Fatalf("election after the resign = %+v, want the lease and no request", res)
	}

	// A request of a lease that expired asks no one; one made of a live
	// lease does not pass to the client that takes it over once it expires.
	f.ExpireLeader(ctx, t, d)
	if clients, err := exec.ClientList(ctx); err != nil {
		t.Fatal(err)
	} else {
		for _, c := range clients {
			if c.Leader {
				t.Errorf("client %d listed as leader of an expired lease", c.ID)
			}
		}
	}
	for _, of := range []int64{0, b} {
		if id := requestResign(ctx, t, exec, of); id != 0 {
			t.Fatalf("request (of %d) of an expired lease asked client %d", of, id)
		}
	}
	if res := leaderAttempt(ctx, t, exec, b); !res.Leader || res.ResignRequested {
		t.Fatalf("renewal of an expired lease = %+v, want the lease and no request", res)
	}
	if id := requestResign(ctx, t, exec, 0); id != b {
		t.Fatalf("request asked client %d, want %d", id, b)
	}
	f.ExpireLeader(ctx, t, d)
	if res := leaderAttempt(ctx, t, exec, a); !res.Leader || res.ResignRequested {
		t.Fatalf("takeover of a lease its holder was asked to give up = %+v, want the lease and no request", res)
	}
	if res := leaderAttempt(ctx, t, exec, b); res.Leader || res.ResignRequested {
		t.Fatalf("former holder's attempt = %+v, want neither", res)
	}
	// A term is one live lease: a holder whose renewal comes late, after
	// its lease lapsed, starts a new term, which the request does not reach.
	if id := requestResign(ctx, t, exec, a); id != a {
		t.Fatalf("request asked client %d, want %d", id, a)
	}
	before := leaderAttempt(ctx, t, exec, a).ElectedAt
	f.ExpireLeader(ctx, t, d)
	if res := leaderAttempt(ctx, t, exec, a); !res.Leader || !res.ElectedAt.After(before) || res.ResignRequested {
		t.Fatalf("holder's late renewal of its lapsed lease = %+v, want a new term (after %v) and no request", res, before)
	}
	// Nor does it reach the holder's process under the ID a fence gave it.
	if id := requestResign(ctx, t, exec, a); id != a {
		t.Fatalf("request asked client %d, want %d", id, a)
	}
	before = leaderAttempt(ctx, t, exec, a).ElectedAt
	f.ExpireLeader(ctx, t, d)
	fenced := register(ctx, t, exec)
	if res := leaderAttempt(ctx, t, exec, fenced); !res.Leader || !res.ElectedAt.After(before) || res.ResignRequested {
		t.Fatalf("takeover under a fenced client's new ID = %+v, want a new term (after %v) and no request", res, before)
	}
}

// testLeaderTerm checks the terms a leadership attempt reports by the
// lease's elected_at: taking a free or expired lease, even the client's
// own, starts a term with a later elected_at, and every renewal of the live
// lease returns the term's again, so a client whose takeover's response was
// lost still learns the new term from its next renewal; and a renewal that
// reports a resign request does not extend the lease, so the lease the
// holder was asked to give up expires when it would have, and another
// client takes it then.
func testLeaderTerm[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	a, b := register(ctx, t, exec), register(ctx, t, exec)
	var term time.Time // the elected_at of the last term an attempt reported
	attempt := func(id int64, ttl time.Duration, leader, newTerm, resign bool, what string) {
		t.Helper()
		res, err := exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: id, TTL: ttl})
		if err != nil {
			t.Fatal(err)
		}
		if res.Leader != leader || res.ResignRequested != resign {
			t.Fatalf("%s = %+v, want leader %v, resign requested %v", what, res, leader, resign)
		}
		if !leader {
			if !res.ElectedAt.IsZero() {
				t.Fatalf("%s: elected_at %v without the lease", what, res.ElectedAt)
			}
			return
		}
		switch {
		case res.ElectedAt.IsZero():
			t.Fatalf("%s: no elected_at", what)
		case newTerm && !res.ElectedAt.After(term):
			t.Fatalf("%s: elected_at %v, want a new term's, after %v", what, res.ElectedAt, term)
		case !newTerm && !res.ElectedAt.Equal(term):
			t.Fatalf("%s: elected_at %v, want the term's %v", what, res.ElectedAt, term)
		}
		term = res.ElectedAt
	}
	attempt(a, time.Hour, true, true, false, "first election")
	attempt(a, time.Hour, true, false, false, "renewal of a live lease")
	attempt(b, time.Hour, false, false, false, "another client's attempt on a live lease")
	attempt(a, time.Hour, true, false, false, "second renewal")
	f.ExpireLeader(ctx, t, d)
	// The response of this takeover is lost to the client: it still learns
	// the new term from the renewal after it.
	attempt(a, time.Hour, true, true, false, "holder's retake of its expired lease")
	attempt(a, time.Hour, true, false, false, "renewal after the retake")
	f.ExpireLeader(ctx, t, d)
	attempt(b, time.Hour, true, true, false, "takeover of an expired lease")
	if _, err := exec.LeaderResign(ctx, b); err != nil {
		t.Fatal(err)
	}

	const short = 1500 * time.Millisecond
	attempt(a, short, true, true, false, "election for a short lease")
	expires := time.Now().Add(short)
	if id := requestResign(ctx, t, exec, a); id != a {
		t.Fatalf("request asked client %d, want %d", id, a)
	}
	for range 2 {
		attempt(a, time.Hour, true, false, true, "renewal after the request")
	}
	time.Sleep(time.Until(expires) + 200*time.Millisecond)
	attempt(b, time.Hour, true, true, false, "takeover once the asked holder's lease was due to expire")
}

// testLeaderResignRacesRenewal makes requests while the leader renews,
// again and again: a request that names the leader is never lost to a
// renewal it races, so the leader's next renewal reports it. Rounds
// alternate between a request of any leader and one that names it.
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
		of := int64(0)
		if round%2 == 1 {
			of = a
		}
		wg.Go(func() {
			id, err := exec.LeaderRequestResign(ctx, of)
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
		if _, err := exec.LeaderResign(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
}

// testLeaderResignRacesElection makes a request while another client takes
// over an expired lease, again and again: the request asks either no one,
// before the takeover, or the new leader, after it, never the expired
// holder; and the new leader is asked if and only if the request named it.
// Rounds cycle through a request of any leader, one that names the expired
// holder (a retry made from a view of the cluster before the takeover),
// which must ask no one, and one that names the new leader.
func testLeaderResignRacesElection[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	a, b := register(ctx, t, exec), register(ctx, t, exec)
	for round := range 30 {
		of := []int64{0, a, b}[round%3]
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
			id, err := exec.LeaderRequestResign(ctx, of)
			if err != nil {
				t.Error(err)
			}
			asked = id
		})
		wg.Wait()
		if !elected.Leader || elected.ResignRequested {
			t.Fatalf("round %d: takeover = %+v, want the lease and no request", round, elected)
		}
		if (asked != 0 && asked != b) || (of == a && asked != 0) {
			t.Fatalf("round %d: request of %d asked client %d, want no one or the new leader %d (no one for a request of the expired holder %d)", round, of, asked, b, a)
		}
		if res := leaderAttempt(ctx, t, exec, b); !res.Leader || res.ResignRequested != (asked == b) {
			t.Fatalf("round %d: new leader's renewal = %+v after a request that asked client %d", round, res, asked)
		}
		if _, err := exec.LeaderResign(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
}
