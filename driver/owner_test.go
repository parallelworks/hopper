package driver

import (
	"context"
	"testing"
)

type attempt struct{ name string }

func (a *attempt) String() string { return a.name }

// TestOwner checks that a context carries the owner WithOwner gives it,
// and that one without an owner, or given a nil one, has none.
func TestOwner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if Owner(ctx) != nil {
		t.Error("a context without an owner has one")
	}
	a := &attempt{"job 1"}
	if got := Owner(WithOwner(ctx, a)); got != a {
		t.Errorf("Owner = %v, want %v", got, a)
	}
	if got := Owner(WithOwner(WithOwner(ctx, a), nil)); got != nil {
		t.Errorf("Owner after WithOwner(nil) = %v, want none", got)
	}
}
