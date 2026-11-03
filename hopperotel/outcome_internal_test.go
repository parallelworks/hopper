package hopperotel

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/parallelworks/hopper"
)

func TestOutcomeOf(t *testing.T) {
	stopped, cancel := context.WithCancelCause(context.Background())
	cancel(hopper.ErrClientStopped)
	fenced, cancelFenced := context.WithCancel(context.Background())
	cancelFenced()
	failed := errors.New("failed")
	for _, tc := range []struct {
		ctx  context.Context
		err  error
		want string
	}{
		{context.Background(), nil, "completed"},
		{context.Background(), hopper.Snooze(0), "snoozed"},
		{context.Background(), hopper.Cancel(failed), "canceled"},
		{context.Background(), failed, "failed"},
		{fenced, failed, "failed"},
		{stopped, failed, "interrupted"},
		{stopped, hopper.Cancel(failed), "canceled"},
		{stopped, fmt.Errorf("set output: %w", hopper.ErrOutputTooLarge), "failed"},
		{stopped, nil, "completed"},
	} {
		if got := outcomeOf(tc.ctx, tc.err); got != tc.want {
			t.Errorf("outcomeOf(%v, %v) = %q, want %q", context.Cause(tc.ctx), tc.err, got, tc.want)
		}
	}
}
