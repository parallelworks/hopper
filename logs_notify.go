package hopper

import (
	"context"
	"errors"
	"sync"
)

// LogNotifications coalesces in-process change hints; its zero value is ready to use.
type LogNotifications struct {
	mu   sync.Mutex
	subs map[JobID]map[chan struct{}]struct{}
}

// Watch subscribes to a job until ctx ends and implements LogStoreWatcher.
func (n *LogNotifications) Watch(ctx context.Context, id JobID) (<-chan struct{}, error) {
	if id.IsZero() {
		return nil, errors.New("hopper: a log subscription requires a job ID")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	if n.subs == nil {
		n.subs = map[JobID]map[chan struct{}]struct{}{}
	}
	if n.subs[id] == nil {
		n.subs[id] = map[chan struct{}]struct{}{}
	}
	n.subs[id][ch] = struct{}{}
	n.mu.Unlock()
	context.AfterFunc(ctx, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(n.subs[id], ch)
		if len(n.subs[id]) == 0 {
			delete(n.subs, id)
		}
		close(ch)
	})
	return ch, nil
}

// Notify wakes this job's subscribers without waiting for a reader.
func (n *LogNotifications) Notify(id JobID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.subs[id] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
