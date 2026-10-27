package hopper

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// notifier sends insert notifications for jobs inserted outside a caller's
// transaction, coalesced so that a client sends at most one notification per
// queue per interval however many jobs it inserts. It fires on the trailing
// edge, after the inserts of the window have committed, so a notified worker
// always finds the jobs. Stream appends are coalesced the same way, per
// topic, on the stream channel.
//
// Inserts inside a caller's transaction notify from within the transaction
// instead, since only Postgres knows when it commits.
type notifier struct {
	exec     driver.Executor
	logger   *slog.Logger
	interval time.Duration

	mu      sync.Mutex
	pending map[string]struct{}
	streams map[string]struct{}
	// timer is the timer that flushes what is pending, until it fires.
	// timers counts the timers armed whose flush hasn't finished, waiting
	// to fire or firing, and fired is signaled as each finishes.
	timer  *time.Timer
	timers int
	fired  *sync.Cond
}

func newNotifier(exec driver.Executor, logger *slog.Logger, interval time.Duration) *notifier {
	n := &notifier{exec: exec, logger: logger, interval: interval, pending: map[string]struct{}{}, streams: map[string]struct{}{}}
	n.fired = sync.NewCond(&n.mu)
	return n
}

// mark schedules a notification for queue.
func (n *notifier) mark(queue string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pending[queue] = struct{}{}
	n.arm()
}

// markStream schedules a stream notification for topic.
func (n *notifier) markStream(topic string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.streams[topic] = struct{}{}
	n.arm()
}

// arm starts the timer, unless one is waiting to fire. The caller holds
// n.mu.
func (n *notifier) arm() {
	if n.timer == nil {
		n.timers++
		n.timer = time.AfterFunc(n.interval, n.fire)
	}
}

// fire is the timer's: it flushes what is pending, and counts its timer
// finished.
func (n *notifier) fire() {
	n.mu.Lock()
	n.timer = nil
	n.mu.Unlock()
	n.flush(context.Background())
	n.mu.Lock()
	n.timers--
	n.fired.Broadcast()
	n.mu.Unlock()
}

// flushNow sends anything pending without waiting for the timer, for as
// long as ctx allows. A timer that has fired is waited for, until its
// flush has finished, so that no flush runs once flushNow returns but one
// that a later mark starts; once ctx ends, flushNow waits no more, and
// such a flush finishes on its own.
func (n *notifier) flushNow(ctx context.Context) {
	stop := context.AfterFunc(ctx, func() {
		n.mu.Lock()
		n.fired.Broadcast()
		n.mu.Unlock()
	})
	defer stop()
	n.mu.Lock()
	for n.timers > 0 {
		if n.timer != nil && n.timer.Stop() {
			n.timer = nil
			n.timers--
			continue
		}
		if ctx.Err() != nil {
			break
		}
		n.fired.Wait()
	}
	n.mu.Unlock()
	n.flush(ctx)
}

// flush sends what is pending, waiting for each notification at most 5s,
// and no longer than ctx allows.
func (n *notifier) flush(ctx context.Context) {
	n.mu.Lock()
	queues := make([]string, 0, len(n.pending))
	for q := range n.pending {
		queues = append(queues, q)
	}
	clear(n.pending)
	topics := make([]string, 0, len(n.streams))
	for t := range n.streams {
		topics = append(topics, t)
	}
	clear(n.streams)
	n.mu.Unlock()
	if len(queues) == 0 && len(topics) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if len(queues) > 0 {
		if err := n.exec.Notify(ctx, driver.ChannelInsert, queues); err != nil {
			// Workers still poll, so a lost notification costs latency, not work.
			n.logger.WarnContext(ctx, "hopper: notify inserts", "queues", queues, "error", err)
		}
	}
	if len(topics) > 0 {
		if err := n.exec.Notify(ctx, driver.ChannelStream, topics); err != nil {
			n.logger.WarnContext(ctx, "hopper: notify stream appends", "topics", topics, "error", err)
		}
	}
}
