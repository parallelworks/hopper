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
	timer   *time.Timer
}

func newNotifier(exec driver.Executor, logger *slog.Logger, interval time.Duration) *notifier {
	return &notifier{exec: exec, logger: logger, interval: interval, pending: map[string]struct{}{}, streams: map[string]struct{}{}}
}

// mark schedules a notification for queue.
func (n *notifier) mark(queue string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pending[queue] = struct{}{}
	if n.timer == nil {
		n.timer = time.AfterFunc(n.interval, n.flush)
	}
}

// markStream schedules a stream notification for topic.
func (n *notifier) markStream(topic string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.streams[topic] = struct{}{}
	if n.timer == nil {
		n.timer = time.AfterFunc(n.interval, n.flush)
	}
}

// flushNow sends anything pending without waiting for the timer.
func (n *notifier) flushNow() {
	n.mu.Lock()
	timer := n.timer
	n.mu.Unlock()
	if timer != nil && timer.Stop() {
		n.flush()
	}
}

func (n *notifier) flush() {
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
	n.timer = nil
	n.mu.Unlock()
	if len(queues) == 0 && len(topics) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
