package hopper

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// listenLoop keeps one notification connection open, multiplexing every
// channel. Producers poll regardless, so a missing or dropped listener costs
// pickup latency, not work; the loop reconnects with backoff.
func (c *Client[TTx]) listenLoop(ctx context.Context) {
	backoff := time.Second
	warned := false
	for {
		err := c.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, driver.ErrNotSupported) {
			c.logger.InfoContext(ctx, "hopper: notifications not supported by the driver; polling")
			return
		}
		if !warned {
			c.logger.WarnContext(ctx, "hopper: listener unavailable; polling until it reconnects", "error", err)
			warned = true
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
		if c.listening.Load() {
			warned = false
			backoff = time.Second
		}
	}
}

// listenOnce connects, subscribes and dispatches notifications until the
// connection fails or ctx is done.
func (c *Client[TTx]) listenOnce(ctx context.Context) error {
	l, err := c.driver.Listener(ctx)
	if err != nil {
		return err
	}
	defer l.Close(context.WithoutCancel(ctx)) //nolint:errcheck // best effort on a failed connection
	if err := l.Listen(ctx, driver.ChannelInsert, driver.ChannelLeader, driver.ChannelControl, driver.ChannelDone); err != nil {
		return err
	}
	c.listening.Store(true)
	defer c.listening.Store(false)
	for {
		n, err := l.Next(ctx)
		if err != nil {
			return err
		}
		switch n.Channel {
		case driver.ChannelInsert:
			c.wakeQueue(n.Payload)
		case driver.ChannelLeader:
			c.pokeLeader()
		case driver.ChannelControl:
			c.control(n.Payload)
		case driver.ChannelDone:
			if id, err := ParseJobID(n.Payload); err == nil {
				c.signalDone(id)
			}
		}
	}
}

// control applies an operator action from ChannelControl.
func (c *Client[TTx]) control(payload string) {
	action, arg, ok := strings.Cut(payload, ":")
	if !ok {
		return
	}
	switch action {
	case "cancel":
		if id, err := ParseJobID(arg); err == nil {
			c.cancelLocal(id)
		}
	case "pause":
		c.setPaused(arg, true)
	case "resume":
		c.setPaused(arg, false)
	}
}
