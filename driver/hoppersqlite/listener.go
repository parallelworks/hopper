package hoppersqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// listener implements driver.Listener on hopper_notifications. It reads
// the rows committed after the last it read, on a connection of its own:
// at once when a transaction of this process that wrote some commits, and
// every listenInterval otherwise, for those of other processes, which it
// reads only when PRAGMA data_version says another connection has
// committed since its last look.
type listener struct {
	d        *Driver
	conn     *sql.Conn
	channels map[string]bool
	poke     chan struct{}
	unlisten func()
	// last is the seq of the last notification read, and version the
	// connection's data_version when it was read.
	last, version int64
	pending       []driver.Notification
}

// listenPage is how many notifications a listener reads at a time.
const listenPage = 1000

func (d *Driver) newListener(ctx context.Context) (*listener, error) {
	conn, err := d.sqlDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	l := &listener{d: d, conn: conn, channels: map[string]bool{}, poke: make(chan struct{}, 1), version: -1}
	l.unlisten = d.db.listen(l.poke)
	// Notifications committed from now on are delivered.
	if err := conn.QueryRowContext(ctx, `SELECT coalesce(max(seq), 0) FROM hopper_notifications`).Scan(&l.last); err != nil {
		_ = l.Close(ctx)
		return nil, err
	}
	return l, nil
}

// Listen implements driver.Listener.
func (l *listener) Listen(_ context.Context, channels ...string) error {
	for _, c := range channels {
		l.channels[c] = true
	}
	return nil
}

// Next implements driver.Listener.
func (l *listener) Next(ctx context.Context) (driver.Notification, error) {
	poked := true
	for {
		if len(l.pending) > 0 {
			n := l.pending[0]
			l.pending = l.pending[1:]
			return n, nil
		}
		if err := l.read(ctx, poked); err != nil {
			return driver.Notification{}, err
		}
		if len(l.pending) > 0 {
			continue
		}
		timer := time.NewTimer(l.d.listenInterval)
		select {
		case <-l.poke:
			poked = true
		case <-timer.C:
			poked = false
		case <-ctx.Done():
			timer.Stop()
			return driver.Notification{}, ctx.Err()
		}
		timer.Stop()
	}
}

// read reads the notifications committed since the last read, if this
// process poked the listener or another connection has committed since.
func (l *listener) read(ctx context.Context, poked bool) error {
	var version int64
	if err := l.conn.QueryRowContext(ctx, "PRAGMA data_version").Scan(&version); err != nil {
		return err
	}
	if !poked && version == l.version {
		return nil
	}
	l.version = version
	rows, err := l.conn.QueryContext(ctx, `SELECT seq, channel, payload FROM hopper_notifications WHERE seq > ? ORDER BY seq LIMIT ?`, l.last, listenPage)
	if err != nil {
		return err
	}
	defer rows.Close()
	read := 0
	for rows.Next() {
		var n driver.Notification
		if err := rows.Scan(&l.last, &n.Channel, &n.Payload); err != nil {
			return err
		}
		read++
		if l.channels[n.Channel] {
			l.pending = append(l.pending, n)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// A full page leaves more to read: look again without waiting.
	if read == listenPage {
		select {
		case l.poke <- struct{}{}:
		default:
		}
	}
	return nil
}

// Close implements driver.Listener.
func (l *listener) Close(context.Context) error {
	l.unlisten()
	err := l.conn.Close()
	if errors.Is(err, sql.ErrConnDone) {
		return nil
	}
	return err
}
