package zk

import (
	"context"
	"sync"
	"time"
)

const (
	defaultReservoirLimit = 2048
)

// WatcherOption represents an option for a watcher.
type WatcherOption func(*watcherOptions)

// WithWatcherInvalidateOnDisconnect returns a WatcherOption that configures the watcher to be invalidated on disconnect.
func WithWatcherInvalidateOnDisconnect() WatcherOption {
	return func(opts *watcherOptions) {
		opts.invalidateOnDisconnect = true
	}
}

// WithWatcherReservoirLimit returns a WatcherOption that configures the reservoir limit for a persistent watcher.
// The reservoir limit is the absolute maximum number of events that can be queued before the watcher is forcefully closed.
// If the given reservoir lLimit is <= 0, the default value of 2048 will be used.
func WithWatcherReservoirLimit(reservoirLimit int) WatcherOption {
	return func(opts *watcherOptions) {
		if reservoirLimit <= 0 {
			reservoirLimit = defaultReservoirLimit
		}
		opts.reservoirLimit = reservoirLimit
	}
}

// WithStallCallback returns a WatcherOption that configures a callback function for when we hit the reservoir limit.
func WithStallCallback(stallCallback func()) WatcherOption {
	return func(opts *watcherOptions) {
		opts.stallCallback = stallCallback
	}
}

type watcherOptions struct {
	// If true, the watcher will be invalidated if the connection is lost.
	invalidateOnDisconnect bool
	// The pump reservoir limit for persistent watchers. Defaults to defaultReservoirLimit.
	reservoirLimit int
	// Called when the pump reservoir limit is hit and we stop processing Events
	stallCallback func()
}

type watcherKey struct {
	path string
	kind watcherKind
}

type watcher interface {
	// options returns the options for the watcher.
	options() watcherOptions

	// eventChan returns the channel to consume events from.
	eventChan() <-chan Event

	// notify is called by the connection when an event is received.
	// Returns true if the watcher accepted it; false otherwise (ie: watcher is dead).
	// The caller should discard the watcher if false is returned.
	notify(ev Event) bool

	// close closes the watcher.
	// Once closed, notify() will no longer accept events, and eventChan() will eventually be closed.
	close()
}

func newFireOnceWatcher(opts watcherOptions) *fireOnceWatcher {
	ch := make(chan Event, 2) // Buffer to hold 1 watch event + 1 close event.
	return &fireOnceWatcher{
		opts:    opts,
		ch:      ch,
		closeFn: sync.OnceFunc(func() { close(ch) }),
	}
}

// fireOnceWatcher is an implementation of watcher that fires a single watch event (ie: for GetW, ExistsW, ChildrenW).
type fireOnceWatcher struct {
	opts    watcherOptions
	ch      chan Event
	closeFn func()
}

func (w *fireOnceWatcher) eventChan() <-chan Event {
	return w.ch
}

func (w *fireOnceWatcher) notify(ev Event) (ok bool) {
	// This is a bit ugly, but it's not impossible for a watcher to be notified after it's been closed.
	// It's a compromise that allows us to have finer-grained locking in the connection's receive loop.
	// It's also not worth synchronizing notify() and close(), since this is a very rare case.
	defer func() {
		_ = recover() // Ignore panics from closed channel.
	}()

	w.ch <- ev
	return true
}

func (w *fireOnceWatcher) options() watcherOptions {
	return w.opts
}

func (w *fireOnceWatcher) close() {
	w.closeFn()
}

func newPersistentWatcher(opts watcherOptions) *persistentWatcher {
	if opts.reservoirLimit == 0 {
		opts.reservoirLimit = defaultReservoirLimit
	}

	return &persistentWatcher{
		opts: opts,
		pump: newPump[Event](uint32(opts.reservoirLimit), opts.stallCallback),
	}
}

// persistentWatcher is an implementation of watcher for persistent watches.
type persistentWatcher struct {
	opts watcherOptions
	pump *pump[Event]
}

func (w *persistentWatcher) eventChan() <-chan Event {
	return w.pump.outChan()
}

func (w *persistentWatcher) notify(ev Event) bool {
	return w.pump.give(context.Background(), ev)
}

func (w *persistentWatcher) options() watcherOptions {
	return w.opts
}

func (w *persistentWatcher) close() {
	// Closing input will allow the pump to drain and stop naturally, as long as output is consumed.
	w.pump.closeInput() // Idempotent.

	// If output is not consumed, then the pump may not be able to stop, causing a goroutine leak.
	// To protect against this, we'll wait up to 5 minutes for the pump to stop, after which we force it.
	if !w.pump.isStopRequested() {
		go func(p *pump[Event]) { // Monitor the pump in a new goroutine.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			if p.waitUntilStopped(ctx) == context.DeadlineExceeded {
				p.stop() // Force stop; idempotent.
			}
		}(w.pump)
	}
}
