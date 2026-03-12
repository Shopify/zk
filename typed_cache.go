package zk

import (
	"iter"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// TypedEntry is a deserialized cache entry holding a value and its stat.
type TypedEntry[T any] struct {
	Value *T
	Stat  *Stat
}

// TypedCacheOption configures a TypedCache.
type TypedCacheOption[T any] func(*TypedCache[T])

// WithTypedCacheListener returns an option that chains an inner TreeCacheListener.
// The inner listener receives all events after TypedCache has processed them.
func WithTypedCacheListener[T any](l TreeCacheListener) TypedCacheOption[T] {
	return func(tc *TypedCache[T]) {
		tc.inner = l
	}
}

// TypedCache wraps a TreeCache and maintains a pre-deserialized view of the tree.
// It deserializes data once on write (ZK event) and serves concrete objects on read,
// avoiding repeated deserialization on every access.
//
// TypedCache implements TreeCacheListener and installs itself as the listener on
// the underlying TreeCache. Use WithTypedCacheListener to chain an additional listener.
type TypedCache[T any] struct {
	*TreeCache
	unmarshal func([]byte, any) error
	inner     TreeCacheListener
	entries   atomic.Pointer[sync.Map] // path (relative) -> *TypedEntry[T]
	logger    *slog.Logger
}

// NewTypedCache creates a new TypedCache wrapping the given TreeCache.
// The unmarshal function is used to deserialize node data into T.
// It must accept a pointer to T as the second argument (e.g. json.Unmarshal, sonic.Unmarshal).
// TypedCache installs itself as the TreeCache's listener; use WithTypedCacheListener
// to chain an additional listener that receives all events after TypedCache processes them.
func NewTypedCache[T any](cache *TreeCache, unmarshal func([]byte, any) error, opts ...TypedCacheOption[T]) *TypedCache[T] {
	tc := &TypedCache[T]{
		TreeCache: cache,
		unmarshal: unmarshal,
		logger:    cache.logger,
	}
	tc.entries.Store(&sync.Map{})
	for _, opt := range opts {
		opt(tc)
	}
	// Install ourselves as the TreeCache listener.
	cache.listener = tc
	return tc
}

// GetTyped returns the deserialized value and stat for the given path.
// The path follows the same convention as TreeCache.Get (absolute if absolutePaths is set,
// relative otherwise).
func (tc *TypedCache[T]) GetTyped(path string) (*T, *Stat, error) {
	internalPath, err := tc.toInternalPath(path)
	if err != nil {
		return nil, nil, err
	}

	m := tc.entries.Load()
	v, ok := m.Load(internalPath)
	if !ok {
		return nil, nil, ErrNoNode
	}
	entry := v.(*TypedEntry[T])
	return entry.Value, entry.Stat, nil
}

// OnSyncStarted implements TreeCacheListener.
func (tc *TypedCache[T]) OnSyncStarted() {
	if tc.inner != nil {
		tc.inner.OnSyncStarted()
	}
}

// OnSyncStopped implements TreeCacheListener.
func (tc *TypedCache[T]) OnSyncStopped(err error) {
	if tc.inner != nil {
		tc.inner.OnSyncStopped(err)
	}
}

// OnSyncError implements TreeCacheListener.
func (tc *TypedCache[T]) OnSyncError(err error) {
	if tc.inner != nil {
		tc.inner.OnSyncError(err)
	}
}

// OnTreeSynced implements TreeCacheListener.
// Builds a fresh typed map by walking the TreeCache, then atomically swaps it in.
// This handles both initial sync and re-sync after reconnection.
func (tc *TypedCache[T]) OnTreeSynced(elapsed time.Duration) {
	newMap := &sync.Map{}

	// Walk the underlying TreeCache to populate the typed map.
	tc.treeMutex.RLock()
	tc.rootNode.walk("", func(path string, node *treeCacheNode) {
		if node.data == nil {
			return
		}
		var val T
		if err := tc.unmarshal(node.data, &val); err != nil {
			tc.logger.Debug("typed cache: skipping non-deserializable node", "path", path, "error", err)
			return
		}
		newMap.Store(path, &TypedEntry[T]{Value: &val, Stat: node.stat})
	})
	tc.treeMutex.RUnlock()

	tc.entries.Store(newMap)

	if tc.inner != nil {
		tc.inner.OnTreeSynced(elapsed)
	}
}

// OnNodeCreated implements TreeCacheListener.
func (tc *TypedCache[T]) OnNodeCreated(path string, data []byte, stat *Stat) {
	if data != nil {
		var val T
		if err := tc.unmarshal(data, &val); err == nil {
			tc.entries.Load().Store(path, &TypedEntry[T]{Value: &val, Stat: stat})
		}
	}
	if tc.inner != nil {
		tc.inner.OnNodeCreated(path, data, stat)
	}
}

// OnNodeDeleting implements TreeCacheListener.
func (tc *TypedCache[T]) OnNodeDeleting(path string, data []byte, stat *Stat) {
	if tc.inner != nil {
		tc.inner.OnNodeDeleting(path, data, stat)
	}
}

// OnNodeDeleted implements TreeCacheListener.
func (tc *TypedCache[T]) OnNodeDeleted(path string) {
	tc.entries.Load().Delete(path)
	if tc.inner != nil {
		tc.inner.OnNodeDeleted(path)
	}
}

// OnNodeDataChanged implements TreeCacheListener.
func (tc *TypedCache[T]) OnNodeDataChanged(path string, data []byte, stat *Stat) {
	if data != nil {
		var val T
		if err := tc.unmarshal(data, &val); err == nil {
			tc.entries.Load().Store(path, &TypedEntry[T]{Value: &val, Stat: stat})
		}
	}
	if tc.inner != nil {
		tc.inner.OnNodeDataChanged(path, data, stat)
	}
}

// walk visits all nodes in the tree rooted at this node, calling fn for each.
func (tcn *treeCacheNode) walk(path string, fn func(path string, node *treeCacheNode)) {
	if path == "" {
		path = "/"
	}
	fn(path, tcn)
	for name, child := range tcn.children {
		childPath := path
		if childPath == "/" {
			childPath = "/" + name
		} else {
			childPath = path + "/" + name
		}
		child.walk(childPath, fn)
	}
}

// AllTyped returns an iterator over all typed entries in the cache.
// Paths follow the cache's path convention (absolute or relative).
func (tc *TypedCache[T]) AllTyped() iter.Seq2[string, TypedEntry[T]] {
	return func(yield func(string, TypedEntry[T]) bool) {
		m := tc.entries.Load()
		m.Range(func(key, value any) bool {
			path := key.(string)
			entry := value.(*TypedEntry[T])
			if tc.absolutePaths {
				path = tc.rootPath + path
			}
			return yield(path, TypedEntry[T]{Value: entry.Value, Stat: entry.Stat})
		})
	}
}

// Len returns the number of typed entries in the cache.
func (tc *TypedCache[T]) Len() int {
	n := 0
	tc.entries.Load().Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

// contextKey is unused but satisfies the compiler for generic instantiation.
var _ TreeCacheListener = (*TypedCache[struct{}])(nil)
