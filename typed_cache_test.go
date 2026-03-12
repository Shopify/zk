package zk

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"
)

type testNode struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func TestTypedCache_InitialSyncPopulatesEntries(t *testing.T) {
	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			// Create tree with JSON data.
			_, err := c.Create(context.Background(), "/test-typed", []byte("{}"), 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create root: %v", err)
			}
			for i := range 3 {
				data, _ := json.Marshal(testNode{Name: "node-" + strconv.Itoa(i), Value: i})
				_, err := c.Create(context.Background(), "/test-typed/child-"+strconv.Itoa(i), data, 0, WorldACL(PermAll))
				if err != nil {
					t.Fatalf("failed to create child-%d: %v", i, err)
				}
			}

			cache := NewTreeCache(c, "/test-typed", WithTreeCacheIncludeData(true))
			typed := NewTypedCache[testNode](cache, json.Unmarshal)

			ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
			defer cancel()

			syncErrCh := make(chan error, 1)
			go func() {
				defer close(syncErrCh)
				syncErrCh <- cache.Sync(ctx)
			}()

			if err := cache.WaitForInitialSync(ctx); err != nil {
				t.Fatalf("initial sync failed: %v", err)
			}

			// Verify typed entries.
			for i := range 3 {
				val, stat, err := typed.GetTyped("/child-" + strconv.Itoa(i))
				if err != nil {
					t.Fatalf("GetTyped child-%d: %v", i, err)
				}
				if val.Name != "node-"+strconv.Itoa(i) || val.Value != i {
					t.Fatalf("GetTyped child-%d: got %+v, want name=node-%d value=%d", i, val, i, i)
				}
				if stat == nil {
					t.Fatalf("GetTyped child-%d: stat is nil", i)
				}
			}

			// Verify Len.
			// Root node "{}" deserializes to zero-value testNode, so it counts.
			if n := typed.Len(); n != 4 {
				t.Fatalf("Len: got %d, want 4", n)
			}

			cancel()
			if err := <-syncErrCh; err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})
}

func TestTypedCache_NodeCreateUpdateDelete(t *testing.T) {
	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			_, err := c.Create(context.Background(), "/test-typed", []byte("{}"), 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create root: %v", err)
			}

			cache := NewTreeCache(c, "/test-typed", WithTreeCacheIncludeData(true))
			typed := NewTypedCache[testNode](cache, json.Unmarshal)

			ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
			defer cancel()

			syncErrCh := make(chan error, 1)
			go func() {
				defer close(syncErrCh)
				syncErrCh <- cache.Sync(ctx)
			}()

			if err := cache.WaitForInitialSync(ctx); err != nil {
				t.Fatalf("initial sync failed: %v", err)
			}

			// Create a node.
			data, _ := json.Marshal(testNode{Name: "new", Value: 42})
			_, _ = c.Create(context.Background(), "/test-typed/new", data, 0, WorldACL(PermAll))
			time.Sleep(100 * time.Millisecond)

			val, _, err := typed.GetTyped("/new")
			if err != nil {
				t.Fatalf("GetTyped after create: %v", err)
			}
			if val.Name != "new" || val.Value != 42 {
				t.Fatalf("GetTyped after create: got %+v", val)
			}

			// Update the node.
			data, _ = json.Marshal(testNode{Name: "updated", Value: 99})
			_, _ = c.Set(context.Background(), "/test-typed/new", data, -1)
			time.Sleep(100 * time.Millisecond)

			val, _, err = typed.GetTyped("/new")
			if err != nil {
				t.Fatalf("GetTyped after update: %v", err)
			}
			if val.Name != "updated" || val.Value != 99 {
				t.Fatalf("GetTyped after update: got %+v", val)
			}

			// Delete the node.
			_ = c.Delete(context.Background(), "/test-typed/new", -1)
			time.Sleep(100 * time.Millisecond)

			_, _, err = typed.GetTyped("/new")
			if err != ErrNoNode {
				t.Fatalf("GetTyped after delete: expected ErrNoNode, got %v", err)
			}

			cancel()
			if err := <-syncErrCh; err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})
}

func TestTypedCache_InnerListenerChaining(t *testing.T) {
	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			_, err := c.Create(context.Background(), "/test-typed", []byte("{}"), 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create root: %v", err)
			}

			mock := NewTreeCacheListenerMock()
			cache := NewTreeCache(c, "/test-typed", WithTreeCacheIncludeData(true))
			_ = NewTypedCache[testNode](cache, json.Unmarshal,
				WithTypedCacheListener[testNode](mock))

			ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
			defer cancel()

			syncErrCh := make(chan error, 1)
			go func() {
				defer close(syncErrCh)
				syncErrCh <- cache.Sync(ctx)
			}()

			if err := cache.WaitForInitialSync(ctx); err != nil {
				t.Fatalf("initial sync failed: %v", err)
			}

			// Inner listener should have received OnTreeSynced.
			if n := mock.OnTreeSyncedCalled(); n != 1 {
				t.Fatalf("expected OnTreeSynced called 1 time, got %d", n)
			}

			// Create a node and verify inner listener gets called.
			data, _ := json.Marshal(testNode{Name: "chained", Value: 1})
			_, _ = c.Create(context.Background(), "/test-typed/chained", data, 0, WorldACL(PermAll))
			time.Sleep(100 * time.Millisecond)

			if n := mock.OnNodeCreatedCalled(); n != 1 {
				t.Fatalf("expected OnNodeCreated called 1 time, got %d", n)
			}

			cancel()
			if err := <-syncErrCh; err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})
}

func TestTypedCache_AbsolutePaths(t *testing.T) {
	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			_, err := c.Create(context.Background(), "/test-typed", []byte("{}"), 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create root: %v", err)
			}
			data, _ := json.Marshal(testNode{Name: "abs", Value: 7})
			_, err = c.Create(context.Background(), "/test-typed/abs", data, 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create node: %v", err)
			}

			cache := NewTreeCache(c, "/test-typed",
				WithTreeCacheIncludeData(true),
				WithTreeCacheAbsolutePaths(true))
			typed := NewTypedCache[testNode](cache, json.Unmarshal)

			ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
			defer cancel()

			syncErrCh := make(chan error, 1)
			go func() {
				defer close(syncErrCh)
				syncErrCh <- cache.Sync(ctx)
			}()

			if err := cache.WaitForInitialSync(ctx); err != nil {
				t.Fatalf("initial sync failed: %v", err)
			}

			// GetTyped with absolute path.
			val, _, err := typed.GetTyped("/test-typed/abs")
			if err != nil {
				t.Fatalf("GetTyped with absolute path: %v", err)
			}
			if val.Name != "abs" || val.Value != 7 {
				t.Fatalf("GetTyped: got %+v", val)
			}

			cancel()
			if err := <-syncErrCh; err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})
}

func TestTypedCache_NonDeserializableNodesSkipped(t *testing.T) {
	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			_, err := c.Create(context.Background(), "/test-typed", []byte("{}"), 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create root: %v", err)
			}
			// Create a node with valid JSON.
			data, _ := json.Marshal(testNode{Name: "valid", Value: 1})
			_, _ = c.Create(context.Background(), "/test-typed/valid", data, 0, WorldACL(PermAll))
			// Create a node with invalid JSON.
			_, _ = c.Create(context.Background(), "/test-typed/invalid", []byte("not json!!!"), 0, WorldACL(PermAll))

			cache := NewTreeCache(c, "/test-typed", WithTreeCacheIncludeData(true))
			typed := NewTypedCache[testNode](cache, json.Unmarshal)

			ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
			defer cancel()

			syncErrCh := make(chan error, 1)
			go func() {
				defer close(syncErrCh)
				syncErrCh <- cache.Sync(ctx)
			}()

			if err := cache.WaitForInitialSync(ctx); err != nil {
				t.Fatalf("initial sync failed: %v", err)
			}

			// Valid node should be accessible.
			val, _, err := typed.GetTyped("/valid")
			if err != nil {
				t.Fatalf("GetTyped valid: %v", err)
			}
			if val.Name != "valid" {
				t.Fatalf("GetTyped valid: got %+v", val)
			}

			// Invalid node should not be in typed cache.
			_, _, err = typed.GetTyped("/invalid")
			if err != ErrNoNode {
				t.Fatalf("GetTyped invalid: expected ErrNoNode, got %v", err)
			}

			cancel()
			if err := <-syncErrCh; err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})
}

func TestTypedCache_ConcurrentReads(t *testing.T) {
	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			_, err := c.Create(context.Background(), "/test-typed", []byte("{}"), 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create root: %v", err)
			}
			for i := range 10 {
				data, _ := json.Marshal(testNode{Name: "node", Value: i})
				_, _ = c.Create(context.Background(), "/test-typed/n-"+strconv.Itoa(i), data, 0, WorldACL(PermAll))
			}

			cache := NewTreeCache(c, "/test-typed", WithTreeCacheIncludeData(true))
			typed := NewTypedCache[testNode](cache, json.Unmarshal)

			ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
			defer cancel()

			syncErrCh := make(chan error, 1)
			go func() {
				defer close(syncErrCh)
				syncErrCh <- cache.Sync(ctx)
			}()

			if err := cache.WaitForInitialSync(ctx); err != nil {
				t.Fatalf("initial sync failed: %v", err)
			}

			// Hammer GetTyped from multiple goroutines concurrently.
			var wg sync.WaitGroup
			for range 10 {
				wg.Go(func() {
					for range 100 {
						for i := range 10 {
							val, _, err := typed.GetTyped("/n-" + strconv.Itoa(i))
							if err != nil {
								t.Errorf("concurrent GetTyped: %v", err)
								return
							}
							if val.Value != i {
								t.Errorf("concurrent GetTyped: got value %d, want %d", val.Value, i)
								return
							}
						}
					}
				})
			}
			wg.Wait()

			cancel()
			if err := <-syncErrCh; err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})
}

func TestTypedCache_ResyncClearsStaleEntries(t *testing.T) {
	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c1 *Conn, _ <-chan Event) {
			c1.reconnectLatch = make(chan struct{})

			_, err := c1.Create(context.Background(), "/test-typed", []byte("{}"), 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create root: %v", err)
			}
			data, _ := json.Marshal(testNode{Name: "before", Value: 1})
			_, _ = c1.Create(context.Background(), "/test-typed/before", data, 0, WorldACL(PermAll))

			syncDoneChan := make(chan struct{}, 1)
			cache := NewTreeCache(c1, "/test-typed", WithTreeCacheIncludeData(true))
			typed := NewTypedCache[testNode](cache, json.Unmarshal,
				WithTypedCacheListener[testNode](&TreeCacheListenerFuncs{
					OnSyncStoppedFunc: func(_ error) { close(syncDoneChan) },
					OnTreeSyncedFunc:  func(_ time.Duration) { syncDoneChan <- struct{}{} },
				}))

			ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
			defer cancel()

			syncErrCh := make(chan error, 1)
			go func() {
				defer close(syncErrCh)
				syncErrCh <- cache.Sync(ctx)
			}()

			if err := cache.WaitForInitialSync(ctx); err != nil {
				t.Fatalf("initial sync failed: %v", err)
			}

			// Eat first syncDone.
			select {
			case <-syncDoneChan:
			default:
			}

			// Verify "before" exists.
			val, _, err := typed.GetTyped("/before")
			if err != nil {
				t.Fatalf("GetTyped before disconnect: %v", err)
			}
			if val.Name != "before" {
				t.Fatalf("GetTyped: got %+v", val)
			}

			// Simulate disconnect.
			_ = c1.conn.Close()

			// While disconnected, delete "before" and create "after" on a new connection.
			WithConnectAll(t, tc, func(t *testing.T, c2 *Conn, _ <-chan Event) {
				_ = c2.Delete(context.Background(), "/test-typed/before", -1)
				data, _ := json.Marshal(testNode{Name: "after", Value: 2})
				_, _ = c2.Create(context.Background(), "/test-typed/after", data, 0, WorldACL(PermAll))
			})

			close(c1.reconnectLatch) // Unblock reconnection.

			// Wait for re-sync.
			select {
			case <-syncDoneChan:
			case <-ctx.Done():
				t.Fatalf("timed out waiting for re-sync")
			}

			// "before" should be gone, "after" should exist.
			_, _, err = typed.GetTyped("/before")
			if err != ErrNoNode {
				t.Fatalf("GetTyped stale 'before': expected ErrNoNode, got %v", err)
			}

			val, _, err = typed.GetTyped("/after")
			if err != nil {
				t.Fatalf("GetTyped 'after': %v", err)
			}
			if val.Name != "after" || val.Value != 2 {
				t.Fatalf("GetTyped 'after': got %+v", val)
			}

			cancel()
			if err := <-syncErrCh; err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})
}
