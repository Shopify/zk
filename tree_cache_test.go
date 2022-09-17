package zk

import (
	"context"
	"testing"
	"time"
)

func TestTreeCache_ConsistentAfterInitialSync(t *testing.T) {
	initialNodes := []struct {
		path        string
		cachePath   string
		data        string
		numChildren int32
	}{
		{
			path:        "/test-tree-cache",
			cachePath:   "/",
			data:        "root",
			numChildren: 2,
		},
		{
			path:        "/test-tree-cache/child1",
			cachePath:   "/child1",
			data:        "child1",
			numChildren: 1,
		},
		{
			path:        "/test-tree-cache/child1/child1_1",
			cachePath:   "/child1/child1_1",
			data:        "child1_1",
			numChildren: 0,
		},
		{
			path:        "/test-tree-cache/child2",
			cachePath:   "/child2",
			data:        "child2",
			numChildren: 1,
		},
		{
			path:        "/test-tree-cache/child2/child2_1",
			cachePath:   "/child2/child2_1",
			data:        "child2_1",
			numChildren: 0,
		},
	}

	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			for _, n := range initialNodes {
				_, err := c.Create(n.path, []byte(n.data), 0, WorldACL(PermAll))
				if err != nil {
					t.Fatalf("failed to create node %s: %v", n.path, err)
				}
			}

			cache := NewTreeCache(c, "/test-tree-cache", true)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			syncErrCh := make(chan error, 1)

			// Start syncing cache in the background.
			go func() {
				defer close(syncErrCh)
				syncErrCh <- cache.Sync(ctx)
			}()

			// Wait for the first full sync to complete.
			err := cache.WaitForInitialSync(ctx)
			if err != nil {
				t.Fatalf("initial cache sync failed: %v", err)
			}

			// Check that the cache contains the expected nodes.
			for _, n := range initialNodes {
				data, stat, err := cache.Get(n.cachePath)
				if err != nil {
					t.Fatalf("failed to get cache node %s: %v", n.cachePath, err)
				}
				if string(data) != n.data {
					t.Fatalf("cahce node %s data mismatch: expected %s, got %s", n.cachePath, n.data, string(data))
				}
				if stat.NumChildren != n.numChildren {
					t.Fatalf("cache node %s numChildren mismatch: expected %d, got %d", n.cachePath, n.numChildren, stat.NumChildren)
				}
			}

			cancel()
			if err := <-syncErrCh; err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})
}

func TestTreeCache_WatchesNodeCreate(t *testing.T) {
	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			_, err := c.Create("/test-tree-cache", nil, 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create node %s: %v", "/test-tree-cache", err)
			}

			cache := NewTreeCache(c, "/test-tree-cache", true)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			syncErrCh := make(chan error, 1)

			// Start syncing cache in the background.
			go func() {
				defer close(syncErrCh)
				syncErrCh <- cache.Sync(ctx)
			}()

			// Wait for the first full sync to complete.
			err = cache.WaitForInitialSync(ctx)
			if err != nil {
				t.Fatalf("initial cache sync failed: %v", err)
			}

			_, _ = c.Create("/test-tree-cache/child1", []byte("child1"), 0, WorldACL(PermAll))
			time.Sleep(time.Millisecond * 100)

			data, stat, err := cache.Get("/child1")
			if err != nil {
				t.Fatalf("failed to get cache node /child1: %v", err)
			}
			if string(data) != "child1" {
				t.Fatalf("cahce node /child1 data mismatch: expected %s, got %s", "child1", string(data))
			}
			if stat.NumChildren != 0 {
				t.Fatalf("cache node /child1 numChildren mismatch: expected %d, got %d", 0, stat.NumChildren)
			}

			// Root node should report 1 child in stat.
			_, stat, err = cache.Exists("/")
			if err != nil {
				t.Fatalf("failed to get cache node /: %v", err)
			}
			if stat.NumChildren != 1 {
				t.Fatalf("cache node / numChildren mismatch: expected %d, got %d", 1, stat.NumChildren)
			}

			cancel()
			if err := <-syncErrCh; err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})
}

func TestTreeCache_WatchesNodeUpdate(t *testing.T) {
	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			_, err := c.Create("/test-tree-cache", nil, 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create node %s: %v", "/test-tree-cache", err)
			}
			_, err = c.Create("/test-tree-cache/child1", []byte("foo"), 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create node %s: %v", "/test-tree-cache/child1", err)
			}

			cache := NewTreeCache(c, "/test-tree-cache", true)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			syncErrCh := make(chan error, 1)

			// Start syncing cache in the background.
			go func() {
				defer close(syncErrCh)
				syncErrCh <- cache.Sync(ctx)
			}()

			// Wait for the first full sync to complete.
			err = cache.WaitForInitialSync(ctx)
			if err != nil {
				t.Fatalf("initial cache sync failed: %v", err)
			}

			_, _ = c.Set("/test-tree-cache/child1", []byte("bar"), 0)
			time.Sleep(time.Millisecond * 100)

			data, stat, err := cache.Get("/child1")
			if err != nil {
				t.Fatalf("failed to get cache node /child1: %v", err)
			}
			if string(data) != "bar" {
				t.Fatalf("cahce node /child1 data mismatch: expected %s, got %s", "bar", string(data))
			}
			if stat.NumChildren != 0 {
				t.Fatalf("cache node /child1 numChildren mismatch: expected %d, got %d", 0, stat.NumChildren)
			}

			cancel()
			if err := <-syncErrCh; err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})
}

func TestTreeCache_WatchesNodeDelete(t *testing.T) {
	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			_, err := c.Create("/test-tree-cache", nil, 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create node %s: %v", "/test-tree-cache", err)
			}
			_, err = c.Create("/test-tree-cache/child1", []byte("foo"), 0, WorldACL(PermAll))
			if err != nil {
				t.Fatalf("failed to create node %s: %v", "/test-tree-cache/child1", err)
			}

			cache := NewTreeCache(c, "/test-tree-cache", true)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			syncErrCh := make(chan error, 1)

			// Start syncing cache in the background.
			go func() {
				defer close(syncErrCh)
				syncErrCh <- cache.Sync(ctx)
			}()

			// Wait for the first full sync to complete.
			err = cache.WaitForInitialSync(ctx)
			if err != nil {
				t.Fatalf("initial cache sync failed: %v", err)
			}

			_ = c.Delete("/test-tree-cache/child1", -1)
			time.Sleep(time.Millisecond * 100)

			ok, _, err := cache.Exists("/child1")
			if err != nil {
				t.Fatalf("failed to get cache node /child1: %v", err)
			}
			if ok {
				t.Fatalf("cache node /child1 should not exist")
			}

			cancel()
			if err := <-syncErrCh; err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})
}
