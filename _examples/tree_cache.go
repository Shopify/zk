package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/zk"
)

func main() {
	c, _, err := zk.Connect([]string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"}, time.Second) //*10)
	if err != nil {
		panic(err)
	}

	cache := zk.NewTreeCache(c, "/foo", true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		// Sync blocks until ctx is canceled.
		_ = cache.Sync(ctx)
	}()

	// Wait for the initial sync to complete.
	err = cache.WaitForInitialSync(ctx)
	if err != nil {
		panic(err)
	}

	// Get data from the cache.
	data, stat, err := cache.Get("/bar") // Effectively: c.Get("/foo/bar")
	if err != nil {
		panic(err)
	}
	fmt.Printf("data: %s, stat: %+v", string(data), stat)

	// Get children from the cache.
	children, stat, err := cache.Children("/bar") // Effectively: c.Children("/foo/bar")
	if err != nil {
		panic(err)
	}
	fmt.Printf("children: %+v, stat: %+v", children, stat)

	// Walk leaves of cache.
	walker := cache.Walker("/bar") // Effectively: c.Walker("/foo/bar")
	err = walker.LeavesOnly().
		BreadthFirst().
		Walk(func(ctx context.Context, path string, stat *zk.Stat) error {
			fmt.Printf("path: %s, stat: %+v", path, stat)
			return nil
		})
	if err != nil {
		panic(err)
	}
}
