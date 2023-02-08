package main

import (
	"log"
	"time"

	"github.com/Shopify/zk"
)

func main() {
	c, events, err := zk.Connect([]string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"}, time.Second) //*10)
	if err != nil {
		panic(err)
	}
	go func() {
		for e := range events {
			log.Printf("SessionEvent: %+v", e)
		}
		log.Printf("SessionEvent closed")
	}()

	// Walk breath-first.
	err = c.TreeWalker("/foo").
		BreadthFirst().
		Walk(func(p string, stat *zk.Stat) error {
			log.Printf("Got %s", p)
			return nil
		})
	if err != nil {
		panic(err)
	}

	// Walk depth-first and visit leaves only.
	err = c.TreeWalker("/foo").
		DepthFirst().
		LeavesOnly().
		Walk(func(p string, stat *zk.Stat) error {
			log.Printf("Got %s", p)
			return nil
		})
	if err != nil {
		panic(err)
	}

	// Walk breath-first with parallel traversal and receive events by channel.
	ch := c.TreeWalker("/foo").
		BreadthFirst().
		Concurrency(4).
		WalkChan(8) // You can tune the buffer size.
	for e := range ch {
		if e.Err != nil {
			panic(e.Err)
		}
		log.Printf("Got %s", e.Path)
	}
}
