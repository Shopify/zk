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
	err = c.AddWatchCtxAsync(context.Background(), "/", true, func(ctx context.Context, e zk.Event) {
		fmt.Printf("Got event: %+v\n", e)
		c.GetCtxAsync(ctx, e.Path, func(ctx context.Context, b []byte, s *zk.Stat, err error) {
			if err != nil {
				fmt.Printf("%s err => %+v\n", e.Path, err)
			} else {
				fmt.Printf("%s (%+v): %+v\n", e.Path, s, b)
			}
		})
	})
	if err != nil {
		panic(err)
	}
	for {
		time.Sleep(time.Second)
	}
}
