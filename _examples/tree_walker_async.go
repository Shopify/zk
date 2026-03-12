package main

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/Shopify/zk"
)

var (
	zkConn = struct{}{}
)

func walk(ctx context.Context, parent string, childrens []string, stat *zk.Stat, err error) {
	if ctx.Err() != nil || err != nil {
		fmt.Printf("%s err1 => %+v\n", parent, err)
		return
	}
	c := ctx.Value(zkConn).(*zk.Conn)

	if stat.NumChildren == 0 {
		c.GetCtxAsync(ctx, parent, func(ctx context.Context, b []byte, s *zk.Stat, err error) {
			if err != nil {
				fmt.Printf("%s err2 => %+v\n", parent, err)
			} else {
				fmt.Printf("Leaf %s => %+v\n", parent, b)
			}
		})
	} else {
		for _, child := range childrens {
			c.ChildrenCtxAsync(ctx, path.Join(parent, child), walk)
		}
	}
}

func main() {
	c, _, err := zk.Connect([]string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"}, time.Second) //*10)
	if err != nil {
		panic(err)
	}

	ctx := context.WithValue(context.Background(), zkConn, c)
	c.ChildrenCtxAsync(ctx, "/", walk)
	for {
		time.Sleep(time.Second)
	}
}
