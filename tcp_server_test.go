package zk

import (
	"fmt"
	"math/rand/v2"
	"net"
	"testing"
	"time"
)

func WithListenServer(t *testing.T, test func(server string)) {
	startPort := int(rand.Int32N(6000) + 10000)
	server := net.JoinHostPort("localhost", fmt.Sprint(startPort))
	l, err := net.Listen("tcp", server)
	if err != nil {
		t.Fatalf("Failed to start listen server: %v", err)
	}
	defer l.Close()

	go func() {
		conn, err := l.Accept()
		if err != nil {
			t.Logf("Failed to accept connection: %s", err.Error())
		}

		handleRequest(conn)
	}()

	test(server)
}

// Handles incoming requests.
func handleRequest(conn net.Conn) {
	time.Sleep(5 * time.Second)
	conn.Close()
}
