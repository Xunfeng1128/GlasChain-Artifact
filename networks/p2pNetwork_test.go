package networks

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"blockEmulator/params"
)

func testPool(t *testing.T) (*pooledConn, net.Conn) {
	t.Helper()
	oldDelay, oldJitter, oldBandwidth := params.Delay, params.JitterRange, params.Bandwidth
	CloseAllConnInPool()
	params.Delay, params.JitterRange, params.Bandwidth = 0, 0, 1<<25
	InitNetworkTools()
	client, server := net.Pipe()
	pc := &pooledConn{conn: client, writeGate: make(chan struct{}, 1)}
	connMaplock.Lock()
	connectionPool["test"] = pc
	connMaplock.Unlock()
	t.Cleanup(func() {
		CloseAllConnInPool()
		server.Close()
		params.Delay, params.JitterRange, params.Bandwidth = oldDelay, oldJitter, oldBandwidth
	})
	return pc, server
}

func TestSendContextCancelsBlockedWrite(t *testing.T) {
	_, _ = testPool(t) // Nobody reads the other end.
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := TcpDialContext(ctx, []byte("blocked"), "test"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("canceled write remained blocked")
	}
}

func TestCloseDoesNotWaitForWriter(t *testing.T) {
	pc, _ := testPool(t)
	pc.writeGate <- struct{}{}
	done := make(chan error, 1)
	go func() { done <- TcpDialContext(context.Background(), []byte("waiting"), "test") }()
	closed := make(chan struct{})
	go func() { CloseAllConnInPool(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close waited for writer gate")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("send unexpectedly succeeded after close")
		}
	case <-time.After(time.Second):
		t.Fatal("queued send was not canceled")
	}
	<-pc.writeGate
}

func TestConcurrentFramesDoNotInterleave(t *testing.T) {
	_, peer := testPool(t)
	const count = 16
	read := make(chan map[string]bool, 1)
	go func() {
		seen := make(map[string]bool)
		scanner := bufio.NewScanner(peer)
		for len(seen) < count && scanner.Scan() {
			seen[scanner.Text()] = true
		}
		read <- seen
	}()
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := TcpDialContext(context.Background(), []byte(fmt.Sprintf("message-%d", i)), "test"); err != nil {
				t.Errorf("send: %v", err)
			}
		}(i)
	}
	wg.Wait()
	select {
	case seen := <-read:
		if len(seen) != count {
			t.Fatalf("frames=%v", seen)
		}
	case <-time.After(time.Second):
		t.Fatal("missing framed messages")
	}
}

func TestBandwidthWaitRespectsCancellation(t *testing.T) {
	_, peer := testPool(t)
	_ = peer
	params.Bandwidth = 1
	InitNetworkTools()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := TcpDialContext(ctx, []byte("more than one byte"), "test"); err == nil {
		t.Fatal("bandwidth/write wait did not cancel")
	}
}
