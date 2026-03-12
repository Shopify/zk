package zk

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func Test_pump_NoConsumerLag(t *testing.T) {
	var stalled atomic.Bool
	stallCallback := func() {
		t.Log("pump has stalled")
		stalled.Store(true)
	}

	p := newPump[int](defaultReservoirLimit, stallCallback)
	defer p.stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Start receiving items from pump in a new goroutine.
	// We block for 1 ms every 256 items received and for 10 ms every 1024 items received.
	consumerErr := make(chan error, 1)
	go func() {
		startTime := time.Now()
		defer close(consumerErr)

		for i := range 65536 {
			item, ok := p.take(ctx)
			if !ok {
				if ctx.Err() != nil {
					consumerErr <- ctx.Err()
				} else {
					consumerErr <- errors.New("expected to receive an item, but take returned false")
				}
				break
			}
			if item != i {
				consumerErr <- fmt.Errorf("expected to receive item %d, but got %d", i, item)
				break
			}
		}
		t.Logf("consumer took %v to receive all items", time.Since(startTime))
	}()

	// Send items to the pump in this goroutine.
	// This will give items as fast as possible without pausing.
	startTime := time.Now()
	for i := range 65536 {
		ok := p.give(ctx, i)
		if !ok {
			t.Fatalf("expected pump to be accept item: %d", i)
		}
	}
	t.Logf("producer took %v to send all items", time.Since(startTime))

	p.closeInput()
	err := p.waitUntilStopped(ctx)
	if err != nil {
		t.Fatalf("expected pump to stop cleanly, but saw error: %v", err)
	}

	if stalled.Load() {
		t.Fatalf("expected pump to not stall")
	}

	select {
	case err, ok := <-consumerErr:
		if ok {
			t.Fatalf("expected consumer to not see error, but saw: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for consumer to finish")
	}

	stats := p.stats()
	if stats.intakeTotal != 65536 {
		t.Fatalf("expected stats.intakeTotal to be 32768, but was %d", stats.intakeTotal)
	}
	if stats.dischargeTotal != 65536 {
		t.Fatalf("expected stats.dischargeTotal to be 32768, but was %d", stats.dischargeTotal)
	}

	t.Logf("peek reservoir size: %d", stats.reservoirPeek)
}

func Test_pump_LowConsumerLag(t *testing.T) {
	var stalled atomic.Bool
	stallCallback := func() {
		t.Log("pump has stalled")
		stalled.Store(true)
	}

	p := newPump[int](defaultReservoirLimit, stallCallback)
	defer p.stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Start receiving items from pump in a new goroutine.
	consumerErr := make(chan error, 1)
	go func() {
		startTime := time.Now()
		defer close(consumerErr)

		for i := range 65536 {
			item, ok := p.take(ctx)
			if !ok {
				if ctx.Err() != nil {
					consumerErr <- ctx.Err()
				} else {
					consumerErr <- errors.New("expected to receive an item, but take returned false")
				}
				break
			}
			if item != i {
				consumerErr <- fmt.Errorf("expected to receive item %d, but got %d", i, item)
				break
			}
			// Block for 1 ms every 128 items received.
			if i%128 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}
		t.Logf("consumer took %v to receive all items", time.Since(startTime))
	}()

	// Send items to the pump in this goroutine.
	// This will give items as fast as possible without pausing.
	startTime := time.Now()
	for i := range 65536 {
		ok := p.give(ctx, i)
		if !ok {
			t.Fatalf("expected pump to be accept item: %d", i)
		}
	}
	t.Logf("producer took %v to send all items", time.Since(startTime))

	p.closeInput()
	err := p.waitUntilStopped(ctx)
	if err != nil {
		t.Fatalf("expected pump to stop cleanly, but saw error: %v", err)
	}

	if stalled.Load() {
		t.Fatalf("expected pump to not stall")
	}

	select {
	case err, ok := <-consumerErr:
		if ok {
			t.Fatalf("expected consumer to not see error, but saw: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for consumer to finish")
	}

	stats := p.stats()
	if stats.intakeTotal != 65536 {
		t.Fatalf("expected stats.intakeTotal to be 32768, but was %d", stats.intakeTotal)
	}
	if stats.dischargeTotal != 65536 {
		t.Fatalf("expected stats.dischargeTotal to be 32768, but was %d", stats.dischargeTotal)
	}

	t.Logf("peek reservoir size: %d", stats.reservoirPeek)
}

func Test_pump_HighConsumerLag(t *testing.T) {
	var stalled atomic.Bool
	stallCallback := func() {
		t.Log("pump has stalled")
		stalled.Store(true)
	}

	p := newPump[int](defaultReservoirLimit, stallCallback)
	defer p.stop()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Start receiving items from pump in a new goroutine.
	consumerErr := make(chan error, 1)
	go func() {
		startTime := time.Now()
		defer close(consumerErr)

		for i := range 65536 {
			item, ok := p.take(ctx)
			if !ok {
				if ctx.Err() != nil {
					consumerErr <- ctx.Err()
				} else {
					consumerErr <- errors.New("expected to receive an item, but take returned false")
				}
				break
			}
			if item != i {
				consumerErr <- fmt.Errorf("expected to receive item %d, but got %d", i, item)
				break
			}
			// Block for 100 ms every 1024 items.
			if i%1024 == 0 {
				time.Sleep(100 * time.Millisecond)
			}
		}
		t.Logf("consumer took %v to receive all items", time.Since(startTime))
	}()

	// Send items to the pump in this goroutine.
	// This will give items as fast as possible without pausing.
	startTime := time.Now()
	for i := range 65536 {
		ok := p.give(ctx, i)
		if !ok {
			t.Fatalf("expected pump to be accept item: %d", i)
		}
	}
	t.Logf("producer took %v to send all items", time.Since(startTime))

	p.closeInput()
	err := p.waitUntilStopped(ctx)
	if err != nil {
		t.Fatalf("expected pump to stop cleanly, but saw error: %v", err)
	}

	if stalled.Load() {
		t.Fatalf("expected pump to not stall")
	}

	select {
	case err, ok := <-consumerErr:
		if ok {
			t.Fatalf("expected consumer to not see error, but saw: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for consumer to finish")
	}

	stats := p.stats()
	if stats.intakeTotal != 65536 {
		t.Fatalf("expected stats.intakeTotal to be 32768, but was %d", stats.intakeTotal)
	}
	if stats.dischargeTotal != 65536 {
		t.Fatalf("expected stats.dischargeTotal to be 32768, but was %d", stats.dischargeTotal)
	}

	t.Logf("peek reservoir size: %d", stats.reservoirPeek)
}

func Test_pump_Stall(t *testing.T) {
	var stalled atomic.Bool
	stallCallback := func() {
		t.Log("pump has stalled")
		stalled.Store(true)
	}

	p := newPump[int](defaultReservoirLimit, stallCallback)
	defer p.stop()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Send items to the pump in this goroutine.
	// This will give items as fast as possible without pausing.
	for i := range 65536 {
		ok := p.give(ctx, i)
		if !ok {
			break // Expected to fail when pump is stalled.
		}
	}

	// No consumer to receive items, so pump should stall and stop naturally.
	err := p.waitUntilStopped(ctx)
	if err != nil {
		t.Fatalf("expected pump to stop cleanly, but saw error: %v", err)
	}

	if !stalled.Load() {
		t.Fatalf("expected pump to stall")
	}

	stats := p.stats()
	t.Logf("peek reservoir size: %d", stats.reservoirPeek)

	// We should see the output channel closed.
	for {
		select {
		case _, ok := <-p.outChan():
			if !ok {
				return // Saw end of channel.
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for consumer to finish")
		}
	}
}

func Test_pump_offer_Accepted(t *testing.T) {
	p := newPump[int](16, nil)
	defer p.stop()

	ok := p.offer(1)
	if !ok {
		t.Fatalf("expected pump to accept item")
	}
}

func Test_pump_offer_RejectedAfterInputClosed(t *testing.T) {
	p := newPump[int](16, nil)
	defer p.stop()

	p.closeInput()

	ok := p.offer(1)
	if ok {
		t.Fatalf("expected pump to not accept item after input is closed")
	}
}

func Test_pump_offer_RejectedAfterStopped(t *testing.T) {
	p := newPump[int](16, nil)
	p.stop()

	ok := p.offer(1)
	if ok {
		t.Fatalf("expected pump to not accept item after stopped")
	}
}

func Test_pump_give_Accepted(t *testing.T) {
	p := newPump[int](16, nil)
	defer p.stop()

	ok := p.give(t.Context(), 1)
	if !ok {
		t.Fatalf("expected pump to accept item")
	}
}

func Test_pump_give_RejectedAfterInputClosed(t *testing.T) {
	p := newPump[int](16, nil)
	defer p.stop()

	p.closeInput()

	ok := p.give(t.Context(), 1)
	if ok {
		t.Fatalf("expected pump to not accept item after input is closed")
	}
}

func Test_pump_give_RejectedAfterStopped(t *testing.T) {
	p := newPump[int](16, nil)
	p.stop()

	ok := p.give(t.Context(), 1)
	if ok {
		t.Fatalf("expected pump to not accept item after stopped")
	}
}

func Test_pump_poll_Accepted(t *testing.T) {
	p := newPump[int](16, nil)
	defer p.stop()

	_ = p.give(t.Context(), 1)
	time.Sleep(100 * time.Millisecond)

	item, ok := p.poll()
	if !ok {
		t.Fatalf("expected pump to receive item")
	}
	if item != 1 {
		t.Fatalf("expected pump to receive item 1, but got %d", item)
	}
}

func Test_pump_poll_RejectedAfterStopped(t *testing.T) {
	p := newPump[int](16, nil)
	p.stop()

	_, ok := p.poll()
	if ok {
		t.Fatalf("expected pump to not receive item,")
	}
}

func Test_pump_take_Accepted(t *testing.T) {
	p := newPump[int](16, nil)
	defer p.stop()

	_ = p.give(t.Context(), 1)

	item, ok := p.take(t.Context())
	if !ok {
		t.Fatalf("expected pump to receive item")
	}
	if item != 1 {
		t.Fatalf("expected pump to receive item 1, but got %d", item)
	}
}
