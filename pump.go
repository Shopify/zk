package zk

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// The % usage of reservoir limit after which the pump will begin blocking sends to output.
	pumpReservoirBlockingUsage = 0.75
)

const (
	pumpConditionInputClosed = pumpCondition(iota)
	pumpConditionInputEmpty
	pumpConditionReservoirFull
	pumpConditionReservoirEmpty
	pumpConditionOutputFull
	pumpConditionStopRequested
)

type pumpCondition int

var pumpConditionNames = map[pumpCondition]string{
	pumpConditionInputClosed:    "InputClosed",
	pumpConditionInputEmpty:     "InputEmpty",
	pumpConditionReservoirFull:  "ReservoirFull",
	pumpConditionReservoirEmpty: "ReservoirEmpty",
	pumpConditionOutputFull:     "OutputFull",
	pumpConditionStopRequested:  "StopRequested",
}

func (c pumpCondition) String() string {
	if name := pumpConditionNames[c]; name != "" {
		return name
	}
	return "Unknown"
}

// newPump returns a new pump[T] with the given reservoir size limit and optional stall callback func.
func newPump[T any](reservoirLimit uint32, stallCallback func()) *pump[T] {
	input := make(chan T, 32)
	stopRequested := make(chan struct{})

	p := &pump[T]{
		input:          input,
		output:         make(chan T, 32),
		reservoir:      newRingBuffer[T](32),
		started:        make(chan struct{}),
		stopRequested:  stopRequested,
		stopped:        make(chan struct{}),
		reservoirLimit: reservoirLimit,
		stallCallback:  stallCallback,
		stopFn:         sync.OnceFunc(func() { close(stopRequested) }),
		closeInputFn:   sync.OnceFunc(func() { close(input) }),
	}

	go p.run()
	<-p.started // Wait for pump to start.

	return p
}

// pump is a generic mediator between a producer and consumer that may operate at different rates.
// The producer is responsible for calling offer or give to send values to the pump's input buffer (fixed size).
// The consumer is responsible for calling poll or take to receive values from the pump's output buffer (fixed size).
// Alternatively, the consumer may read directly from the pump's output buffer (see outChan).
//
// The pump will drain the input buffer to the output buffer as fast as possible.
// When the output buffer is full, items overflow into a growable reservoir.
// The reservoir will grow to a maximum size before the pump stalls and stops accepting new items.
//
// The natural way to stop the pump is to close the input side (see closeInput).
// This will ensure all items are drained from the input buffer to the output buffer before the pump stops automatically.
// Alternatively, the pump may be stopped immediately by calling stop, but this discards items in the input buffer and reservoir.
type pump[T any] struct {
	input          chan T         // Fixed-size input channel for producers.
	output         chan T         // Fixed-size output channel for consumers.
	reservoir      *ringBuffer[T] // Growable buffer to deal with input surge and/or output backpressure.
	started        chan struct{}  // Closed when the pump has started running.
	stopRequested  chan struct{}  // Closed when stop has been called.
	stopped        chan struct{}  // Closed after pump has stopped running.
	reservoirLimit uint32         // The size limit for reservoir.
	stallCallback  func()         // An optional callback to invoke when the pump stalls.
	stopFn         func()         // Idempotent function to close stopRequested.
	closeInputFn   func()         // Idempotent function to close input.
	intakeTotal    atomic.Int64   // Total number of values received from input.
	dischargeTotal atomic.Int64   // Total number of values sent to output.
	reservoirPeak  atomic.Int32   // Maximum observed size of reservoir.
}

type pumpStats struct {
	intakeTotal    int64 // Total number of values received from input.
	dischargeTotal int64 // Total number of values sent to output.
	reservoirPeek  int32 // Maximum size of reservoir.
}

// stats returns the pump's statistics.
func (p *pump[T]) stats() pumpStats {
	return pumpStats{
		intakeTotal:    p.intakeTotal.Load(),
		dischargeTotal: p.dischargeTotal.Load(),
		reservoirPeek:  p.reservoirPeak.Load(),
	}
}

// outChan returns the raw output channel for the pump.
func (p *pump[T]) outChan() <-chan T {
	return p.output
}

// offer makes a non-blocking attempt to send the given value to the pump's input buffer.
// Returns true if the value was accepted; false otherwise.
// The value will not be accepted if the input buffer is full, the pump's input is closed or if the pump is stopped.
func (p *pump[T]) offer(t T) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			// Gracefully handle the case where input was closed (which would cause a panic).
			// pump stall will cause premature closure of input channel which races with producer.
			ok = false
		}
	}()

	if p.isStopRequested() {
		return false
	}

	select {
	case p.input <- t:
		ok = true
	case <-p.stopRequested:
	default:
	}

	return
}

// give makes a blocking attempt to send the given value to the pump's input buffer.
// Returns true if the value was accepted; false otherwise.
// The value will be rejected if the pump's input is closed, if the pump is stopped, or if the context is canceled.
// If the pump's input buffer is full, this method will block until the value is accepted or rejected.
func (p *pump[T]) give(ctx context.Context, t T) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			// Gracefully handle the case where input was closed.
			// pump stall will cause premature closure of input channel which races with producer.
			ok = false
		}
	}()

	if p.isStopRequested() {
		return false
	}

	select {
	case p.input <- t:
		ok = true
	case <-p.stopRequested:
	case <-ctx.Done():
	}

	return
}

// poll makes a non-blocking attempt to receive a value from the pump's output buffer.
// Returns the value and true if a value was received; otherwise returns the zero value and false.
// The attempt to receive fails if the output buffer is empty or if the pump is stopped.
func (p *pump[T]) poll() (T, bool) {
	var zero T

	select {
	case t, ok := <-p.output:
		if ok {
			return t, true
		}
	case <-p.stopRequested:
	default:
	}

	return zero, false
}

// take makes a blocking attempt to receive a value from the pump's output buffer.
// Returns the value and true if a value was received; otherwise returns the zero value and false.
// The attempt to receive fails if the pump is stopped or if the context is canceled.
// If the pump's output buffer is empty, this method will block until the value is received or the attempt fails.
func (p *pump[T]) take(ctx context.Context) (T, bool) {
	var zero T

	select {
	case t, ok := <-p.output:
		if ok {
			return t, true
		}
	case <-p.stopRequested:
	case <-ctx.Done():
	}

	return zero, false
}

// closeInput prevents any further values from being accepted into the pump's input buffer and allows the pump to drain.
// After all previously received values have been drained to the output buffer, the pump will stop naturally.
// This method is idempotent and safe to call multiple times.
func (p *pump[T]) closeInput() {
	p.closeInputFn()
}

// stop immediately halts the pump and discards any values that have not yet been drained to the output buffer.
// It is preferable to call closeInput instead, which allows the pump to drain naturally.
// This method is idempotent and safe to call multiple times.
func (p *pump[T]) stop() {
	p.stopFn()
}

// isStopRequested returns true if the pump has been requested to stop; false otherwise.
func (p *pump[T]) isStopRequested() bool {
	select {
	case <-p.stopRequested:
		return true
	default:
		return false
	}
}

// waitUntilStopped blocks until the pump has stopped.
// If the given context is canceled, this method returns immediately with an error.
func (p *pump[T]) waitUntilStopped(ctx context.Context) error {
	select {
	case <-p.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run is the main pump loop.
func (p *pump[T]) run() {
	defer close(p.stopped) // On exit, signal that that pump has stopped.
	defer close(p.output)  // On exit, close the output buffer.

	tm := time.NewTimer(0) // Reusable timer for blocking ops.
	defer tm.Stop()

	close(p.started) // Signal that we have started.

	endOfInput := false
	for !endOfInput {
		if p.reservoir.isEmpty() {
			// The reservoir is empty, so try to fill the output directly from the input.
			intakeCount, dischargeCount, cond := p.transferDirect(tm)
			p.intakeTotal.Add(intakeCount)
			p.dischargeTotal.Add(dischargeCount)
			switch cond {
			case pumpConditionInputClosed:
				endOfInput = true
				continue
			case pumpConditionStopRequested:
				return
			}
		}

		// The reservoir is not empty.
		// Intake and discharge at the same time (blocking on both).
		intakeCount, dischargeCount, cond := p.intakeAndDischarge(tm)
		p.intakeTotal.Add(intakeCount)
		p.dischargeTotal.Add(dischargeCount)
		switch cond {
		case pumpConditionInputClosed:
			endOfInput = true
			continue
		case pumpConditionStopRequested:
			return
		case pumpConditionReservoirEmpty:
			continue // We can try a direct transfer once again.
		case pumpConditionReservoirFull:
			// The output is full and the reservoir limit has been reached. This is fatal!
			p.stop()
			if p.stallCallback != nil {
				p.stallCallback()
			}
			return
		}
	}

	// The input is dry, so drain what remains from the reservoir.
	if !p.reservoir.isEmpty() {
		count, _ := p.discharge()
		p.dischargeTotal.Add(count)
	}
}

// transferDirect drains the input directly into the output.
// Returns when the input is closed, the pump is stopped, or the output is full.
func (p *pump[T]) transferDirect(tm *time.Timer) (intakeCount, dischargeCount int64, cond pumpCondition) {
	for {
		var t T
		var ok bool

		select {
		case t, ok = <-p.input:
			if !ok {
				cond = pumpConditionInputClosed
				return
			}
			intakeCount++
		case <-p.stopRequested:
			cond = pumpConditionStopRequested
			return
		}

		// Try non-blocking send to output.
		select {
		case p.output <- t:
			dischargeCount++
			continue
		default: // Output is full, so we must block.
		}

		// Try (time-limited) blocking send to output.
		safeResetTimer(tm, time.Millisecond)
		select {
		case p.output <- t:
			dischargeCount++
		case <-p.stopRequested:
			cond = pumpConditionStopRequested
			return
		case <-tm.C:
			// Output still full, so we begin filling the reservoir.
			p.pushReservoir(t)
			cond = pumpConditionOutputFull
			return
		}
	}
}

// intakeAndDischarge concurrently drains the input into reservoir and fills the output from reservoir.
// Returns when the input is closed, the pump is stopped, or the reservoir limit has been reached.
func (p *pump[T]) intakeAndDischarge(tm *time.Timer) (intakeCount, dischargeCount int64, cond pumpCondition) {
	blockingThreshold := uint32(float64(p.reservoirLimit) * pumpReservoirBlockingUsage)

	for {
		rlen := p.reservoir.len()
		if rlen > p.reservoirLimit { // No hope of draining the reservoir fast enough.
			cond = pumpConditionReservoirFull
			return
		}

		out, outOK := p.reservoir.peek()
		if !outOK {
			cond = pumpConditionReservoirEmpty
			return
		}

		if rlen == p.reservoir.cap() || rlen >= blockingThreshold {
			// Reservoir needs to grow, or has reached threshold for blocking send.
			// Try a (time-limited) blocking send to output before further intake.
			// This will give the consumer a chance to catch up.
			safeResetTimer(tm, time.Millisecond)
			select {
			case p.output <- out:
				_, _ = p.reservoir.pop()
				dischargeCount++
				continue
			case <-p.stopRequested:
				cond = pumpConditionStopRequested
				return
			case <-tm.C:
				// Output is still full. Continue with intake + discharge, below.
			}
		}

		select {
		case p.output <- out:
			_, _ = p.reservoir.pop()
			dischargeCount++
		case in, inOK := <-p.input:
			if !inOK {
				cond = pumpConditionInputClosed
				return
			}
			p.pushReservoir(in)
			intakeCount++
		case <-p.stopRequested:
			cond = pumpConditionStopRequested
			return
		}
	}
}

// discharge drains the reservoir into the output.
// Returns when the pump is stopped or the reservoir is empty.
func (p *pump[T]) discharge() (count int64, cond pumpCondition) {
	for {
		t, ok := p.reservoir.pop()
		if !ok {
			cond = pumpConditionReservoirEmpty
			return
		}

		select {
		case p.output <- t:
			count++
		case <-p.stopRequested:
			cond = pumpConditionStopRequested
			return
		}
	}
}

// pushReservoir forces the given item into the reservoir; increases its capacity if necessary.
func (p *pump[T]) pushReservoir(t T) {
	if !p.reservoir.offer(t) {
		// We need to grow the reservoir.
		p.reservoir.ensureCapacity(p.reservoir.cap() * 2)
		_ = p.reservoir.offer(t)
	}

	// Keep track of the peak reservoir size.
	l := int32(p.reservoir.len())
	if l > p.reservoirPeak.Load() {
		p.reservoirPeak.Store(l)
	}
}
