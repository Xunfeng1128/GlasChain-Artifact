package pbft_all

import (
	"blockEmulator/message"
	"sync"
	"testing"
	"time"
)

// The guard must return the original installation deadline, not a consumed
// opportunity or the generic consensus-busy result. The latter has no timer
// in the event loop and could stall forever without another control message.
func TestCLPAEventBoundaryRetryOutcome(t *testing.T) {
	for _, mode := range []struct {
		name     string
		snapshot bool
	}{{"CLPA", false}, {"GLAS_Pending", true}} {
		t.Run(mode.name, func(t *testing.T) {
			for _, delta := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
				t.Run(delta.String(), func(t *testing.T) {
					p, _, _ := clpaEventFixture(t, 8*time.Second, mode.snapshot)
					installed := time.Unix(1700000000, 0)
					p.lastPartitionCommitTime.Store(installed.UnixMilli())
					deadline := installed.Add(8 * time.Second)
					proposal, consumed, retryAt := p.tryCLPAProposalWithClock(false, func() time.Time { return deadline.Add(delta) })
					if delta < 0 {
						if proposal != nil || consumed || !retryAt.Equal(deadline) {
							t.Fatalf("early guard: proposal=%v consumed=%t retryAt=%s; want only original deadline %s", proposal != nil, consumed, retryAt, deadline)
						}
						if p.pbftStage.Load() != 1 || !p.sequenceLock.TryLock() {
							t.Fatal("boundary wait retained consensus or sequence ownership")
						}
						p.sequenceLock.Unlock()
						if !p.pbftLock.TryLock() {
							t.Fatal("boundary wait retained PBFT lock")
						}
						p.pbftLock.Unlock()
						return
					}
					if proposal == nil || proposal.RequestMsg.RequestType != message.BlockRequest || !consumed || !retryAt.IsZero() {
						t.Fatalf("ready guard: proposal=%v consumed=%t retryAt=%s", proposal != nil, consumed, retryAt)
					}
					p.sequenceLock.Unlock()
				})
			}
		})
	}
}

// A local clock seam recreates a timer waking while the wall-clock guard is
// still early. It never changes the machine clock or global package state.
type clpaBoundaryRetryClock struct {
	mu        sync.Mutex
	deadline  time.Time
	remaining time.Duration
	armed     bool
	shifted   bool
	offset    time.Duration
	shiftedAt chan time.Time
}

func (clock *clpaBoundaryRetryClock) now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	realNow := time.Now()
	if realNow.Before(clock.deadline) {
		clock.armed = true
	}
	if clock.armed && !clock.shifted && !realNow.Before(clock.deadline) {
		clock.shifted = true
		clock.offset = realNow.Sub(clock.deadline) + clock.remaining
		clock.shiftedAt <- realNow
	}
	return realNow.Add(-clock.offset)
}

type clpaBoundaryRetryEvent struct {
	proposal *message.PrePrepare
	realAt   time.Time
	wallAt   time.Time
}

func startCLPABoundaryRetryScheduler(t *testing.T, p *PbftConsensusNode, clock *clpaBoundaryRetryClock) (<-chan clpaBoundaryRetryEvent, <-chan struct{}) {
	t.Helper()
	events := make(chan clpaBoundaryRetryEvent, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runCLPAEventProposalScheduler(func(proposal *message.PrePrepare) {
			event := clpaBoundaryRetryEvent{proposal: proposal, realAt: time.Now(), wallAt: clock.now()}
			// Complete the local consensus stub without emitting any control wake.
			// Both the retry and following cadence must be timer-driven.
			p.pbftLock.Lock()
			p.sequenceID++
			p.pbftStage.Store(1)
			p.sequenceLock.Unlock()
			p.pbftLock.Unlock()
			select {
			case events <- event:
			case <-p.clpaControlContext().Done():
			}
		}, clock.now)
	}()
	t.Cleanup(func() {
		p.stopSignal.Store(true)
		p.cancelCLPAControl()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("boundary retry scheduler did not stop")
		}
	})
	return events, done
}

func waitCLPABoundaryClockShift(t *testing.T, clock *clpaBoundaryRetryClock) time.Time {
	t.Helper()
	select {
	case at := <-clock.shiftedAt:
		return at
	case <-time.After(3 * time.Second):
		t.Fatal("test did not reach an early timer wake")
		return time.Time{}
	}
}

func nextCLPABoundaryRetryEvent(t *testing.T, events <-chan clpaBoundaryRetryEvent) clpaBoundaryRetryEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("boundary retry stalled without a control wake")
		return clpaBoundaryRetryEvent{}
	}
}

func TestCLPAEventBoundaryRetryNeedsNoMessageAndKeepsCadence(t *testing.T) {
	for _, mode := range []struct {
		name     string
		snapshot bool
	}{{"CLPA", false}, {"GLAS_Pending", true}} {
		t.Run(mode.name, func(t *testing.T) {
			const interval = 600 * time.Millisecond
			const remaining = 40 * time.Millisecond
			p, _, _ := clpaEventFixture(t, interval, mode.snapshot)
			installed := time.UnixMilli(time.Now().UnixMilli())
			p.lastPartitionCommitTime.Store(installed.UnixMilli())
			clock := &clpaBoundaryRetryClock{deadline: installed.Add(interval), remaining: remaining, shiftedAt: make(chan time.Time, 1)}
			events, _ := startCLPABoundaryRetryScheduler(t, p, clock)
			shiftedAt := waitCLPABoundaryClockShift(t, clock)
			first := nextCLPABoundaryRetryEvent(t, events)
			if first.proposal.RequestMsg.RequestType != message.BlockRequest || first.wallAt.Before(clock.deadline) {
				t.Fatal("ordinary proposal bypassed the full installation interval")
			}
			if elapsed := first.realAt.Sub(shiftedAt); elapsed < remaining-time.Millisecond || elapsed >= interval/2 {
				t.Fatalf("early timer should add only %s remaining, not a new %s interval; elapsed=%s", remaining, interval, elapsed)
			}
			second := nextCLPABoundaryRetryEvent(t, events)
			if gap := second.wallAt.Sub(first.wallAt); gap < interval-20*time.Millisecond || gap > interval+150*time.Millisecond {
				t.Fatalf("normal cadence changed after boundary retry: %s", gap)
			}
		})
	}
}

func TestCLPAEventBoundaryRetryNewInstallReplacesDeadline(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "CLPA", true: "GLAS_Pending"}[snapshot], func(t *testing.T) {
			const interval = 600 * time.Millisecond
			p, _, _ := clpaEventFixture(t, interval, snapshot)
			installed := time.UnixMilli(time.Now().UnixMilli())
			p.lastPartitionCommitTime.Store(installed.UnixMilli())
			clock := &clpaBoundaryRetryClock{deadline: installed.Add(interval), remaining: 200 * time.Millisecond, shiftedAt: make(chan time.Time, 1)}
			events, _ := startCLPABoundaryRetryScheduler(t, p, clock)
			waitCLPABoundaryClockShift(t, clock)
			// Another committed installation supersedes the pending retry. Publish
			// it under the same lock as real commits, then notify the event loop.
			p.pbftLock.Lock()
			newInstalled := time.UnixMilli(clock.now().UnixMilli())
			p.lastPartitionCommitTime.Store(newInstalled.UnixMilli())
			p.pbftLock.Unlock()
			p.wakeCLPAControl()
			first := nextCLPABoundaryRetryEvent(t, events)
			if deadline := newInstalled.Add(interval); first.wallAt.Before(deadline) || first.wallAt.Sub(deadline) > 200*time.Millisecond {
				t.Fatalf("new installation deadline not honored: proposal=%s deadline=%s", first.wallAt, deadline)
			}
		})
	}
}

func TestCLPAEventBoundaryRetryCancellation(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "CLPA", true: "GLAS_Pending"}[snapshot], func(t *testing.T) {
			p, _, _ := clpaEventFixture(t, 2*time.Second, snapshot)
			// Begin close to the first deadline, then create a long remaining
			// wait so cancellation cannot pass merely because the timer expired.
			installed := time.UnixMilli(time.Now().Add(-1900 * time.Millisecond).UnixMilli())
			p.lastPartitionCommitTime.Store(installed.UnixMilli())
			clock := &clpaBoundaryRetryClock{deadline: installed.Add(2 * time.Second), remaining: 800 * time.Millisecond, shiftedAt: make(chan time.Time, 1)}
			events, done := startCLPABoundaryRetryScheduler(t, p, clock)
			waitCLPABoundaryClockShift(t, clock)
			p.cancelCLPAControl()
			select {
			case <-done:
			case <-time.After(400 * time.Millisecond):
				t.Fatal("stop waited for the remaining ordinary deadline")
			}
			select {
			case <-events:
				t.Fatal("cancelled boundary retry proposed a block")
			default:
			}
		})
	}
}
