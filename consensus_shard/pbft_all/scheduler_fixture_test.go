package pbft_all

import (
	"blockEmulator/message"
	"sync/atomic"
	"testing"
	"time"
)

type glasSchedulerTestInside struct {
	controlReady       atomic.Bool
	controlCalls       atomic.Int64
	ordinaryCalls      atomic.Int64
	ordinaryBuildDelay time.Duration // configured before starting the scheduler
}

func (fake *glasSchedulerTestInside) HandleinPropose() (bool, *message.Request) {
	fake.ordinaryCalls.Add(1)
	if fake.ordinaryBuildDelay > 0 {
		time.Sleep(fake.ordinaryBuildDelay)
	}
	return true, &message.Request{RequestType: message.BlockRequest, ReqTime: time.Now()}
}

func (*glasSchedulerTestInside) HandleinPrePrepare(*message.PrePrepare) bool             { return true }
func (*glasSchedulerTestInside) HandleinPrepare(*message.Prepare) bool                   { return true }
func (*glasSchedulerTestInside) HandleinCommit(*message.Commit) bool                     { return true }
func (*glasSchedulerTestInside) HandleReqestforOldSeq(*message.RequestOldMessage) bool   { return true }
func (*glasSchedulerTestInside) HandleforSequentialRequest(*message.SendOldMessage) bool { return true }

type glasSchedulerTestEvent struct {
	proposal *message.PrePrepare
	at       time.Time
}

func nextGLASSchedulerTestEvent(t *testing.T, events <-chan glasSchedulerTestEvent) glasSchedulerTestEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("GLAS proposal did not become ready within one second")
		return glasSchedulerTestEvent{}
	}
}

func waitGLASSchedulerTest(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("GLAS scheduler condition timed out")
		}
	}
}
