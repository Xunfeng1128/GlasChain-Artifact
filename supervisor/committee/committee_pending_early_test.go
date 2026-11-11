package committee

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"blockEmulator/message"
	"blockEmulator/params"
)

func TestEarlyPendingAckRequiresSameTargetAndPlan(t *testing.T) {
	job := &earlyPendingJob{epoch: 1, wire: message.PendingEarlyPlan{TargetSeq: 7, Digest: "same-plan"}}
	matching := message.PendingEarlyPlanAck{Epoch: 1, TargetSeq: 7, Digest: "same-plan"}
	if !earlyPendingAckMatches(job, matching) {
		t.Fatal("matching plan acknowledgement rejected")
	}
	for _, ack := range []message.PendingEarlyPlanAck{
		{Epoch: 2, TargetSeq: 7, Digest: "same-plan"},
		{Epoch: 1, TargetSeq: 6, Digest: "same-plan"},
		{Epoch: 1, TargetSeq: 7, Digest: ""},
		{Epoch: 1, TargetSeq: 7, Digest: "other-plan"},
	} {
		if earlyPendingAckMatches(job, ack) {
			t.Fatalf("accepted mismatched plan acknowledgement %+v", ack)
		}
	}
}

func earlyPendingBoundaryFixture(t *testing.T) (*GLASPendingCommitteeModule, *earlyPendingJob) {
	t.Helper()
	module := fullWindowCommitteeFixture(t)
	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	job := &earlyPendingJob{
		epoch: 1,
		wire:  message.PendingEarlyPlan{TargetSeq: 7, Digest: "same-plan"},
		reports: map[uint64]message.PendingEarlyReport{
			0: {ObservedBlockHash: "first", LastBlockDigest: "expected"},
			1: {ObservedBlockHash: "second", LastBlockDigest: "same"},
		},
		plan: pendingComputedPlan{
			mapping: map[string]uint64{"a": 1},
		},
		cancel: cancel,
		staged: make(map[uint64]bool),
	}
	module.earlyState = &earlyPendingState{
		epoch: 1, job: job, wake: make(chan struct{}, 1), reports: job.reports,
		fifth: map[uint64]string{0: "first", 1: "second"},
		last:  map[uint64]string{0: "different", 1: "same"},
	}
	return module, job
}

func TestPendingEarlyBoundaryWaitsForSameJobAndMatchingStageAcks(t *testing.T) {
	module, job := earlyPendingBoundaryFixture(t)
	state := module.earlyState
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	boundary := time.Now()
	type result struct {
		mapping map[string]uint64
		err     error
	}
	done := make(chan result, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		mapping, err := module.waitPendingEarly(ctx, 1, boundary)
		done <- result{mapping, err}
	}()
	defer func() {
		cancel()
		<-finished
	}()

	state.mu.Lock()
	job.ready = time.Now()
	state.notify()
	state.mu.Unlock()
	module.acceptEarlyPendingAck(message.PendingEarlyPlanAck{Epoch: 1, TargetSeq: 7, Digest: "same-plan", FromShard: 0})
	module.acceptEarlyPendingAck(message.PendingEarlyPlanAck{Epoch: 1, TargetSeq: 7, Digest: "wrong", FromShard: 1})

	state.mu.Lock()
	stage, err := state.boundaryStatus(1)
	sameJob := state.job == job && state.active == nil
	state.mu.Unlock()
	if err != nil || stage != "staging 1/2" || !sameJob {
		t.Fatalf("boundary selected another plan while waiting: stage=%q same_job=%t err=%v", stage, sameJob, err)
	}

	module.acceptEarlyPendingAck(message.PendingEarlyPlanAck{Epoch: 1, TargetSeq: 7, Digest: "same-plan", FromShard: 1})
	got := <-done
	if got.err != nil || got.mapping["a"] != 1 || state.active != job || state.epoch != 2 ||
		!job.ready.After(boundary) {
		t.Fatalf("boundary did not select the original late plan: %+v", got)
	}
}

func TestPendingEarlyBoundaryTimesOutWithoutReplanning(t *testing.T) {
	module, job := earlyPendingBoundaryFixture(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	mapping, err := module.waitPendingEarly(ctx, 1, time.Now())
	if mapping != nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "stage=planning") {
		t.Fatalf("wrong timeout result: mapping=%v err=%v", mapping, err)
	}
	if state := module.earlyState; state.active != nil || state.job != job || state.epoch != 1 {
		t.Fatal("timeout replaced the early job or advanced the epoch")
	}
}

func TestPendingEarlyBoundaryRejectsConflictingFifthBlock(t *testing.T) {
	module, _ := earlyPendingBoundaryFixture(t)
	module.earlyState.fifth[0] = "different"
	mapping, err := module.waitPendingEarly(context.Background(), 1, time.Now())
	if mapping != nil || err == nil || !strings.Contains(err.Error(), "shard 0 reported a stale fifth block") {
		t.Fatalf("wrong stale-boundary result: mapping=%v err=%v", mapping, err)
	}
}

func fullWindowCommitteeFixture(t *testing.T) *GLASPendingCommitteeModule {
	t.Helper()
	old := params.ShardNum
	params.ShardNum = 2
	t.Cleanup(func() { params.ShardNum = old })
	return &GLASPendingCommitteeModule{CLPACommitteeModule: &CLPACommitteeModule{
		IpNodeTable: map[uint64]map[uint64]string{0: {0: "zero"}, 1: {0: "one"}},
	}}
}

func TestFormalReconfigurationWaitsForAllSixOrdinaryBlocks(t *testing.T) {
	module := fullWindowCommitteeFixture(t)
	oldMethod, oldEarly, oldBlocks := params.ConsensusMethod, params.GLASPendingFullWindowEarly, params.ExperimentBlocksPerEpoch
	t.Cleanup(func() {
		params.ConsensusMethod, params.GLASPendingFullWindowEarly, params.ExperimentBlocksPerEpoch = oldMethod, oldEarly, oldBlocks
	})
	params.ConsensusMethod, params.GLASPendingFullWindowEarly, params.ExperimentBlocksPerEpoch = 12, true, 6
	module.shardMaxSeq = map[uint64]uint64{0: 6, 1: 5}
	if module.reconfigurationDue() {
		t.Fatal("fast shard advanced the epoch")
	}
	module.shardMaxSeq[1] = 6
	if !module.reconfigurationDue() {
		t.Fatal("first complete ordinary epoch did not advance")
	}
	module.curEpoch = 1
	module.shardMaxSeq[0], module.shardMaxSeq[1] = 13, 13
	if !module.reconfigurationDue() {
		t.Fatal("second complete ordinary epoch did not advance")
	}
}
