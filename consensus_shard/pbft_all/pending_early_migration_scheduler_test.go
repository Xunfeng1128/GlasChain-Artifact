package pbft_all

import (
	"testing"
	"time"

	"blockEmulator/message"
	"blockEmulator/params"
)

func TestFullWindowMigrationStartsOnActivationBeforeOrdinaryTick(t *testing.T) {
	const interval = 500 * time.Millisecond
	for _, activateFirst := range []bool{false, true} {
		name := "prepare_before_activation"
		if activateFirst {
			name = "activation_before_prepare_and_local_commit"
		}
		t.Run(name, func(t *testing.T) {
			p, inside, outside := clpaEventFixture(t, interval, true)
			enableFullWindowEarlyTest(t)
			inside.cdm.ControlPolling = true
			p.pbftChainConfig.ShardNums = 1
			const epoch = 1
			target := params.ExperimentEpochTarget(epoch)
			p.sequenceID = target
			if activateFirst {
				p.sequenceID--
				p.pbftStage.Store(2)
			}
			inside.observeCLPANormalCommit(p.sequenceID - 1)
			// An empty map is a valid activated plan.
			plan := message.PartitionModifiedMap{Epoch: epoch, PartitionModified: map[string]uint64{}}
			staged := message.PendingEarlyPlan{TargetSeq: target, Plan: plan,
				Digest: message.PartitionPlanDigest(plan.PartitionModified, nil)}
			if ack := outside.stagePendingEarlyPlan(&staged); ack == nil {
				t.Fatal("valid full-window plan did not stage")
			}
			prepare := clpaEventWire(t, message.PartitionPrepare{Epoch: epoch, TargetSeq: target})
			activate := clpaEventWire(t, message.PendingEarlyActivate{Epoch: epoch, TargetSeq: target, Digest: staged.Digest})
			started := time.Now()
			events := startCLPAEventScheduler(t, p, true)
			assertWaiting := func(stage string) {
				t.Helper()
				select {
				case event := <-events:
					t.Fatalf("%s proposed %v before the boundary was ready", stage, event.proposal.RequestMsg.RequestType)
				case <-time.After(25 * time.Millisecond):
				}
				inside.cdm.PartitionStateLock.Lock()
				defer inside.cdm.PartitionStateLock.Unlock()
				if inside.cdm.ControlEpoch != 0 || inside.cdm.ControlReadySent || inside.cdm.ControlMigrationSent {
					t.Fatalf("%s entered reconfiguration before the boundary was ready", stage)
				}
			}
			if activateFirst {
				outside.handlePendingEarlyActivate(activate)
				assertWaiting("activation without Prepare")
				outside.handlePartitionPrepare(prepare)
				assertWaiting("activation before the sixth local block committed")
				p.pbftLock.Lock()
				inside.observeCLPANormalCommit(target - 1)
				p.sequenceID = target
				p.pbftStage.Store(1)
				p.pbftLock.Unlock()
				p.wakeCLPAControl()
			} else {
				outside.handlePartitionPrepare(prepare)
				assertWaiting("Prepare without activation")
				outside.handlePendingEarlyActivate(activate)
			}

			proposed := nextGLASSchedulerTestEvent(t, events)
			if proposed.proposal.RequestMsg.RequestType != message.PartitionReq ||
				proposed.proposal.SeqID != target || !proposed.at.Before(started.Add(interval)) {
				t.Fatalf("partition at sequence %d after %s did not use the early proposal opportunity",
					proposed.proposal.SeqID, proposed.at.Sub(started))
			}
			installed := time.UnixMilli(p.lastPartitionCommitTime.Load())
			for i := 0; i < 5; i++ {
				outside.handlePendingEarlyActivate(activate)
				outside.handlePartitionPrepare(prepare)
			}
			next := nextGLASSchedulerTestEvent(t, events)
			if next.proposal.RequestMsg.RequestType != message.BlockRequest ||
				next.proposal.SeqID != target+1 || next.at.Before(installed.Add(interval)) {
				t.Fatalf("expected an ordinary proposal after the full installation interval, got %v at %s",
					next.proposal.RequestMsg.RequestType, next.at.Sub(installed))
			}
		})
	}
}
