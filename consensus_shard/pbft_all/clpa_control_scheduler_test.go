package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func clpaEventFixture(t *testing.T, interval time.Duration, snapshot bool) (*PbftConsensusNode, *CLPAPbftInsideExtraHandleMod, *CLPARelayOutsideModule) {
	t.Helper()
	unifiedPBFTFixture(t)
	p := unifiedPBFTNode(t, 0)
	p.pbftChainConfig.BlockInterval = uint64(interval / time.Millisecond)
	p.sequenceID = 2
	p.pStop = make(chan uint64)
	p.pbftStage.Store(1)
	p.conditionalVarpbftLock = *sync.NewCond(&p.pbftLock)
	cdm := dataSupport.NewCLPADataSupport()
	cdm.ControlEventsEnabled, cdm.PendingSnapshotMode, cdm.PendingSnapshotObservedSeq = true, snapshot, p.sequenceID
	inside := &CLPAPbftInsideExtraHandleMod{pbftNode: p, cdm: cdm}
	outside := &CLPARelayOutsideModule{pbftNode: p, cdm: cdm}
	p.ihm, p.ohm = inside, outside
	t.Cleanup(func() { p.stopSignal.Store(true); p.cancelCLPAControl() })
	return p, inside, outside
}

func clpaEventWire(t *testing.T, value interface{}) []byte {
	t.Helper()
	wire, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func startCLPAEventScheduler(t *testing.T, p *PbftConsensusNode, commit bool) <-chan glasSchedulerTestEvent {
	t.Helper()
	events := make(chan glasSchedulerTestEvent, 64)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runCLPAProposalScheduler(func(proposal *message.PrePrepare) {
			at := time.Now()
			if commit {
				p.pbftLock.Lock()
				if proposal.RequestMsg.RequestType == message.PartitionReq {
					inside := p.ihm.(*CLPAPbftInsideExtraHandleMod)
					inside.accountTransfer_do(message.DecodeAccountTransferMsg(proposal.RequestMsg.Msg.Content))
					p.lastPartitionCommitTime.Store(time.Now().UnixMilli())
				}
				p.sequenceID++
				p.pbftStage.Store(1)
				p.sequenceLock.Unlock()
				p.pbftLock.Unlock()
				p.wakeCLPAControl()
			}
			events <- glasSchedulerTestEvent{proposal: proposal, at: at}
		})
	}()
	t.Cleanup(func() {
		p.stopSignal.Store(true)
		p.cancelCLPAControl()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("CLPA scheduler did not stop")
		}
	})
	return events
}

func TestCLPAEventReadyRelayCollectionAndDuplicateFences(t *testing.T) {
	p, inside, outside := clpaEventFixture(t, 8*time.Second, false)
	if err := p.installExecutionTransactions([]*core.Transaction{unifiedPBFTTx("outgoing")}, false); err != nil {
		t.Fatal(err)
	}
	outside.handlePartitionPrepare(clpaEventWire(t, message.PartitionPrepare{Epoch: 1, TargetSeq: 2}))
	mapping := message.PartitionModifiedMap{Epoch: 1, PartitionModified: map[string]uint64{"a": 1}}
	outside.handlePartitionMsg(clpaEventWire(t, mapping))
	outside.handlePartitionMsg(clpaEventWire(t, mapping))
	if len(inside.cdm.ModifiedMap) != 1 {
		t.Fatal("duplicate map appended")
	}
	if proposal, _, _ := p.tryCLPAProposal(true); proposal != nil {
		t.Fatal("bypassed Ready")
	}
	outside.handlePartitionReady(clpaEventWire(t, message.PartitionReady{Epoch: 1, FromShard: 1, NowSeqID: 2, RelaySeq: 1}))
	if proposal, _, _ := p.tryCLPAProposal(true); proposal != nil || inside.cdm.ControlMigrationSent {
		t.Fatal("bypassed final relay acknowledgement")
	}
	outside.handleRelay(clpaEventWire(t, message.Relay{SenderShardID: 1, SenderSeq: 1}))
	outside.handleRelay(clpaEventWire(t, message.Relay{SenderShardID: 1, SenderSeq: 0}))
	if p.seqIDMap[1] != 1 {
		t.Fatal("late relay regressed acknowledgement")
	}
	if proposal, _, _ := p.tryCLPAProposal(true); proposal != nil || !inside.cdm.ControlMigrationSent {
		t.Fatal("relay event failed to progress or bypassed collection")
	}
	if p.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("migration did not extract outgoing work")
	}
	batch := message.AccountStateAndTx{Epoch: 1, FromShard: 1}
	outside.handleAccountStateAndTxMsg(clpaEventWire(t, batch))
	outside.handleAccountStateAndTxMsg(clpaEventWire(t, batch))
	proposal, _, _ := p.tryCLPAProposal(true)
	if proposal == nil || proposal.RequestMsg.RequestType != message.PartitionReq {
		t.Fatal("complete migration did not propose")
	}
	atm := message.DecodeAccountTransferMsg(proposal.RequestMsg.Msg.Content)
	if atm.ATid != 1 || atm.TargetSeq != 2 {
		t.Fatal("partition lost epoch or target")
	}
	for i := 0; i < 10; i++ {
		p.wakeCLPAControl()
		if duplicate, _, _ := p.tryCLPAProposal(true); duplicate != nil {
			t.Fatal("duplicate partition proposal")
		}
	}
	p.sequenceLock.Unlock()
	inside.cdm.PartitionStateLock.Lock()
	inside.cdm.AccountTransferRound = 1
	inside.cdm.PartitionOn = false
	inside.cdm.PartitionStateLock.Unlock()
	inside.cdm.CollectLock.Lock()
	inside.cdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	inside.cdm.CollectOver = false
	inside.cdm.CollectLock.Unlock()
	outside.handlePartitionMsg(clpaEventWire(t, mapping))
	outside.handleAccountStateAndTxMsg(clpaEventWire(t, batch))
	if inside.cdm.PartitionOn || len(inside.cdm.AccountStateTx) != 0 {
		t.Fatal("stale epoch reactivated migration")
	}
}

func TestCLPAEventWakeHooksAndNoNormalAcceleration(t *testing.T) {
	p, inside, outside := clpaEventFixture(t, 100*time.Millisecond, false)
	events := startCLPAEventScheduler(t, p, true)
	started := time.Now()
	for i := 0; i < 20; i++ {
		p.wakeCLPAControl()
	}
	first := nextGLASSchedulerTestEvent(t, events)
	if first.proposal.RequestMsg.RequestType != message.BlockRequest || first.at.Sub(started) < 85*time.Millisecond {
		t.Fatal("control wake accelerated ordinary block")
	}
	// A single-shard partition needs no remote Ready or migration batch.
	p.pbftLock.Lock()
	p.pbftChainConfig.ShardNums = 1
	target := p.sequenceID
	p.pbftLock.Unlock()
	outside.handlePartitionPrepare(clpaEventWire(t, message.PartitionPrepare{Epoch: 1, TargetSeq: target}))
	outside.handlePartitionMsg(clpaEventWire(t, message.PartitionModifiedMap{Epoch: 1, PartitionModified: map[string]uint64{}}))
	partition := nextGLASSchedulerTestEvent(t, events)
	if partition.proposal.RequestMsg.RequestType != message.PartitionReq {
		t.Fatal("map event did not propose partition")
	}
	boundary := time.UnixMilli(p.lastPartitionCommitTime.Load())
	for i := 0; i < 50; i++ {
		p.wakeCLPAControl()
	}
	next := nextGLASSchedulerTestEvent(t, events)
	if next.proposal.RequestMsg.RequestType != message.BlockRequest || next.at.Sub(boundary) < 98*time.Millisecond {
		t.Fatalf("ordinary block before full interval after install: %s", next.at.Sub(boundary))
	}
	if inside.cdm.AccountTransferRound != 1 {
		t.Fatal("partition did not install")
	}
}

func TestCLPAEventStopAndCoalescing(t *testing.T) {
	p, _, _ := clpaEventFixture(t, 8*time.Second, false)
	for i := 0; i < 100; i++ {
		p.wakeCLPAControl()
	}
	if len(p.clpaControlWakeChannel()) != 1 {
		t.Fatal("wakes did not coalesce")
	}
	p.stopSignal.Store(true)
	p.cancelCLPAControl()
	done := make(chan struct{})
	go func() {
		p.runCLPAProposalScheduler(func(*message.PrePrepare) { t.Error("stopped proposal") })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stopped scheduler blocked")
	}
	select {
	case <-p.clpaControlContext().Done():
	default:
		t.Fatal("control I/O not cancelled")
	}
}

func TestCLPAEventReorderedMapReadyAndMigrationWaitForPrepare(t *testing.T) {
	p, inside, outside := clpaEventFixture(t, 8*time.Second, false)
	outside.handlePartitionMsg(clpaEventWire(t, message.PartitionModifiedMap{Epoch: 1, PartitionModified: map[string]uint64{}}))
	outside.handlePartitionReady(clpaEventWire(t, message.PartitionReady{Epoch: 1, FromShard: 1, NowSeqID: 2, RelaySeq: 1}))
	outside.handleAccountStateAndTxMsg(clpaEventWire(t, message.AccountStateAndTx{Epoch: 1, FromShard: 1}))
	outside.handleRelay(clpaEventWire(t, message.Relay{SenderShardID: 1, SenderSeq: 1}))
	if proposal, _, _ := p.tryCLPAProposal(true); proposal != nil || inside.cdm.ControlReadySent || inside.cdm.ControlMigrationSent {
		t.Fatal("reordered data bypassed missing Prepare")
	}
	outside.handlePartitionMsg(clpaEventWire(t, message.PartitionModifiedMap{Epoch: 3, PartitionModified: map[string]uint64{"a": 1}}))
	outside.handlePartitionReady(clpaEventWire(t, message.PartitionReady{Epoch: 3, FromShard: 1, NowSeqID: 8, RelaySeq: 7}))
	outside.handleAccountStateAndTxMsg(clpaEventWire(t, message.AccountStateAndTx{Epoch: 3, FromShard: 1}))
	if len(inside.cdm.ModifiedMap) != 1 || inside.cdm.ReadySeq[1] != 2 || inside.cdm.AccountStateTx[1].Epoch != 1 {
		t.Fatal("unbounded future data overwrote next epoch")
	}
	outside.handlePartitionPrepare(clpaEventWire(t, message.PartitionPrepare{Epoch: 1, TargetSeq: 2}))
	if proposal, _, _ := p.tryCLPAProposal(true); proposal == nil {
		t.Fatal("Prepare failed to consume reordered state")
	}
	p.sequenceLock.Unlock()
}

func TestCLPAEventCallbacksWakeSchedulerWithoutTick(t *testing.T) {
	p, inside, outside := clpaEventFixture(t, 8*time.Second, false)
	events := startCLPAEventScheduler(t, p, false)
	outside.handlePartitionPrepare(clpaEventWire(t, message.PartitionPrepare{Epoch: 1, TargetSeq: 2}))
	outside.handlePartitionMsg(clpaEventWire(t, message.PartitionModifiedMap{Epoch: 1, PartitionModified: map[string]uint64{}}))
	waitGLASSchedulerTest(t, func() bool {
		inside.cdm.PartitionStateLock.Lock()
		defer inside.cdm.PartitionStateLock.Unlock()
		return inside.cdm.ControlReadySent
	})
	outside.handlePartitionReady(clpaEventWire(t, message.PartitionReady{Epoch: 1, FromShard: 1, NowSeqID: 2, RelaySeq: 1}))
	outside.handleRelay(clpaEventWire(t, message.Relay{SenderShardID: 1, SenderSeq: 1}))
	waitGLASSchedulerTest(t, func() bool {
		inside.cdm.PartitionStateLock.Lock()
		defer inside.cdm.PartitionStateLock.Unlock()
		return inside.cdm.ControlMigrationSent
	})
	outside.handleAccountStateAndTxMsg(clpaEventWire(t, message.AccountStateAndTx{Epoch: 1, FromShard: 1}))
	event := nextGLASSchedulerTestEvent(t, events)
	if event.proposal.RequestMsg.RequestType != message.PartitionReq {
		t.Fatal("control callback produced ordinary request")
	}
	p.sequenceLock.Unlock()
}

func TestCLPAEventReadyRejectsInvalidShardsAndZeroSequence(t *testing.T) {
	p, inside, outside := clpaEventFixture(t, 8*time.Second, false)
	outside.handlePartitionPrepare(clpaEventWire(t, message.PartitionPrepare{Epoch: 1, TargetSeq: 2}))
	outside.handlePartitionMsg(clpaEventWire(t, message.PartitionModifiedMap{Epoch: 1, PartitionModified: map[string]uint64{}}))
	for _, pr := range []message.PartitionReady{{Epoch: 1, FromShard: 99, NowSeqID: 2}, {Epoch: 1, FromShard: 1, NowSeqID: 0}, {Epoch: 2, FromShard: 1, NowSeqID: 2, RelaySeq: 1}} {
		outside.handlePartitionReady(clpaEventWire(t, pr))
	}
	if proposal, _, _ := p.tryCLPAProposal(true); proposal != nil || inside.getPartitionReady() {
		t.Fatal("invalid Ready opened barrier")
	}
}

func TestCLPAEventConsecutivePartitionsFenceOnlyOrdinaryRelay(t *testing.T) {
	p, inside, outside := clpaEventFixture(t, 8*time.Second, false)
	inside.observeCLPANormalCommit(1)
	outside.handleRelay(clpaEventWire(t, message.Relay{SenderShardID: 1, SenderSeq: 1}))
	for epoch := uint64(1); epoch <= 2; epoch++ {
		target := p.sequenceID
		outside.handlePartitionPrepare(clpaEventWire(t, message.PartitionPrepare{Epoch: epoch, TargetSeq: target}))
		outside.handlePartitionMsg(clpaEventWire(t, message.PartitionModifiedMap{Epoch: epoch, PartitionModified: map[string]uint64{}}))
		// The previous PBFT sequence is a partition in round two. The required
		// relay remains the last ordinary block (one), not target minus one.
		outside.handlePartitionReady(clpaEventWire(t, message.PartitionReady{Epoch: epoch, FromShard: 1, NowSeqID: target, RelaySeq: 1}))
		outside.handleAccountStateAndTxMsg(clpaEventWire(t, message.AccountStateAndTx{Epoch: epoch, FromShard: 1}))
		proposal, _, _ := p.tryCLPAProposal(true)
		if proposal == nil {
			t.Fatalf("partition %d stalled on a partition-only sequence", epoch)
		}
		p.pbftLock.Lock()
		inside.accountTransfer_do(message.DecodeAccountTransferMsg(proposal.RequestMsg.Msg.Content))
		p.sequenceID++
		p.pbftStage.Store(1)
		p.sequenceLock.Unlock()
		p.pbftLock.Unlock()
	}
	if inside.cdm.LastNormalCommittedSeq != 1 || inside.cdm.AccountTransferRound != 2 {
		t.Fatal("consecutive partitions advanced ordinary relay fence")
	}
}
