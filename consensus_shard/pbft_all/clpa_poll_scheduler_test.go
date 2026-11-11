package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"bytes"
	"testing"
	"time"
)

func waitCLPAPollTest(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("CLPA polling condition did not become ready")
		}
	}
}

func nextCLPAPollTestEvent(t *testing.T, events <-chan glasSchedulerTestEvent) glasSchedulerTestEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("CLPA polling proposal did not become ready")
		return glasSchedulerTestEvent{}
	}
}

func clpaPollTestPhase(inside *CLPAPbftInsideExtraHandleMod) (ready, migration bool) {
	inside.cdm.PartitionStateLock.Lock()
	defer inside.cdm.PartitionStateLock.Unlock()
	return inside.cdm.ControlReadySent, inside.cdm.ControlMigrationSent
}

func assertCLPAPollLocksAvailable(t *testing.T, p *PbftConsensusNode) {
	t.Helper()
	// Finite phase work may briefly own a lock. A message wait must not.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if p.pbftLock.TryLock() {
			sequenceFree := p.sequenceLock.TryLock()
			if sequenceFree {
				p.sequenceLock.Unlock()
			}
			p.pbftLock.Unlock()
			if sequenceFree {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("polling message wait retained a PBFT or sequence lock")
}

func TestCLPAPollReadyRelayAndMigrationWaitWithoutLocks(t *testing.T) {
	p, inside, outside := clpaEventFixture(t, 60*time.Millisecond, false)
	inside.cdm.ControlPolling = true
	if err := p.installExecutionTransactions([]*core.Transaction{unifiedPBFTTx("poll-outgoing")}, false); err != nil {
		t.Fatal(err)
	}
	outside.handlePartitionPrepare(clpaEventWire(t, message.PartitionPrepare{Epoch: 1, TargetSeq: 2}))
	outside.handlePartitionMsg(clpaEventWire(t, message.PartitionModifiedMap{Epoch: 1, PartitionModified: map[string]uint64{"a": 1}}))
	events := startCLPAEventScheduler(t, p, false)
	waitCLPAPollTest(t, func() bool { ready, _ := clpaPollTestPhase(inside); return ready })
	assertCLPAPollLocksAvailable(t, p)
	outside.handlePartitionReady(clpaEventWire(t, message.PartitionReady{Epoch: 1, FromShard: 1, NowSeqID: 2, RelaySeq: 1}))
	// Even after a complete one-second check, Ready without the required Relay
	// must not extract outgoing work or propose a migration.
	time.Sleep(1100 * time.Millisecond)
	if _, migration := clpaPollTestPhase(inside); migration || p.CurChain.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("polling bypassed the final ordinary-block relay fence")
	}
	assertCLPAPollLocksAvailable(t, p)
	outside.handleRelay(clpaEventWire(t, message.Relay{SenderShardID: 1, SenderSeq: 1}))
	outside.handleRelay(clpaEventWire(t, message.Relay{SenderShardID: 1, SenderSeq: 0}))
	waitCLPAPollTest(t, func() bool { _, migration := clpaPollTestPhase(inside); return migration })
	assertCLPAPollLocksAvailable(t, p)
	if p.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("outgoing migration did not extract its queued record")
	}
	batch := message.AccountStateAndTx{Epoch: 1, FromShard: 1}
	outside.handleAccountStateAndTxMsg(clpaEventWire(t, batch))
	outside.handleAccountStateAndTxMsg(clpaEventWire(t, batch))
	proposal := nextCLPAPollTestEvent(t, events).proposal
	if proposal.RequestMsg.RequestType != message.PartitionReq {
		t.Fatal("polling produced an ordinary block instead of the collected migration")
	}
	atm := message.DecodeAccountTransferMsg(proposal.RequestMsg.Msg.Content)
	if atm.ATid != 1 || atm.TargetSeq != 2 {
		t.Fatal("polling lost the epoch/target installation fence")
	}
	p.stopSignal.Store(true)
	p.cancelCLPAControl()
	p.sequenceLock.Unlock() // This fixture deliberately does not commit proposals.
}

func TestCLPAPollCancelInterruptsOneSecondMessageWait(t *testing.T) {
	for _, phase := range []string{"ready", "migration"} {
		t.Run(phase, func(t *testing.T) {
			p, inside, outside := clpaEventFixture(t, 40*time.Millisecond, false)
			inside.cdm.ControlPolling = true
			outside.handlePartitionPrepare(clpaEventWire(t, message.PartitionPrepare{Epoch: 1, TargetSeq: 2}))
			outside.handlePartitionMsg(clpaEventWire(t, message.PartitionModifiedMap{Epoch: 1, PartitionModified: map[string]uint64{}}))
			if phase == "migration" {
				outside.handlePartitionReady(clpaEventWire(t, message.PartitionReady{Epoch: 1, FromShard: 1, NowSeqID: 2, RelaySeq: 1}))
				outside.handleRelay(clpaEventWire(t, message.Relay{SenderShardID: 1, SenderSeq: 1}))
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				p.runCLPAProposalScheduler(func(*message.PrePrepare) { t.Error("incomplete migration produced a proposal") })
			}()
			t.Cleanup(func() {
				p.stopSignal.Store(true)
				p.cancelCLPAControl()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("cancelled polling scheduler remained alive")
				}
			})
			waitCLPAPollTest(t, func() bool {
				ready, migration := clpaPollTestPhase(inside)
				return ready && (phase == "ready" || migration)
			})
			assertCLPAPollLocksAvailable(t, p)
			p.cancelCLPAControl()
			select {
			case <-done:
			case <-time.After(200 * time.Millisecond):
				t.Fatal("cancellation waited for the one-second polling interval")
			}
			assertCLPAPollLocksAvailable(t, p)
		})
	}
}

func TestCLPAPollSingleShardInstallKeepsFullOrdinaryInterval(t *testing.T) {
	const interval = 120 * time.Millisecond
	p, inside, outside := clpaEventFixture(t, interval, false)
	inside.cdm.ControlPolling = true
	p.pbftChainConfig.ShardNums = 1
	outside.handlePartitionPrepare(clpaEventWire(t, message.PartitionPrepare{Epoch: 1, TargetSeq: 2}))
	outside.handlePartitionMsg(clpaEventWire(t, message.PartitionModifiedMap{Epoch: 1, PartitionModified: map[string]uint64{}}))
	events := startCLPAEventScheduler(t, p, true)
	if partition := nextCLPAPollTestEvent(t, events); partition.proposal.RequestMsg.RequestType != message.PartitionReq {
		t.Fatal("single-shard polling failed to complete a partition without remote packets")
	}
	boundary := time.UnixMilli(p.lastPartitionCommitTime.Load())
	for i := 0; i < 50; i++ {
		p.wakeCLPAControl()
	}
	next := nextCLPAPollTestEvent(t, events)
	if next.proposal.RequestMsg.RequestType != message.BlockRequest || next.at.Sub(boundary) < interval-2*time.Millisecond {
		t.Fatalf("ordinary proposal did not preserve a full post-install interval: %s", next.at.Sub(boundary))
	}
}

func TestCLPAPollLateInstallDiscardsOverdueOrdinaryOpportunity(t *testing.T) {
	const interval = 80 * time.Millisecond
	p, inside, outside := clpaEventFixture(t, interval, false)
	inside.cdm.ControlPolling = true
	p.pbftChainConfig.ShardNums = 1
	outside.handlePartitionPrepare(clpaEventWire(t, message.PartitionPrepare{Epoch: 1, TargetSeq: 2}))
	outside.handlePartitionMsg(clpaEventWire(t, message.PartitionModifiedMap{Epoch: 1, PartitionModified: map[string]uint64{}}))
	events := startCLPAEventScheduler(t, p, false)
	partition := nextCLPAPollTestEvent(t, events)
	if partition.proposal.RequestMsg.RequestType != message.PartitionReq {
		t.Fatal("expected a partition proposal")
	}
	// Keep consensus busy past the previously scheduled ordinary opportunity.
	// Installing then must start a fresh full interval rather than immediately
	// consuming that overdue opportunity.
	time.Sleep(interval + 30*time.Millisecond)
	select {
	case <-events:
		t.Fatal("ordinary proposal bypassed an uncommitted migration")
	default:
	}
	p.pbftLock.Lock()
	inside.accountTransfer_do(message.DecodeAccountTransferMsg(partition.proposal.RequestMsg.Msg.Content))
	p.lastPartitionCommitTime.Store(time.Now().UnixMilli())
	boundary := time.UnixMilli(p.lastPartitionCommitTime.Load())
	p.sequenceID++
	p.pbftStage.Store(1)
	p.sequenceLock.Unlock()
	p.pbftLock.Unlock()
	ordinary := nextCLPAPollTestEvent(t, events)
	if ordinary.proposal.RequestMsg.RequestType != message.BlockRequest || ordinary.at.Sub(boundary) < interval-2*time.Millisecond {
		t.Fatalf("overdue ordinary proposal bypassed the post-install interval: %s", ordinary.at.Sub(boundary))
	}
	p.stopSignal.Store(true)
	p.cancelCLPAControl()
	p.sequenceLock.Unlock() // The fixture deliberately leaves this proposal uncommitted.
}

func TestCLPAPollInstallationReceiptAndQueryRemainEnabled(t *testing.T) {
	node, outside, wires := clpaInstallFixture(t, 0)
	outside.cdm.ControlPolling = true
	outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 1}))
	clpaInstallNoWire(t, wires)
	request := clpaInstallRequest(1, 1, map[string]uint64{"c": 0})
	awaitPBFTOrderingCall(t, clpaInstallCommit(t, node, request))
	installed := clpaInstallReceipt(t, wires)
	if installed.Epoch != 1 || installed.TargetSeq != 1 || installed.CommittedSeq != 1 {
		t.Fatalf("polling did not preserve the committed installation receipt: %+v", installed)
	}
	head := append([]byte(nil), node.CurChain.CurrentBlock.Hash...)
	boundary := node.lastPartitionCommitTime.Load()
	outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 1}))
	if replay := clpaInstallReceipt(t, wires); replay != installed {
		t.Fatal("polling installation query did not replay the committed receipt")
	}
	if boundary != node.lastPartitionCommitTime.Load() || !bytes.Equal(head, node.CurChain.CurrentBlock.Hash) {
		t.Fatal("receipt replay changed the ledger or post-install deadline")
	}
}

func TestCLPAPollCatchUpRetainsInstallationReceipt(t *testing.T) {
	node, outside, wires := clpaInstallFixture(t, 1)
	outside.cdm.ControlPolling = true
	request := clpaInstallRequest(1, 1, map[string]uint64{"a": 1})
	node.catchUpPending, node.catchUpStart, node.catchUpEnd = true, 1, 1
	node.handleSendOldSeq(pbftOrderingJSON(t, message.SendOldMessage{SeqStartHeight: 1, SeqEndHeight: 1,
		OldRequest: []*message.Request{request}}))
	if outside.cdm.LatestPartitionInstall == nil || outside.cdm.LatestPartitionInstall.Epoch != 1 || node.sequenceID != 2 {
		t.Fatal("polling catch-up did not retain its installation receipt")
	}
	clpaInstallNoWire(t, wires) // A follower retains the receipt without sending it.
	node.view.Store(1)
	outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 1}))
	if replay := clpaInstallReceipt(t, wires); replay != *outside.cdm.LatestPartitionInstall {
		t.Fatal("new leader could not replay the polling catch-up receipt")
	}
}
