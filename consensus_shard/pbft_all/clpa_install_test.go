package pbft_all

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
)

// These are transport endpoints for unit tests, not running consensus nodes.
// A gated endpoint lets a real late-forward write remain unfinished while the
// test observes whether the local installation has been acknowledged early.
func clpaInstallCapture(t *testing.T, gate <-chan struct{}) (string, <-chan []byte, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	wires := make(chan []byte, 16)
	accepted := make(chan struct{}, 16)
	stopped := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			readers.Add(1)
			go func() {
				defer readers.Done()
				defer conn.Close()
				if gate != nil {
					_ = conn.(*net.TCPConn).SetReadBuffer(4096)
				}
				accepted <- struct{}{}
				if gate != nil {
					select {
					case <-gate:
						_ = conn.(*net.TCPConn).SetReadBuffer(1 << 20)
					case <-stopped:
						return
					}
				}
				reader := bufio.NewReader(conn)
				for {
					wire, err := reader.ReadBytes('\n')
					if err != nil {
						return
					}
					select {
					case wires <- bytes.TrimSuffix(wire, []byte{'\n'}):
					case <-stopped:
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		close(stopped)
		_ = listener.Close()
		networks.CloseAllConnInPool()
		readers.Wait()
	})
	return listener.Addr().String(), wires, accepted
}

func clpaInstallFixture(t *testing.T, nodeID uint64) (*PbftConsensusNode, *CLPARelayOutsideModule, <-chan []byte) {
	t.Helper()
	unifiedPBFTFixture(t)
	node := unifiedPBFTNode(t, nodeID)
	oldDelay, oldJitter, oldBandwidth := params.Delay, params.JitterRange, params.Bandwidth
	params.Delay, params.JitterRange, params.Bandwidth = 0, 0, 1<<28
	networks.InitNetworkTools()
	t.Cleanup(func() {
		params.Delay, params.JitterRange, params.Bandwidth = oldDelay, oldJitter, oldBandwidth
	})
	address, wires, _ := clpaInstallCapture(t, nil)
	cdm := dataSupport.NewCLPADataSupport()
	cdm.ControlEventsEnabled = true
	inside := &CLPAPbftInsideExtraHandleMod{cdm: cdm, pbftNode: node}
	outside := &CLPARelayOutsideModule{cdm: cdm, pbftNode: node}
	node.ihm, node.ohm = inside, outside
	node.sequenceID = 1
	node.RunningNode = &shard.Node{NodeID: nodeID}
	node.ip_nodeTable = map[uint64]map[uint64]string{params.SupervisorShard: {0: address}}
	node.cntCommitConfirm = make(map[string]map[*shard.Node]bool)
	node.isReply = make(map[string]bool)
	node.conditionalVarpbftLock = *sync.NewCond(&node.pbftLock)
	t.Cleanup(func() {
		node.stopSignal.Store(true)
		node.cancelCLPAControl()
	})
	return node, outside, wires
}

func clpaInstallRequest(epoch, target uint64, mapping map[string]uint64) *message.Request {
	return &message.Request{RequestType: message.PartitionReq,
		ReqTime: time.Unix(int64(epoch), 0).UTC(),
		Msg: message.RawMessage{Content: (&message.AccountTransferMsg{
			ATid: epoch, TargetSeq: target, ModifiedMap: mapping,
		}).Encode()}}
}

func clpaInstallCommit(t *testing.T, node *PbftConsensusNode, request *message.Request) <-chan struct{} {
	t.Helper()
	sequence := node.sequenceID
	node.cacheRequest(sequence, string(getDigest(request)), request)
	node.prePrepareAccepted = true
	node.pbftStage.Store(3)
	if node.NodeID == uint64(node.view.Load()) {
		node.sequenceLock.Lock()
	}
	return pbftOrderingCall(node.handleCommit, pbftOrderingJSON(t, message.Commit{
		SeqID: sequence, Digest: getDigest(request), SenderNode: &shard.Node{NodeID: node.NodeID},
	}))
}

func clpaInstallReceipt(t *testing.T, wires <-chan []byte) message.PartitionInstalled {
	t.Helper()
	var receipt message.PartitionInstalled
	if err := json.Unmarshal(awaitPBFTOrderingWire(t, wires, message.CPartitionInstalled), &receipt); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func clpaInstallNoWire(t *testing.T, wires <-chan []byte) {
	t.Helper()
	select {
	case wire := <-wires:
		kind, _ := message.SplitMessage(wire)
		t.Fatalf("unexpected %s message", kind)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestCLPAInstallReceiptWaitsForCommitAndLateForwardCompletion(t *testing.T) {
	node, outside, receipts := clpaInstallFixture(t, 0)
	gate := make(chan struct{})
	address, forwards, accepted := clpaInstallCapture(t, gate)
	node.ip_nodeTable[1] = map[uint64]string{0: address}
	late := unifiedPBFTTx("late-forward-before-install-receipt")
	// Exceed the socket buffers so returning from the deferred forward write
	// requires the receiver to be released, rather than merely accepting TCP.
	late.Signature = make([]byte, 8<<20)
	if err := node.installExecutionTransactions([]*core.Transaction{late}, false); err != nil {
		t.Fatal(err)
	}
	outside.cdm.PendingSnapshotPaused = true
	request := clpaInstallRequest(1, 1, map[string]uint64{"a": 1})
	done := clpaInstallCommit(t, node, request)
	var release sync.Once
	t.Cleanup(func() {
		release.Do(func() { close(gate) })
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("commit did not finish after releasing the forward receiver")
		}
	})
	select {
	case <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("commit did not reach its deferred late-forward write")
	}
	outside.cdm.PartitionStateLock.Lock()
	round, paused, saved := outside.cdm.AccountTransferRound, outside.cdm.PendingSnapshotPaused, outside.cdm.LatestPartitionInstall
	outside.cdm.PartitionStateLock.Unlock()
	if round != 1 || !paused || saved != nil {
		t.Errorf("unfinished forwarding exposed an installation receipt: round=%d paused=%t receipt=%+v", round, paused, saved)
	}
	node.requestMu.RLock()
	committed := node.committedRequests[1]
	node.requestMu.RUnlock()
	if committed {
		t.Error("PBFT marked the request committed before its local apply returned")
	}
	outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 1}))
	clpaInstallNoWire(t, receipts)
	release.Do(func() { close(gate) })
	awaitPBFTOrderingCall(t, done)
	var forward message.ExecutionForward
	if err := json.Unmarshal(awaitPBFTOrderingWire(t, forwards, message.CExecutionForward), &forward); err != nil {
		t.Fatal(err)
	}
	if forward.Epoch != 1 || len(forward.Txs) != 1 || core.TransactionExecutionID(forward.Txs[0]) != core.TransactionExecutionID(late) {
		t.Fatal("late work was not reconciled into the newly committed routing epoch")
	}
	want := message.PartitionInstalled{Epoch: 1, FromShard: 0, TargetSeq: 1, CommittedSeq: 1,
		MappingDigest: message.PartitionMappingDigest(map[string]uint64{"a": 1})}
	if got := clpaInstallReceipt(t, receipts); got != want {
		t.Fatalf("installation receipt=%+v, want %+v", got, want)
	}
	if outside.cdm.PendingSnapshotPaused || node.CurChain.Txpool.GetTxQueueLen() != 0 || !node.committedRequests[1] || node.sequenceID != 2 {
		t.Fatal("acknowledged commit left pending installation or unreconciled source work")
	}
}

func TestCLPAInstallQueryReplaysCommittedReceiptWithoutReapplying(t *testing.T) {
	node, outside, wires := clpaInstallFixture(t, 0)
	outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 1}))
	clpaInstallNoWire(t, wires)
	request := clpaInstallRequest(1, 1, map[string]uint64{"c": 0})
	awaitPBFTOrderingCall(t, clpaInstallCommit(t, node, request))
	installed := clpaInstallReceipt(t, wires)
	head := append([]byte(nil), node.CurChain.CurrentBlock.Hash...)
	queue := node.CurChain.Txpool.Snapshot()
	boundary := node.lastPartitionCommitTime.Load()
	for _, epoch := range []uint64{0, 2} {
		outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: epoch}))
	}
	clpaInstallNoWire(t, wires)
	for repeat := 0; repeat < 2; repeat++ {
		outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 1}))
		if replay := clpaInstallReceipt(t, wires); replay != installed {
			t.Fatalf("query changed the saved receipt: got %+v want %+v", replay, installed)
		}
	}
	// A duplicate committed quorum and direct duplicate receipt observation
	// cannot create another block or reset the ordinary proposal deadline.
	node.handleCommit(pbftOrderingJSON(t, message.Commit{SeqID: 1, Digest: getDigest(request), SenderNode: node.RunningNode}))
	node.recordCLPAPartitionInstall(request, 1)
	clpaInstallNoWire(t, wires)
	if !bytes.Equal(head, node.CurChain.CurrentBlock.Hash) || !reflect.DeepEqual(queue, node.CurChain.Txpool.Snapshot()) ||
		outside.cdm.AccountTransferRound != 1 || node.sequenceID != 2 || boundary != node.lastPartitionCommitTime.Load() {
		t.Fatal("receipt replay reapplied installation or changed consensus scheduling")
	}
	node.stopSignal.Store(true)
	outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 1}))
	clpaInstallNoWire(t, wires)
}

func TestCLPAInstallFollowerStoresReceiptAndCurrentLeaderAnswersQuery(t *testing.T) {
	node, outside, wires := clpaInstallFixture(t, 1)
	awaitPBFTOrderingCall(t, clpaInstallCommit(t, node, clpaInstallRequest(1, 1, nil)))
	if outside.cdm.LatestPartitionInstall == nil || outside.cdm.LatestPartitionInstall.Epoch != 1 {
		t.Fatal("follower failed to retain its committed installation receipt")
	}
	outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 1}))
	clpaInstallNoWire(t, wires)
	node.view.Store(1)
	outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 1}))
	if got := clpaInstallReceipt(t, wires); got != *outside.cdm.LatestPartitionInstall {
		t.Fatal("new current leader could not replay its existing committed receipt")
	}
	node.view.Store(0)
	node.sendCLPAInstallReceipt(*outside.cdm.LatestPartitionInstall)
	clpaInstallNoWire(t, wires)
}

func TestCLPAInstallCatchUpKeepsNewestReceiptAcrossDuplicateAndStaleReplay(t *testing.T) {
	node, outside, wires := clpaInstallFixture(t, 1)
	first := clpaInstallRequest(1, 1, map[string]uint64{"a": 1})
	second := clpaInstallRequest(2, 2, map[string]uint64{"c": 0})
	response := pbftOrderingJSON(t, message.SendOldMessage{SeqStartHeight: 1, SeqEndHeight: 2,
		OldRequest: []*message.Request{first, second}})
	node.catchUpPending, node.catchUpStart, node.catchUpEnd = true, 1, 2
	node.handleSendOldSeq(response)
	want := message.PartitionInstalled{Epoch: 2, FromShard: 0, TargetSeq: 2, CommittedSeq: 2,
		MappingDigest: message.PartitionMappingDigest(map[string]uint64{"c": 0})}
	if outside.cdm.LatestPartitionInstall == nil || *outside.cdm.LatestPartitionInstall != want || node.sequenceID != 3 {
		t.Fatalf("catch-up did not retain the newest partition receipt: %+v", outside.cdm.LatestPartitionInstall)
	}
	head := append([]byte(nil), node.CurChain.CurrentBlock.Hash...)
	boundary := node.lastPartitionCommitTime.Load()
	node.handleSendOldSeq(response) // No outstanding recovery request.
	node.catchUpPending, node.catchUpStart, node.catchUpEnd = true, 1, 2
	node.handleSendOldSeq(response) // Entirely overlapping recovery response.
	node.recordCLPAPartitionInstall(first, 1)
	if *outside.cdm.LatestPartitionInstall != want || !bytes.Equal(head, node.CurChain.CurrentBlock.Hash) ||
		node.sequenceID != 3 || outside.cdm.AccountTransferRound != 2 || node.lastPartitionCommitTime.Load() != boundary {
		t.Fatal("stale catch-up or receipt observation regressed or reapplied installation")
	}
	clpaInstallNoWire(t, wires)
	node.view.Store(1)
	outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 1}))
	clpaInstallNoWire(t, wires)
	outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 2}))
	if got := clpaInstallReceipt(t, wires); got != want {
		t.Fatalf("catch-up query receipt=%+v, want %+v", got, want)
	}
}

type clpaInstallFailureLog struct {
	failed chan string
}

func (sink *clpaInstallFailureLog) Write(line []byte) (int, error) {
	if bytes.Contains(line, []byte("installed receipt send failed")) {
		select {
		case sink.failed <- string(line):
		default:
		}
	}
	return len(line), nil
}

func TestCLPAInstallSendFailureRetainsReceiptForQueryRecovery(t *testing.T) {
	node, outside, wires := clpaInstallFixture(t, 0)
	workingAddress := node.ip_nodeTable[params.SupervisorShard][0]
	closedListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refusedAddress := closedListener.Addr().String()
	if err := closedListener.Close(); err != nil {
		t.Fatal(err)
	}
	failed := make(chan string, 1)
	node.pl.Plog = log.New(&clpaInstallFailureLog{failed: failed}, "", 0)
	node.ip_nodeTable[params.SupervisorShard][0] = refusedAddress
	if err := node.installExecutionTransactions([]*core.Transaction{unifiedPBFTTx("retained-during-receipt-recovery")}, false); err != nil {
		t.Fatal(err)
	}
	awaitPBFTOrderingCall(t, clpaInstallCommit(t, node, clpaInstallRequest(1, 1, map[string]uint64{"c": 0})))
	// The logger signal is emitted only after the real asynchronous socket
	// attempt returns an error; recovery never depends on an arbitrary sleep.
	select {
	case failure := <-failed:
		if !bytes.Contains([]byte(failure), []byte(refusedAddress)) || !bytes.Contains([]byte(failure), []byte("connection refused")) {
			t.Fatalf("receipt did not fail at the closed local endpoint: %s", failure)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receipt transport failure was not reported without terminating the worker")
	}
	if outside.cdm.LatestPartitionInstall == nil {
		t.Fatal("failed delivery discarded the already committed installation receipt")
	}
	retained := *outside.cdm.LatestPartitionInstall
	want := message.PartitionInstalled{Epoch: 1, FromShard: 0, TargetSeq: 1, CommittedSeq: 1,
		MappingDigest: message.PartitionMappingDigest(map[string]uint64{"c": 0})}
	if retained != want || !node.committedRequests[1] || outside.cdm.AccountTransferRound != 1 || node.sequenceID != 2 {
		t.Fatalf("delivery failure changed the committed installation: receipt=%+v", retained)
	}
	head := append([]byte(nil), node.CurChain.CurrentBlock.Hash...)
	queue := node.CurChain.Txpool.Snapshot()
	boundary := node.lastPartitionCommitTime.Load()
	node.ip_nodeTable[params.SupervisorShard][0] = workingAddress
	outside.handlePartitionInstallQuery(pbftOrderingJSON(t, message.PartitionInstallQuery{Epoch: 1}))
	if recovered := clpaInstallReceipt(t, wires); recovered != retained {
		t.Fatalf("query recovered a different receipt: got %+v want %+v", recovered, retained)
	}
	if *outside.cdm.LatestPartitionInstall != retained || !bytes.Equal(head, node.CurChain.CurrentBlock.Hash) ||
		!reflect.DeepEqual(queue, node.CurChain.Txpool.Snapshot()) || node.sequenceID != 2 ||
		outside.cdm.AccountTransferRound != 1 || node.lastPartitionCommitTime.Load() != boundary {
		t.Fatal("query recovery reapplied installation or reset its proposal deadline")
	}
}

func TestCLPAInstallShutdownCancelsBlockedLateForward(t *testing.T) {
	node, _, receipts := clpaInstallFixture(t, 0)
	gate := make(chan struct{})
	address, _, accepted := clpaInstallCapture(t, gate)
	node.ip_nodeTable[1] = map[uint64]string{0: address}
	late := unifiedPBFTTx("late-forward-stop")
	late.Signature = make([]byte, 8<<20)
	if err := node.installExecutionTransactions([]*core.Transaction{late}, false); err != nil {
		t.Fatal(err)
	}
	done := clpaInstallCommit(t, node, clpaInstallRequest(1, 1, map[string]uint64{"a": 1}))
	select {
	case <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("commit did not reach blocked forwarding")
	}
	node.stopSignal.Store(true)
	node.cancelCLPAControl()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not release blocked forward and PBFT lock")
	}
	if !node.pbftLock.TryLock() {
		t.Fatal("cancelled commit retained PBFT lock")
	}
	node.pbftLock.Unlock()
	clpaInstallNoWire(t, receipts)
}
