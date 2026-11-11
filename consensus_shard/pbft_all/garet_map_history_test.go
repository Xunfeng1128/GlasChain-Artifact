package pbft_all

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
)

// Keep nonempty maps in these fixtures: group-only/empty deltas cannot detect
// accidental retention or destructive clear(map) of an aliased PBFT payload.
func garetHistoryTransfer(digest string, epoch uint64, groups []uint64) *message.AccountTransferMsg {
	return &message.AccountTransferMsg{
		ATid: epoch, TargetSeq: epoch,
		ModifiedMap: map[string]uint64{garetTestAddr(10): groups[0], garetTestAddr(30): groups[1]},
		GARET:       &message.GARETPlacement{CatalogDigest: digest, Epoch: epoch, Placement: append([]uint64(nil), groups...)},
	}
}

func garetHistoryRequest(transfer *message.AccountTransferMsg) *message.Request {
	return &message.Request{RequestType: message.PartitionReq,
		ReqTime: time.Unix(int64(transfer.ATid), 0).UTC(),
		Msg:     message.RawMessage{Content: transfer.Encode()}}
}

func assertGARETHistory(t *testing.T, inside *CLPAPbftInsideExtraHandleMod, epoch uint64, latest map[string]uint64) {
	t.Helper()
	if inside.cdm.AccountTransferRound != epoch || uint64(len(inside.cdm.ModifiedMap)) != epoch {
		t.Fatalf("history indexes changed: round=%d len=%d want=%d", inside.cdm.AccountTransferRound, len(inside.cdm.ModifiedMap), epoch)
	}
	for i, mapping := range inside.cdm.ModifiedMap {
		if uint64(i)+1 < epoch && mapping != nil {
			t.Fatalf("epoch %d still retains committed map %d (%d entries)", epoch, i+1, len(mapping))
		}
	}
	if !reflect.DeepEqual(inside.cdm.ModifiedMap[epoch-1], latest) {
		t.Fatal("latest committed map was cleared or changed")
	}
}

func TestGARETHistoryReleaseAfterRealLeaderAndFollowerInstalls(t *testing.T) {
	unifiedPBFTFixture(t)
	leader, catalog := garetTestNode(t, 0, 0)
	follower, _ := garetTestNode(t, 0, 1)
	leaderData, followerData := dataSupport.NewCLPADataSupport(), dataSupport.NewCLPADataSupport()
	leaderData.ControlEventsEnabled, leaderData.ControlPolling = true, true
	followerData.ControlEventsEnabled, followerData.ControlPolling = true, true
	leaderInside := &CLPAPbftInsideExtraHandleMod{pbftNode: leader, cdm: leaderData}
	followerInside := &CLPAPbftInsideExtraHandleMod{pbftNode: follower, cdm: followerData}
	outside := &CLPARelayOutsideModule{pbftNode: leader, cdm: leaderData}
	leader.ihm, leader.ohm = leaderInside, outside
	follower.ihm = followerInside
	var announced []*message.AccountTransferMsg
	var requests []*message.Request
	var digests []string
	for epoch := uint64(1); epoch <= 5; epoch++ {
		transfer := garetHistoryTransfer(catalog.Digest(), epoch, []uint64{epoch % 2, 1 - epoch%2})
		announced = append(announced, transfer)
		leaderData.PendingEpoch, leaderData.PendingTargetSeq = epoch, epoch
		pm := &message.PartitionModifiedMap{Epoch: epoch, PartitionModified: transfer.ModifiedMap, GARET: transfer.GARET}
		outside.acceptCLPAEventMap(pm)
		outside.acceptCLPAEventMap(pm)
		if uint64(len(leaderData.ModifiedMap)) != epoch || !reflect.DeepEqual(leaderData.ModifiedMap[epoch-1], transfer.ModifiedMap) {
			t.Fatal("pending map was discarded or exact retransmission duplicated it")
		}
		// A valid but different next-epoch proposal must still be rejected even
		// after prior committed slots have been released.
		conflicting := garetHistoryTransfer(catalog.Digest(), epoch, []uint64{1 - epoch%2, epoch % 2})
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("conflicting pending map was accepted")
				}
			}()
			outside.acceptCLPAEventMap(&message.PartitionModifiedMap{Epoch: epoch,
				PartitionModified: conflicting.ModifiedMap, GARET: conflicting.GARET})
		}()
		ok, request := leaderInside.proposePartition()
		if !ok {
			t.Fatal("leader could not construct a partition request from retained pending map")
		}
		requests = append(requests, request)
		digests = append(digests, message.PartitionPlanDigest(transfer.ModifiedMap, transfer.GARET))
		// The leader already has the map from control delivery; the follower
		// obtains it only from the encoded committed request.
		for _, inside := range []*CLPAPbftInsideExtraHandleMod{leaderInside, followerInside} {
			applied := inside.applyPartitionRequest(request)
			assertGARETHistory(t, inside, epoch, transfer.ModifiedMap)
			if inside.pbftNode.CurChain.GARETEpoch() != epoch || inside.pbftNode.CurChain.Get_PartitionMap(garetTestAddr(10)) != epoch%2 {
				t.Fatal("history release changed current group routing")
			}
			if inside == followerInside {
				inside.pbftNode.recordDecodedCLPAPartitionInstall(applied, epoch)
				if inside.cdm.LatestPartitionInstall.MappingDigest != digests[epoch-1] {
					t.Fatal("history release changed the installation receipt digest")
				}
			}
		}
		if !bytes.Equal(leader.CurChain.CurrentBlock.Hash, follower.CurChain.CurrentBlock.Hash) {
			t.Fatal("leader and follower committed different partition state")
		}
		outside.acceptCLPAEventMap(pm)
		if leaderData.PartitionOn || uint64(len(leaderData.ModifiedMap)) != epoch {
			t.Fatal("stale committed map reopened the control round")
		}
	}
	for i, transfer := range announced {
		if len(transfer.ModifiedMap) != 2 || message.PartitionPlanDigest(transfer.ModifiedMap, transfer.GARET) != digests[i] {
			t.Fatalf("releasing history destructively changed an aliased map at epoch %d", i+1)
		}
		decoded := message.DecodeAccountTransferMsg(requests[i].Msg.Content)
		if message.PartitionPlanDigest(decoded.ModifiedMap, decoded.GARET) != digests[i] {
			t.Fatalf("encoded PBFT history changed at epoch %d", i+1)
		}
	}
}

func TestGARETHistoryReleasePreservesSequentialCatchUpAndLateStages(t *testing.T) {
	unifiedPBFTFixture(t)
	node, inside, outside := garetSharedWorker(t, 1)
	_, catalog := garetTestNode(t, 0, 1)
	var requests []*message.Request
	for epoch := uint64(1); epoch <= 3; epoch++ {
		requests = append(requests, garetHistoryRequest(garetHistoryTransfer(catalog.Digest(), epoch,
			[]uint64{epoch % 2, 1 - epoch%2})))
	}
	if !inside.HandleforSequentialRequest(&message.SendOldMessage{SeqStartHeight: 1, SeqEndHeight: 3, OldRequest: requests}) {
		t.Fatal("sequential catch-up did not apply complete partition history")
	}
	latest := message.DecodeAccountTransferMsg(requests[2].Msg.Content)
	assertGARETHistory(t, inside, 3, latest.ModifiedMap)
	if node.sequenceID != 4 {
		t.Fatal("catch-up sequence was changed by history release")
	}
	for i, request := range requests {
		node.recordCLPAPartitionInstall(request, uint64(i)+1)
		transfer := message.DecodeAccountTransferMsg(request.Msg.Content)
		if inside.cdm.LatestPartitionInstall.MappingDigest != message.PartitionPlanDigest(transfer.ModifiedMap, transfer.GARET) {
			t.Fatal("catch-up receipt could not be reconstructed from its request")
		}
	}
	// Exercise admission on a leader after its catch-up; no network endpoints
	// are needed because every reconciled pending stage belongs to this shard.
	node.view.Store(int32(node.NodeID))
	// Epoch 2 assigned group 0 to shard 0. Its late TOP/Relay2 now belong to
	// shard 1 under epoch 3, despite epoch 2's map slot being nil.
	top, relay := garetTestTx("history-late-top", 10, 30, false), garetTestTx("history-late-relay", 30, 20, true)
	node.unifiedExecutionMu.Lock()
	out, err := outside.acceptExecutionForwardLocked(&message.ExecutionForward{Epoch: 2, FromShard: 0, Txs: []*core.Transaction{top, relay}})
	node.unifiedExecutionMu.Unlock()
	if err != nil || len(out) != 0 || node.CurChain.Txpool.GetTxQueueLen() != 2 {
		t.Fatal("late stages depended on a released map instead of current routing", out, err)
	}
	// A message for the next epoch remains buffered until that epoch installs.
	next := garetTestTx("history-future-top", 10, 20, false)
	node.unifiedExecutionMu.Lock()
	out, err = outside.acceptExecutionForwardLocked(&message.ExecutionForward{Epoch: 4, FromShard: 0, Txs: []*core.Transaction{next}})
	node.unifiedExecutionMu.Unlock()
	if err != nil || len(out) != 0 || len(inside.cdm.FutureExecutionForwards) != 1 || node.CurChain.Txpool.GetTxQueueLen() != 2 {
		t.Fatal("future stages were not buffered at their original boundary", out, err)
	}
	transfer := garetHistoryTransfer(catalog.Digest(), 4, []uint64{1, 0})
	inside.accountTransfer_do(transfer)
	assertGARETHistory(t, inside, 4, transfer.ModifiedMap)
	if len(inside.cdm.FutureExecutionForwards) != 0 || node.CurChain.Txpool.GetTxQueueLen() != 3 {
		t.Fatal("installation did not reconcile buffered stages after history release")
	}
	queue := node.CurChain.Txpool.Snapshot().TxQueue
	if queue[0].SourceTxHash != top.SourceTxHash || queue[0].Relayed || queue[1].SourceTxHash != relay.SourceTxHash || !queue[1].Relayed || queue[2].SourceTxHash != next.SourceTxHash {
		t.Fatal("history release changed stage identity or retained queue order")
	}
}

func TestGARETHistoryReleaseLeavesNonGARETSharedPathUnchanged(t *testing.T) {
	unifiedPBFTFixture(t)
	node := unifiedPBFTNode(t, 1)
	inside := &CLPAPbftInsideExtraHandleMod{pbftNode: node, cdm: dataSupport.NewCLPADataSupport()}
	var maps []map[string]uint64
	for epoch := uint64(1); epoch <= 4; epoch++ {
		mapping := map[string]uint64{"a": epoch % 2, "b": 1 - epoch%2}
		maps = append(maps, mapping)
		inside.accountTransfer_do(&message.AccountTransferMsg{ATid: epoch, ModifiedMap: mapping})
	}
	if node.CurChain.GARETEnabled() || inside.cdm.AccountTransferRound != 4 || !reflect.DeepEqual(inside.cdm.ModifiedMap, maps) {
		t.Fatal("GARET-only cleanup changed the non-GARET shared installation path")
	}
}
