package pbft_all

import (
	"reflect"
	"testing"

	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
)

func TestLateExecutionForwardWaitsForCommittedMapAndPreservesStages(t *testing.T) {
	unifiedPBFTFixture(t)
	source := unifiedPBFTNode(t, 0)
	sourceData := dataSupport.NewCLPADataSupport()
	top, relay := unifiedPBFTTx("late-top"), unifiedPBFTTx("late-relay")
	relay.Relayed, relay.Sender, relay.Recipient = true, "c", "a"
	// These stages arrive after source migration extraction, before commit.
	if err := source.installExecutionTransactions([]*core.Transaction{top}, false); err != nil {
		t.Fatal(err)
	}
	if err := source.installExecutionTransactions([]*core.Transaction{relay}, true); err != nil {
		t.Fatal(err)
	}
	source.CurChain.Update_PartitionMap("a", 1)
	sourceData.AccountTransferRound = 1
	sourceInside := &CLPAPbftInsideExtraHandleMod{pbftNode: source, cdm: sourceData}
	source.unifiedExecutionMu.Lock()
	out, err := sourceInside.flushExecutionForwardsLocked()
	source.unifiedExecutionMu.Unlock()
	if err != nil || len(out) != 1 || len(out[1].Txs) != 2 || source.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatalf("late stages remained at former owner: %+v %v", out, err)
	}
	receiver := unifiedPBFTNode(t, 1)
	receiver.ShardID, receiver.pbftChainConfig.ShardID = 1, 1
	receiverData := dataSupport.NewCLPADataSupport()
	receiverOutside := &CLPARelayOutsideModule{pbftNode: receiver, cdm: receiverData}
	beforeSequences := map[uint64]uint64{0: 7}
	receiver.seqIDMap[0] = 7
	receiver.unifiedExecutionMu.Lock()
	next, err := receiverOutside.acceptExecutionForwardLocked(out[1])
	receiver.unifiedExecutionMu.Unlock()
	if err != nil || len(next) != 0 || receiver.CurChain.Txpool.GetTxQueueLen() != 0 || len(receiverData.FutureExecutionForwards) != 1 {
		t.Fatal("future-map forward was not buffered", next, err)
	}
	// Caller mutation cannot change the queued future message.
	out[1].Txs[0].GLASCallRelations[0].Callee = "mutated-sender-object"
	receiver.CurChain.Update_PartitionMap("a", 1)
	receiverData.AccountTransferRound = 1
	receiverInside := &CLPAPbftInsideExtraHandleMod{pbftNode: receiver, cdm: receiverData}
	receiver.unifiedExecutionMu.Lock()
	next, err = receiverInside.flushExecutionForwardsLocked()
	receiver.unifiedExecutionMu.Unlock()
	if err != nil || len(next) != 0 || len(receiverData.FutureExecutionForwards) != 0 {
		t.Fatal("committed map did not drain eligible forward", next, err)
	}
	queue := receiver.CurChain.Txpool.Snapshot().TxQueue
	if len(queue) != 2 || queue[0].Relayed || !queue[1].Relayed || queue[0].SourceTxHash != top.SourceTxHash || queue[1].SourceTxHash != relay.SourceTxHash {
		t.Fatal("forward lost or recreated an execution stage", queue)
	}
	if !queue[0].Time.Equal(top.Time) || !queue[1].Time.Equal(relay.Time) || queue[0].GLASCallRelations[0].Callee != "c" || !reflect.DeepEqual(receiver.seqIDMap, beforeSequences) {
		t.Fatal("forward changed original time, trace, or Relay sequence bookkeeping")
	}
	// A network replay is idempotent through the common execution ID guard.
	receiver.unifiedExecutionMu.Lock()
	next, err = receiverOutside.acceptExecutionForwardLocked(&message.ExecutionForward{Epoch: 1, FromShard: 0, Txs: queue})
	receiver.unifiedExecutionMu.Unlock()
	if err != nil || len(next) != 0 || receiver.CurChain.Txpool.GetTxQueueLen() != 2 {
		t.Fatal("forward replay duplicated stages", next, err)
	}
}

func TestStaleIncomingRoutesByCurrentMapAndRetainedQueueKeepsOrder(t *testing.T) {
	unifiedPBFTFixture(t)
	node := unifiedPBFTNode(t, 0)
	data := dataSupport.NewCLPADataSupport()
	data.AccountTransferRound = 2
	outside := &CLPARelayOutsideModule{pbftNode: node, cdm: data}
	local1, moved, local2 := unifiedPBFTTx("local1"), unifiedPBFTTx("moved"), unifiedPBFTTx("local2")
	local1.Sender, local2.Sender = "b", "b"
	if err := node.installExecutionTransactions([]*core.Transaction{local1, moved, local2}, false); err != nil {
		t.Fatal(err)
	}
	node.CurChain.Update_PartitionMap("a", 1)
	inside := &CLPAPbftInsideExtraHandleMod{pbftNode: node, cdm: data}
	node.unifiedExecutionMu.Lock()
	out, err := inside.flushExecutionForwardsLocked()
	node.unifiedExecutionMu.Unlock()
	if err != nil || len(out[1].Txs) != 1 || out[1].Epoch != 2 {
		t.Fatal("wrong-owner record not forwarded", out, err)
	}
	queue := node.CurChain.Txpool.Snapshot().TxQueue
	if len(queue) != 2 || queue[0].SourceTxHash != "local1" || queue[1].SourceTxHash != "local2" {
		t.Fatal("retained local order changed", queue)
	}
	node.unifiedExecutionMu.Lock()
	out, err = outside.acceptExecutionForwardLocked(&message.ExecutionForward{Epoch: 1, FromShard: 1, Txs: []*core.Transaction{unifiedPBFTTx("stale-wire")}})
	node.unifiedExecutionMu.Unlock()
	if err != nil || len(out[1].Txs) != 1 || out[1].Epoch != 2 || node.CurChain.Txpool.GetTxQueueLen() != 2 {
		t.Fatal("stale-map message was installed at wrong owner", out, err)
	}
}

func TestExecutionForwardRejectsUnreachableFutureEpoch(t *testing.T) {
	unifiedPBFTFixture(t)
	node := unifiedPBFTNode(t, 0)
	data := dataSupport.NewCLPADataSupport()
	outside := &CLPARelayOutsideModule{pbftNode: node, cdm: data}
	node.unifiedExecutionMu.Lock()
	_, err := outside.acceptExecutionForwardLocked(&message.ExecutionForward{Epoch: 2, FromShard: 1, Txs: []*core.Transaction{unifiedPBFTTx("future")}})
	node.unifiedExecutionMu.Unlock()
	if err == nil || len(data.FutureExecutionForwards) != 0 || node.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("unreachable future epoch buffered phantom work", err)
	}
}
