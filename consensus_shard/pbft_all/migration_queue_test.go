package pbft_all

import (
	"reflect"
	"testing"

	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
)

func TestSharedMigrationQueueOrdersSourcesAndKeepsEachSourceOrder(t *testing.T) {
	local := []*core.Transaction{unifiedPBFTTx("L1"), unifiedPBFTTx("L2")}
	a, b := unifiedPBFTTx("A"), unifiedPBFTTx("B")
	c, d := unifiedPBFTTx("C"), unifiedPBFTTx("D")
	for repeat := 0; repeat < 100; repeat++ {
		batches := map[uint64]*message.AccountStateAndTx{
			9: {FromShard: 9, Txs: []*core.Transaction{c, d}},
			1: {FromShard: 1, Txs: []*core.Transaction{a, b}},
			4: {FromShard: 4},
		}
		_, incoming, err := orderedMigrationContents(batches)
		if err != nil {
			t.Fatal(err)
		}
		merged := append(append([]*core.Transaction(nil), local...), incoming...)
		if !reflect.DeepEqual(merged, []*core.Transaction{local[0], local[1], a, b, c, d}) {
			t.Fatal("merge changed source order, local prefix, or within-source order")
		}
	}
}

func TestCLPAMigrationProposeAndCommitAppendSharedOrderedQueue(t *testing.T) {
	unifiedPBFTFixture(t)
	node := unifiedPBFTNode(t, 0)
	node.pbftChainConfig.ShardNums = 4
	local := unifiedPBFTTx("local")
	if err := node.installExecutionTransactions([]*core.Transaction{local}, false); err != nil {
		t.Fatal(err)
	}
	cdm := dataSupport.NewCLPADataSupport()
	cdm.ModifiedMap = append(cdm.ModifiedMap, map[string]uint64{"a": 0})
	cdm.AccountStateTx = map[uint64]*message.AccountStateAndTx{
		3: {FromShard: 3, Txs: []*core.Transaction{unifiedPBFTTx("source3a"), unifiedPBFTTx("source3b")}},
		1: {FromShard: 1, Txs: []*core.Transaction{unifiedPBFTTx("source1")}},
		2: {FromShard: 2},
	}
	inside := &CLPAPbftInsideExtraHandleMod{cdm: cdm, pbftNode: node}
	ok, request := inside.proposePartition()
	if !ok || node.CurChain.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("unified migration installed records before map commit")
	}
	inside.accountTransfer_do(message.DecodeAccountTransferMsg(request.Msg.Content))
	got := make([]string, 0)
	for _, tx := range node.CurChain.Txpool.Snapshot().TxQueue {
		got = append(got, tx.SourceTxHash)
	}
	if !reflect.DeepEqual(got, []string{"local", "source1", "source3a", "source3b"}) {
		t.Fatalf("committed order: %v", got)
	}
}

func TestSharedMigrationRejectsMalformedBatch(t *testing.T) {
	for _, batch := range []*message.AccountStateAndTx{nil, {FromShard: 2}, {FromShard: 1, Addrs: []string{"a"}}, {FromShard: 1, Txs: []*core.Transaction{nil}}} {
		if _, _, err := orderedMigrationContents(map[uint64]*message.AccountStateAndTx{1: batch}); err == nil {
			t.Fatalf("malformed migration accepted: %+v", batch)
		}
	}
}
