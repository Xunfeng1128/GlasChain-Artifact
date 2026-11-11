package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/partition/txallo"
	"blockEmulator/supervisor/supervisor_log"
	"io"
	"log"
	"reflect"
	"testing"
)

func TestTxAlloObservesCompletedCountsWithoutChangingGas(t *testing.T) {
	module := &TxAlloContribCommitteeModule{
		graph: txallo.NewGraph(), shardMaxSeq: make(map[uint64]uint64), modifiedMap: make(map[string]uint64),
		sl: &supervisor_log.SupervisorLog{Slog: log.New(io.Discard, "", 0)},
	}
	inner := &core.Transaction{Sender: "alice", Recipient: "bob", GasUsed: 21000}
	relay1 := &core.Transaction{Sender: "excluded", Recipient: "bob", GasUsed: 999999}
	relay2 := &core.Transaction{Sender: "alice", Recipient: "carol", GasUsed: 9000000}
	module.HandleBlockInfo(&message.BlockInfoMsg{
		BlockHeight: 7, SenderShardID: 1, Epoch: 1, BlockBodyLength: 3,
		InnerShardTxs: []*core.Transaction{inner}, Relay1Txs: []*core.Transaction{relay1}, Relay2Txs: []*core.Transaction{relay2},
	})
	if module.graph.LogicalTransactions() != 2 || module.graph.VertexCount() != 3 || module.shardMaxSeq[1] != 7 {
		t.Fatalf("wrong completed graph: txs=%d vertices=%v height=%v", module.graph.LogicalTransactions(), module.graph.Vertices(), module.shardMaxSeq)
	}
	if inner.GasUsed != 21000 || relay2.GasUsed != 9000000 {
		t.Fatal("planning changed execution gas")
	}
	current := map[string]uint64{"alice": 0, "bob": 0, "carol": 1}
	result, err := txallo.Optimize(module.graph, current, txallo.DefaultConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	if result.Before.LogicalTransactions != 2 || result.Before.CrossTransactions != 1 || result.Before.CrossTransactionRate != 0.5 {
		t.Fatalf("planning used gas or relay stages instead of logical counts: %+v", result.Before)
	}
	other := txallo.NewGraph()
	for _, tx := range []*core.Transaction{inner, relay2} {
		copyTx := *tx
		copyTx.GasUsed = 1
		if err := other.AddTransaction(copyTx.Sender, copyTx.Recipient); err != nil {
			t.Fatal(err)
		}
	}
	changedGasResult, err := txallo.Optimize(other, current, txallo.DefaultConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Placement, changedGasResult.Placement) {
		t.Fatal("changing only execution gas changed the count-based placement")
	}
	module.modifiedMap["alice"] = 1
	module.graph.Reset()
	if module.graph.LogicalTransactions() != 0 || module.fetchModifiedMap("alice") != 1 {
		t.Fatal("round reset must clear history while retaining the committed map")
	}
}
