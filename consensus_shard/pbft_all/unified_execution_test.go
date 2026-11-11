package pbft_all

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"testing"
	"time"

	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func unifiedPBFTFixture(t *testing.T) {
	t.Helper()
	oldSwitch, oldGas, oldMode, oldPath := params.UnifiedExecution, params.UnifiedCrossCallGas, params.BlockCapacityMode, params.DatabaseWrite_path
	t.Cleanup(func() {
		params.UnifiedExecution, params.UnifiedCrossCallGas, params.BlockCapacityMode, params.DatabaseWrite_path = oldSwitch, oldGas, oldMode, oldPath
	})
	params.UnifiedExecution, params.UnifiedCrossCallGas, params.BlockCapacityMode = true, 20, "gas"
	params.DatabaseWrite_path = t.TempDir() + "/"
}

func unifiedPBFTNode(t *testing.T, nodeID uint64) *PbftConsensusNode {
	t.Helper()
	cfg := &params.ChainConfig{ShardID: 0, NodeID: nodeID, ShardNums: 2, Nodes_perShard: 4, BlockGasLimit: 100}
	bc, err := chain.NewBlockChain(cfg, rawdb.NewMemoryDatabase())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bc.CloseBlockChain)
	for address, shard := range map[string]uint64{"a": 0, "b": 0, "c": 1, "incoming": 1} {
		bc.Update_PartitionMap(address, shard)
	}
	return &PbftConsensusNode{NodeID: nodeID, node_nums: 4, CurChain: bc, pbftChainConfig: cfg,
		seqIDMap: make(map[uint64]uint64), pl: &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)}}
}

func unifiedPBFTTx(source string) *core.Transaction {
	tx := core.NewTransaction("a", "b", big.NewInt(1), 0, time.Unix(1, 0))
	tx.SourceTxHash, tx.GLASBaseGas, tx.GasUsed, tx.GLASCallGasPerCall = source, 40, 40, 20
	tx.GLASCallRelations = []core.GLASCallRelation{{Caller: "b", Callee: "c"}}
	return tx
}

// Exercise real injection handlers, not just arithmetic helpers: the baseline
// can ignore calls in its planner while the worker charges exactly as GLAS.
func TestUnifiedBaselineAndGLASInjectionChargeSameWork(t *testing.T) {
	unifiedPBFTFixture(t)
	raw, dynamic, glas := unifiedPBFTNode(t, 0), unifiedPBFTNode(t, 1), unifiedPBFTNode(t, 2)
	dynamic.view.Store(1)
	glas.view.Store(2)
	tx := unifiedPBFTTx("same-logical-transaction")
	if err := core.PriceExecutionStage(tx, raw.CurChain.Get_PartitionMap); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(message.InjectTxs{Txs: []*core.Transaction{tx}})
	(&RawRelayOutsideModule{pbftNode: raw}).handleInjectTx(payload)
	(&CLPARelayOutsideModule{pbftNode: dynamic}).handleInjectTx(payload)
	(&CLPARelayOutsideModule{pbftNode: glas}).handleInjectTx(payload)
	for _, node := range []*PbftConsensusNode{raw, dynamic, glas} {
		if node.CurChain.Txpool.GetTxQueueLen() != 1 {
			t.Fatalf("node %d lost transaction", node.NodeID)
		}
		got := node.CurChain.Txpool.TxQueue[0]
		if got.GasUsed != 60 || got.GLASAttachedGas != 20 || got.GLASBaseGas != 40 || got.SourceTxHash != tx.SourceTxHash {
			t.Fatalf("node %d charged different work: %+v", node.NodeID, got)
		}
	}
}

func TestUnifiedPreviewCommitAndRelayKeepReceiptAndRejectReplay(t *testing.T) {
	unifiedPBFTFixture(t)
	leader, follower := unifiedPBFTNode(t, 0), unifiedPBFTNode(t, 1)
	for _, node := range []*PbftConsensusNode{leader, follower} {
		node.CurChain.Update_PartitionMap("b", 1)
		node.CurChain.Update_PartitionMap("c", 0)
	}
	tx := unifiedPBFTTx("cross-top")
	if err := leader.installExecutionTransactions([]*core.Transaction{tx}, false); err != nil {
		t.Fatal(err)
	}
	block := leader.generateExecutionBlock()
	if leader.CurChain.Txpool.GetTxQueueLen() != 1 || block.Body[0].GasUsed != 40 {
		t.Fatal("proposal removed work or charged attached Gas on Relay1")
	}
	for _, node := range []*PbftConsensusNode{leader, follower} {
		if err := node.commitUnifiedExecutionBlock(block); err != nil {
			t.Fatal(err)
		}
	}
	if leader.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("commit did not remove exact proposal records")
	}
	relay, err := leader.executionRelay2(block.Body[0], 3)
	if err != nil {
		t.Fatal(err)
	}
	if relay.GasUsed != 60 || !relay.Relayed || block.Body[0].GasUsed != 40 || block.Body[0].Relayed {
		t.Fatal("Relay2 billing mutated Relay1 receipt or missed attached Gas")
	}
	if err := follower.commitUnifiedExecutionBlock(block); err == nil {
		t.Fatal("replayed physical execution was accepted")
	}
	if err := leader.installExecutionTransactions([]*core.Transaction{relay}, false); err == nil {
		t.Fatal("Relay2 accepted through TOP injection")
	}
}

func TestUnifiedMigrationRepricesRetainedAndArrivingWork(t *testing.T) {
	unifiedPBFTFixture(t)
	leader, follower := unifiedPBFTNode(t, 0), unifiedPBFTNode(t, 1)
	retained := unifiedPBFTTx("retained")
	if err := leader.installExecutionTransactions([]*core.Transaction{retained}, false); err != nil {
		t.Fatal(err)
	}
	old := leader.CurChain.Txpool.TxQueue[0]
	incoming := unifiedPBFTTx("arriving")
	incoming.Sender = "incoming"
	transfer := &message.AccountTransferMsg{ModifiedMap: map[string]uint64{"c": 0, "incoming": 0}, ATid: 1}
	for _, node := range []*PbftConsensusNode{leader, follower} {
		cdm := dataSupport.NewCLPADataSupport()
		if node.NodeID == 0 {
			cdm.ReceivedNewTx = []*core.Transaction{incoming}
		}
		(&CLPAPbftInsideExtraHandleMod{pbftNode: node, cdm: cdm}).accountTransfer_do(transfer)
	}
	if leader.CurChain.Txpool.GetTxQueueLen() != 2 || follower.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("migration lost leader work or created a follower pending queue")
	}
	for _, tx := range leader.CurChain.Txpool.TxQueue {
		if tx.GasUsed != 40 || tx.GLASAttachedGas != 0 {
			t.Fatal("new map did not reprice pending execution")
		}
	}
	if old.GasUsed != 60 || !bytes.Equal(leader.CurChain.CurrentBlock.Hash, follower.CurChain.CurrentBlock.Hash) {
		t.Fatal("migration changed an old receipt or replica migration heads diverged")
	}
	if err := follower.CurChain.IsValidBlock(leader.generateExecutionBlock()); err != nil {
		t.Fatalf("follower cannot validate post-migration block: %v", err)
	}
}
