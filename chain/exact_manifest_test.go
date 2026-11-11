package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
)

func testExactManifestBlockChain() *BlockChain {
	db := rawdb.NewMemoryDatabase()
	trieDB := trie.NewDatabase(db)
	stateTrie := trie.NewEmpty(trieDB)
	header := &core.BlockHeader{
		Number:    0,
		StateRoot: stateTrie.Hash().Bytes(),
		TxRoot:    GetTxTreeRoot(nil),
		Bloom:     *GetBloomFilter(nil),
	}
	genesis := core.NewBlock(header, nil)
	genesis.Hash = header.Hash()
	return &BlockChain{
		db:           db,
		triedb:       trieDB,
		ChainConfig:  &params.ChainConfig{ShardID: 0, BlockGasLimit: 100},
		CurrentBlock: genesis,
		Txpool:       core.NewTxPool(),
		PartitionMap: make(map[string]uint64),
	}
}

func TestGenerateExactManifestBlockRepeatedAndFailedProposalKeepsPool(t *testing.T) {
	originalCapacityMode := params.BlockCapacityMode
	params.BlockCapacityMode = "gas"
	defer func() { params.BlockCapacityMode = originalCapacityMode }()

	bc := testExactManifestBlockChain()
	outside := core.NewTransaction("outside", "recipient", big.NewInt(0), 1, time.Now())
	outside.SourceTxHash = "outside"
	outside.GasUsed = 5
	planned := core.NewTransaction("entry", "callee", big.NewInt(0), 2, time.Now())
	planned.IsGLASCallTask = true
	planned.GLASCallTaskID = "planned-call"
	planned.GLASCallCallee = "callee"
	planned.SourceTxHash = "source"
	planned.GasUsed = 40
	bc.Txpool.AddTxs2Pool([]*core.Transaction{outside, planned})
	ids := []string{core.TransactionExecutionID(planned)}

	for proposal := 0; proposal < 2; proposal++ {
		block, ready, err := bc.GenerateExactManifestBlock(7, ids)
		if err != nil || !ready {
			t.Fatalf("proposal %d: block=%#v ready=%v err=%v", proposal, block, ready, err)
		}
		if block == nil || len(block.Body) != 1 || block.Body[0] != planned {
			t.Fatalf("proposal %d body=%#v", proposal, block)
		}
		if len(bc.Txpool.TxQueue) != 2 || bc.Txpool.TxQueue[0] != outside || bc.Txpool.TxQueue[1] != planned {
			t.Fatalf("proposal %d modified pool=%#v", proposal, bc.Txpool.TxQueue)
		}
		if !outside.Broker1LeavePoolTime.IsZero() || !planned.Broker1LeavePoolTime.IsZero() {
			t.Fatalf("proposal %d wrote leave-pool timestamp", proposal)
		}
	}

	block, ready, err := bc.GenerateExactManifestBlock(7, []string{ids[0], "CALL:not-arrived"})
	if err != nil || ready || block != nil {
		t.Fatalf("failed proposal: block=%#v ready=%v err=%v", block, ready, err)
	}
	if len(bc.Txpool.TxQueue) != 2 || bc.Txpool.TxQueue[0] != outside || bc.Txpool.TxQueue[1] != planned {
		t.Fatalf("failed proposal modified pool=%#v", bc.Txpool.TxQueue)
	}
	if !outside.Broker1LeavePoolTime.IsZero() || !planned.Broker1LeavePoolTime.IsZero() {
		t.Fatal("failed proposal wrote leave-pool timestamp")
	}
}

func TestPreviewDependencyAwareTieredPriorityBlockMatchesRealSelection(t *testing.T) {
	originalCapacityMode := params.BlockCapacityMode
	params.BlockCapacityMode = "gas"
	defer func() { params.BlockCapacityMode = originalCapacityMode }()

	bc := testExactManifestBlockChain()
	makeCall := func(id, dependency string, gas uint64, nonce uint64) *core.Transaction {
		tx := core.NewTransaction("entry", "callee", big.NewInt(0), nonce, time.Now())
		tx.IsGLASCallTask = true
		tx.GLASCallTaskID = id
		tx.GLASCallCallee = "callee"
		tx.SourceTxHash = dependency
		tx.GasUsed = gas
		return tx
	}
	first := makeCall("first", "dep-a", 60, 1)
	blocked := makeCall("blocked", "dep-b", 50, 2)
	filler := makeCall("filler", "dep-c", 40, 3)
	bc.Txpool.AddTxs2Pool([]*core.Transaction{first, blocked, filler})
	priority := func(*core.Transaction) int { return 0 }
	dependency := func(tx *core.Transaction) string { return tx.SourceTxHash }

	preview := bc.PreviewDependencyAwareTieredPriorityBlock(7, priority, 1, dependency)
	if preview == nil || len(preview.Body) != 2 || preview.Body[0] != first || preview.Body[1] != filler {
		t.Fatalf("preview body=%#v", preview)
	}
	if len(bc.Txpool.TxQueue) != 3 || bc.Txpool.TxQueue[0] != first || bc.Txpool.TxQueue[1] != blocked || bc.Txpool.TxQueue[2] != filler {
		t.Fatalf("preview modified queue=%#v", bc.Txpool.TxQueue)
	}
	if !first.Broker1LeavePoolTime.IsZero() || !blocked.Broker1LeavePoolTime.IsZero() || !filler.Broker1LeavePoolTime.IsZero() {
		t.Fatal("preview wrote leave-pool timestamps")
	}

	real := bc.GenerateDependencyAwareTieredPriorityBlock(7, priority, 1, dependency)
	if real == nil || len(real.Body) != 2 || real.Body[0] != first || real.Body[1] != filler {
		t.Fatalf("real body diverged from preview=%#v", real)
	}
	if len(bc.Txpool.TxQueue) != 1 || bc.Txpool.TxQueue[0] != blocked {
		t.Fatalf("real generation retained=%#v", bc.Txpool.TxQueue)
	}
}
