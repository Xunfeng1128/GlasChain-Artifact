package chain

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"blockEmulator/core"
	"blockEmulator/params"
)

func enableUnifiedTest(t *testing.T) {
	t.Helper()
	oldMode, oldUnified, oldUnit := params.BlockCapacityMode, params.UnifiedExecution, params.UnifiedCrossCallGas
	params.BlockCapacityMode, params.UnifiedExecution, params.UnifiedCrossCallGas = "gas", true, 20
	t.Cleanup(func() {
		params.BlockCapacityMode, params.UnifiedExecution, params.UnifiedCrossCallGas = oldMode, oldUnified, oldUnit
	})
}

func unifiedTestTx(id, sender string, base uint64) *core.Transaction {
	tx := core.NewTransaction(sender, "recipient", big.NewInt(0), 1, time.Unix(1, 0))
	tx.SourceTxHash, tx.GLASBaseGas, tx.GLASCallGasPerCall = id, base, 20
	return tx
}

func unifiedTestChain() *BlockChain {
	bc := testExactManifestBlockChain()
	bc.PartitionMap = map[string]uint64{"a": 0, "b": 0, "c": 0, "d": 0, "recipient": 0, "remote": 1}
	return bc
}

func TestUnifiedGenerateBlockPreviewsCommonDependencyPolicy(t *testing.T) {
	enableUnifiedTest(t)
	bc := unifiedTestChain()
	first, blocked, sameSender, filler := unifiedTestTx("first", "a", 60), unifiedTestTx("blocked", "b", 50), unifiedTestTx("same-sender", "b", 10), unifiedTestTx("filler", "c", 20)
	relay := unifiedTestTx("relay", "remote", 20)
	relay.Relayed = true
	priced, err := bc.PrepareUnifiedTransactions([]*core.Transaction{first, blocked, sameSender, filler, relay})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bc.Txpool.AddTxs2PoolUniqueByExecutionID(priced); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		block := bc.GenerateBlock(0)
		if len(block.Body) != 3 || block.Body[0].SourceTxHash != "relay" || block.Body[1].SourceTxHash != "first" || block.Body[2].SourceTxHash != "filler" {
			t.Fatalf("unexpected shared selection: %+v", block.Body)
		}
		if err := bc.IsValidBlock(block); err != nil {
			t.Fatal(err)
		}
		if bc.Txpool.GetTxQueueLen() != 5 {
			t.Fatal("proposal preview removed records")
		}
		for _, tx := range priced {
			if !tx.Broker1LeavePoolTime.IsZero() {
				t.Fatal("proposal preview changed leave-pool timestamps")
			}
		}
	}
	// The original configuration continues strict prefix packing and removes
	// only its selected records, without the opt-in Relay2 priority.
	params.UnifiedExecution = false
	legacy := bc.GenerateBlock(0)
	if len(legacy.Body) != 1 || legacy.Body[0].SourceTxHash != "first" || bc.Txpool.GetTxQueueLen() != 4 {
		t.Fatal("legacy FIFO behavior changed")
	}
}

func TestUnifiedAdmissionAndMigrationPreserveBaseAndReceipts(t *testing.T) {
	enableUnifiedTest(t)
	bc := unifiedTestChain()
	tx := unifiedTestTx("wait", "a", 80)
	tx.GLASCallRelations = []core.GLASCallRelation{{Caller: "recipient", Callee: "remote"}, {Caller: "recipient", Callee: "remote"}}
	priced, err := bc.PrepareUnifiedTransactions([]*core.Transaction{tx})
	if err != nil || priced[0].GasUsed != 120 {
		t.Fatalf("base-valid oversize surcharge should wait: %v %+v", err, priced)
	}
	if tx.GasUsed != 0 || tx.GLASAttachedGas != 0 {
		t.Fatal("admission mutated received stage")
	}
	if _, err := bc.Txpool.AddTxs2PoolUniqueByExecutionID(priced); err != nil {
		t.Fatal(err)
	}
	if got := bc.GenerateBlock(0); len(got.Body) != 0 {
		t.Fatal("oversize stage was undercharged or packed")
	}
	oldReceipt := priced[0]
	bc.Update_PartitionMap("remote", 0)
	if err := bc.RepriceUnifiedPool(); err != nil {
		t.Fatal(err)
	}
	if oldReceipt.GasUsed != 120 || bc.Txpool.TxQueue[0] == oldReceipt {
		t.Fatal("mapping repricing changed an earlier proposal object")
	}
	if got := bc.GenerateBlock(0); len(got.Body) != 1 || got.Body[0].GasUsed != 80 || core.BaseTransactionGas(got.Body[0]) != 80 {
		t.Fatal("new mapping did not restore packable immutable base")
	}
	tooLarge := unifiedTestTx("base-overflow", "b", 101)
	if _, err := bc.PrepareUnifiedTransactions([]*core.Transaction{tooLarge}); err == nil {
		t.Fatal("admitted impossible base Gas")
	}
}

func TestUnifiedStrictBlockChecksPriceOwnerDuplicatesAndCapacity(t *testing.T) {
	enableUnifiedTest(t)
	bc := unifiedTestChain()
	a, b := unifiedTestTx("a", "a", 60), unifiedTestTx("b", "b", 50)
	priced, err := bc.PrepareUnifiedTransactions([]*core.Transaction{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if err := bc.ValidateUnifiedBlock(priced); !errors.Is(err, core.ErrExecutionGasLimit) {
		t.Fatalf("accepted aggregate over-limit block: %v", err)
	}
	if err := bc.ValidateUnifiedBlock([]*core.Transaction{priced[0], priced[0]}); !errors.Is(err, core.ErrExecutionIDNotUnique) {
		t.Fatalf("accepted duplicate execution: %v", err)
	}
	bad := *priced[0]
	bad.GasUsed--
	if bc.ValidateUnifiedBlock([]*core.Transaction{&bad}) == nil {
		t.Fatal("repaired an undercharged block instead of rejecting it")
	}
	bad = *priced[0]
	bad.Sender = "remote"
	if bc.ValidateUnifiedBlock([]*core.Transaction{&bad}) == nil {
		t.Fatal("accepted foreign execution owner")
	}
	bad = *priced[0]
	bad.GLASBaseGas = 0
	if bc.ValidateUnifiedBlock([]*core.Transaction{&bad}) == nil {
		t.Fatal("accepted legacy record bypassing shared accounting")
	}
	if err := bc.Txpool.CommitExecutionRecordsWithoutPool(priced[:1], 100); err != nil {
		t.Fatal(err)
	}
	if err := bc.ValidateUnifiedBlock(priced[:1]); !errors.Is(err, core.ErrExecutionAlreadyCommitted) {
		t.Fatalf("accepted already committed stage: %v", err)
	}
}
