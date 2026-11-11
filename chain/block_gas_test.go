package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"math/big"
	"testing"
	"time"
)

func testGasBlockChain() *BlockChain {
	return &BlockChain{
		ChainConfig: &params.ChainConfig{ShardID: 0},
		PartitionMap: map[string]uint64{
			"sender":    0,
			"recipient": 1,
			"local":     0,
		},
	}
}

func TestGasUsedForPackingChargesFullGasToBothRelayStages(t *testing.T) {
	bc := testGasBlockChain()
	tx := core.NewTransaction("sender", "recipient", big.NewInt(1), 1, time.Now())
	tx.GasUsed = 10_000_001

	if got := bc.gasUsedForPacking(tx); got != 10_000_001 {
		t.Fatalf("relay1 expected full gas 10000001, got %d", got)
	}
	tx.Relayed = true
	if got := bc.gasUsedForPacking(tx); got != 10_000_001 {
		t.Fatalf("relay2 expected full gas 10000001, got %d", got)
	}
}

func TestGasUsedForPackingKeepsInnerAndBrokerGas(t *testing.T) {
	bc := testGasBlockChain()

	inner := core.NewTransaction("sender", "local", big.NewInt(1), 1, time.Now())
	inner.GasUsed = 10_000_000
	if got := bc.gasUsedForPacking(inner); got != 10_000_000 {
		t.Fatalf("inner tx expected full gas, got %d", got)
	}

	broker := core.NewTransaction("sender", "recipient", big.NewInt(1), 2, time.Now())
	broker.GasUsed = 10_000_000
	broker.HasBroker = true
	if got := bc.gasUsedForPacking(broker); got != 10_000_000 {
		t.Fatalf("broker tx expected existing full-gas behavior, got %d", got)
	}
}

func TestGasUsedForPackingChargesFullIntrinsicGasToBothRelayStages(t *testing.T) {
	bc := testGasBlockChain()
	tx := core.NewTransaction("sender", "recipient", big.NewInt(1), 1, time.Now())

	first := bc.gasUsedForPacking(tx)
	tx.Relayed = true
	second := bc.gasUsedForPacking(tx)
	if first != core.IntrinsicGas || second != core.IntrinsicGas {
		t.Fatalf("intrinsic gas charged as relay1=%d relay2=%d", first, second)
	}
}
