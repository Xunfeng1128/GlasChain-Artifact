package core

import (
	"errors"
	"math/big"
	"testing"
	"time"
)

func executionTestTransaction(source, sender, recipient string, gas uint64) *Transaction {
	tx := NewTransaction(sender, recipient, big.NewInt(0), 1, time.Unix(1, 0))
	tx.SourceTxHash = source
	tx.GLASBaseGas, tx.GLASCallGasPerCall = gas, 20
	tx.GLASCallRelations = []GLASCallRelation{{Caller: "b", Callee: "c"}, {Caller: "b", Callee: "c"}}
	return tx
}

func TestExecutionGasLocalAndRelayUseSameCallOccurrences(t *testing.T) {
	mapping := map[string]uint64{"a": 0, "b": 0, "c": 1}
	resolve := func(address string) uint64 { return mapping[address] }
	local := executionTestTransaction("local", "a", "b", 40)
	if err := PriceExecutionStage(local, resolve); err != nil {
		t.Fatal(err)
	}
	if local.GasUsed != 80 || local.GLASAttachedGas != 40 || local.GLASCrossCallCount != 2 || BaseTransactionGas(local) != 40 {
		t.Fatalf("duplicate call occurrences not charged individually: %+v", local)
	}
	mapping["b"] = 1
	mapping["c"] = 0
	top := executionTestTransaction("cross", "a", "b", 40)
	if err := PriceExecutionStage(top, resolve); err != nil {
		t.Fatal(err)
	}
	if top.GasUsed != 40 || top.GLASAttachedGas != 0 {
		t.Fatal("RELAY1 must charge only base Gas")
	}
	relay, err := NewExecutionRelay2(top, 7, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if relay.GasUsed != 80 || relay.GLASAttachedGas != 40 || !relay.Relayed || !relay.GLASStageEpochSet || relay.GLASStageEpoch != 7 {
		t.Fatalf("incorrect RELAY2: %+v", relay)
	}
	if top.Relayed || top.GasUsed != 40 || top.GLASAttachedGas != 0 {
		t.Fatal("relay construction changed committed RELAY1")
	}
	if err := PriceExecutionStage(relay, resolve); err != nil || relay.GasUsed != 80 {
		t.Fatal("repeated repricing compounded surcharge", err)
	}
	mapping["c"] = 1
	if err := PriceExecutionStage(relay, resolve); err != nil || relay.GasUsed != 40 || BaseTransactionGas(relay) != 40 {
		t.Fatal("mapping change did not reprice from immutable base", err)
	}
	relay.GLASCallRelations[0].Callee = "changed"
	if top.GLASCallRelations[0].Callee != "c" {
		t.Fatal("relay shares mutable call metadata with committed record")
	}
}

func TestExecutionMetadataCannotBypassBilling(t *testing.T) {
	tx := executionTestTransaction("source", "a", "b", 40)
	for name, change := range map[string]func(*Transaction){
		"missing base":   func(tx *Transaction) { tx.GLASBaseGas = 0 },
		"missing source": func(tx *Transaction) { tx.SourceTxHash = "" },
		"wrong unit":     func(tx *Transaction) { tx.GLASCallGasPerCall = 0 },
		"broker":         func(tx *Transaction) { tx.HasBroker = true },
		"synthetic call": func(tx *Transaction) { tx.IsGLASCallTask = true },
	} {
		t.Run(name, func(t *testing.T) {
			bad := *tx
			change(&bad)
			if ValidateExecutionRecord(&bad, 20) == nil {
				t.Fatal("accepted malformed common execution metadata")
			}
		})
	}
	legacy := &Transaction{GasUsed: 42}
	if BaseTransactionGas(legacy) != 42 || BaseTransactionGas(&Transaction{}) != IntrinsicGas {
		t.Fatal("legacy planner Gas fallback changed")
	}
}

func TestExecutionDedupRejectsDifferentCallMetadataWithSameStageGas(t *testing.T) {
	tx := executionTestTransaction("source", "a", "b", 40)
	mapping := map[string]uint64{"a": 0, "b": 1, "c": 0, "d": 0}
	resolve := func(address string) uint64 { return mapping[address] }
	if err := PriceExecutionStage(tx, resolve); err != nil {
		t.Fatal(err)
	}
	pool := NewTxPool()
	if _, err := pool.AddTxs2PoolUniqueByExecutionID([]*Transaction{tx}); err != nil {
		t.Fatal(err)
	}
	changed := *tx
	changed.GLASCallRelations = append([]GLASCallRelation(nil), tx.GLASCallRelations...)
	changed.GLASCallRelations[0].Callee = "d"
	if changed.GasUsed != tx.GasUsed {
		t.Fatal("fixture must retain identical current-stage Gas")
	}
	if _, err := pool.AddTxs2PoolUniqueByExecutionID([]*Transaction{&changed}); !errors.Is(err, ErrExecutionIDConflict) {
		t.Fatalf("same stage Gas hid conflicting call metadata: %v", err)
	}
	if pool.GetTxQueueLen() != 1 {
		t.Fatal("failed batch changed pool")
	}
	if n, err := pool.AddTxs2PoolUniqueByExecutionID([]*Transaction{tx}); err != nil || n != 0 {
		t.Fatal("identical callback must remain idempotent", n, err)
	}
}
