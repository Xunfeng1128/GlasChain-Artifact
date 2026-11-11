package core

import (
	"fmt"
	"math"
)

// GLASCallRelation represents one successful SCA->SCA call occurrence, not a
// deduplicated edge. The reader filters invalid traces before creating it.
type GLASCallRelation struct {
	Caller string
	Callee string
}

// PriceGLASStageGas is the shared, overflow-safe arithmetic for one physical
// stage. crossCallCount counts call occurrences, including duplicate edges.
// A cross-shard TOP/RELAY1 does not pay attached gas; local TOP and RELAY2 do.
// This helper does not validate records or mutate transaction metadata.
func PriceGLASStageGas(baseGas, gasPerCall, crossCallCount uint64, paysAttached bool) (gasUsed uint64, attachedGas uint64, err error) {
	if !paysAttached || gasPerCall == 0 {
		return baseGas, 0, nil
	}
	if crossCallCount > (math.MaxUint64-baseGas)/gasPerCall {
		return 0, 0, fmt.Errorf("GLAS attached gas overflow")
	}
	attachedGas = crossCallCount * gasPerCall
	return baseGas + attachedGas, attachedGas, nil
}

// PriceGLASStage prices an uncommitted TOP or RELAY2 against its execution map.
// It is idempotent: never derive the base from a previously augmented GasUsed.
// TOP on distinct endpoint shards pays only G. Local TOP and every RELAY2 pay
// G + (# remote internal calls)*unit. Legacy records remain unchanged.
func PriceGLASStage(tx *Transaction, shardFor func(string) uint64) error {
	if tx == nil || tx.GLASBaseGas == 0 {
		return nil
	}
	if tx.IsGLASCallTask || tx.HasBroker || shardFor == nil {
		return fmt.Errorf("invalid attached-call gas record %s", TransactionExecutionID(tx))
	}
	var count uint64
	for _, call := range tx.GLASCallRelations {
		if call.Caller == "" || call.Callee == "" {
			return fmt.Errorf("empty GLAS call endpoint for %s", TransactionExecutionID(tx))
		}
		if shardFor(call.Caller) != shardFor(call.Callee) {
			count++
		}
	}
	gasUsed, extra, err := PriceGLASStageGas(tx.GLASBaseGas, tx.GLASCallGasPerCall, count,
		tx.Relayed || shardFor(tx.Sender) == shardFor(tx.Recipient))
	if err != nil {
		return fmt.Errorf("%w for %s", err, TransactionExecutionID(tx))
	}
	tx.GLASCrossCallCount = count
	tx.GLASAttachedGas = extra
	tx.GasUsed = gasUsed
	tx.ExecDelayMs = ComputeTxExecDelayMs(tx.GasUsed)
	return nil
}

// ValidateGLASStageGas checks rather than repairing an incoming/committed record.
func ValidateGLASStageGas(tx *Transaction, shardFor func(string) uint64, limit uint64) error {
	if tx == nil || tx.GLASBaseGas == 0 {
		return nil
	}
	priced := *tx
	if err := PriceGLASStage(&priced, shardFor); err != nil {
		return err
	}
	if priced.GasUsed != tx.GasUsed || priced.GLASAttachedGas != tx.GLASAttachedGas ||
		priced.GLASCrossCallCount != tx.GLASCrossCallCount || priced.ExecDelayMs != tx.ExecDelayMs {
		return fmt.Errorf("GLAS stage %s has gas inconsistent with execution map", TransactionExecutionID(tx))
	}
	if limit > 0 && tx.GasUsed > limit {
		return fmt.Errorf("GLAS stage %s gas=%d exceeds block gas limit=%d", TransactionExecutionID(tx), tx.GasUsed, limit)
	}
	return nil
}

// ValidateGLASAdmissionGas permits a correctly priced, base-valid record to
// wait in the pool even when its map-dependent surcharge cannot fit a block.
// Proposals and committed blocks must still use ValidateGLASStageGas.
func ValidateGLASAdmissionGas(tx *Transaction, shardFor func(string) uint64, limit uint64) error {
	if tx == nil || tx.GLASBaseGas == 0 {
		return nil
	}
	if limit > 0 && tx.GLASBaseGas > limit {
		return fmt.Errorf("GLAS stage %s baseGas=%d exceeds block gas limit=%d", TransactionExecutionID(tx), tx.GLASBaseGas, limit)
	}
	return ValidateGLASStageGas(tx, shardFor, 0)
}

// NewGLASRelay2 must not mutate the committed RELAY1 or its block receipt.
func NewGLASRelay2(top *Transaction, epoch uint64, shardFor func(string) uint64) (*Transaction, error) {
	if top == nil || top.IsGLASCallTask {
		return nil, fmt.Errorf("invalid RELAY1 record")
	}
	relay := *top
	relay.GLASCallRelations = append([]GLASCallRelation(nil), top.GLASCallRelations...)
	relay.Relayed = true
	relay.GLASStageEpoch, relay.GLASStageEpochSet = epoch, true
	if err := PriceGLASStage(&relay, shardFor); err != nil {
		return nil, err
	}
	return &relay, nil
}
