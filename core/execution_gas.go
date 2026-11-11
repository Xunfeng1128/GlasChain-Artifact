package core

import "fmt"

// BaseTransactionGas is the immutable workload weight for Gas-aware planners.
// Execution surcharges must not feed back into their historical graph weights.
func BaseTransactionGas(tx *Transaction) uint64 {
	if tx != nil && tx.GLASBaseGas > 0 {
		return tx.GLASBaseGas
	}
	return EffectiveGasUsed(tx)
}

// The execution helpers intentionally reuse GLAS's attached-call arithmetic.
// Keeping the original entry points preserves existing configurations and
// planner simulations; all opt-in comparison methods use these same rules.
func PriceExecutionStage(tx *Transaction, shardFor func(string) uint64) error {
	return PriceGLASStage(tx, shardFor)
}

func ValidateExecutionStageGas(tx *Transaction, shardFor func(string) uint64, limit uint64) error {
	return ValidateGLASStageGas(tx, shardFor, limit)
}

func ValidateExecutionAdmissionGas(tx *Transaction, shardFor func(string) uint64, limit uint64) error {
	return ValidateGLASAdmissionGas(tx, shardFor, limit)
}

// ValidateExecutionAdmissionBaseGas checks only the immutable base-capacity
// constraint. Migration may use it after PriceExecutionStage has just computed
// the trusted price, or for an unchanged internally priced pool record. Wire
// records and blocks still require the full price validators above.
func ValidateExecutionAdmissionBaseGas(tx *Transaction, limit uint64) error {
	if tx != nil && limit > 0 && tx.GLASBaseGas > limit {
		return fmt.Errorf("GLAS stage %s baseGas=%d exceeds block gas limit=%d", TransactionExecutionID(tx), tx.GLASBaseGas, limit)
	}
	return nil
}

func NewExecutionRelay2(top *Transaction, epoch uint64, shardFor func(string) uint64) (*Transaction, error) {
	return NewGLASRelay2(top, epoch, shardFor)
}

// ValidateExecutionRecord prevents a legacy or synthetic record from silently
// bypassing opt-in billing. Price validation and shard ownership are separate.
func ValidateExecutionRecord(tx *Transaction, gasPerCall uint64) error {
	if tx == nil || tx.GLASBaseGas == 0 || tx.SourceTxHash == "" || len(tx.TxHash) == 0 ||
		tx.Sender == "" || tx.Recipient == "" || tx.Value == nil || tx.Value.Sign() < 0 ||
		tx.IsGLASCallTask || tx.HasBroker {
		return fmt.Errorf("invalid unified execution record")
	}
	if tx.GLASCallGasPerCall != gasPerCall {
		return fmt.Errorf("execution %s cross-call unit cost=%d, expected=%d", TransactionExecutionID(tx), tx.GLASCallGasPerCall, gasPerCall)
	}
	for _, call := range tx.GLASCallRelations {
		if call.Caller == "" || call.Callee == "" {
			return fmt.Errorf("execution %s has an empty call endpoint", TransactionExecutionID(tx))
		}
	}
	return nil
}

// ExecutionPriority matches GLAS map_only packing: already-started Relay2
// records precede untouched TOP records. Legacy synthetic calls retain their
// existing priority for callers outside unified execution.
func ExecutionPriority(tx *Transaction) int {
	if tx != nil && (tx.Relayed || tx.IsGLASCallTask) {
		return 0
	}
	return 1
}

// ExecutionDependency keeps each sender's TOP order and each logical
// transaction's descendant order while allowing independent records to fill
// block Gas left over by a record that does not fit.
func ExecutionDependency(tx *Transaction) string {
	if tx == nil {
		return ""
	}
	if tx.Relayed || tx.IsGLASCallTask {
		if tx.SourceTxHash == "" {
			return ""
		}
		return "logical:" + tx.SourceTxHash
	}
	if tx.Sender == "" {
		return ""
	}
	return "sender:" + tx.Sender
}
