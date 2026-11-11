package core

// EffectiveGasUsed returns the gas charged by the block packer for a complete
// transaction. Transactions without an explicit gas value retain the existing
// intrinsic-gas fallback.
func EffectiveGasUsed(tx *Transaction) uint64 {
	if tx == nil {
		return 0
	}
	if tx.GasUsed == 0 {
		return IntrinsicGas
	}
	return tx.GasUsed
}

// RelayStageGasUsed returns the gas charged to either execution stage of a
// regular cross-shard transaction. Both relay1 and relay2 reserve the full
// transaction gas in their respective shard blocks.
func RelayStageGasUsed(totalGas uint64) uint64 {
	return totalGas
}
