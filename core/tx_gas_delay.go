package core

import (
	"blockEmulator/params"
	"math"
)

// IntrinsicGas is the fixed intrinsic cost of every Ethereum transaction
// (signature recovery, decoding, nonce/balance checks). gasUsed above this
// floor represents actual EVM execution work.
const IntrinsicGas = 21000

// GasExecUnits returns the execution work of a tx: gasUsed spent above the
// intrinsic floor, clamped to non-negative. Monotone and unbounded.
func GasExecUnits(gasUsed uint64) float64 {
	if gasUsed <= IntrinsicGas {
		return 0
	}
	return float64(gasUsed - IntrinsicGas)
}

// ComputeTxExecDelayMs models a single tx's execution delay in ms (unrounded):
//
//	delay = FixedPerTxMs + GasSlopeMsPerGas * max(0, gasUsed-IntrinsicGas)
//
// Linear, monotone, no saturation cap.
func ComputeTxExecDelayMs(gasUsed uint64) float64 {
	if !params.EnableGasCostModel {
		return 0
	}
	return params.FixedPerTxMs + params.GasSlopeMsPerGas*GasExecUnits(gasUsed)
}

// ComputeBlockExecDelayMs sums the modeled execution delay of a block body
// in ms (unrounded).
func ComputeBlockExecDelayMs(txs []*Transaction) float64 {
	if !params.EnableGasCostModel {
		return 0
	}
	total := 0.0
	for _, tx := range txs {
		total += tx.ExecDelayMs
	}
	return total
}

// ScaleDelayMs scales a delay by a factor and rounds once to whole ms.
// This is the single rounding point of the gas-cost model.
func ScaleDelayMs(delayMs float64, factor float64) int64 {
	if delayMs <= 0 || factor <= 0 {
		return 0
	}
	return int64(math.Round(delayMs * factor))
}
