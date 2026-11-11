package optimizer

import (
	"math"
	"testing"
)

func TestWeightedCostUsesExtraGasAndCapacity(t *testing.T) {
	base := ExecutionScore{BaseDemandGas: 100, NewCrossGas: 80, CapacityPressure: 1,
		TopLevelSources: 10, TopLevelCrossSources: 1}
	proposal := ExecutionScore{BaseDemandGas: 100, NewCrossGas: 10, CapacityPressure: 1,
		TopLevelSources: 10, TopLevelCrossSources: 8, Moves: 5}
	if !BetterFinalExecutionScore(proposal, base, SelectionWeightedCost, .5) {
		t.Fatal("extra Gas cost did not determine the winner")
	}
	// No TOP is NOT a reason to drop variable remaining internal-call costs.
	base.TopLevelSources, proposal.TopLevelSources = 0, 0
	proposal.CapacityPressure = 2
	if !BetterFinalExecutionScore(proposal, base, SelectionWeightedCost, 1) ||
		BetterFinalExecutionScore(proposal, base, SelectionWeightedCost, 0) {
		t.Fatal("carry-only cost or lambda endpoint semantics are wrong")
	}
	if BetterFinalExecutionScore(base, base, SelectionWeightedCost, .5) {
		t.Fatal("cost tie replaced incumbent")
	}
	if (ExecutionScore{}).FinalObjectiveValue(SelectionWeightedCost, 1) != 0 ||
		!math.IsInf((ExecutionScore{NewCrossGas: 1}).CostRatio(), 1) {
		t.Fatal("zero denominator handling is wrong")
	}
	if ObjectiveVersion(SelectionWeightedCost) != "cost_capacity_v1" {
		t.Fatal("objective version is ambiguous")
	}
}
