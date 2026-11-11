package optimizer

import (
	"crypto/sha256"
	"encoding/binary"
	"math"

	"blockEmulator/glasfreight/model"
)

const SelectionWeightedCost = "weighted_cost"

// ExecutionScore describes the complete remaining demand under a candidate map.
type ExecutionScore struct {
	NewCrossGas           uint64
	BaseDemandGas         uint64
	Moves                 int
	FullDemandMaxGas      uint64
	FullDemandTotalGas    uint64
	FullDemandPerShardGas []uint64
	CapacityPressure      float64
	TopLevelSources       uint64
	TopLevelCrossSources  uint64
	CarrySources          uint64
}

// CostRatio measures avoidable future cross-shard Gas relative to base demand.
func (score ExecutionScore) CostRatio() float64 {
	if score.BaseDemandGas == 0 {
		if score.NewCrossGas != 0 {
			return math.Inf(1)
		}
		return 0
	}
	return float64(score.NewCrossGas) / float64(score.BaseDemandGas)
}

// FinalObjectiveValue combines capacity pressure and the extra-Gas ratio.
func (score ExecutionScore) FinalObjectiveValue(_ string, weight float64) float64 {
	return (1-weight)*score.CapacityPressure + weight*score.CostRatio()
}

func ObjectiveVersion(_ string) string { return "cost_capacity_v1" }

// BetterFinalExecutionScore breaks an objective tie by actual move count.
func BetterFinalExecutionScore(left, right ExecutionScore, mode string, weight float64) bool {
	leftValue, rightValue := left.FinalObjectiveValue(mode, weight), right.FinalObjectiveValue(mode, weight)
	if leftValue != rightValue {
		return leftValue < rightValue
	}
	return left.Moves < right.Moves
}

// executionPlacementDigest hashes the shard assignment in a fixed address order.
func executionPlacementDigest(placement model.Placement, addresses []string) [32]byte {
	hash := sha256.New()
	var shard [8]byte
	for _, address := range addresses {
		binary.BigEndian.PutUint64(shard[:], placement[address])
		_, _ = hash.Write(shard[:])
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}
