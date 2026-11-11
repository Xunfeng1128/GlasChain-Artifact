// Package optimizer implements GlasChain's cost-FENNEL placement search.
package optimizer

import (
	"fmt"
	"math"
	"sort"

	"blockEmulator/glasfreight/graph"
	"blockEmulator/glasfreight/model"
)

type Config struct {
	ShardCount           uint64
	RhoCall              float64
	BlockGasLimit        uint64
	BlocksPerEpoch       uint64
	FennelGamma          float64
	Restarts             int
	RefinementPasses     int
	RefinementCandidates int
	MaxMoves             int
	ObjectiveWeight      float64
	SelectionMode        string
	ExecutionBaseline    model.Placement
}

func (config Config) Validate() error {
	if config.SelectionMode != SelectionWeightedCost {
		return fmt.Errorf("cost_fennel requires weighted_cost final selection")
	}
	if config.ObjectiveWeight < 0 || config.ObjectiveWeight > 1 || math.IsNaN(config.ObjectiveWeight) || math.IsInf(config.ObjectiveWeight, 0) {
		return fmt.Errorf("objective weight must be finite and in [0,1]")
	}
	if config.ShardCount == 0 || config.RhoCall < 0 || config.RhoCall > 1 || math.IsNaN(config.RhoCall) || config.BlockGasLimit == 0 {
		return fmt.Errorf("positive shard count/block gas limit and rhoCall in [0,1] are required")
	}
	if config.FennelGamma <= 1 || math.IsNaN(config.FennelGamma) {
		return fmt.Errorf("FENNEL gamma must be greater than 1")
	}
	if config.Restarts != 2 || config.RefinementPasses != 0 || config.RefinementCandidates < 1 || config.MaxMoves < 0 {
		return fmt.Errorf("cost_fennel requires two orders and disabled refinement")
	}
	return nil
}

type CandidateAudit struct {
	Kind                 string
	Restart              int
	MaxBlocks            uint64
	MaxShardGas          float64
	LoadDeviation        float64
	CrossRatio           float64
	GasPerTxCV           float64
	Moves                int
	Feasible             bool
	SnapshotCapacityFits bool
	Selected             bool
}

type Result struct {
	Placement    model.Placement
	Moves        []model.Move
	Before       graph.Metrics
	After        graph.Metrics
	SnapshotFits bool
	Audits       []CandidateAudit
}

func clonePlacement(source model.Placement) model.Placement {
	clone := make(model.Placement, len(source))
	for address, shard := range source {
		clone[address] = shard
	}
	return clone
}

func overlayPlacement(base, overlay model.Placement) model.Placement {
	result := clonePlacement(base)
	for address, shard := range overlay {
		result[address] = shard
	}
	return result
}

func countMoves(current, candidate model.Placement) int {
	moves := 0
	for address, shard := range candidate {
		if current[address] != shard {
			moves++
		}
	}
	return moves
}

func validatePlacement(g *graph.Graph, placement model.Placement, shardCount uint64) error {
	for _, address := range g.SortedVertices() {
		shard, ok := placement[address]
		if !ok {
			return fmt.Errorf("current placement is missing %s", address)
		}
		if shard >= shardCount {
			return fmt.Errorf("current placement has invalid shard %d for %s", shard, address)
		}
	}
	return nil
}

func serviceLoad(stats model.VertexStats, rhoCall float64) float64 {
	load := stats.ServiceGas(rhoCall)
	if load <= 0 {
		return 1
	}
	return load
}

func limitMoves(g *graph.Graph, current, candidate model.Placement, maxMoves int) model.Placement {
	if maxMoves <= 0 || countMoves(current, candidate) <= maxMoves {
		return candidate
	}
	limited := clonePlacement(current)
	moved := make([]string, 0)
	for address, shard := range candidate {
		if current[address] != shard {
			moved = append(moved, address)
		}
	}
	sort.SliceStable(moved, func(i, j int) bool {
		left, right := g.Vertices[moved[i]].HotGas(), g.Vertices[moved[j]].HotGas()
		if left != right {
			return left > right
		}
		return moved[i] < moved[j]
	})
	for index := 0; index < maxMoves && index < len(moved); index++ {
		limited[moved[index]] = candidate[moved[index]]
	}
	return limited
}

func sortedPlacementAddresses(placement model.Placement) []string {
	addresses := make([]string, 0, len(placement))
	for address := range placement {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	return addresses
}
