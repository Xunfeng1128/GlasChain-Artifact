package planner

import (
	"fmt"
	"math"

	"blockEmulator/glasfreight/graph"
	"blockEmulator/glasfreight/model"
	"blockEmulator/glasfreight/optimizer"
)

func validateConfig(config *Config) error {
	if config.ShardCount == 0 || config.BlocksPerEpoch == 0 || config.BlockGasLimit == 0 {
		return fmt.Errorf("positive shard count, blocks per epoch and block gas limit are required")
	}
	if config.Iterations == 0 {
		config.Iterations = 1
	}
	if config.Iterations != 1 || config.EvaluationBudget != 3 ||
		config.OptimizerConfig.Restarts != 2 || config.OptimizerConfig.RefinementPasses != 0 {
		return fmt.Errorf("cost_fennel requires one pass, two orders, zero refinement and budget 3")
	}
	weight := 0.5
	if config.ObjectiveWeight != nil {
		weight = *config.ObjectiveWeight
	}
	if weight < 0 || weight > 1 || math.IsNaN(weight) || math.IsInf(weight, 0) {
		return fmt.Errorf("objective weight must be finite and in [0,1]")
	}
	config.ObjectiveWeight = &weight
	if config.BlocksPerEpoch > math.MaxUint64/config.BlockGasLimit {
		return fmt.Errorf("weighted_cost epoch gas capacity overflows uint64")
	}
	if !config.AttachedCallObjective || config.Prepare == nil || config.Expand == nil {
		return fmt.Errorf("weighted_cost requires the attached-call TOP/RELAY2 Prepare/Expand contract")
	}
	config.OptimizerConfig.ShardCount = config.ShardCount
	config.OptimizerConfig.BlockGasLimit = config.BlockGasLimit
	config.OptimizerConfig.BlocksPerEpoch = config.BlocksPerEpoch
	config.OptimizerConfig.ObjectiveWeight = weight
	config.OptimizerConfig.SelectionMode = optimizer.SelectionWeightedCost
	return nil
}

func completePlacementForRecord(placement model.Placement, record FrozenRecord, config Config) error {
	addresses := []string{record.PlacementOwner(), record.From, record.To, record.Caller, record.Callee}
	for _, call := range record.Calls {
		addresses = append(addresses, call.Caller, call.Callee)
	}
	for _, address := range addresses {
		if address == "" {
			continue
		}
		if _, exists := placement[address]; exists {
			continue
		}
		shard, err := resolveAddress(placement, address, config.ShardCount, config.ResolveShard)
		if err != nil {
			return err
		}
		placement[address] = shard
	}
	return nil
}

func completePlacementForSnapshot(pools FrozenPools, placement model.Placement, config Config) (model.Placement, error) {
	completed := clonePlacement(placement)
	for _, records := range pools.Records {
		for _, record := range records {
			if err := completePlacementForRecord(completed, record, config); err != nil {
				return nil, fmt.Errorf("complete placement for %s: %w", record.ID, err)
			}
		}
	}
	return completed, nil
}

func completePlacementForGraph(g *graph.Graph, placement model.Placement, config Config) (model.Placement, error) {
	completed := clonePlacement(placement)
	for _, address := range g.SortedVertices() {
		if _, exists := completed[address]; exists {
			continue
		}
		shard, err := resolveAddress(completed, address, config.ShardCount, config.ResolveShard)
		if err != nil {
			return nil, err
		}
		completed[address] = shard
	}
	return completed, nil
}

func countMoves(current, candidate model.Placement) int {
	moves := 0
	for address, shard := range candidate {
		if original, exists := current[address]; exists && original != shard {
			moves++
		}
	}
	return moves
}
