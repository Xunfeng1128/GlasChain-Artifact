package planner

import (
	"fmt"
	"math"
	"time"

	"blockEmulator/glasfreight/model"
	"blockEmulator/glasfreight/optimizer"
)

// PendingResult records a placement and its score on the complete planning
// window, including the descendants of unstarted TOP records.
type PendingResult struct {
	Placement        model.Placement
	Score            optimizer.ExecutionScore
	BaselineScore    optimizer.ExecutionScore
	Evaluations      []ExecutionEvaluation
	Optimization     optimizer.Result
	SelectionMode    string
	ObjectiveWeight  float64
	ObjectiveVersion string
	DemandOnly       bool
}

// PlanPendingDemand scores the incumbent and the distinct proposals from two
// canonical cost-FENNEL orders. Actual move count breaks a score tie. A
// complete tie retains the incumbent.
//
// Prepare/Expand must implement the attached-call TOP/RELAY2 execution contract.
// Expanding remaining physical stages here sums cost; it does not schedule
// blocks, simulate time or materialize an execution plan. Each evaluated map
// is checked for D+C closure, integer overflow and single-stage executability.
// A priced stage above the block limit fails explicitly. No input pools,
// current map or configuration are modified.
func PlanPendingDemand(pools FrozenPools, current model.Placement, input Config) (PendingResult, error) {
	config := input
	if config.PlannerMode != "cost_fennel" || config.SelectionMode != optimizer.SelectionWeightedCost {
		return PendingResult{}, fmt.Errorf("pending demand planning requires cost_fennel and weighted_cost")
	}
	if err := validateConfig(&config); err != nil {
		return PendingResult{}, err
	}
	if err := config.OptimizerConfig.Validate(); err != nil {
		return PendingResult{}, err
	}
	if math.IsInf(config.OptimizerConfig.FennelGamma, 0) {
		return PendingResult{}, fmt.Errorf("pending demand planning requires finite FENNEL gamma")
	}
	if err := validateFrozenPools(pools, config.ShardCount); err != nil {
		return PendingResult{}, err
	}
	seed, err := completePlacementForSnapshot(pools, current, config)
	if err != nil {
		return PendingResult{}, err
	}
	for address, shard := range seed {
		if address == "" || shard >= config.ShardCount {
			return PendingResult{}, fmt.Errorf("invalid current placement %q -> %d", address, shard)
		}
	}
	planningGraph, err := BuildFrozenGraph(pools)
	if err != nil {
		return PendingResult{}, fmt.Errorf("build pending graph: %w", err)
	}
	costGraph, err := buildFrozenCostGraph(pools)
	if err != nil {
		return PendingResult{}, fmt.Errorf("build pending cost graph: %w", err)
	}
	seed, err = completePlacementForGraph(planningGraph, seed, config)
	if err != nil {
		return PendingResult{}, err
	}
	original := clonePlacement(seed)
	demandConfig := SimulationConfig{
		ShardCount: config.ShardCount, BlocksPerEpoch: config.BlocksPerEpoch,
		BlockGasLimit: config.BlockGasLimit, Expand: config.Expand, ResolveShard: config.ResolveShard,
		Prepare: func(record FrozenRecord, placement model.Placement) (FrozenRecord, error) {
			priced, err := config.Prepare(record, placement)
			if err != nil {
				return FrozenRecord{}, err
			}
			if priced.GasUsed > config.BlockGasLimit {
				return FrozenRecord{}, fmt.Errorf("pending stage %s (%s) requires %d Gas, above block limit %d", record.ID, record.Stage, priced.GasUsed, config.BlockGasLimit)
			}
			return priced, nil
		},
	}
	result := PendingResult{SelectionMode: config.SelectionMode, ObjectiveWeight: *config.ObjectiveWeight,
		ObjectiveVersion: optimizer.ObjectiveVersion(config.SelectionMode), DemandOnly: true}
	scorePlacement := func(placement model.Placement) (optimizer.ExecutionScore, error) {
		demand, err := EvaluateFrozenDemand(pools, placement, demandConfig)
		if err != nil {
			return optimizer.ExecutionScore{}, err
		}
		if err := validateFrozenCostClosure(costGraph, placement, demand); err != nil {
			return optimizer.ExecutionScore{}, err
		}
		cross, err := evaluateFrozenCross(pools, placement, demandConfig)
		if err != nil {
			return optimizer.ExecutionScore{}, err
		}
		return optimizer.ExecutionScore{
			NewCrossGas: demand.NewCrossGas, BaseDemandGas: costGraph.BaseDemandGas,
			Moves: countMoves(original, placement), FullDemandMaxGas: demand.MaxShardGas,
			FullDemandTotalGas: demand.TotalGas, FullDemandPerShardGas: demand.PerShardGas,
			CapacityPressure: float64(demand.MaxShardGas) / (float64(config.BlocksPerEpoch) * float64(config.BlockGasLimit)),
			TopLevelSources:  cross.TopLevelSources, TopLevelCrossSources: cross.TopLevelCrossSources, CarrySources: cross.CarrySources,
		}, nil
	}
	scores := make(map[string]optimizer.ExecutionScore)
	evaluate := func(placement model.Placement) (optimizer.ExecutionScore, error) {
		key := placementDigest(placement)
		if score, exists := scores[key]; exists {
			return score, nil
		}
		if len(scores) >= config.EvaluationBudget {
			return optimizer.ExecutionScore{}, fmt.Errorf("pending map evaluation budget exhausted")
		}
		started := time.Now()
		score, err := scorePlacement(placement)
		if err != nil {
			return optimizer.ExecutionScore{}, err
		}
		improved := result.Placement == nil || optimizer.BetterFinalExecutionScore(score, result.Score, result.SelectionMode, result.ObjectiveWeight)
		if improved {
			result.Placement, result.Score = clonePlacement(placement), score
		}
		iteration := 1
		if len(scores) == 0 {
			iteration = 0
			result.BaselineScore = score
		}
		scores[key] = score
		result.Evaluations = append(result.Evaluations, ExecutionEvaluation{
			Index: len(result.Evaluations), Iteration: iteration, PlacementDigest: key, Score: score,
			Improved: improved, DurationMS: time.Since(started).Milliseconds(),
		})
		return score, nil
	}
	if _, err := evaluate(seed); err != nil {
		return PendingResult{}, fmt.Errorf("initial pending demand evaluation: %w", err)
	}
	if len(planningGraph.Vertices) != 0 {
		searchConfig := config.OptimizerConfig
		searchConfig.ExecutionBaseline = original
		result.Optimization, err = optimizer.OptimizeCostFennel(planningGraph, clonePlacement(seed), searchConfig, costGraph, evaluate)
		if err != nil {
			return PendingResult{}, fmt.Errorf("pending cost_fennel search: %w", err)
		}
	}
	return result, nil
}
