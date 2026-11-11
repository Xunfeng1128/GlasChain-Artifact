package optimizer

import (
	"fmt"
	"math"
	"sort"

	"blockEmulator/glasfreight/graph"
	"blockEmulator/glasfreight/model"
)

// CostGraph is the current planning window's exact cut-cost view. It is kept
// separate from legacy logical affinity and historical/trace gas weights.
// EdgeGas aggregates occurrence costs (not distinct logical relationships).
type CostGraph struct {
	BaseDemandGas uint64
	EdgeGas       map[model.EdgeKey]uint64
}

// OptimizeCostFennel proposes the two canonical FENNEL orders. The incumbent
// and distinct proposals are ranked by the caller's exact demand evaluator.
func OptimizeCostFennel(g *graph.Graph, current model.Placement, config Config, cost *CostGraph,
	evaluate func(model.Placement) (ExecutionScore, error)) (Result, error) {
	if config.SelectionMode != SelectionWeightedCost {
		return Result{}, fmt.Errorf("cost_fennel requires weighted_cost final selection")
	}
	if cost == nil || cost.BaseDemandGas == 0 {
		return Result{}, fmt.Errorf("cost_fennel requires positive fixed base demand")
	}
	if config.BlocksPerEpoch == 0 || config.BlockGasLimit == 0 || config.BlocksPerEpoch > ^uint64(0)/config.BlockGasLimit {
		return Result{}, fmt.Errorf("cost_fennel requires a positive non-overflowing epoch gas capacity")
	}
	if math.IsInf(config.FennelGamma, 0) {
		return Result{}, fmt.Errorf("cost_fennel requires finite FENNEL gamma")
	}
	if g == nil {
		return Result{}, fmt.Errorf("cost_fennel requires a graph")
	}
	for edge := range cost.EdgeGas {
		_, fromOK := g.Vertices[edge.From]
		_, toOK := g.Vertices[edge.To]
		if !fromOK || !toOK || edge.From == "" || edge.To == "" ||
			(edge.Kind != model.TopLevelTransaction && edge.Kind != model.InternalContractCall) {
			return Result{}, fmt.Errorf("cost_fennel has invalid cost edge %s", edge.String())
		}
	}
	return optimizeFennelFinal(g, current, config, cost, evaluate)
}

func optimizeFennelFinal(g *graph.Graph, current model.Placement, config Config, cost *CostGraph,
	evaluate func(model.Placement) (ExecutionScore, error)) (Result, error) {
	if err := config.Validate(); err != nil {
		return Result{}, err
	}
	if evaluate == nil || len(g.Vertices) == 0 {
		return Result{}, fmt.Errorf("cost_fennel requires a nonempty graph and evaluator")
	}
	if err := validatePlacement(g, current, config.ShardCount); err != nil {
		return Result{}, err
	}
	baseline := current
	if config.ExecutionBaseline != nil {
		baseline = config.ExecutionBaseline
	}
	addresses := sortedPlacementAddresses(current)
	for _, address := range addresses {
		if shard, ok := baseline[address]; !ok || shard >= config.ShardCount || current[address] >= config.ShardCount {
			return Result{}, fmt.Errorf("invalid baseline placement for %s", address)
		}
	}
	result := Result{SnapshotFits: true}
	seen := make(map[[32]byte]bool)
	var best model.Placement
	var bestScore ExecutionScore
	bestAudit := 0
	test := func(kind string, restart int, placement model.Placement) error {
		candidate := overlayPlacement(current, placement)
		moves := countMoves(baseline, candidate)
		if config.MaxMoves > 0 && moves > config.MaxMoves {
			return fmt.Errorf("cost_fennel candidate exceeds migration budget")
		}
		key := executionPlacementDigest(candidate, addresses)
		if seen[key] {
			return nil
		}
		seen[key] = true
		score, err := evaluate(clonePlacement(candidate))
		if err != nil {
			return err
		}
		score.Moves = moves
		metrics, err := graph.Evaluate(g, candidate, config.ShardCount, config.RhoCall, config.BlockGasLimit)
		if err != nil {
			return err
		}
		result.Audits = append(result.Audits, CandidateAudit{Kind: kind, Restart: restart,
			MaxBlocks: metrics.MaxRequiredBlocks, MaxShardGas: metrics.MaxShardGas,
			LoadDeviation: metrics.MaxRelativeGasDeviation, CrossRatio: metrics.CrossTxRatio,
			GasPerTxCV: metrics.GasPerTxCV, Moves: moves, Feasible: true, SnapshotCapacityFits: true})
		if best == nil {
			result.Before = metrics
		}
		if best == nil || BetterFinalExecutionScore(score, bestScore, config.SelectionMode, config.ObjectiveWeight) {
			best, bestScore, bestAudit = candidate, score, len(result.Audits)-1
			result.After = metrics
		}
		return nil
	}
	if err := test("execution_baseline", 0, current); err != nil {
		return Result{}, err
	}
	for index, order := range costVertexOrders(g, config, cost) {
		generated, err := costFennelPlacement(g, order, config, cost)
		if err != nil {
			return Result{}, err
		}
		candidate := overlayPlacement(current, generated)
		candidate = limitMoves(g, baseline, candidate, config.MaxMoves)
		if err := test("execution_cost_fennel", index, candidate); err != nil {
			return Result{}, err
		}
	}
	result.Placement = clonePlacement(best)
	result.Audits[bestAudit].Selected = true
	for _, address := range addresses {
		if baseline[address] != best[address] {
			result.Moves = append(result.Moves, model.Move{Address: address, Kind: g.Vertices[address].Kind,
				From: baseline[address], To: best[address]})
		}
	}
	return result, nil
}

func costIncidentWeights(cost *CostGraph) (map[string]float64, map[string][]model.EdgeKey) {
	keys := make([]model.EdgeKey, 0, len(cost.EdgeGas))
	for key := range cost.EdgeGas {
		// Self edges never cross and cannot contribute to placement affinity.
		if key.From != key.To && cost.EdgeGas[key] > 0 {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].From != keys[j].From {
			return keys[i].From < keys[j].From
		}
		if keys[i].To != keys[j].To {
			return keys[i].To < keys[j].To
		}
		return keys[i].Kind < keys[j].Kind
	})
	weights := make(map[string]float64)
	incident := make(map[string][]model.EdgeKey)
	for _, key := range keys {
		weight := float64(cost.EdgeGas[key])
		weights[key.From] += weight
		weights[key.To] += weight
		incident[key.From] = append(incident[key.From], key)
		incident[key.To] = append(incident[key.To], key)
	}
	return weights, incident
}

func costVertexOrders(g *graph.Graph, config Config, cost *CostGraph) [][]string {
	weights, _ := costIncidentWeights(cost)
	orders := [][]string{g.SortedVertices(), g.SortedVertices()}
	for index, order := range orders {
		byCost := index == 1
		sort.SliceStable(order, func(i, j int) bool {
			left, right := serviceLoad(g.Vertices[order[i]], config.RhoCall), serviceLoad(g.Vertices[order[j]], config.RhoCall)
			if byCost && weights[order[i]] != weights[order[j]] {
				return weights[order[i]] > weights[order[j]]
			}
			if left != right {
				return left > right
			}
			if weights[order[i]] != weights[order[j]] {
				return weights[order[i]] > weights[order[j]]
			}
			return order[i] < order[j]
		})
	}
	return orders
}

func costFennelPlacement(g *graph.Graph, order []string, config Config, cost *CostGraph) (model.Placement, error) {
	placement := make(model.Placement, len(order))
	assigned := make(map[string]bool, len(order))
	load := make([]float64, int(config.ShardCount))
	_, incident := costIncidentWeights(cost)
	capacity := float64(config.BlocksPerEpoch * config.BlockGasLimit)
	baseDemand := float64(cost.BaseDemandGas)
	vertexService := func(address string) (float64, error) {
		value := g.Vertices[address].ServiceGas(config.RhoCall)
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, fmt.Errorf("cost_fennel non-finite service load for %s", address)
		}
		if value <= 0 {
			value = 1 // Preserve the old zero-service construction tie-break.
		}
		return value, nil
	}
	preseed := len(order)
	if preseed > int(config.ShardCount) {
		preseed = int(config.ShardCount)
	}
	for index, address := range order[:preseed] {
		vertexLoad, err := vertexService(address)
		if err != nil {
			return nil, err
		}
		shard := uint64(index)
		placement[address], assigned[address] = shard, true
		load[shard] += vertexLoad
	}
	for _, address := range order[preseed:] {
		vertexLoad, err := vertexService(address)
		if err != nil {
			return nil, err
		}
		bestShard, bestScore := uint64(0), math.Inf(-1)
		for shard := uint64(0); shard < config.ShardCount; shard++ {
			if math.IsInf(load[shard]+vertexLoad, 0) {
				return nil, fmt.Errorf("cost_fennel service load overflow for %s on shard %d", address, shard)
			}
			affinity := 0.0
			for _, key := range incident[address] {
				other := key.From
				if other == address {
					other = key.To
				}
				if assigned[other] && placement[other] == shard {
					affinity += float64(cost.EdgeGas[key])
				}
			}
			// ServiceGas is intentionally a static construction estimate. This
			// convex proxy is not an exact marginal change of bottleneck H.
			score := config.ObjectiveWeight * affinity / baseDemand
			if config.ObjectiveWeight < 1 {
				score -= (1 - config.ObjectiveWeight) / float64(config.ShardCount) *
					(math.Pow((load[shard]+vertexLoad)/capacity, config.FennelGamma) - math.Pow(load[shard]/capacity, config.FennelGamma))
			}
			if math.IsNaN(score) || math.IsInf(score, 0) {
				return nil, fmt.Errorf("cost_fennel non-finite placement score for %s on shard %d (gamma=%g)", address, shard, config.FennelGamma)
			}
			if score > bestScore+1e-12 || (math.Abs(score-bestScore) <= 1e-12 &&
				(load[shard] < load[bestShard]-1e-12 || (math.Abs(load[shard]-load[bestShard]) <= 1e-12 && shard < bestShard))) {
				bestShard, bestScore = shard, score
			}
		}
		placement[address], assigned[address] = bestShard, true
		load[bestShard] += vertexLoad
	}
	return placement, nil
}
