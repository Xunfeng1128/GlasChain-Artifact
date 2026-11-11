package optimizer

import (
	"math"
	"reflect"
	"testing"

	"blockEmulator/glasfreight/graph"
	"blockEmulator/glasfreight/model"
)

func executionTestGraph(t *testing.T, loads ...float64) *graph.Graph {
	t.Helper()
	g := graph.New()
	for index, load := range loads {
		address := string(rune('a' + index))
		if err := g.AddCarriedStage(model.StageObservation{
			SourceHash: address, Owner: address, GasUsed: load, Weight: 1, Kind: model.Relay2Stage,
		}, false); err != nil {
			t.Fatal(err)
		}
	}
	return g
}

func executionTestConfig(shards uint64) Config {
	return Config{
		ShardCount: shards, RhoCall: 0.25, BlockGasLimit: 50,
		FennelGamma: 1.5, Restarts: 2, RefinementPasses: 0, RefinementCandidates: 8,
		SelectionMode:   SelectionWeightedCost,
		ObjectiveWeight: 0.5,
	}
}

func TestCostFennelWeightsAndOrders(t *testing.T) {
	g := executionTestGraph(t, 100, 90, 10)
	cost := &CostGraph{BaseDemandGas: 200, EdgeGas: map[model.EdgeKey]uint64{
		{From: "a", To: "c", Kind: model.TopLevelTransaction}:  100,
		{From: "b", To: "c", Kind: model.InternalContractCall}: 5,
	}}
	cfg := executionTestConfig(2)
	cfg.Restarts, cfg.RefinementPasses, cfg.BlocksPerEpoch = 2, 0, 4
	cfg.SelectionMode = SelectionWeightedCost
	orders := costVertexOrders(g, cfg, cost)
	if !reflect.DeepEqual(orders, [][]string{{"a", "b", "c"}, {"c", "a", "b"}}) {
		t.Fatalf("service/cost orders=%v", orders)
	}
	// The preseed is identical at both lambda endpoints. Only the remaining
	// vertex switches between the least loaded shard and largest saved cost.
	cfg.ObjectiveWeight = 0
	loadOnly, err := costFennelPlacement(g, orders[0], cfg, cost)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ObjectiveWeight = 1
	costOnly, err := costFennelPlacement(g, orders[0], cfg, cost)
	if err != nil {
		t.Fatal(err)
	}
	if loadOnly["a"] != 0 || loadOnly["b"] != 1 || loadOnly["c"] != 1 ||
		costOnly["a"] != 0 || costOnly["b"] != 1 || costOnly["c"] != 0 {
		t.Fatalf("unexpected lambda endpoint placements: load=%v cost=%v", loadOnly, costOnly)
	}
	// Cost affinity must not be sourced from LogicalAffinity or trace gas.
	if err := g.AddObservedAffinity(model.EdgeKey{From: "b", To: "c", Kind: model.TopLevelTransaction}, 10000, 10000, 10000, false, false); err != nil {
		t.Fatal(err)
	}
	if changed, err := costFennelPlacement(g, orders[0], cfg, cost); err != nil || !reflect.DeepEqual(changed, costOnly) {
		t.Fatalf("legacy edge weights leaked into cost affinity: %v, %v", changed, err)
	}
}

func TestCostFennelDeduplicatesAndReevaluatesClippedMaps(t *testing.T) {
	g := executionTestGraph(t, 100, 90, 20, 10)
	cost := &CostGraph{BaseDemandGas: 220, EdgeGas: map[model.EdgeKey]uint64{
		{From: "a", To: "d", Kind: model.TopLevelTransaction}:  100,
		{From: "b", To: "c", Kind: model.InternalContractCall}: 80,
	}}
	cfg := executionTestConfig(2)
	cfg.Restarts, cfg.RefinementPasses, cfg.BlocksPerEpoch, cfg.MaxMoves = 2, 0, 4, 1
	cfg.SelectionMode = SelectionWeightedCost
	baseline := model.Placement{"a": 0, "b": 0, "c": 0, "d": 0, "outside": 1}
	before := clonePlacement(baseline)
	addresses := sortedPlacementAddresses(baseline)
	want := map[[32]byte]bool{executionPlacementDigest(baseline, addresses): true}
	for _, order := range costVertexOrders(g, cfg, cost) {
		generated, err := costFennelPlacement(g, order, cfg, cost)
		if err != nil {
			t.Fatal(err)
		}
		candidate := overlayPlacement(baseline, generated)
		candidate = limitMoves(g, baseline, candidate, cfg.MaxMoves)
		want[executionPlacementDigest(candidate, addresses)] = true
	}
	seen := map[[32]byte]bool{}
	result, err := OptimizeCostFennel(g, baseline, cfg, cost, func(m model.Placement) (ExecutionScore, error) {
		key := executionPlacementDigest(m, addresses)
		if seen[key] || countMoves(baseline, m) > 1 || m["outside"] != 1 {
			t.Fatalf("duplicate, unclipped or incomplete evaluated map: %v", m)
		}
		if len(seen) == 0 && !reflect.DeepEqual(m, baseline) {
			t.Fatal("baseline was not evaluated first")
		}
		seen[key] = true
		if countMoves(baseline, m) == 0 {
			return ExecutionScore{CapacityPressure: 2, BaseDemandGas: cost.BaseDemandGas}, nil
		}
		return ExecutionScore{CapacityPressure: 1, BaseDemandGas: cost.BaseDemandGas}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, want) || len(seen) > 3 || len(result.Moves) != 1 {
		t.Fatalf("unexpected evaluated maps or winning clipped move: seen=%d want=%d result=%+v", len(seen), len(want), result)
	}
	if !reflect.DeepEqual(before, baseline) {
		t.Fatal("cost construction mutated baseline or ran local search")
	}
	for _, audit := range result.Audits {
		if audit.Kind != "execution_baseline" && audit.Kind != "execution_cost_fennel" {
			t.Fatalf("incorrect cost candidate audit kind: %s", audit.Kind)
		}
	}
	// Equal exact scores keep the baseline despite a different proxy map.
	result, err = OptimizeCostFennel(g, baseline, cfg, cost, func(model.Placement) (ExecutionScore, error) {
		return ExecutionScore{CapacityPressure: 1, BaseDemandGas: cost.BaseDemandGas}, nil
	})
	if err != nil || !reflect.DeepEqual(result.Placement, baseline) {
		t.Fatalf("an equal exact score displaced baseline: result=%+v err=%v", result, err)
	}
}

func TestCostFennelRejectsNonFiniteConstructionScores(t *testing.T) {
	g := executionTestGraph(t, 100, 90, 10)
	cost := &CostGraph{BaseDemandGas: 200, EdgeGas: map[model.EdgeKey]uint64{
		{From: "a", To: "c", Kind: model.TopLevelTransaction}: 100,
	}}
	cfg := executionTestConfig(2)
	cfg.BlocksPerEpoch, cfg.FennelGamma = 1, math.MaxFloat64
	order := []string{"a", "b", "c"}
	if _, err := costFennelPlacement(g, order, cfg, cost); err == nil {
		t.Fatal("finite gamma causing Inf-Inf was silently accepted")
	}
	// Pure cost does not evaluate the irrelevant convex-load power.
	cfg.ObjectiveWeight = 1
	if placement, err := costFennelPlacement(g, order, cfg, cost); err != nil || placement["c"] != 0 {
		t.Fatalf("pure cost incorrectly rejected an unused load exponent: %v, %v", placement, err)
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		for _, address := range []string{"a", "c"} {
			original := g.Vertices[address]
			changed := original
			changed.IncomingTxGas = value
			g.Vertices[address] = changed
			if _, err := costFennelPlacement(g, order, cfg, cost); err == nil {
				t.Fatalf("non-finite service load accepted for %s: %g", address, value)
			}
			g.Vertices[address] = original
		}
	}
}

func TestCostFennelRejectsInvalidDemandCapacityAndEdges(t *testing.T) {
	g := executionTestGraph(t, 100, 90)
	baseline := model.Placement{"a": 0, "b": 1}
	cfg := executionTestConfig(2)
	cfg.Restarts, cfg.RefinementPasses, cfg.BlocksPerEpoch = 2, 0, 4
	cfg.SelectionMode = SelectionWeightedCost
	evaluate := func(model.Placement) (ExecutionScore, error) { return ExecutionScore{}, nil }
	for _, cost := range []*CostGraph{nil, {}, {BaseDemandGas: 1, EdgeGas: map[model.EdgeKey]uint64{{From: "missing", To: "a"}: 1}}} {
		if _, err := OptimizeCostFennel(g, baseline, cfg, cost, evaluate); err == nil {
			t.Fatalf("invalid cost view accepted: %+v", cost)
		}
	}
	cost := &CostGraph{BaseDemandGas: 190}
	for _, blocks := range []uint64{0, ^uint64(0)} {
		cfg.BlocksPerEpoch = blocks
		if _, err := OptimizeCostFennel(g, baseline, cfg, cost, evaluate); err == nil {
			t.Fatalf("invalid epoch capacity accepted: blocks=%d", blocks)
		}
	}
	cfg.BlocksPerEpoch, cfg.SelectionMode = 4, "completion_first"
	if _, err := OptimizeCostFennel(g, baseline, cfg, cost, evaluate); err == nil {
		t.Fatal("cost_fennel accepted a mismatched final objective")
	}
}
