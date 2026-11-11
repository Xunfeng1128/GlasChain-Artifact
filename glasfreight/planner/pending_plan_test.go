package planner

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"blockEmulator/glasfreight/model"
	"blockEmulator/glasfreight/optimizer"
)

// The expected scores and placements were checked against the independent
// finite-horizon oracle before pruning its implementation from the artifact.
func TestPendingDemandRetainsFormalCandidates(t *testing.T) {
	type candidate struct {
		digest        string
		cross, demand uint64
		moves         int
		loads         []uint64
	}
	baseDigest := "0c8d936bf7bbf4674d181a853f594baff1736d055ac7fa98096e9f8f431c34f3"
	whole := []candidate{
		{baseDigest, 79, 289, 0, []uint64{161, 43, 85, 0}},
		{"8ff215cf4776a152d8ca8d9b2fdf2714673c14c6e1a43f323f94475f873a63ad", 79, 289, 2, []uint64{161, 85, 43, 0}},
		{"5bfff5cbfed3a68324576665e1cff2fc5b30e74baffe450a9f1f15b1f8f3d99f", 150, 360, 2, []uint64{140, 150, 0, 70}},
	}
	carry := []candidate{
		{baseDigest, 15, 85, 0, []uint64{0, 0, 85, 0}},
		{"894cebaeff16abe9da64729c8ef9c3422a5df617a0355a9b2eb30fe792d881ec", 5, 75, 2, []uint64{75, 0, 0, 0}},
	}
	for _, weight := range []float64{0, .5, 1} {
		for _, carryOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("weight=%g/carryOnly=%t", weight, carryOnly), func(t *testing.T) {
				pools, baseline, sim := objectiveFixture()
				candidates, winner := whole, 0
				if carryOnly {
					delete(pools.Records, 0)
					candidates, winner = carry, 1
				} else if weight == 0 {
					winner = 2
				}
				beforePools, beforeMap := pools.Clone(), clonePlacement(baseline)
				config := costPlannerTestConfig("cost_fennel", sim, weight)
				config.OptimizerConfig.MaxMoves = 2
				got, err := PlanPendingDemand(pools, baseline, config)
				if err != nil {
					t.Fatal(err)
				}
				if !got.DemandOnly || got.ObjectiveVersion != "cost_capacity_v1" || len(got.Evaluations) != len(candidates) ||
					placementDigest(got.Placement) != candidates[winner].digest {
					t.Fatalf("unexpected winner or candidate count: %+v", got)
				}
				for index, evaluation := range got.Evaluations {
					score, want := evaluation.Score, candidates[index]
					if evaluation.PlacementDigest != want.digest || score.NewCrossGas != want.cross || score.FullDemandTotalGas != want.demand ||
						score.Moves != want.moves || !reflect.DeepEqual(score.FullDemandPerShardGas, want.loads) ||
						score.FullDemandTotalGas != score.BaseDemandGas+score.NewCrossGas {
						t.Fatalf("candidate semantics changed: %+v vs %+v", evaluation, want)
					}
					if optimizer.BetterFinalExecutionScore(score, got.Score, got.SelectionMode, weight) {
						t.Fatal("did not retain best candidate")
					}
				}
				if !reflect.DeepEqual(beforePools, pools) || !reflect.DeepEqual(beforeMap, baseline) {
					t.Fatal("planner mutated caller input")
				}
			})
		}
	}
}

func TestPendingDemandRetainsCarriedBaseAndRepeatedCallOccurrences(t *testing.T) {
	pools, baseline, sim := objectiveFixture()
	result, err := PlanPendingDemand(pools, baseline, costPlannerTestConfig("cost_fennel", sim, .5))
	if err != nil {
		t.Fatal(err)
	}
	// TOP-local: 100+3*7; TOP-remote: 40+(40+3);
	// carried Relay2: 70+3*5. The earlier source stage of carry is sunk.
	score := result.BaselineScore
	if score.BaseDemandGas != 210 || score.NewCrossGas != 79 || score.FullDemandTotalGas != 289 ||
		!reflect.DeepEqual(score.FullDemandPerShardGas, []uint64{161, 43, 85, 0}) || score.CarrySources != 1 || score.TopLevelSources != 2 {
		t.Fatalf("lost repeated call cost or reintroduced sunk source stage: %+v", score)
	}
}

func TestPendingDemandEmptyWindowRetainsCurrentMap(t *testing.T) {
	_, baseline, sim := objectiveFixture()
	config := costPlannerTestConfig("cost_fennel", sim, .5)
	result, err := PlanPendingDemand(FrozenPools{ShardCount: 4, Records: map[uint64][]FrozenRecord{}}, baseline, config)
	if err != nil || !reflect.DeepEqual(result.Placement, baseline) || len(result.Evaluations) != 1 || result.Score.Moves != 0 || result.Score.FullDemandTotalGas != 0 {
		t.Fatalf("empty window: result=%+v err=%v", result, err)
	}
}

func TestPendingDemandRejectsMalformedWindowAndPricing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*FrozenPools, model.Placement, *Config)
		want   string
	}{
		{"duplicate stage", func(p *FrozenPools, _ model.Placement, _ *Config) {
			p.Records[0] = append(p.Records[0], p.Records[0][0])
		}, "duplicate"},
		{"invalid owner shard", func(_ *FrozenPools, m model.Placement, _ *Config) { m["a"] = 9 }, "invalid current placement"},
		{"missing owner", func(p *FrozenPools, _ model.Placement, _ *Config) {
			p.Records[0][0].Owner, p.Records[0][0].From = "", ""
		}, "owner"},
		{"wrong remaining stage", func(p *FrozenPools, _ model.Placement, _ *Config) { p.Records[2][0].Stage = CallTaskStage }, "TOP/RELAY2"},
		{"inconsistent expansion", func(_ *FrozenPools, _ model.Placement, c *Config) {
			c.Expand = func(FrozenRecord, model.Placement) ([]FrozenRecord, error) { return nil, nil }
		}, "pricing mismatch"},
		{"oversized priced stage", func(_ *FrozenPools, _ model.Placement, c *Config) { c.BlockGasLimit = 110 }, "above block limit"},
		{"overflowed epoch capacity", func(_ *FrozenPools, _ model.Placement, c *Config) { c.BlocksPerEpoch = math.MaxUint64 }, "overflows"},
		{"base sum overflow", func(p *FrozenPools, m model.Placement, c *Config) {
			c.BlocksPerEpoch, c.BlockGasLimit = 1, math.MaxUint64
			m["b"] = m["a"]
			p.Records = map[uint64][]FrozenRecord{0: {
				{ID: "x", SourceHash: "x", Stage: TopLevelStage, From: "a", To: "b", GasUsed: math.MaxUint64/2 + 1, BaseGasUsed: math.MaxUint64/2 + 1},
				{ID: "y", SourceHash: "y", Stage: TopLevelStage, From: "a", To: "b", GasUsed: math.MaxUint64/2 + 1, BaseGasUsed: math.MaxUint64/2 + 1},
			}}
		}, "overflow"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pools, placement, sim := objectiveFixture()
			config := costPlannerTestConfig("cost_fennel", sim, .5)
			tc.mutate(&pools, placement, &config)
			if _, err := PlanPendingDemand(pools, placement, config); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestPendingDemandChecksGeneratedStageCapacity(t *testing.T) {
	pools := FrozenPools{ShardCount: 3, Records: map[uint64][]FrozenRecord{0: {{
		ID: "top", SourceHash: "source", Stage: TopLevelStage, From: "a", To: "b",
		GasUsed: 100, BaseGasUsed: 100, CallGasPerCall: 20, Calls: []model.Call{workloadCall("b", "c")},
	}}}}
	placement := model.Placement{"a": 0, "b": 1, "c": 2}
	sim := objectiveTestConfig(3)
	sim.BlockGasLimit = 110
	_, err := PlanPendingDemand(pools, placement, costPlannerTestConfig("cost_fennel", sim, .5))
	if err == nil || !strings.Contains(err.Error(), "RELAY2:source") || !strings.Contains(err.Error(), "above block limit") {
		t.Fatalf("generated Relay2 surcharge escaped capacity validation: %v", err)
	}
}

func TestPendingDemandRejectsUnsupportedConfig(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.PlannerMode = "cost_greedy" },
		func(c *Config) { c.SelectionMode = "completion_first" },
		func(c *Config) { c.AttachedCallObjective = false },
		func(c *Config) { c.Prepare = nil },
		func(c *Config) { c.Expand = nil },
		func(c *Config) { c.EvaluationBudget = 2 },
		func(c *Config) { c.Iterations = 2 },
		func(c *Config) { c.OptimizerConfig.Restarts = 1 },
		func(c *Config) { c.OptimizerConfig.RefinementPasses = 1 },
	} {
		pools, placement, sim := objectiveFixture()
		config := costPlannerTestConfig("cost_fennel", sim, .5)
		mutate(&config)
		if _, err := PlanPendingDemand(pools, placement, config); err == nil {
			t.Fatal("unsupported configuration accepted")
		}
	}
}
