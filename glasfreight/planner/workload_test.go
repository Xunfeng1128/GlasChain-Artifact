package planner

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"blockEmulator/glasfreight/graph"
	"blockEmulator/glasfreight/model"
	"blockEmulator/glasfreight/optimizer"
)

func workloadCall(from, to string) model.Call {
	return model.Call{Caller: from, Callee: to, IsCCGEdge: true, CallerIsSCA: true, CalleeIsSCA: true}
}

func TestFrozenCostCutEqualsExactDemand(t *testing.T) {
	pools, placement, config := objectiveFixture()
	originalPools := pools.Clone()
	cost, err := buildFrozenCostGraph(pools)
	if err != nil {
		t.Fatal(err)
	}
	if cost.BaseDemandGas != 210 || cost.EdgeGas[model.EdgeKey{From: "b", To: "c", Kind: model.InternalContractCall}] != 24 {
		t.Fatalf("base/occurrence cost incorrect: %+v", cost)
	}
	if _, exists := cost.EdgeGas[model.EdgeKey{From: "a", To: "d", Kind: model.TopLevelTransaction}]; exists {
		t.Fatal("carry introduced a historical TOP cost")
	}
	check := func() {
		t.Helper()
		demand, err := EvaluateFrozenDemand(pools, placement, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateFrozenCostClosure(cost, placement, demand); err != nil {
			t.Fatal(err)
		}
	}
	check()
	random := rand.New(rand.NewSource(20261008))
	for index := 0; index < 150; index++ {
		for _, account := range []string{"a", "b", "c", "d", "e"} {
			placement[account] = uint64(random.Intn(4))
		}
		check()
	}
	if !reflect.DeepEqual(pools, originalPools) {
		t.Fatal("cost graph mutated frozen input")
	}
}

func TestFrozenCostCarryOnlyEmptyAndValidation(t *testing.T) {
	pools, placement, config := objectiveFixture()
	delete(pools.Records, 0)
	cost, err := buildFrozenCostGraph(pools)
	if err != nil || cost.BaseDemandGas != 70 {
		t.Fatalf("carry-only base cost: %v %v", cost, err)
	}
	demand, err := EvaluateFrozenDemand(pools, placement, config)
	if err != nil || validateFrozenCostClosure(cost, placement, demand) != nil || demand.NewCrossGas != 15 {
		t.Fatalf("carry-only cost: %+v err=%v", demand, err)
	}
	pools.Records = map[uint64][]FrozenRecord{}
	cost, err = buildFrozenCostGraph(pools)
	if err != nil || cost.BaseDemandGas != 0 || validateFrozenCostClosure(cost, placement, FrozenDemand{}) != nil {
		t.Fatal("empty cost input rejected", err)
	}
	for _, stage := range []StageKind{TopLevelStage, CallTaskStage} {
		r := testRecord("bad", "bad", "a", "b", 10, 1)
		r.Stage = stage // zero BaseGasUsed / legacy CALL must not inherit cost semantics
		pools.Records[0] = []FrozenRecord{r}
		if _, err := buildFrozenCostGraph(pools); err == nil {
			t.Fatal("unsupported cost root accepted")
		}
	}
	// Fixed-base overflow and edge aggregation overflow must fail before floats.
	pools, _, _ = objectiveFixture()
	pools.Records[0][0].BaseGasUsed = math.MaxUint64
	if _, err := buildFrozenCostGraph(pools); err == nil {
		t.Fatal("base overflow accepted")
	}
	pools, _, _ = objectiveFixture()
	pools.Records[0][0].CallGasPerCall = math.MaxUint64
	if _, err := buildFrozenCostGraph(pools); err == nil {
		t.Fatal("repeated edge overflow accepted")
	}
	cost = &optimizer.CostGraph{BaseDemandGas: math.MaxUint64, EdgeGas: map[model.EdgeKey]uint64{
		{From: "a", To: "c", Kind: model.TopLevelTransaction}: 1,
	}}
	placement["a"], placement["c"] = 0, 1
	if err := validateFrozenCostClosure(cost, placement, FrozenDemand{}); err == nil {
		t.Fatal("total cost overflow accepted")
	}
}

func TestBuildFrozenGraphDoesNotDoubleChargeTopOrInventStages(t *testing.T) {
	top := testRecord("TOP:t", "t", "a", "b", 100, 0)
	top.Calls = []model.Call{workloadCall("b", "c")}
	pools := FrozenPools{ShardCount: 2, Records: map[uint64][]FrozenRecord{0: {top}}}
	g, err := BuildFrozenGraph(pools)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Transactions) != 1 || len(g.Stages) != 0 {
		t.Fatalf("TOP also became a stage: %+v", g)
	}
	for _, test := range []struct {
		name      string
		placement model.Placement
		wantGas   float64
	}{
		{"local", model.Placement{"a": 0, "b": 0, "c": 0}, 100},
		{"cross", model.Placement{"a": 0, "b": 1, "c": 0}, 225},
	} {
		t.Run(test.name, func(t *testing.T) {
			metrics, err := graph.Evaluate(g, test.placement, 2, 0.25, 1000)
			if err != nil {
				t.Fatal(err)
			}
			if metrics.TotalExecutedGas != test.wantGas || metrics.TotalLogicalTxs != 1 {
				t.Fatalf("metrics = %+v, want gas %v and one logical TOP", metrics, test.wantGas)
			}
		})
	}
}

func TestBuildFrozenGraphCarriesStayPhysicalWithAffinityOnly(t *testing.T) {
	relay := FrozenRecord{ID: "RELAY2:r", SourceHash: "r", Stage: Relay2Stage, From: "a", To: "b", GasUsed: 17,
		Calls: []model.Call{workloadCall("b", "c"), workloadCall("b", "c"), workloadCall("c", "d")}}
	call := FrozenRecord{ID: "CALL:c", SourceHash: "c", Stage: CallTaskStage, Caller: "x", Callee: "y", GasUsed: 3}
	pools := FrozenPools{ShardCount: 2, Records: map[uint64][]FrozenRecord{0: {relay}, 1: {call}}}
	g, err := BuildFrozenGraph(pools)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Transactions) != 0 || len(g.Stages) != 2 {
		t.Fatalf("carry was replayed as TOP: %+v", g)
	}
	if _, exists := g.Edges[model.EdgeKey{From: "a", To: "b", Kind: model.TopLevelTransaction}]; exists {
		t.Fatal("Relay2 recreated its already started top-level relation")
	}
	for _, edge := range []model.EdgeKey{
		{From: "b", To: "c", Kind: model.InternalContractCall},
		{From: "c", To: "d", Kind: model.InternalContractCall},
	} {
		if got := g.Edges[edge]; got.LogicalAffinity != 0.5 || got.Gas != 0 || got.AttributedGas != 0 {
			t.Fatalf("potential call affinity added execution gas or duplicate weight: %+v", got)
		}
	}
	placement := model.Placement{"a": 0, "b": 0, "c": 0, "d": 0, "x": 0, "y": 0}
	metrics, err := graph.Evaluate(g, placement, 2, 0.25, 100)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.TotalExecutedGas != 20 || metrics.TotalLogicalTxs != 0 {
		t.Fatalf("co-location canceled or duplicated carry: %+v", metrics)
	}
}

// Test expansion intentionally returns a seven-gas CALL, allowing the closure
// tests to assert it preserves callback-provided integer costs rather than
// replacing them with a fractional rho proxy.
func workloadExpander(record FrozenRecord, placement model.Placement) ([]FrozenRecord, error) {
	if record.Stage == CallTaskStage {
		return nil, nil
	}
	if record.Stage == TopLevelStage && placement[record.From] != placement[record.To] {
		child := record.clone()
		child.ID, child.Stage, child.Owner = "RELAY2:"+record.SourceHash, Relay2Stage, record.To
		return []FrozenRecord{child}, nil
	}
	var children []FrozenRecord
	for _, call := range record.Calls {
		if placement[call.Caller] != placement[call.Callee] {
			children = append(children, FrozenRecord{
				ID: "CALL:" + record.SourceHash + ":" + call.Callee, SourceHash: record.SourceHash,
				Stage: CallTaskStage, Caller: call.Caller, Callee: call.Callee, GasUsed: 7,
			})
		}
	}
	return children, nil
}

func TestEvaluateFrozenDemandIsMapDependentAndRetainsExistingCarry(t *testing.T) {
	top := testRecord("TOP:t", "t", "a", "b", 100, 0)
	top.Calls = []model.Call{workloadCall("b", "c")}
	relay := FrozenRecord{ID: "RELAY2:r", SourceHash: "r", Stage: Relay2Stage, From: "x", To: "y", GasUsed: 40,
		Calls: []model.Call{workloadCall("y", "z")}}
	call := FrozenRecord{ID: "CALL:old", SourceHash: "old", Stage: CallTaskStage, Caller: "x", Callee: "z", GasUsed: 3}
	pools := FrozenPools{ShardCount: 2, Records: map[uint64][]FrozenRecord{0: {top, call}, 1: {relay}}}
	before := pools.Clone()
	for _, test := range []struct {
		name      string
		placement model.Placement
		want      FrozenDemand
	}{
		{"local", model.Placement{"a": 0, "b": 0, "c": 0, "x": 0, "y": 0, "z": 0},
			FrozenDemand{TotalGas: 143, MaxShardGas: 143, PerShardGas: []uint64{143, 0}}},
		{"cross", model.Placement{"a": 0, "b": 1, "c": 0, "x": 0, "y": 1, "z": 0},
			FrozenDemand{NewCrossGas: 114, TotalGas: 257, MaxShardGas: 140, PerShardGas: []uint64{117, 140}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// No horizon or block gas is needed for a cost closure.
			got, err := EvaluateFrozenDemand(pools, test.placement, SimulationConfig{ShardCount: 2, Expand: workloadExpander})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("demand = %+v, want %+v", got, test.want)
			}
		})
	}
	if !reflect.DeepEqual(pools, before) {
		t.Fatal("cost closure mutated frozen pools")
	}
}

func TestEvaluateFrozenDemandWithoutExpanderAndEmptyWorkload(t *testing.T) {
	pools := FrozenPools{ShardCount: 2, Records: map[uint64][]FrozenRecord{0: {
		{ID: "CALL:c", Stage: CallTaskStage, Callee: "c", GasUsed: 7},
	}}}
	got, err := EvaluateFrozenDemand(pools, nil, SimulationConfig{ShardCount: 2, ResolveShard: func(string) (uint64, error) { return 1, nil }})
	if err != nil || got.TotalGas != 7 || got.NewCrossGas != 0 || got.MaxShardGas != 7 || got.PerShardGas[1] != 7 {
		t.Fatalf("nil expander/resolver demand = %+v, err=%v", got, err)
	}
	got, err = EvaluateFrozenDemand(FrozenPools{ShardCount: 2}, nil, SimulationConfig{ShardCount: 2})
	if err != nil || got.TotalGas != 0 || len(got.PerShardGas) != 2 {
		t.Fatalf("empty demand = %+v, err=%v", got, err)
	}
}

func TestFrozenTopCrossCountsUnionOnceAndExcludesCarry(t *testing.T) {
	first := testRecord("old-queued-top", "old-queued-top", "a", "b", 11, 1)
	first.Calls = []model.Call{workloadCall("b", "x"), workloadCall("b", "x")}
	second := testRecord("top-2", "top-2", "c", "d", 2, 2)
	ignored := workloadCall("d", "x")
	ignored.CalleeIsSCA = false
	second.Calls = []model.Call{ignored}
	carry := FrozenRecord{ID: "carry", SourceHash: "carry", Stage: Relay2Stage, From: "z", To: "h", GasUsed: 3,
		Calls: []model.Call{workloadCall("h", "x")}}
	pools := FrozenPools{ShardCount: 2, Records: map[uint64][]FrozenRecord{0: {first, second, carry}}}
	before := pools.Clone()
	for _, test := range []struct {
		name      string
		placement model.Placement
		cross     uint64
	}{
		{"internal-only", model.Placement{"a": 0, "b": 0, "c": 0, "d": 0, "h": 0, "x": 1, "z": 1}, 1},
		{"top-and-internal", model.Placement{"a": 1, "b": 0, "c": 0, "d": 0, "h": 0, "x": 1, "z": 1}, 1},
		{"all-top-local", model.Placement{"a": 0, "b": 0, "c": 0, "d": 0, "h": 1, "x": 0, "z": 0}, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := SimulationConfig{ShardCount: 2}
			if _, err := EvaluateFrozenDemand(pools, test.placement, config); err != nil {
				t.Fatal(err)
			}
			counts, err := evaluateFrozenCross(pools, test.placement, config)
			if err != nil || counts.TopLevelSources != 2 || counts.TopLevelCrossSources != test.cross || counts.CarrySources != 1 {
				t.Fatalf("fixed TOP union counts=%+v err=%v", counts, err)
			}
		})
	}
	if !reflect.DeepEqual(before, pools) {
		t.Fatal("logical classification mutated input or call metadata")
	}
}

func TestFrozenWorkloadRejectsInvalidRoots(t *testing.T) {
	valid := testRecord("TOP:t", "t", "a", "b", 1, 0)
	for _, test := range []struct {
		name    string
		records []FrozenRecord
	}{
		{"duplicate IDs", []FrozenRecord{valid, valid}},
		{"invalid stage", []FrozenRecord{{ID: "bad", Owner: "a", GasUsed: 1, Stage: StageKind(99)}}},
		{"zero gas", []FrozenRecord{{ID: "bad", From: "a", GasUsed: 0, Stage: TopLevelStage}}},
		{"TOP and existing carry", []FrozenRecord{valid, {ID: "RELAY2:t", SourceHash: "t", From: "a", To: "b", GasUsed: 1, Stage: Relay2Stage}}},
		{"duplicate logical TOP", []FrozenRecord{valid, testRecord("TOP:other", "t", "a", "b", 1, 0)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			pools := FrozenPools{ShardCount: 1, Records: map[uint64][]FrozenRecord{0: test.records}}
			if _, err := BuildFrozenGraph(pools); err == nil {
				t.Fatal("BuildFrozenGraph accepted invalid roots")
			}
			if _, err := EvaluateFrozenDemand(pools, model.Placement{"a": 0, "b": 0}, SimulationConfig{ShardCount: 1}); err == nil {
				t.Fatal("EvaluateFrozenDemand accepted invalid roots")
			}
		})
	}
}

func TestFrozenWorkloadAllowsMultipleCommittedCallsForOneSource(t *testing.T) {
	pools := FrozenPools{ShardCount: 1, Records: map[uint64][]FrozenRecord{0: {
		{ID: "CALL:s:b", SourceHash: "s", Stage: CallTaskStage, Caller: "a", Callee: "b", GasUsed: 3},
		{ID: "CALL:s:c", SourceHash: "s", Stage: CallTaskStage, Caller: "a", Callee: "c", GasUsed: 4},
	}}}
	g, err := BuildFrozenGraph(pools)
	if err != nil || len(g.Stages) != 2 || len(g.Transactions) != 0 {
		t.Fatalf("multiple carried calls graph = %+v, err=%v", g, err)
	}
	demand, err := EvaluateFrozenDemand(pools, model.Placement{"a": 0, "b": 0, "c": 0}, SimulationConfig{ShardCount: 1})
	if err != nil || demand.TotalGas != 7 || demand.NewCrossGas != 0 {
		t.Fatalf("multiple carried calls demand = %+v, err=%v", demand, err)
	}
}

func TestEvaluateFrozenDemandRejectsInvalidDescendantLifecycles(t *testing.T) {
	for _, stages := range [][2]StageKind{
		{TopLevelStage, TopLevelStage}, {Relay2Stage, TopLevelStage},
		{Relay2Stage, Relay2Stage}, {CallTaskStage, TopLevelStage},
		{CallTaskStage, CallTaskStage}, {TopLevelStage, StageKind(99)},
	} {
		t.Run(fmt.Sprintf("%s-to-%s", stages[0], stages[1]), func(t *testing.T) {
			root := FrozenRecord{ID: "root", SourceHash: "s", Owner: "a", Stage: stages[0], GasUsed: 1}
			expand := func(FrozenRecord, model.Placement) ([]FrozenRecord, error) {
				return []FrozenRecord{{ID: "fresh", SourceHash: "s", Owner: "a", Stage: stages[1], GasUsed: 1}}, nil
			}
			_, err := EvaluateFrozenDemand(FrozenPools{ShardCount: 1, Records: map[uint64][]FrozenRecord{0: {root}}},
				model.Placement{"a": 0}, SimulationConfig{ShardCount: 1, Expand: expand})
			if err == nil || !strings.Contains(err.Error(), "stage transition") {
				t.Fatalf("want rejected stage progression, got %v", err)
			}
		})
	}
}

func TestEvaluateFrozenDemandRejectsDuplicateInvalidAndOverflowingChildren(t *testing.T) {
	for _, test := range []struct {
		name       string
		rootGas    uint64
		child      FrozenRecord
		wantErr    string
		expandFail bool
	}{
		{"duplicate ID", 1, FrozenRecord{ID: "root", SourceHash: "s", Owner: "a", Stage: Relay2Stage, GasUsed: 1}, "duplicate", false},
		{"different source", 1, FrozenRecord{ID: "child", SourceHash: "other", Owner: "a", Stage: Relay2Stage, GasUsed: 1}, "logical source", false},
		{"invalid child", 1, FrozenRecord{ID: "child", SourceHash: "s", Owner: "a", Stage: Relay2Stage}, "zero gas", false},
		{"overflow", math.MaxUint64, FrozenRecord{ID: "child", SourceHash: "s", Owner: "a", Stage: Relay2Stage, GasUsed: 1}, "overflow", false},
		{"expansion failure", 1, FrozenRecord{}, "broken expansion", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := FrozenRecord{ID: "root", SourceHash: "s", Owner: "a", Stage: TopLevelStage, GasUsed: test.rootGas}
			expand := func(record FrozenRecord, _ model.Placement) ([]FrozenRecord, error) {
				if test.expandFail {
					return nil, errors.New("broken expansion")
				}
				if record.Stage == TopLevelStage {
					return []FrozenRecord{test.child}, nil
				}
				return nil, nil
			}
			_, err := EvaluateFrozenDemand(FrozenPools{ShardCount: 1, Records: map[uint64][]FrozenRecord{0: {root}}},
				model.Placement{"a": 0}, SimulationConfig{ShardCount: 1, Expand: expand})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("want error containing %q, got %v", test.wantErr, err)
			}
		})
	}
}
