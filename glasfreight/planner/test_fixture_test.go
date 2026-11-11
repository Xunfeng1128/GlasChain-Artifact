package planner

import (
	"blockEmulator/core"
	"blockEmulator/glasfreight/model"
	"blockEmulator/glasfreight/optimizer"
)

func objectiveTestConfig(shards uint64) SimulationConfig {
	config := SimulationConfig{ShardCount: shards, BlocksPerEpoch: 3, BlockGasLimit: 1000}
	config.Prepare = func(record FrozenRecord, placement model.Placement) (FrozenRecord, error) {
		if record.BaseGasUsed == 0 {
			return record, nil
		}
		var count uint64
		for _, call := range record.Calls {
			if call.IsCCGEdge && call.CallerIsSCA && call.CalleeIsSCA && call.Caller != "" && call.Callee != "" && placement[call.Caller] != placement[call.Callee] {
				count++
			}
		}
		pays := record.Stage == Relay2Stage || placement[record.From] == placement[record.To]
		var err error
		record.GasUsed, record.AttachedCallGas, err = core.PriceGLASStageGas(record.BaseGasUsed, record.CallGasPerCall, count, pays)
		return record, err
	}
	config.Expand = func(record FrozenRecord, placement model.Placement) ([]FrozenRecord, error) {
		if record.Stage != TopLevelStage || placement[record.From] == placement[record.To] {
			return nil, nil
		}
		record.ID = "RELAY2:" + record.SourceHash
		record.Stage, record.Owner, record.Priority = Relay2Stage, record.To, 0
		return []FrozenRecord{record}, nil
	}
	return config
}

func objectiveFixture() (FrozenPools, model.Placement, SimulationConfig) {
	placement := model.Placement{"a": 0, "b": 0, "c": 1, "d": 2, "e": 3, "idle": 0, "missing": 2}
	makeRecord := func(id, from, to string, stage StageKind, gas, unit uint64, calls ...model.Call) FrozenRecord {
		owner := from
		if stage == Relay2Stage {
			owner = to
		}
		return FrozenRecord{ID: stage.String() + ":" + id, SourceHash: id, Stage: stage, Owner: owner,
			From: from, To: to, GasUsed: gas, BaseGasUsed: gas, CallGasPerCall: unit, Calls: calls}
	}
	invalid := workloadCall("missing", "c")
	invalid.IsCCGEdge = false
	noContract := workloadCall("a", "missing")
	noContract.CalleeIsSCA = false
	empty := workloadCall("", "c")
	pools := FrozenPools{ShardCount: 4, Records: map[uint64][]FrozenRecord{
		0: {
			makeRecord("local", "a", "b", TopLevelStage, 100, 7, workloadCall("b", "c"), workloadCall("b", "c"), workloadCall("a", "d"), workloadCall("a", "a"), invalid, noContract, empty),
			makeRecord("remote", "a", "c", TopLevelStage, 40, 3, workloadCall("c", "d")),
		},
		2: {makeRecord("carry", "a", "d", Relay2Stage, 70, 5, workloadCall("b", "c"), workloadCall("b", "c"), workloadCall("d", "e"))},
	}}
	return pools, placement, objectiveTestConfig(4)
}

func costPlannerTestConfig(mode string, sim SimulationConfig, weight float64) Config {
	return Config{PlannerMode: mode, SelectionMode: optimizer.SelectionWeightedCost,
		ShardCount: sim.ShardCount, BlocksPerEpoch: sim.BlocksPerEpoch, BlockGasLimit: sim.BlockGasLimit,
		Iterations: 1, EvaluationBudget: 3, ObjectiveWeight: &weight, AttachedCallObjective: true,
		Prepare: sim.Prepare, Expand: sim.Expand,
		OptimizerConfig: optimizer.Config{RhoCall: .25, FennelGamma: 1.5, Restarts: 2, RefinementCandidates: 8}}
}

func testRecord(id, source, from, to string, gas uint64, arrival uint64) FrozenRecord {
	return FrozenRecord{
		ID: id, SourceHash: source, Stage: TopLevelStage,
		From: from, To: to, GasUsed: gas, ArrivalSeq: arrival,
		DependencyKey: from,
	}
}
