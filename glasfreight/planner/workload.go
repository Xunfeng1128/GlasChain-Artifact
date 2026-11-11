package planner

import (
	"fmt"
	"math"

	"blockEmulator/glasfreight/graph"
	"blockEmulator/glasfreight/model"
	"blockEmulator/glasfreight/optimizer"
)

// FrozenDemand is the gas cost of completing all physical roots in a frozen
// pool and all descendants they would create under one placement. It is a
// cost closure, not a prediction of how much work fits in an epoch: block
// packing, priorities, and inter-stage barriers are deliberately not applied.
type FrozenDemand struct {
	NewCrossGas uint64
	TotalGas    uint64
	MaxShardGas uint64
	PerShardGas []uint64
}

// WindowConfig bounds planning work, not actual block capacity. The gas target
// is soft: one whole physical root may cross it on each participating shard.
// Record/call limits are hard and must be positive, as must the gas target.
type WindowConfig struct {
	GasBudgetPerShard uint64
	MaxRecords        int
	MaxCallRelations  int
}

type WindowStats struct {
	FullRecords           int
	FullCallRelations     int
	SelectedRecords       int
	SelectedCallRelations int
	GasBudgetPerShard     uint64
	PerShardGas           []uint64
	Truncated             bool
	RecordLimitHit        bool
	CallLimitHit          bool
}

type affinityObservation struct {
	Source string
	Edge   model.EdgeKey
}

func validateFrozenWorkload(pools FrozenPools, shardCount uint64) error {
	if err := validateFrozenPools(pools, shardCount); err != nil {
		return err
	}
	rootStages := make(map[string]StageKind)
	for shard := uint64(0); shard < shardCount; shard++ {
		for _, record := range pools.Records[shard] {
			if !validWorkloadStage(record.Stage) {
				return fmt.Errorf("execution record %s has unknown stage %d", record.ID, record.Stage)
			}
			source := frozenRecordSource(record)
			if previous, exists := rootStages[source]; exists && (previous != CallTaskStage || record.Stage != CallTaskStage) {
				return fmt.Errorf("frozen source %s has conflicting physical roots %s and %s", source, previous, record.Stage)
			}
			rootStages[source] = record.Stage
		}
	}
	return nil
}

func validWorkloadStage(stage StageKind) bool {
	return stage == TopLevelStage || stage == Relay2Stage || stage == CallTaskStage
}

func frozenRecordSource(record FrozenRecord) string {
	if record.SourceHash != "" {
		return record.SourceHash
	}
	return record.ID
}

// BuildFrozenGraph uses every physical root in the supplied planning input,
// including roots that would not fit in the current placement's next epoch.
// The input may be all validated frozen Q or its selected planning window;
// this function does not reintroduce roots excluded from that window. Only
// unstarted TOP roots are logical transaction observations. Existing Relay2/
// CALL records are irreversible carried work, never replayed as fresh transactions.
// The graph proposes placements; EvaluateFrozenDemand scores their map-dependent costs.
func BuildFrozenGraph(pools FrozenPools) (*graph.Graph, error) {
	if err := validateFrozenWorkload(pools, pools.ShardCount); err != nil {
		return nil, err
	}
	g := graph.New()
	seenAffinity := make(map[affinityObservation]bool)
	addAffinity := func(source string, edge model.EdgeKey, affinity float64) error {
		observation := affinityObservation{Source: source, Edge: edge}
		if seenAffinity[observation] {
			return nil
		}
		seenAffinity[observation] = true
		// Affinity is only a proposal signal. No potential/sunk relation creates
		// another stage or changes the already committed execution gas.
		return g.AddObservedAffinity(edge, 1, 0, affinity, true, true)
	}
	for shard := uint64(0); shard < pools.ShardCount; shard++ {
		for _, record := range pools.Records[shard] {
			source := frozenRecordSource(record)
			baseGas := record.GasUsed
			if record.BaseGasUsed > 0 {
				baseGas = record.BaseGasUsed
			}
			if record.Stage == TopLevelStage {
				tx := model.Transaction{
					AttachedCallModel: record.BaseGasUsed > 0, CallGasPerCall: record.CallGasPerCall,
					SourceHash: source, From: record.From, To: record.To,
					GasUsed: baseGas, HasGasUsed: true,
					FromIsContract: record.FromIsContract, ToIsContract: record.ToIsContract,
					Category: record.Category, Calls: append([]model.Call(nil), record.Calls...),
				}
				if err := g.AddTransaction(&tx); err != nil {
					return nil, fmt.Errorf("add frozen TOP %s: %w", record.ID, err)
				}
				continue
			}

			kind, isContract := model.Relay2Stage, record.ToIsContract
			if record.Stage == CallTaskStage {
				kind, isContract = model.CallTaskStage, true
			}
			var callEdges []model.EdgeKey
			for _, call := range record.Calls {
				if call.IsCCGEdge && call.CallerIsSCA && call.CalleeIsSCA && call.Caller != "" && call.Callee != "" {
					callEdges = append(callEdges, model.EdgeKey{From: call.Caller, To: call.Callee, Kind: model.InternalContractCall})
				}
			}
			if err := g.AddCarriedStage(model.StageObservation{
				AttachedCallModel: record.BaseGasUsed > 0, CallGasPerCall: record.CallGasPerCall, CallEdges: callEdges,
				SourceHash: source, Owner: record.PlacementOwner(),
				GasUsed: float64(baseGas), Weight: 1, Kind: kind,
			}, isContract); err != nil {
				return nil, fmt.Errorf("add frozen carry %s: %w", record.ID, err)
			}

			if record.Stage == Relay2Stage {
				// Relay2's base cost cannot be canceled, but attached call gas
				// (or legacy generated CALL cost) depends on the map. Include every relation, even
				// when it happened to be local under the previous placement.
				seen := make(map[model.EdgeKey]bool)
				var edges []model.EdgeKey
				for _, call := range record.Calls {
					if !call.IsCCGEdge || !call.CallerIsSCA || !call.CalleeIsSCA || call.Caller == "" || call.Callee == "" {
						continue
					}
					edge := model.EdgeKey{From: call.Caller, To: call.Callee, Kind: model.InternalContractCall}
					if !seen[edge] {
						seen[edge] = true
						edges = append(edges, edge)
					}
				}
				for _, edge := range edges {
					if err := addAffinity(source, edge, 1/float64(len(edges))); err != nil {
						return nil, err
					}
				}
			} else if record.Caller != "" && record.Callee != "" {
				affinity := record.Affinity
				if affinity <= 0 || math.IsNaN(affinity) || math.IsInf(affinity, 0) {
					affinity = 1
				}
				if err := addAffinity(source, model.EdgeKey{
					From: record.Caller, To: record.Callee, Kind: model.InternalContractCall,
				}, affinity); err != nil {
					return nil, err
				}
			}
		}
	}
	return g, nil
}

func validObjectiveCall(call model.Call) bool {
	return call.IsCCGEdge && call.CallerIsSCA && call.CalleeIsSCA && call.Caller != "" && call.Callee != ""
}

// buildFrozenCostGraph keeps cost weights independent of LogicalAffinity and
// trace gas. Its cut equals future cross-shard execution gas C under the native
// attached-call contract. Each occurrence is charged, including repeated calls;
// a carried RELAY2 has no avoidable historical sender->recipient edge.
// No records or call traces are copied or retained here.
func buildFrozenCostGraph(pools FrozenPools) (*optimizer.CostGraph, error) {
	if err := validateFrozenWorkload(pools, pools.ShardCount); err != nil {
		return nil, err
	}
	cost := &optimizer.CostGraph{EdgeGas: make(map[model.EdgeKey]uint64)}
	addEdge := func(edge model.EdgeKey, gas uint64) error {
		if math.MaxUint64-cost.EdgeGas[edge] < gas {
			return fmt.Errorf("cost edge gas overflow: %s -> %s", edge.From, edge.To)
		}
		cost.EdgeGas[edge] += gas
		return nil
	}
	for shard := uint64(0); shard < pools.ShardCount; shard++ {
		for _, record := range pools.Records[shard] {
			if record.BaseGasUsed == 0 || (record.Stage != TopLevelStage && record.Stage != Relay2Stage) ||
				(record.Stage == TopLevelStage && (record.From == "" || record.To == "" || record.PlacementOwner() != record.From)) ||
				(record.Stage == Relay2Stage && record.PlacementOwner() != record.To) {
				return nil, fmt.Errorf("weighted_cost requires native attached-call TOP/RELAY2 roots: %s", record.ID)
			}
			if math.MaxUint64-cost.BaseDemandGas < record.BaseGasUsed {
				return nil, fmt.Errorf("fixed base demand gas overflow at %s", record.ID)
			}
			cost.BaseDemandGas += record.BaseGasUsed
			if record.Stage == TopLevelStage {
				if err := addEdge(model.EdgeKey{From: record.From, To: record.To, Kind: model.TopLevelTransaction}, record.BaseGasUsed); err != nil {
					return nil, err
				}
			}
			for _, call := range record.Calls {
				if validObjectiveCall(call) {
					if err := addEdge(model.EdgeKey{From: call.Caller, To: call.Callee, Kind: model.InternalContractCall}, record.CallGasPerCall); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	return cost, nil
}

// Every evaluated map (including MaxMoves-clipped maps) must agree with real
// Prepare/Expand pricing. An unsupported callback must fail, not report a
// deceptively exact cost objective. All arithmetic remains uint64 until ratios.
func validateFrozenCostClosure(cost *optimizer.CostGraph, placement model.Placement, demand FrozenDemand) error {
	var cut uint64
	for edge, gas := range cost.EdgeGas {
		from, hasFrom := placement[edge.From]
		to, hasTo := placement[edge.To]
		if !hasFrom || !hasTo {
			return fmt.Errorf("incomplete cost placement: %s -> %s", edge.From, edge.To)
		}
		if from != to {
			if math.MaxUint64-cut < gas {
				return fmt.Errorf("cross-shard cut gas overflow")
			}
			cut += gas
		}
	}
	if math.MaxUint64-cost.BaseDemandGas < cut {
		return fmt.Errorf("base plus cross-shard gas overflow")
	}
	if cut != demand.NewCrossGas || cost.BaseDemandGas+cut != demand.TotalGas {
		return fmt.Errorf("weighted_cost attached pricing mismatch: cut=%d C=%d D=%d total=%d", cut, demand.NewCrossGas, cost.BaseDemandGas, demand.TotalGas)
	}
	return nil
}

func validDemandSuccessor(parent, child StageKind) bool {
	switch parent {
	case TopLevelStage:
		return child == Relay2Stage || child == CallTaskStage
	case Relay2Stage:
		return child == CallTaskStage
	default:
		return false
	}
}

// EvaluateFrozenDemand closes the physical roots over the same Expand callback
// used by Simulate. Existing carry keeps its base cost even if its endpoints
// become co-located. NewCrossGas includes generated Relay2/legacy CALL costs
// and unpaid attached-call gas on initial roots, including carried Relay2.
// Strict stage progression limits every root to at most two descendant levels
// and rejects cycles even when an expander invents fresh execution IDs.
func EvaluateFrozenDemand(pools FrozenPools, placement model.Placement, config SimulationConfig) (FrozenDemand, error) {
	if err := validateFrozenWorkload(pools, config.ShardCount); err != nil {
		return FrozenDemand{}, err
	}
	seenIDs := make(map[string]bool)
	for shard := uint64(0); shard < config.ShardCount; shard++ {
		for _, record := range pools.Records[shard] {
			seenIDs[record.ID] = true
		}
	}
	demand := FrozenDemand{PerShardGas: make([]uint64, config.ShardCount)}
	for shard := uint64(0); shard < config.ShardCount; shard++ {
		for _, record := range pools.Records[shard] {
			if err := accumulateFrozenDemand(record, false, placement, config, seenIDs, &demand); err != nil {
				return FrozenDemand{}, err
			}
		}
	}
	return demand, nil
}

type frozenCrossCounts struct {
	TopLevelSources      uint64
	TopLevelCrossSources uint64
	CarrySources         uint64
}

// evaluateFrozenCross reads initial roots after the demand closure validated
// this same immutable input. Generated descendants cannot change its TOP
// denominator. Legacy CALL roots remain cost-only work, not fresh TOP sources.
func evaluateFrozenCross(pools FrozenPools, placement model.Placement, config SimulationConfig) (frozenCrossCounts, error) {
	var counts frozenCrossCounts
	for shard := uint64(0); shard < config.ShardCount; shard++ {
		for _, record := range pools.Records[shard] {
			switch record.Stage {
			case TopLevelStage:
				counts.TopLevelSources++
				cross, err := frozenTopCross(record, placement, config)
				if err != nil {
					return frozenCrossCounts{}, fmt.Errorf("TOP cross classification %s: %w", record.ID, err)
				}
				if cross {
					counts.TopLevelCrossSources++
				}
			case Relay2Stage:
				counts.CarrySources++
			}
		}
	}
	return counts, nil
}

// frozenTopCross classifies a fixed, unstarted TOP source, including sources
// outside the simulation horizon. It does not count carry or call occurrences
// as extra logical transactions and does not allocate a copy of the call trace.
func frozenTopCross(record FrozenRecord, placement model.Placement, config SimulationConfig) (bool, error) {
	from, err := resolveAddress(placement, record.From, config.ShardCount, config.ResolveShard)
	if err != nil {
		return false, err
	}
	to, err := resolveAddress(placement, record.To, config.ShardCount, config.ResolveShard)
	if err != nil {
		return false, err
	}
	if from != to {
		return true, nil
	}
	for _, call := range record.Calls {
		if !call.IsCCGEdge || !call.CallerIsSCA || !call.CalleeIsSCA || call.Caller == "" || call.Callee == "" {
			continue
		}
		caller, err := resolveAddress(placement, call.Caller, config.ShardCount, config.ResolveShard)
		if err != nil {
			return false, err
		}
		callee, err := resolveAddress(placement, call.Callee, config.ShardCount, config.ResolveShard)
		if err != nil {
			return false, err
		}
		if caller != callee {
			return true, nil
		}
	}
	return false, nil
}

// accumulateFrozenDemand is shared by whole-pool scoring and window admission,
// so both use the same remaining-stage pricing and expansion rules. seenIDs is
// preseeded with every physical root, including roots outside the window.
func accumulateFrozenDemand(record FrozenRecord, generated bool, placement model.Placement, config SimulationConfig, seenIDs map[string]bool, demand *FrozenDemand) error {
	// Stage progression bounds recursion depth to three physical stages. A
	// depth-first walk avoids retaining another full pool of cloned call traces.
	var priceErr error
	record, priceErr = prepareRecord(record, placement, config)
	if priceErr != nil {
		return priceErr
	}
	ownerShard, err := resolveAddress(placement, record.PlacementOwner(), config.ShardCount, config.ResolveShard)
	if err != nil {
		return fmt.Errorf("cost record %s: %w", record.ID, err)
	}
	// A generated RELAY2 contributes its whole incremental stage cost.
	// A frozen TOP/carried RELAY2 contributes only its not-yet-paid surcharge.
	newCrossGas := record.AttachedCallGas
	if generated {
		newCrossGas = record.GasUsed
	}
	if math.MaxUint64-demand.TotalGas < record.GasUsed ||
		math.MaxUint64-demand.PerShardGas[ownerShard] < record.GasUsed ||
		math.MaxUint64-demand.NewCrossGas < newCrossGas {
		return fmt.Errorf("frozen demand gas overflow at record %s", record.ID)
	}
	demand.TotalGas += record.GasUsed
	demand.PerShardGas[ownerShard] += record.GasUsed
	if demand.PerShardGas[ownerShard] > demand.MaxShardGas {
		demand.MaxShardGas = demand.PerShardGas[ownerShard]
	}
	demand.NewCrossGas += newCrossGas
	if config.Expand == nil {
		return nil
	}
	children, err := config.Expand(record.clone(), placement)
	if err != nil {
		return fmt.Errorf("expand cost record %s: %w", record.ID, err)
	}
	for _, child := range children {
		if err := child.validate(); err != nil {
			return fmt.Errorf("cost child of %s: %w", record.ID, err)
		}
		if !validDemandSuccessor(record.Stage, child.Stage) {
			return fmt.Errorf("invalid cost stage transition %s -> %s (%s -> %s)", record.ID, child.ID, record.Stage, child.Stage)
		}
		if record.SourceHash != child.SourceHash {
			return fmt.Errorf("cost child %s changes logical source of %s", child.ID, record.ID)
		}
		if seenIDs[child.ID] {
			return fmt.Errorf("expanded duplicate execution record ID %s in frozen demand", child.ID)
		}
		seenIDs[child.ID] = true
		if err := accumulateFrozenDemand(child, true, placement, config, seenIDs, demand); err != nil {
			return err
		}
	}
	return nil
}
