// Package graph builds the heterogeneous EOA/SCA workload graph and evaluates
// candidate placements in the same gas unit used by the block packer.
package graph

import (
	"fmt"
	"math"
	"sort"

	"blockEmulator/core"
	"blockEmulator/glasfreight/model"
)

const historyPruneThreshold = 1e-4

type Graph struct {
	Vertices     map[string]model.VertexStats
	Edges        map[model.EdgeKey]model.EdgeStats
	Transactions []model.TxObservation
	Stages       []model.StageObservation
}

func New() *Graph {
	return &Graph{
		Vertices: make(map[string]model.VertexStats),
		Edges:    make(map[model.EdgeKey]model.EdgeStats),
	}
}

func effectiveGas(tx *model.Transaction) uint64 {
	if tx != nil && tx.HasGasUsed && tx.GasUsed > 0 {
		return tx.GasUsed
	}
	return core.IntrinsicGas
}

func promoteKind(current model.VertexKind, isContract bool) model.VertexKind {
	if isContract || current == model.SmartContract {
		return model.SmartContract
	}
	return model.EOA
}

func (g *Graph) touchVertex(address string, isContract bool) model.VertexStats {
	stats := g.Vertices[address]
	stats.Kind = promoteKind(stats.Kind, isContract)
	return stats
}

// AddPendingTopLevel records the top-level result made available by the
// pre-execution oracle. It uses the pre-executed GasUsed directly, while call
// traces remain unavailable and therefore cannot affect internal-call
// placement in this mode.
func (g *Graph) AddPendingTopLevel(tx model.PendingTxObservation) error {
	if g == nil || tx.From == "" || tx.To == "" {
		return fmt.Errorf("graph and pending transaction endpoints are required")
	}
	gas := tx.GasUsed
	if gas <= 0 || math.IsNaN(gas) || math.IsInf(gas, 0) {
		return fmt.Errorf("pending transaction %s has invalid pre-executed gasUsed %f", tx.SourceHash, gas)
	}

	from := g.touchVertex(tx.From, tx.FromIsContract)
	from.OutgoingTxCount++
	from.OutgoingTxGas += gas
	g.Vertices[tx.From] = from
	to := g.touchVertex(tx.To, tx.ToIsContract)
	to.IncomingTxCount++
	to.IncomingTxGas += gas
	g.Vertices[tx.To] = to

	key := model.EdgeKey{From: tx.From, To: tx.To, Kind: model.TopLevelTransaction}
	edge := g.Edges[key]
	edge.Count++
	edge.Gas += gas
	edge.AttributedGas += gas
	edge.LogicalAffinity++
	g.Edges[key] = edge
	g.Transactions = append(g.Transactions, model.TxObservation{
		SourceHash: tx.SourceHash,
		From:       tx.From,
		To:         tx.To,
		GasUsed:    gas,
		Weight:     1,
	})
	return nil
}

// AddCarriedStage adds only the remaining execution demand of an already
// started logical transaction. It deliberately does not create a transaction
// observation, so the logical transaction and its cross status are not
// counted for a second time.
func (g *Graph) AddCarriedStage(stage model.StageObservation, isContract bool) error {
	if g == nil || stage.Owner == "" {
		return fmt.Errorf("graph and carried-stage owner are required")
	}
	if stage.GasUsed <= 0 || math.IsNaN(stage.GasUsed) || math.IsInf(stage.GasUsed, 0) {
		stage.GasUsed = float64(core.IntrinsicGas)
	}
	if stage.Weight <= 0 {
		stage.Weight = 1
	}
	stats := g.touchVertex(stage.Owner, isContract)
	// Remaining execution demand is anchored at the account that owns the
	// queue record. This lets Fennel/LPT see carried load without inventing a
	// completed transaction edge.
	stats.IncomingTxGas += stage.GasUsed * stage.Weight
	if stage.AttachedCallModel {
		stats.AttachedCallBudget += float64(stage.CallGasPerCall) * float64(len(stage.CallEdges)) * stage.Weight
	}
	g.Vertices[stage.Owner] = stats
	g.Stages = append(g.Stages, stage)
	return nil
}

// AddObservedAffinity records a dependency that became visible before its
// logical transaction fully completed. It guides placement without creating a
// second transaction observation or adding another execution-stage load.
func (g *Graph) AddObservedAffinity(key model.EdgeKey, count, gas, logicalAffinity float64, fromIsContract, toIsContract bool) error {
	if g == nil || key.From == "" || key.To == "" {
		return fmt.Errorf("graph and observed-affinity endpoints are required")
	}
	g.Vertices[key.From] = g.touchVertex(key.From, fromIsContract)
	g.Vertices[key.To] = g.touchVertex(key.To, toIsContract)
	edge := g.Edges[key]
	edge.Count += count
	edge.Gas += gas
	edge.AttributedGas += gas
	edge.LogicalAffinity += logicalAffinity
	g.Edges[key] = edge
	return nil
}

// AddTransaction records one fully completed logical transaction. Top-level
// edges cover EOA/SCA senders and recipients. Successful SCA->SCA trace calls
// are additional dependency edges, not additional logical transactions.
func (g *Graph) AddTransaction(tx *model.Transaction) error {
	if g == nil || tx == nil || tx.From == "" || tx.To == "" {
		return fmt.Errorf("graph and complete transaction endpoints are required")
	}
	gas := float64(effectiveGas(tx))
	from := g.touchVertex(tx.From, tx.FromIsContract)
	from.OutgoingTxCount++
	from.OutgoingTxGas += gas
	g.Vertices[tx.From] = from
	to := g.touchVertex(tx.To, tx.ToIsContract)
	to.IncomingTxCount++
	to.IncomingTxGas += gas
	g.Vertices[tx.To] = to

	validCalls := make([]model.Call, 0, len(tx.Calls))
	for _, call := range tx.Calls {
		if call.IsCCGEdge && call.CallerIsSCA && call.CalleeIsSCA && call.Caller != "" && call.Callee != "" {
			validCalls = append(validCalls, call)
		}
	}
	attributedGas := 0.0
	if tx.AttachedCallModel {
		entry := g.Vertices[tx.To]
		entry.AttachedCallBudget += float64(tx.CallGasPerCall) * float64(len(validCalls))
		g.Vertices[tx.To] = entry
	}
	if len(validCalls) > 0 {
		attributedGas = gas / float64(len(validCalls))
	}
	uniqueCallKeys := make(map[model.EdgeKey]bool)
	for _, call := range validCalls {
		uniqueCallKeys[model.EdgeKey{From: call.Caller, To: call.Callee, Kind: model.InternalContractCall}] = true
	}
	topAffinity := 1.0
	callAffinity := 0.0
	if len(uniqueCallKeys) > 0 {
		// A transaction with internal dependencies contributes half of its
		// additive seeding affinity to the top-level edge and half across its
		// distinct call edges. This prevents transactions with deep traces from
		// dominating the logical cross-transaction objective merely because
		// they contain more trace rows.
		topAffinity = 0.5
		callAffinity = 0.5 / float64(len(uniqueCallKeys))
	}
	topKey := model.EdgeKey{From: tx.From, To: tx.To, Kind: model.TopLevelTransaction}
	topEdge := g.Edges[topKey]
	topEdge.Count++
	topEdge.Gas += gas
	topEdge.AttributedGas += gas
	topEdge.LogicalAffinity += topAffinity
	g.Edges[topKey] = topEdge

	observation := model.TxObservation{
		AttachedCallModel: tx.AttachedCallModel, CallGasPerCall: tx.CallGasPerCall,
		SourceHash: tx.SourceHash, From: tx.From, To: tx.To,
		GasUsed: gas, Weight: 1,
		CallEdges: make([]model.EdgeKey, 0, len(validCalls)),
	}
	for _, call := range validCalls {
		callGas := attributedGas
		if call.HasGasUsed && call.GasUsed > 0 {
			callGas = float64(call.GasUsed)
		}
		key := model.EdgeKey{From: call.Caller, To: call.Callee, Kind: model.InternalContractCall}
		edge := g.Edges[key]
		edge.Count++
		edge.Gas += callGas
		edge.AttributedGas += attributedGas
		if uniqueCallKeys[key] {
			edge.LogicalAffinity += callAffinity
			delete(uniqueCallKeys, key)
		}
		if call.HasGasUsed {
			edge.CallGasSamples++
		}
		g.Edges[key] = edge

		caller := g.touchVertex(call.Caller, true)
		caller.OutgoingCallCount++
		caller.OutgoingCallGas += callGas
		caller.OutgoingCallAttributedGas += attributedGas
		g.Vertices[call.Caller] = caller
		callee := g.touchVertex(call.Callee, true)
		callee.IncomingCallCount++
		callee.IncomingCallGas += callGas
		if !tx.AttachedCallModel {
			callee.IncomingCallAttributedGas += attributedGas
		}
		g.Vertices[call.Callee] = callee
		observation.CallEdges = append(observation.CallEdges, key)
	}
	g.Transactions = append(g.Transactions, observation)
	return nil
}

func (g *Graph) Clone() *Graph {
	if g == nil {
		return New()
	}
	clone := New()
	for address, stats := range g.Vertices {
		clone.Vertices[address] = stats
	}
	for key, stats := range g.Edges {
		clone.Edges[key] = stats
	}
	clone.Transactions = make([]model.TxObservation, len(g.Transactions))
	for index, tx := range g.Transactions {
		clone.Transactions[index] = tx
		clone.Transactions[index].CallEdges = append([]model.EdgeKey(nil), tx.CallEdges...)
	}
	clone.Stages = append([]model.StageObservation(nil), g.Stages...)
	for index := range clone.Stages {
		clone.Stages[index].CallEdges = append([]model.EdgeKey(nil), g.Stages[index].CallEdges...)
	}
	return clone
}

func scaleVertex(stats model.VertexStats, factor float64) model.VertexStats {
	stats.AttachedCallBudget *= factor
	stats.IncomingTxCount *= factor
	stats.IncomingTxGas *= factor
	stats.OutgoingTxCount *= factor
	stats.OutgoingTxGas *= factor
	stats.IncomingCallCount *= factor
	stats.IncomingCallGas *= factor
	stats.IncomingCallAttributedGas *= factor
	stats.OutgoingCallCount *= factor
	stats.OutgoingCallGas *= factor
	stats.OutgoingCallAttributedGas *= factor
	return stats
}

func addVertex(left, right model.VertexStats) model.VertexStats {
	left.AttachedCallBudget += right.AttachedCallBudget
	left.Kind = promoteKind(left.Kind, right.Kind == model.SmartContract)
	left.IncomingTxCount += right.IncomingTxCount
	left.IncomingTxGas += right.IncomingTxGas
	left.OutgoingTxCount += right.OutgoingTxCount
	left.OutgoingTxGas += right.OutgoingTxGas
	left.IncomingCallCount += right.IncomingCallCount
	left.IncomingCallGas += right.IncomingCallGas
	left.IncomingCallAttributedGas += right.IncomingCallAttributedGas
	left.OutgoingCallCount += right.OutgoingCallCount
	left.OutgoingCallGas += right.OutgoingCallGas
	left.OutgoingCallAttributedGas += right.OutgoingCallAttributedGas
	return left
}

func scaleEdge(stats model.EdgeStats, factor float64) model.EdgeStats {
	stats.Count *= factor
	stats.Gas *= factor
	stats.AttributedGas *= factor
	stats.CallGasSamples *= factor
	stats.LogicalAffinity *= factor
	return stats
}

func addEdge(left, right model.EdgeStats) model.EdgeStats {
	left.Count += right.Count
	left.Gas += right.Gas
	left.AttributedGas += right.AttributedGas
	left.CallGasSamples += right.CallGasSamples
	left.LogicalAffinity += right.LogicalAffinity
	return left
}

// MergeDecay returns H_t = decay*H_(t-1) + G_t. Weighted observations are
// retained so candidate gas loads and cross-transaction costs can be rescored
// exactly instead of being reconstructed from marginal edge totals.
func MergeDecay(history, window *Graph, decay float64) (*Graph, error) {
	if decay < 0 || decay > 1 || math.IsNaN(decay) {
		return nil, fmt.Errorf("history decay must be in [0,1], got %v", decay)
	}
	merged := New()
	if history != nil && decay > 0 {
		for address, stats := range history.Vertices {
			scaled := scaleVertex(stats, decay)
			if scaled.HotGas() > historyPruneThreshold || scaled.OutgoingTxGas > historyPruneThreshold || scaled.OutgoingCallGas > historyPruneThreshold {
				merged.Vertices[address] = scaled
			}
		}
		for key, stats := range history.Edges {
			scaled := scaleEdge(stats, decay)
			if scaled.Count > historyPruneThreshold || scaled.AttributedGas > historyPruneThreshold {
				merged.Edges[key] = scaled
			}
		}
		for _, tx := range history.Transactions {
			tx.Weight *= decay
			if tx.Weight >= historyPruneThreshold {
				tx.CallEdges = append([]model.EdgeKey(nil), tx.CallEdges...)
				merged.Transactions = append(merged.Transactions, tx)
			}
		}
		for _, stage := range history.Stages {
			stage.Weight *= decay
			if stage.Weight >= historyPruneThreshold {
				merged.Stages = append(merged.Stages, stage)
			}
		}
	}
	if window != nil {
		for address, stats := range window.Vertices {
			merged.Vertices[address] = addVertex(merged.Vertices[address], stats)
		}
		for key, stats := range window.Edges {
			merged.Edges[key] = addEdge(merged.Edges[key], stats)
		}
		for _, tx := range window.Transactions {
			tx.CallEdges = append([]model.EdgeKey(nil), tx.CallEdges...)
			merged.Transactions = append(merged.Transactions, tx)
		}
		merged.Stages = append(merged.Stages, window.Stages...)
	}
	return merged, nil
}

// Merge combines two planning graphs at full weight. Unlike MergeDecay, the
// name makes it explicit that neither input is historical decay. Callers must
// still decide separately which graph, if any, is persisted as history.
func Merge(left, right *Graph) (*Graph, error) {
	return MergeDecay(left, right, 1)
}

func (g *Graph) SortedVertices() []string {
	addresses := make([]string, 0, len(g.Vertices))
	for address := range g.Vertices {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	return addresses
}

func (g *Graph) SortedEdges() []model.EdgeKey {
	keys := make([]model.EdgeKey, 0, len(g.Edges))
	for key := range g.Edges {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Kind != keys[j].Kind {
			return keys[i].Kind < keys[j].Kind
		}
		if keys[i].From != keys[j].From {
			return keys[i].From < keys[j].From
		}
		return keys[i].To < keys[j].To
	})
	return keys
}

type ShardLoad struct {
	AttachedCallGas    float64
	BaseTransactionGas float64
	RelayDuplicateGas  float64
	CallTaskGas        float64
	TotalGas           float64
	RequiredBlocks     uint64
	TopLevelStages     float64
	CallTasks          float64
	GasPerTx           float64
}

type Metrics struct {
	ShardLoads              []ShardLoad
	MeanShardGas            float64
	MaxShardGas             float64
	MaxRequiredBlocks       uint64
	MaxRelativeGasDeviation float64
	GasPerTxCV              float64
	TotalLogicalTxs         float64
	TopLevelCrossTxs        float64
	InternalCrossTxs        float64
	UnionCrossTxs           float64
	CrossTxRatio            float64
	CrossOverheadGas        float64
	CrossOverheadRatio      float64
	TotalExecutedGas        float64
	BaseTransactionGas      float64
}

func shardOf(placement model.Placement, address string, shardCount uint64) (uint64, error) {
	shard, ok := placement[address]
	if !ok {
		return 0, fmt.Errorf("placement is missing address %s", address)
	}
	if shard >= shardCount {
		return 0, fmt.Errorf("address %s has invalid shard %d", address, shard)
	}
	return shard, nil
}

// CrossCallTargets returns the number of cross-shard internal calls whose
// callee executes in each target shard. Duplicate trace calls remain distinct.
func CrossCallTargets(tx model.TxObservation, placement model.Placement, shardCount uint64) (map[uint64]int, int, error) {
	targets := make(map[uint64]int)
	total := 0
	for _, edge := range tx.CallEdges {
		caller, err := shardOf(placement, edge.From, shardCount)
		if err != nil {
			return nil, 0, err
		}
		callee, err := shardOf(placement, edge.To, shardCount)
		if err != nil {
			return nil, 0, err
		}
		if caller != callee {
			targets[callee]++
			total++
		}
	}
	return targets, total, nil
}

func Evaluate(g *Graph, placement model.Placement, shardCount uint64, rhoCall float64, blockGasLimit uint64) (Metrics, error) {
	if g == nil || shardCount == 0 || rhoCall < 0 || math.IsNaN(rhoCall) || blockGasLimit == 0 {
		return Metrics{}, fmt.Errorf("valid graph, shard count, non-negative rhoCall and positive block gas limit are required")
	}
	metrics := Metrics{ShardLoads: make([]ShardLoad, int(shardCount))}
	for _, tx := range g.Transactions {
		if tx.Weight <= 0 || tx.GasUsed < 0 {
			continue
		}
		fromShard, err := shardOf(placement, tx.From, shardCount)
		if err != nil {
			return Metrics{}, err
		}
		toShard, err := shardOf(placement, tx.To, shardCount)
		if err != nil {
			return Metrics{}, err
		}
		baseGas := tx.GasUsed * tx.Weight
		metrics.TotalLogicalTxs += tx.Weight
		metrics.BaseTransactionGas += baseGas

		topCross := fromShard != toShard
		if topCross {
			metrics.TopLevelCrossTxs += tx.Weight
			metrics.CrossOverheadGas += baseGas
			// Relay1 executes at sender; Relay2 is the duplicate stage at
			// recipient. Keeping these fields aligned with runtime ownership makes
			// the planner and per-shard execution metrics directly comparable.
			metrics.ShardLoads[fromShard].BaseTransactionGas += baseGas
			metrics.ShardLoads[fromShard].TopLevelStages += tx.Weight
			metrics.ShardLoads[toShard].RelayDuplicateGas += baseGas
			metrics.ShardLoads[toShard].TopLevelStages += tx.Weight
		} else {
			metrics.ShardLoads[toShard].BaseTransactionGas += baseGas
			metrics.ShardLoads[toShard].TopLevelStages += tx.Weight
		}

		targets, crossCalls, err := CrossCallTargets(tx, placement, shardCount)
		if err != nil {
			return Metrics{}, err
		}
		internalCross := crossCalls > 0
		if internalCross {
			metrics.InternalCrossTxs += tx.Weight
			if tx.AttachedCallModel {
				extraGas := float64(tx.CallGasPerCall) * float64(crossCalls) * tx.Weight
				metrics.CrossOverheadGas += extraGas
				metrics.ShardLoads[toShard].AttachedCallGas += extraGas
			} else {
				extraGas := rhoCall * baseGas
				metrics.CrossOverheadGas += extraGas
				for target, count := range targets {
					share := extraGas * float64(count) / float64(crossCalls)
					metrics.ShardLoads[target].CallTaskGas += share
					metrics.ShardLoads[target].CallTasks += tx.Weight
				}
			}
		}
		if topCross || internalCross {
			metrics.UnionCrossTxs += tx.Weight
		}
	}
	for _, stage := range g.Stages {
		if stage.Weight <= 0 || stage.GasUsed < 0 {
			continue
		}
		ownerShard, err := shardOf(placement, stage.Owner, shardCount)
		if err != nil {
			return Metrics{}, err
		}
		stageGas := stage.GasUsed * stage.Weight
		if stage.AttachedCallModel {
			_, count, err := CrossCallTargets(model.TxObservation{CallEdges: stage.CallEdges}, placement, shardCount)
			if err != nil {
				return Metrics{}, err
			}
			extra := float64(count) * float64(stage.CallGasPerCall) * stage.Weight
			metrics.ShardLoads[ownerShard].AttachedCallGas += extra
			metrics.CrossOverheadGas += extra
		}
		switch stage.Kind {
		case model.Relay2Stage:
			metrics.ShardLoads[ownerShard].BaseTransactionGas += stageGas
			metrics.ShardLoads[ownerShard].TopLevelStages += stage.Weight
		case model.CallTaskStage:
			metrics.ShardLoads[ownerShard].CallTaskGas += stageGas
			metrics.ShardLoads[ownerShard].CallTasks += stage.Weight
		}
	}

	for shard := range metrics.ShardLoads {
		load := &metrics.ShardLoads[shard]
		load.TotalGas = load.BaseTransactionGas + load.RelayDuplicateGas + load.CallTaskGas + load.AttachedCallGas
		if load.TotalGas > 0 {
			load.RequiredBlocks = uint64(math.Ceil(load.TotalGas / float64(blockGasLimit)))
		}
		if load.TotalGas > metrics.MaxShardGas {
			metrics.MaxShardGas = load.TotalGas
		}
		if load.RequiredBlocks > metrics.MaxRequiredBlocks {
			metrics.MaxRequiredBlocks = load.RequiredBlocks
		}
		metrics.TotalExecutedGas += load.TotalGas
	}
	if shardCount > 0 {
		metrics.MeanShardGas = metrics.TotalExecutedGas / float64(shardCount)
	}
	if metrics.MeanShardGas > 0 {
		for _, load := range metrics.ShardLoads {
			deviation := math.Abs(load.TotalGas-metrics.MeanShardGas) / metrics.MeanShardGas
			if deviation > metrics.MaxRelativeGasDeviation {
				metrics.MaxRelativeGasDeviation = deviation
			}
		}
	}
	meanGasPerTx := 0.0
	for shard := range metrics.ShardLoads {
		load := &metrics.ShardLoads[shard]
		if load.TopLevelStages > 0 {
			load.GasPerTx = load.TotalGas / load.TopLevelStages
		} else if load.TotalGas > 0 {
			load.GasPerTx = load.TotalGas / 1e-9
		}
		meanGasPerTx += load.GasPerTx
	}
	meanGasPerTx /= float64(shardCount)
	if meanGasPerTx > 0 {
		variance := 0.0
		for _, load := range metrics.ShardLoads {
			delta := load.GasPerTx - meanGasPerTx
			variance += delta * delta
		}
		metrics.GasPerTxCV = math.Sqrt(variance/float64(shardCount)) / meanGasPerTx
	}
	if metrics.TotalLogicalTxs > 0 {
		metrics.CrossTxRatio = metrics.UnionCrossTxs / metrics.TotalLogicalTxs
	}
	if metrics.BaseTransactionGas > 0 {
		metrics.CrossOverheadRatio = metrics.CrossOverheadGas / metrics.BaseTransactionGas
	}
	return metrics, nil
}
