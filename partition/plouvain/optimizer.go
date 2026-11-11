package plouvain

import (
	"fmt"
	"math"
	"sort"
)

const numericEpsilon = 1e-7

type Config struct {
	ShardCount          int
	Beta                float64
	MaxRefinementPasses int
	BlockGasLimit       uint64
}

func (cfg Config) validate() error {
	if cfg.ShardCount < 1 {
		return fmt.Errorf("P-Louvain requires at least one shard")
	}
	if cfg.Beta < 0 || math.IsNaN(cfg.Beta) || math.IsInf(cfg.Beta, 0) {
		return fmt.Errorf("P-Louvain beta must be finite and non-negative")
	}
	if cfg.MaxRefinementPasses < 0 {
		return fmt.Errorf("P-Louvain refinement passes cannot be negative")
	}
	if cfg.BlockGasLimit == 0 {
		return fmt.Errorf("P-Louvain gas baseline requires a positive block gas limit")
	}
	return nil
}

type ShardLoad struct {
	ShardID          uint64
	AccountCount     int
	InnerGas         float64
	CrossIncidentGas float64
	ModeledLoad      float64
	PhysicalGas      float64
	RequiredBlocks   uint64
}

type Metrics struct {
	ShardLoads           []ShardLoad
	LogicalTransactions  uint64
	LogicalGas           float64
	CrossTransactions    uint64
	CrossTransactionRate float64
	CrossOverheadGas     float64
	CrossGasRatio        float64
	TotalPhysicalGas     float64
	MeanPhysicalGas      float64
	MaxPhysicalGas       float64
	MaxModeledLoad       float64
	MaxRequiredBlocks    uint64
	PhysicalGasCV        float64
}

type CommunityStats struct {
	CommunityID   uint64
	AccountCount  int
	InternalGas   float64
	ExternalGas   float64
	AssignedShard uint64
}

type Result struct {
	Placement          map[string]uint64
	Before             Metrics
	After              Metrics
	Communities        []CommunityStats
	MoveOperations     int
	MovedAccounts      int
	RefinementPasses   int
	LouvainLevels      int
	GraphRelationships int
	GraphTransactions  uint64
	GraphGas           float64
}

// Optimize runs the direct Gas-P-Louvain baseline:
//
//  1. gas-weighted Louvain community detection;
//  2. the original community-to-shard heuristic (largest account communities
//     seed shards, remaining communities go to the currently lightest shard);
//  3. neighbor-shard-only single-account refinement using the original
//     pairwise maximum modeled-load test.
//
// current must contain a valid shard for every graph vertex. This keeps the
// package independent from BlockEmulator's hash-placement implementation and
// makes before/after diagnostics explicit.
func Optimize(g *Graph, current map[string]uint64, cfg Config) (Result, error) {
	if err := cfg.validate(); err != nil {
		return Result{}, err
	}
	if g == nil {
		return Result{}, fmt.Errorf("nil P-Louvain graph")
	}
	s := g.makeSnapshot()
	result := Result{
		Placement:          make(map[string]uint64),
		GraphRelationships: s.relationships,
		GraphTransactions:  s.logicalTxs,
		GraphGas:           s.logicalGas,
	}
	if len(s.labels) == 0 {
		result.Before = emptyMetrics(cfg.ShardCount)
		result.After = emptyMetrics(cfg.ShardCount)
		return result, nil
	}
	for _, label := range s.labels {
		shard, ok := current[label]
		if !ok {
			return Result{}, fmt.Errorf("current placement is missing account %s", label)
		}
		if shard >= uint64(cfg.ShardCount) {
			return Result{}, fmt.Errorf("account %s has invalid current shard %d", label, shard)
		}
	}

	before, err := evaluate(s, current, cfg)
	if err != nil {
		return Result{}, err
	}
	result.Before = before

	communities, levels := detectCommunities(makeWeightedGraph(s))
	result.LouvainLevels = levels
	placement, communityStats := assignCommunities(s, communities, cfg)
	moves, movedAccounts, passes := refineAccounts(s, placement, cfg)
	result.MoveOperations = moves
	result.MovedAccounts = movedAccounts
	result.RefinementPasses = passes
	result.Communities = communityStats

	for nodeID, label := range s.labels {
		result.Placement[label] = uint64(placement[nodeID])
	}
	after, err := evaluate(s, result.Placement, cfg)
	if err != nil {
		return Result{}, err
	}
	result.After = after
	return result, nil
}

func emptyMetrics(shardCount int) Metrics {
	metrics := Metrics{ShardLoads: make([]ShardLoad, shardCount)}
	for shard := range metrics.ShardLoads {
		metrics.ShardLoads[shard].ShardID = uint64(shard)
	}
	return metrics
}

type weightedGraph struct {
	adj  []map[int]float64
	self []float64
}

func makeWeightedGraph(s snapshot) weightedGraph {
	g := weightedGraph{adj: make([]map[int]float64, len(s.labels)), self: append([]float64(nil), s.selfGas...)}
	for node := range g.adj {
		g.adj[node] = make(map[int]float64, len(s.gas[node]))
		for neighbor, gas := range s.gas[node] {
			g.adj[node][neighbor] = gas
		}
	}
	return g
}

func (g weightedGraph) size() int { return len(g.adj) }

func (g weightedGraph) degree(node int) float64 {
	degree := 2 * g.self[node]
	for _, weight := range g.adj[node] {
		degree += weight
	}
	return degree
}

func (g weightedGraph) totalDegree() float64 {
	total := 0.0
	for node := range g.adj {
		total += g.degree(node)
	}
	return total
}

// detectCommunities is a deterministic weighted Louvain implementation. It
// performs the same modularity local-move and graph-rebuild stages used by the
// ContribChain source, with stable node/community tie ordering.
func detectCommunities(base weightedGraph) ([]int, int) {
	originalToNode := make([]int, base.size())
	for node := range originalToNode {
		originalToNode[node] = node
	}
	if base.size() == 0 {
		return originalToNode, 0
	}
	current := base
	levels := 0
	for levels < 100 {
		assignment, communityCount := localModularityMove(current)
		levels++
		for original := range originalToNode {
			originalToNode[original] = assignment[originalToNode[original]]
		}
		if communityCount == current.size() {
			break
		}
		current = contractGraph(current, assignment, communityCount)
	}
	return originalToNode, levels
}

func localModularityMove(g weightedGraph) ([]int, int) {
	n := g.size()
	assignment := make([]int, n)
	communityTotal := make([]float64, n)
	degrees := make([]float64, n)
	for node := 0; node < n; node++ {
		assignment[node] = node
		degrees[node] = g.degree(node)
		communityTotal[node] = degrees[node]
	}
	m2 := g.totalDegree()
	if m2 <= 0 {
		return assignment, n
	}
	for pass := 0; pass < 100; pass++ {
		moved := false
		for node := 0; node < n; node++ {
			currentCommunity := assignment[node]
			weightsByCommunity := make(map[int]float64)
			for neighbor, weight := range g.adj[node] {
				weightsByCommunity[assignment[neighbor]] += weight
			}
			communityTotal[currentCommunity] -= degrees[node]

			bestCommunity := currentCommunity
			currentGain := weightsByCommunity[currentCommunity] - communityTotal[currentCommunity]*degrees[node]/m2
			bestGain := math.Max(0, currentGain)
			candidates := make([]int, 0, len(weightsByCommunity))
			for community := range weightsByCommunity {
				if community != currentCommunity {
					candidates = append(candidates, community)
				}
			}
			sort.Ints(candidates)
			for _, community := range candidates {
				gain := weightsByCommunity[community] - communityTotal[community]*degrees[node]/m2
				if gain > bestGain+numericEpsilon {
					bestGain = gain
					bestCommunity = community
				}
			}

			assignment[node] = bestCommunity
			communityTotal[bestCommunity] += degrees[node]
			if bestCommunity != currentCommunity {
				moved = true
			}
		}
		if !moved {
			break
		}
	}
	return renumberCommunities(assignment)
}

func renumberCommunities(assignment []int) ([]int, int) {
	renumbered := make([]int, len(assignment))
	ids := make(map[int]int)
	for node, community := range assignment {
		newID, exists := ids[community]
		if !exists {
			newID = len(ids)
			ids[community] = newID
		}
		renumbered[node] = newID
	}
	return renumbered, len(ids)
}

func contractGraph(g weightedGraph, assignment []int, communityCount int) weightedGraph {
	contracted := weightedGraph{adj: make([]map[int]float64, communityCount), self: make([]float64, communityCount)}
	for community := range contracted.adj {
		contracted.adj[community] = make(map[int]float64)
	}
	for node := 0; node < g.size(); node++ {
		contracted.self[assignment[node]] += g.self[node]
	}
	for node := 0; node < g.size(); node++ {
		for neighbor, weight := range g.adj[node] {
			if node >= neighbor {
				continue
			}
			left, right := assignment[node], assignment[neighbor]
			if left == right {
				contracted.self[left] += weight
				continue
			}
			contracted.adj[left][right] += weight
			contracted.adj[right][left] += weight
		}
	}
	return contracted
}

type communityWorkingStats struct {
	accounts int
	internal float64
	external float64
	assigned int
}

type workingShardLoad struct {
	accounts int
	inner    float64
	cross    float64
}

func (load workingShardLoad) modeled(beta float64) float64 {
	return load.inner + beta*load.cross
}

func assignCommunities(s snapshot, communities []int, cfg Config) ([]int, []CommunityStats) {
	communityCount := 0
	for _, community := range communities {
		if community+1 > communityCount {
			communityCount = community + 1
		}
	}
	stats := make([]communityWorkingStats, communityCount)
	between := make([]map[int]float64, communityCount)
	for community := range stats {
		stats[community].assigned = -1
		between[community] = make(map[int]float64)
	}
	for node, community := range communities {
		stats[community].accounts++
		stats[community].internal += s.selfGas[node]
	}
	for node := range s.labels {
		for neighbor, gas := range s.gas[node] {
			if node >= neighbor {
				continue
			}
			left, right := communities[node], communities[neighbor]
			if left == right {
				stats[left].internal += gas
				continue
			}
			stats[left].external += gas
			stats[right].external += gas
			between[left][right] += gas
			between[right][left] += gas
		}
	}

	order := make([]int, communityCount)
	for community := range order {
		order[community] = community
	}
	sort.SliceStable(order, func(i, j int) bool {
		left, right := stats[order[i]], stats[order[j]]
		if left.accounts != right.accounts {
			return left.accounts > right.accounts
		}
		return order[i] < order[j]
	})
	shardLoads := make([]workingShardLoad, cfg.ShardCount)
	assign := func(community, shard int) {
		coLocatedGas := 0.0
		for other, gas := range between[community] {
			if stats[other].assigned == shard {
				coLocatedGas += gas
			}
		}
		shardLoads[shard].accounts += stats[community].accounts
		shardLoads[shard].inner += stats[community].internal + coLocatedGas
		shardLoads[shard].cross += stats[community].external - 2*coLocatedGas
		shardLoads[shard].cross = cleanZero(shardLoads[shard].cross)
		stats[community].assigned = shard
	}

	seedCount := cfg.ShardCount
	if communityCount < seedCount {
		seedCount = communityCount
	}
	for rank := 0; rank < seedCount; rank++ {
		assign(order[rank], rank)
	}
	for rank := seedCount; rank < len(order); rank++ {
		bestShard := 0
		bestLoad := shardLoads[0].modeled(cfg.Beta)
		for shard := 1; shard < cfg.ShardCount; shard++ {
			load := shardLoads[shard].modeled(cfg.Beta)
			if load < bestLoad-numericEpsilon {
				bestLoad = load
				bestShard = shard
			}
		}
		assign(order[rank], bestShard)
	}

	placement := make([]int, len(s.labels))
	for node, community := range communities {
		placement[node] = stats[community].assigned
	}
	resultStats := make([]CommunityStats, communityCount)
	for community, stat := range stats {
		resultStats[community] = CommunityStats{
			CommunityID:   uint64(community),
			AccountCount:  stat.accounts,
			InternalGas:   stat.internal,
			ExternalGas:   stat.external,
			AssignedShard: uint64(stat.assigned),
		}
	}
	return placement, resultStats
}

func computeWorkingShardLoads(s snapshot, placement []int, shardCount int) []workingShardLoad {
	loads := make([]workingShardLoad, shardCount)
	for node, shard := range placement {
		loads[shard].accounts++
		loads[shard].inner += s.selfGas[node]
	}
	for node := range s.labels {
		for neighbor, gas := range s.gas[node] {
			if node >= neighbor {
				continue
			}
			left, right := placement[node], placement[neighbor]
			if left == right {
				loads[left].inner += gas
			} else {
				loads[left].cross += gas
				loads[right].cross += gas
			}
		}
	}
	return loads
}

func refineAccounts(s snapshot, placement []int, cfg Config) (int, int, int) {
	if cfg.MaxRefinementPasses == 0 || len(placement) == 0 {
		return 0, 0, 0
	}
	loads := computeWorkingShardLoads(s, placement, cfg.ShardCount)
	moveOperations := 0
	moved := make(map[int]bool)
	passes := 0
	for pass := 0; pass < cfg.MaxRefinementPasses; pass++ {
		passMoved := false
		passes++
		for node := range placement {
			source := placement[node]
			weightsByShard := make([]float64, cfg.ShardCount)
			totalNeighborGas := 0.0
			candidate := make([]bool, cfg.ShardCount)
			for neighbor, gas := range s.gas[node] {
				neighborShard := placement[neighbor]
				weightsByShard[neighborShard] += gas
				totalNeighborGas += gas
				if neighborShard != source {
					candidate[neighborShard] = true
				}
			}

			bestShard := source
			bestImprovement := 0.0
			bestSource := loads[source]
			bestDestination := workingShardLoad{}
			for destination := 0; destination < cfg.ShardCount; destination++ {
				if !candidate[destination] {
					continue
				}
				beforeMax := math.Max(loads[source].modeled(cfg.Beta), loads[destination].modeled(cfg.Beta))
				toSource := weightsByShard[source]
				toDestination := weightsByShard[destination]

				newSource := loads[source]
				newSource.inner -= toSource + s.selfGas[node]
				newSource.cross += 2*toSource - totalNeighborGas
				newSource.accounts--
				newSource.inner = cleanZero(newSource.inner)
				newSource.cross = cleanZero(newSource.cross)

				newDestination := loads[destination]
				newDestination.inner += toDestination + s.selfGas[node]
				newDestination.cross += totalNeighborGas - 2*toDestination
				newDestination.accounts++
				newDestination.inner = cleanZero(newDestination.inner)
				newDestination.cross = cleanZero(newDestination.cross)

				afterMax := math.Max(newSource.modeled(cfg.Beta), newDestination.modeled(cfg.Beta))
				improvement := beforeMax - afterMax
				if improvement > bestImprovement+numericEpsilon {
					bestImprovement = improvement
					bestShard = destination
					bestSource = newSource
					bestDestination = newDestination
				}
			}
			if bestShard == source {
				continue
			}
			loads[source] = bestSource
			loads[bestShard] = bestDestination
			placement[node] = bestShard
			moveOperations++
			moved[node] = true
			passMoved = true
		}
		if !passMoved {
			break
		}
	}
	return moveOperations, len(moved), passes
}

func cleanZero(value float64) float64 {
	if math.Abs(value) <= numericEpsilon {
		return 0
	}
	return value
}

func evaluate(s snapshot, placement map[string]uint64, cfg Config) (Metrics, error) {
	metrics := emptyMetrics(cfg.ShardCount)
	metrics.LogicalTransactions = s.logicalTxs
	metrics.LogicalGas = s.logicalGas
	nodeShard := make([]int, len(s.labels))
	for node, label := range s.labels {
		shard, ok := placement[label]
		if !ok {
			return Metrics{}, fmt.Errorf("placement is missing account %s", label)
		}
		if shard >= uint64(cfg.ShardCount) {
			return Metrics{}, fmt.Errorf("account %s has invalid shard %d", label, shard)
		}
		nodeShard[node] = int(shard)
		metrics.ShardLoads[shard].AccountCount++
		metrics.ShardLoads[shard].InnerGas += s.selfGas[node]
	}
	for node := range s.labels {
		for neighbor, gas := range s.gas[node] {
			if node >= neighbor {
				continue
			}
			left, right := nodeShard[node], nodeShard[neighbor]
			if left == right {
				metrics.ShardLoads[left].InnerGas += gas
				continue
			}
			metrics.CrossOverheadGas += gas
			metrics.CrossTransactions += s.transactions[node][neighbor]
			metrics.ShardLoads[left].CrossIncidentGas += gas
			metrics.ShardLoads[right].CrossIncidentGas += gas
		}
	}
	if metrics.LogicalTransactions > 0 {
		metrics.CrossTransactionRate = float64(metrics.CrossTransactions) / float64(metrics.LogicalTransactions)
	}
	if metrics.LogicalGas > 0 {
		metrics.CrossGasRatio = metrics.CrossOverheadGas / metrics.LogicalGas
	}
	for shard := range metrics.ShardLoads {
		load := &metrics.ShardLoads[shard]
		load.ModeledLoad = load.InnerGas + cfg.Beta*load.CrossIncidentGas
		load.PhysicalGas = load.InnerGas + load.CrossIncidentGas
		if load.PhysicalGas > 0 {
			load.RequiredBlocks = uint64(math.Ceil(load.PhysicalGas / float64(cfg.BlockGasLimit)))
		}
		metrics.TotalPhysicalGas += load.PhysicalGas
		metrics.MaxPhysicalGas = math.Max(metrics.MaxPhysicalGas, load.PhysicalGas)
		metrics.MaxModeledLoad = math.Max(metrics.MaxModeledLoad, load.ModeledLoad)
		if load.RequiredBlocks > metrics.MaxRequiredBlocks {
			metrics.MaxRequiredBlocks = load.RequiredBlocks
		}
	}
	metrics.MeanPhysicalGas = metrics.TotalPhysicalGas / float64(cfg.ShardCount)
	if metrics.MeanPhysicalGas > 0 {
		variance := 0.0
		for _, load := range metrics.ShardLoads {
			delta := load.PhysicalGas - metrics.MeanPhysicalGas
			variance += delta * delta
		}
		variance /= float64(cfg.ShardCount)
		metrics.PhysicalGasCV = math.Sqrt(variance) / metrics.MeanPhysicalGas
	}
	return metrics, nil
}
