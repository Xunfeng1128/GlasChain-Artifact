package plouvain_contrib

import (
	"blockEmulator/partition/plouvain_contrib/internal/contribchain"
	"fmt"
	"math"
	"sort"
)

type Config struct {
	ShardCount    int
	Beta          float64
	MaxIterations int
}

func DefaultConfig(shards int) Config { return Config{ShardCount: shards, Beta: 2, MaxIterations: 70} }

func (cfg Config) validate() error {
	if cfg.ShardCount < 1 {
		return fmt.Errorf("P-Louvain requires at least one shard")
	}
	if cfg.Beta < 0 || math.IsNaN(cfg.Beta) || math.IsInf(cfg.Beta, 0) || cfg.Beta > math.MaxFloat32 {
		return fmt.Errorf("P-Louvain beta must be finite, non-negative, and representable as float32")
	}
	if cfg.MaxIterations < 0 {
		return fmt.Errorf("P-Louvain max iterations cannot be negative")
	}
	return nil
}

type ShardLoad struct {
	ShardID                   uint64
	AccountCount              int
	InnerTransactions         uint64
	CrossIncidentTransactions uint64
	ModeledLoad               float64
}

type Metrics struct {
	ShardLoads           []ShardLoad
	LogicalTransactions  uint64
	CrossTransactions    uint64
	CrossTransactionRate float64
	MaxModeledLoad       float64
	ModeledLoadCV        float64
}

type Result struct {
	Placement          map[string]uint64
	Before             Metrics
	After              Metrics
	GraphTransactions  uint64
	GraphRelationships int
	CommunityCount     int
	MovedAccounts      int
	RefinementPasses   int
}

// Optimize runs ContribChain's original deterministic P-Louvain account branch
// (isDeterministic=true, isTxAllo=false) with equal shard performance.
// current is used for diagnostics only: upstream rediscovers communities and
// assigns them to shards at each round, rather than warm-starting the optimizer.
// Zero MaxIterations explicitly disables the third (refinement) stage.
func Optimize(g *Graph, current map[string]uint64, cfg Config) (Result, error) {
	if err := cfg.validate(); err != nil {
		return Result{}, err
	}
	if g == nil {
		return Result{}, fmt.Errorf("nil P-Louvain graph")
	}
	for _, label := range g.labels {
		shard, ok := current[label]
		if !ok || shard >= uint64(cfg.ShardCount) {
			return Result{}, fmt.Errorf("missing or invalid current shard for account %s", label)
		}
	}
	// Upstream uses float32. An undirected graph has total degree 2*N;
	// this bound keeps every integer count exactly representable at all levels.
	if g.transactions > 1<<23 {
		return Result{}, fmt.Errorf("P-Louvain graph exceeds exact float32 count range (8388608 transactions per graph)")
	}
	if cfg.Beta > 0 && float64(g.transactions)*cfg.Beta > math.MaxFloat32 {
		return Result{}, fmt.Errorf("P-Louvain modeled load exceeds float32 range")
	}
	result := Result{Placement: make(map[string]uint64, len(g.labels)), GraphTransactions: g.transactions, GraphRelationships: len(g.edges)}
	result.Before = evaluate(g, current, cfg)
	if len(g.labels) == 0 {
		result.After = evaluate(g, result.Placement, cfg)
		return result, nil
	}
	if cfg.ShardCount == 1 {
		for _, label := range g.labels {
			result.Placement[label] = 0
		}
		result.CommunityCount = 1
		result.After = evaluate(g, result.Placement, cfg)
		return result, nil
	}

	graph := g.upstreamGraph()
	performance := make([]float32, cfg.ShardCount)
	for i := range performance {
		performance[i] = 1
	}
	l := contribchain.NewLouvain(graph, performance, false, float32(cfg.Beta), cfg.ShardCount)
	l.Compute(true, true, false)
	result.CommunityCount = l.GetCommunitiesNum()
	l.CommToShard(float32(cfg.Beta))
	_, placement := l.UpdateNodeShardIndex()
	for i := 0; i < cfg.MaxIterations; i++ {
		improved, next := l.NodeMerge(placement, true, false)
		placement = next
		result.RefinementPasses++
		if !improved {
			break
		}
	}
	for node, shard := range placement {
		if shard < 0 || shard >= cfg.ShardCount {
			return Result{}, fmt.Errorf("upstream P-Louvain returned invalid shard %d", shard)
		}
		label := g.labels[node]
		result.Placement[label] = uint64(shard)
		if current[label] != uint64(shard) {
			result.MovedAccounts++
		}
	}
	result.After = evaluate(g, result.Placement, cfg)
	return result, nil
}

func (g *Graph) upstreamGraph() contribchain.Graph {
	graph := contribchain.NewGraph(len(g.labels))
	// Stable adjacency order removes Go-map iteration noise from ties, without
	// altering source gain formulas, float32 arithmetic, or stopping conditions.
	keys := make([]edgeKey, 0, len(g.edges))
	for key := range g.edges {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].from != keys[j].from {
			return keys[i].from < keys[j].from
		}
		return keys[i].to < keys[j].to
	})
	for _, key := range keys {
		weight := contribchain.WeightType(g.edges[key])
		graph.AddDirectedEdge(key.from, key.to, weight)
		graph.AddDirectedEdge(key.to, key.from, weight)
	}
	return graph
}

func evaluate(g *Graph, placement map[string]uint64, cfg Config) Metrics {
	m := Metrics{ShardLoads: make([]ShardLoad, cfg.ShardCount), LogicalTransactions: g.transactions}
	for shard := range m.ShardLoads {
		m.ShardLoads[shard].ShardID = uint64(shard)
	}
	for _, label := range g.labels {
		m.ShardLoads[placement[label]].AccountCount++
	}
	for edge, count := range g.edges {
		left, right := placement[g.labels[edge.from]], placement[g.labels[edge.to]]
		if left == right {
			m.ShardLoads[left].InnerTransactions += count
		} else {
			m.ShardLoads[left].CrossIncidentTransactions += count
			m.ShardLoads[right].CrossIncidentTransactions += count
			m.CrossTransactions += count
		}
	}
	if g.transactions > 0 {
		m.CrossTransactionRate = float64(m.CrossTransactions) / float64(g.transactions)
	}
	mean := 0.0
	for shard := range m.ShardLoads {
		load := &m.ShardLoads[shard]
		load.ModeledLoad = float64(load.InnerTransactions) + cfg.Beta*float64(load.CrossIncidentTransactions)
		mean += load.ModeledLoad / float64(cfg.ShardCount)
		m.MaxModeledLoad = math.Max(m.MaxModeledLoad, load.ModeledLoad)
	}
	if mean > 0 {
		variance := 0.0
		for _, load := range m.ShardLoads {
			delta := load.ModeledLoad - mean
			variance += delta * delta / float64(cfg.ShardCount)
		}
		m.ModeledLoadCV = math.Sqrt(variance) / mean
	}
	return m
}
