// Package plouvain implements the account-placement portion of ContribChain's
// P-Louvain workflow with GasUsed as the transaction-graph edge weight.
//
// The package deliberately does not add block-capacity constraints, pending
// workload visibility, call-trace awareness, or global bin packing. It is a
// direct gasification baseline: the original relationship/community workflow
// is retained while its transaction-count unit is replaced by gas.
package plouvain

import (
	"fmt"
	"sort"
)

type edgeAggregate struct {
	gas          float64
	transactions uint64
}

// Graph accumulates one undirected account edge per completed logical
// transaction. Repeated transactions between the same pair add to both the
// edge gas and transaction count.
type Graph struct {
	edges        map[string]map[string]edgeAggregate
	logicalGas   float64
	logicalTxs   uint64
	relationship int
}

func NewGraph() *Graph {
	return &Graph{edges: make(map[string]map[string]edgeAggregate)}
}

// AddTransaction adds one completed top-level transaction. gas must already be
// the original base Gas (core.BaseTransactionGas(tx)), excluding execution surcharges.
func (g *Graph) AddTransaction(from, to string, gas uint64) error {
	if g == nil {
		return fmt.Errorf("nil P-Louvain graph")
	}
	if from == "" || to == "" {
		return fmt.Errorf("P-Louvain graph requires non-empty endpoints")
	}
	if gas == 0 {
		return fmt.Errorf("P-Louvain graph requires positive gas")
	}
	if g.edges == nil {
		g.edges = make(map[string]map[string]edgeAggregate)
	}
	if g.edges[from] == nil {
		g.edges[from] = make(map[string]edgeAggregate)
	}
	if g.edges[to] == nil {
		g.edges[to] = make(map[string]edgeAggregate)
	}
	if _, exists := g.edges[from][to]; !exists {
		g.relationship++
	}
	forward := g.edges[from][to]
	forward.gas += float64(gas)
	forward.transactions++
	g.edges[from][to] = forward
	if from != to {
		reverse := g.edges[to][from]
		reverse.gas += float64(gas)
		reverse.transactions++
		g.edges[to][from] = reverse
	}
	g.logicalGas += float64(gas)
	g.logicalTxs++
	return nil
}

func (g *Graph) Reset() {
	if g == nil {
		return
	}
	g.edges = make(map[string]map[string]edgeAggregate)
	g.logicalGas = 0
	g.logicalTxs = 0
	g.relationship = 0
}

func (g *Graph) VertexCount() int {
	if g == nil {
		return 0
	}
	return len(g.edges)
}

func (g *Graph) RelationshipCount() int {
	if g == nil {
		return 0
	}
	return g.relationship
}

func (g *Graph) LogicalTransactions() uint64 {
	if g == nil {
		return 0
	}
	return g.logicalTxs
}

func (g *Graph) LogicalGas() float64 {
	if g == nil {
		return 0
	}
	return g.logicalGas
}

// Vertices returns a stable address order. Stable ordering is important
// because the original deterministic P-Louvain still depends on node order.
func (g *Graph) Vertices() []string {
	if g == nil {
		return nil
	}
	labels := make([]string, 0, len(g.edges))
	for label := range g.edges {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	return labels
}

type snapshot struct {
	labels        []string
	gas           []map[int]float64
	transactions  []map[int]uint64
	selfGas       []float64
	selfTxs       []uint64
	logicalGas    float64
	logicalTxs    uint64
	relationships int
}

func (g *Graph) makeSnapshot() snapshot {
	labels := g.Vertices()
	index := make(map[string]int, len(labels))
	for idx, label := range labels {
		index[label] = idx
	}
	s := snapshot{
		labels:        labels,
		gas:           make([]map[int]float64, len(labels)),
		transactions:  make([]map[int]uint64, len(labels)),
		selfGas:       make([]float64, len(labels)),
		selfTxs:       make([]uint64, len(labels)),
		logicalGas:    g.LogicalGas(),
		logicalTxs:    g.LogicalTransactions(),
		relationships: g.RelationshipCount(),
	}
	for idx := range labels {
		s.gas[idx] = make(map[int]float64)
		s.transactions[idx] = make(map[int]uint64)
	}
	for from, neighbors := range g.edges {
		fromID := index[from]
		for to, aggregate := range neighbors {
			toID := index[to]
			if fromID == toID {
				s.selfGas[fromID] = aggregate.gas
				s.selfTxs[fromID] = aggregate.transactions
				continue
			}
			s.gas[fromID][toID] = aggregate.gas
			s.transactions[fromID][toID] = aggregate.transactions
		}
	}
	return s
}
