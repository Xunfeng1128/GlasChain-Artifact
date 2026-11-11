// Package txallo adapts ContribChain's TxAllo branch to completed top-level
// transaction counts. It is not a Gas-weighted algorithm or a reproduction of
// the complete TxAllo paper. The unmodified algorithm lives in internal/contribchain.
package txallo

import "fmt"

type edgeKey struct{ from, to int }

// Graph retains first-seen account order and aggregates undirected transaction
// counts. A committee must serialize access and add each completed logical
// transaction once (inner-shard completion or Relay2, never Relay1).
type Graph struct {
	labels       []string
	index        map[string]int
	edges        map[edgeKey]uint64
	transactions uint64
}

func NewGraph() *Graph { return &Graph{index: make(map[string]int), edges: make(map[edgeKey]uint64)} }

func (g *Graph) AddTransaction(from, to string) error {
	if g == nil {
		return fmt.Errorf("nil TxAllo graph")
	}
	if from == "" || to == "" {
		return fmt.Errorf("TxAllo requires non-empty endpoints")
	}
	// The upstream execution reader filters self transfers. Its graph/refinement
	// accounting does not support a raw self loop, so reject it explicitly rather
	// than silently double counting it or changing the transplanted algorithm.
	if from == to {
		return fmt.Errorf("TxAllo ContribChain graph does not support self transfers")
	}
	if g.index == nil {
		g.index = make(map[string]int)
	}
	if g.edges == nil {
		g.edges = make(map[edgeKey]uint64)
	}
	add := func(label string) int {
		if id, ok := g.index[label]; ok {
			return id
		}
		id := len(g.labels)
		g.labels = append(g.labels, label)
		g.index[label] = id
		return id
	}
	left, right := add(from), add(to)
	if left > right {
		left, right = right, left
	}
	if g.transactions == ^uint64(0) {
		return fmt.Errorf("TxAllo transaction count overflow")
	}
	g.edges[edgeKey{left, right}]++
	g.transactions++
	return nil
}

func (g *Graph) Vertices() []string {
	if g == nil {
		return nil
	}
	return append([]string(nil), g.labels...)
}
func (g *Graph) VertexCount() int {
	if g == nil {
		return 0
	}
	return len(g.labels)
}
func (g *Graph) RelationshipCount() int {
	if g == nil {
		return 0
	}
	return len(g.edges)
}
func (g *Graph) LogicalTransactions() uint64 {
	if g == nil {
		return 0
	}
	return g.transactions
}
func (g *Graph) Reset() {
	if g != nil {
		*g = *NewGraph()
	}
}
