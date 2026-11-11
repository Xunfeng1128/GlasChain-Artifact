package contribchain

// NewGraph constructs the upstream graph without exposing its private fields.
// AddDirectedEdge must be called for both directions of each non-self edge.
func NewGraph(nodes int) Graph {
	return Graph{incidences: make(Edges, nodes), selfs: make([]WeightType, nodes)}
}
