package partition

import "testing"

func TestWeightedEdgeAccumulatesUndirectedWeight(t *testing.T) {
	var g Graph
	a := Vertex{Addr: "0000000000000000000000000000000000000001"}
	b := Vertex{Addr: "0000000000000000000000000000000000000002"}

	g.AddWeightedEdge(a, b, 21)
	g.AddWeightedEdge(a, b, 79)

	if got := g.EdgeWeight[a][b]; got != 100 {
		t.Fatalf("EdgeWeight[a][b] = %d, want 100", got)
	}
	if got := g.EdgeWeight[b][a]; got != 100 {
		t.Fatalf("EdgeWeight[b][a] = %d, want 100", got)
	}
	if got := len(g.EdgeSet[a]); got != 1 {
		t.Fatalf("len(EdgeSet[a]) = %d, want 1", got)
	}
}
