package partition

import "testing"

func TestComputeEdges2ShardChargesFullGasToBothCrossShardEndpoints(t *testing.T) {
	cs := new(CLPAState)
	cs.Init_CLPAState(0.5, 1, 2)

	sender := Vertex{Addr: "0000000000000000"}
	recipient := Vertex{Addr: "0000000000000001"}
	const gasUsed = uint64(10_000_000)
	cs.AddWeightedEdge(sender, recipient, gasUsed)
	cs.PartitionMap[sender] = 0
	cs.PartitionMap[recipient] = 1

	cs.ComputeEdges2Shard()

	if got := cs.Edges2Shard[0]; got != int(gasUsed) {
		t.Fatalf("sender shard expected full gas %d, got %d", gasUsed, got)
	}
	if got := cs.Edges2Shard[1]; got != int(gasUsed) {
		t.Fatalf("recipient shard expected full gas %d, got %d", gasUsed, got)
	}
	if got := cs.CrossShardEdgeNum; got != int(gasUsed) {
		t.Fatalf("cross-shard gas expected %d, got %d", gasUsed, got)
	}
}
