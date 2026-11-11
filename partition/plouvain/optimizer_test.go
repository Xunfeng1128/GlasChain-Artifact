package plouvain

import (
	"reflect"
	"testing"
)

func TestGraphAggregatesGasWithoutFloat32Loss(t *testing.T) {
	g := NewGraph()
	if err := g.AddTransaction("a", "b", 4_464_384_575); err != nil {
		t.Fatal(err)
	}
	if err := g.AddTransaction("b", "a", 21_000); err != nil {
		t.Fatal(err)
	}
	s := g.makeSnapshot()
	if got, want := g.RelationshipCount(), 1; got != want {
		t.Fatalf("relationships = %d, want %d", got, want)
	}
	if got, want := g.LogicalTransactions(), uint64(2); got != want {
		t.Fatalf("logical txs = %d, want %d", got, want)
	}
	if got, want := s.gas[0][1], float64(4_464_405_575); got != want {
		t.Fatalf("aggregated gas = %.0f, want %.0f", got, want)
	}
	if got, want := s.transactions[0][1], uint64(2); got != want {
		t.Fatalf("edge txs = %d, want %d", got, want)
	}
}

func TestEvaluateChargesCrossGasOnBothEndpointShards(t *testing.T) {
	g := NewGraph()
	if err := g.AddTransaction("a", "b", 100); err != nil {
		t.Fatal(err)
	}
	if err := g.AddTransaction("a", "c", 50); err != nil {
		t.Fatal(err)
	}
	cfg := Config{ShardCount: 2, Beta: 2, MaxRefinementPasses: 70, BlockGasLimit: 100}
	metrics, err := evaluate(g.makeSnapshot(), map[string]uint64{"a": 0, "b": 1, "c": 0}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := metrics.LogicalGas, float64(150); got != want {
		t.Fatalf("logical gas = %.0f, want %.0f", got, want)
	}
	if got, want := metrics.CrossOverheadGas, float64(100); got != want {
		t.Fatalf("cross overhead = %.0f, want %.0f", got, want)
	}
	if got, want := metrics.TotalPhysicalGas, float64(250); got != want {
		t.Fatalf("physical gas = %.0f, want %.0f", got, want)
	}
	if got, want := metrics.ShardLoads[0].PhysicalGas, float64(150); got != want {
		t.Fatalf("shard 0 physical gas = %.0f, want %.0f", got, want)
	}
	if got, want := metrics.ShardLoads[1].PhysicalGas, float64(100); got != want {
		t.Fatalf("shard 1 physical gas = %.0f, want %.0f", got, want)
	}
	if got, want := metrics.MaxRequiredBlocks, uint64(2); got != want {
		t.Fatalf("max required blocks = %d, want %d", got, want)
	}
}

func TestAssignTenCommunitiesAcrossFourShards(t *testing.T) {
	communitySizes := []int{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	nodeCount := 0
	for _, size := range communitySizes {
		nodeCount += size
	}
	s := snapshot{
		labels:       make([]string, nodeCount),
		gas:          make([]map[int]float64, nodeCount),
		transactions: make([]map[int]uint64, nodeCount),
		selfGas:      make([]float64, nodeCount),
		selfTxs:      make([]uint64, nodeCount),
	}
	communities := make([]int, nodeCount)
	node := 0
	for community, size := range communitySizes {
		for offset := 0; offset < size; offset++ {
			s.labels[node] = string(rune('a' + node))
			s.gas[node] = make(map[int]float64)
			s.transactions[node] = make(map[int]uint64)
			communities[node] = community
			// Give each community a distinct, additive gas load while leaving
			// the original account-count ordering as the placement order.
			s.selfGas[node] = float64((10-community)*100) / float64(size)
			node++
		}
	}
	cfg := Config{ShardCount: 4, Beta: 2, MaxRefinementPasses: 70, BlockGasLimit: 30_000_000}
	placement, stats := assignCommunities(s, communities, cfg)
	if len(stats) != 10 {
		t.Fatalf("community count = %d, want 10", len(stats))
	}
	for community := 0; community < 4; community++ {
		if got, want := stats[community].AssignedShard, uint64(community); got != want {
			t.Fatalf("seed community %d assigned to shard %d, want %d", community, got, want)
		}
	}
	seen := make([]bool, 4)
	for _, shard := range placement {
		if shard < 0 || shard >= 4 {
			t.Fatalf("invalid shard %d", shard)
		}
		seen[shard] = true
	}
	for shard, ok := range seen {
		if !ok {
			t.Fatalf("shard %d received no account", shard)
		}
	}
}

func TestOptimizeIsDeterministicAndRefinesModeledLoad(t *testing.T) {
	g := NewGraph()
	for _, tx := range []struct {
		from string
		to   string
		gas  uint64
	}{
		{"a", "b", 20},
		{"a", "c", 80},
		{"d", "e", 70},
		{"d", "f", 10},
		{"c", "d", 5},
	} {
		if err := g.AddTransaction(tx.from, tx.to, tx.gas); err != nil {
			t.Fatal(err)
		}
	}
	current := map[string]uint64{"a": 0, "b": 0, "c": 1, "d": 1, "e": 0, "f": 1}
	cfg := Config{ShardCount: 2, Beta: 2, MaxRefinementPasses: 70, BlockGasLimit: 100}
	first, err := Optimize(g, current, cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Optimize(g, current, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Placement, second.Placement) {
		t.Fatalf("non-deterministic placements: %#v vs %#v", first.Placement, second.Placement)
	}
	if first.After.MaxModeledLoad > first.Before.MaxModeledLoad+numericEpsilon {
		t.Fatalf("modeled maximum increased: before %.0f after %.0f", first.Before.MaxModeledLoad, first.After.MaxModeledLoad)
	}
	if len(first.Communities) == 0 {
		t.Fatal("expected at least one community")
	}
}
