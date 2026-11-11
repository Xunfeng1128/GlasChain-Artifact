package graph

import (
	"math"
	"testing"

	"blockEmulator/glasfreight/model"
)

func sampleTransaction() *model.Transaction {
	return &model.Transaction{
		SourceHash: "tx1", From: "eoa-a", To: "sca-b", GasUsed: 100, HasGasUsed: true,
		ToIsContract: true,
		Calls:        []model.Call{{Caller: "sca-b", Callee: "sca-c", GasUsed: 25, HasGasUsed: true, IsCCGEdge: true, CallerIsSCA: true, CalleeIsSCA: true}},
	}
}

func TestEvaluateChargesRelayAndAggregatedCallTaskGas(t *testing.T) {
	g := New()
	if err := g.AddTransaction(sampleTransaction()); err != nil {
		t.Fatal(err)
	}
	placement := model.Placement{"eoa-a": 0, "sca-b": 1, "sca-c": 0}
	metrics, err := Evaluate(g, placement, 2, 0.5, 100)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.BaseTransactionGas != 100 || metrics.CrossOverheadGas != 150 {
		t.Fatalf("gas totals = base %.0f overhead %.0f", metrics.BaseTransactionGas, metrics.CrossOverheadGas)
	}
	if metrics.ShardLoads[0].BaseTransactionGas != 100 || metrics.ShardLoads[0].CallTaskGas != 50 || metrics.ShardLoads[1].RelayDuplicateGas != 100 || metrics.ShardLoads[1].BaseTransactionGas != 0 {
		t.Fatalf("unexpected shard loads: %+v", metrics.ShardLoads)
	}
	if metrics.TopLevelCrossTxs != 1 || metrics.InternalCrossTxs != 1 || metrics.UnionCrossTxs != 1 {
		t.Fatalf("unexpected cross counts: %+v", metrics)
	}
	if metrics.MaxRequiredBlocks != 2 || metrics.ShardLoads[0].RequiredBlocks != 2 || metrics.ShardLoads[1].RequiredBlocks != 1 {
		t.Fatalf("unexpected required blocks: %+v", metrics.ShardLoads)
	}
}

func TestEvaluateCoLocationRemovesBothCrossCosts(t *testing.T) {
	g := New()
	if err := g.AddTransaction(sampleTransaction()); err != nil {
		t.Fatal(err)
	}
	metrics, err := Evaluate(g, model.Placement{"eoa-a": 1, "sca-b": 1, "sca-c": 1}, 2, 0.5, 100)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.CrossOverheadGas != 0 || metrics.UnionCrossTxs != 0 || metrics.TotalExecutedGas != 100 {
		t.Fatalf("co-located metrics: %+v", metrics)
	}
}

func TestMergeDecayRetainsWeightedHistory(t *testing.T) {
	history := New()
	if err := history.AddTransaction(sampleTransaction()); err != nil {
		t.Fatal(err)
	}
	window := New()
	if err := window.AddTransaction(sampleTransaction()); err != nil {
		t.Fatal(err)
	}
	merged, err := MergeDecay(history, window, 0.5)
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := Evaluate(merged, model.Placement{"eoa-a": 1, "sca-b": 1, "sca-c": 1}, 2, 0.5, 100)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(metrics.TotalLogicalTxs-1.5) > 1e-9 || math.Abs(metrics.BaseTransactionGas-150) > 1e-9 {
		t.Fatalf("decayed metrics = tx %.3f gas %.3f", metrics.TotalLogicalTxs, metrics.BaseTransactionGas)
	}
}

func TestLogicalAffinityCountsOneTransactionDespiteDuplicateCalls(t *testing.T) {
	g := New()
	tx := sampleTransaction()
	tx.Calls = append(tx.Calls, tx.Calls[0])
	if err := g.AddTransaction(tx); err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, edge := range g.Edges {
		total += edge.LogicalAffinity
	}
	if math.Abs(total-1) > 1e-9 {
		t.Fatalf("one logical transaction contributed affinity %.9f, want 1", total)
	}
}

func TestPendingTopLevelUsesPreexecutedGasWithoutCallTrace(t *testing.T) {
	g := New()
	if err := g.AddPendingTopLevel(model.PendingTxObservation{
		SourceHash: "pending", From: "eoa-a", To: "sca-b",
		GasUsed: 150, ToIsContract: true,
	}); err != nil {
		t.Fatal(err)
	}
	if len(g.Transactions) != 1 || len(g.Transactions[0].CallEdges) != 0 {
		t.Fatalf("pending observation leaked internal calls: %+v", g.Transactions)
	}
	if len(g.Edges) != 1 {
		t.Fatalf("pending graph has %d edges, want one top-level edge", len(g.Edges))
	}
	metrics, err := Evaluate(g, model.Placement{"eoa-a": 0, "sca-b": 1}, 2, 0.25, 100)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.BaseTransactionGas != 150 || metrics.TopLevelCrossTxs != 1 || metrics.InternalCrossTxs != 0 {
		t.Fatalf("unexpected pending metrics: %+v", metrics)
	}
}
