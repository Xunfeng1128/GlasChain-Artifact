package graph

import (
	"blockEmulator/glasfreight/model"
	"testing"
)

func TestAttachedGraphCostUsesOccurrencesAndEntryShard(t *testing.T) {
	g := New()
	tx := &model.Transaction{SourceHash: "t", From: "a", To: "b", GasUsed: 100000, HasGasUsed: true, AttachedCallModel: true, CallGasPerCall: 5000, ToIsContract: true,
		Calls: []model.Call{{Caller: "b", Callee: "c", IsCCGEdge: true, CallerIsSCA: true, CalleeIsSCA: true}, {Caller: "b", Callee: "c", IsCCGEdge: true, CallerIsSCA: true, CalleeIsSCA: true}}}
	if err := g.AddTransaction(tx); err != nil {
		t.Fatal(err)
	}
	m := model.Placement{"a": 0, "b": 0, "c": 1}
	for _, rho := range []float64{0, .25, 1} {
		metrics, err := Evaluate(g, m, 2, rho, 200000)
		if err != nil {
			t.Fatal(err)
		}
		if metrics.TotalExecutedGas != 110000 || metrics.ShardLoads[0].AttachedCallGas != 10000 || metrics.ShardLoads[1].TotalGas != 0 || metrics.CrossTxRatio != 1 {
			t.Fatalf("wrong billing: %+v", metrics)
		}
	}
	if g.Vertices["c"].ServiceGas(.25) != 0 || g.Vertices["b"].ServiceGas(.25) != 110000 {
		t.Fatal("service proxy charges remote callee instead of entry")
	}
	m["a"] = 1
	metrics, err := Evaluate(g, m, 2, .25, 200000)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.TotalExecutedGas != 210000 || metrics.ShardLoads[0].TotalGas != 110000 || metrics.ShardLoads[1].TotalGas != 100000 {
		t.Fatalf("wrong relay costs: %+v", metrics)
	}
	g2 := New()
	if err := g2.AddCarriedStage(model.StageObservation{Owner: "b", Kind: model.Relay2Stage, GasUsed: 100000, Weight: 1, AttachedCallModel: true, CallGasPerCall: 5000, CallEdges: g.Transactions[0].CallEdges}, true); err != nil {
		t.Fatal(err)
	}
	metrics, err = Evaluate(g2, m, 2, .25, 200000)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.TotalExecutedGas != 110000 || metrics.TotalLogicalTxs != 0 {
		t.Fatal("carry replayed top or lost attached cost")
	}
}
