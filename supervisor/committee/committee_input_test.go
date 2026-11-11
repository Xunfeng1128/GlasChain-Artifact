package committee

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/partition/plouvain"
	plouvaincontrib "blockEmulator/partition/plouvain_contrib"
	"blockEmulator/partition/txallo"
	"blockEmulator/supervisor/signal"
)

// A stop decision is honored between batches. Already-started PBFT and
// migration handling is intentionally outside this decision point.
func TestE2StopBeforeNextSharedOrRelayDispatch(t *testing.T) {
	configureUnifiedInput(t)
	oldPrefix := params.E2PrefixCount
	params.E2PrefixCount = 1
	t.Cleanup(func() { params.E2PrefixCount = oldPrefix })
	stop := signal.NewStopSignal(2)
	stop.RequestE2Stop()
	clpa := NewCLPACommitteeModule(nil, stop, nil, "unused", 2, 1, 48)
	recorder, _ := newInjectionTestRecorder(t)
	clpa.SetUnifiedRecorder(recorder)
	clpa.MsgSendingControl()
	relay := NewRelayCommitteeModule(nil, stop, nil, "unused", 2, 1)
	relay.SetUnifiedRecorder(recorder)
	relay.MsgSendingControl()
	if progress := recorder.CompletionProgress(); progress.Injected != 0 || !clpa.clpaLastRunningTime.IsZero() {
		t.Fatalf("sender opened a batch or planning round after stop: %+v", progress)
	}
}

func unifiedInputFixture(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	txPath, callPath := filepath.Join(dir, "transactions.csv"), filepath.Join(dir, "contract_calls.csv")
	txs := `blockNumber,transactionIndex,transactionHash,from,to,toCreate,value,gasUsed,receiptStatus,fromIsContract,toIsContract,txCategory
1,0,0x1111111111111111111111111111111111111111111111111111111111111111,0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,,0,40,0,false,true,inter_contract_call
1,1,0x2222222222222222222222222222222222222222222222222222222222222222,0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,,0,40,1,false,true,inter_contract_call
1,2,0x3333333333333333333333333333333333333333333333333333333333333333,0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,,0,20,1,false,false,transfer
`
	calls := `blockNumber,transactionIndex,transactionHash,traceAddress,callDepth,caller,callee,callType,callGasUsed,error,txStatus,callerIsContract,calleeIsContract,isCCGEdge
1,1,0x2222222222222222222222222222222222222222222222222222222222222222,0,1,0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,0xcccccccccccccccccccccccccccccccccccccccc,call,10,,1,true,true,true
`
	if err := os.WriteFile(txPath, []byte(txs), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(callPath, []byte(calls), 0600); err != nil {
		t.Fatal(err)
	}
	return txPath, callPath
}

func configureUnifiedInput(t *testing.T) {
	t.Helper()
	oldEnabled, oldTx, oldCalls, oldGas, oldLimit, oldSpeed := params.UnifiedExecution, params.UnifiedTransactionsFile, params.UnifiedCallsFile, params.UnifiedCrossCallGas, params.BlockGasLimit, params.InjectSpeed
	t.Cleanup(func() {
		params.UnifiedExecution, params.UnifiedTransactionsFile, params.UnifiedCallsFile, params.UnifiedCrossCallGas, params.BlockGasLimit, params.InjectSpeed = oldEnabled, oldTx, oldCalls, oldGas, oldLimit, oldSpeed
	})
	params.UnifiedExecution = true
	params.UnifiedTransactionsFile, params.UnifiedCallsFile = unifiedInputFixture(t)
	params.UnifiedCrossCallGas, params.BlockGasLimit, params.InjectSpeed = 5, 100, 2
}

func TestUnifiedInputAdmitsAndPricesSharedWork(t *testing.T) {
	configureUnifiedInput(t)
	a, b, c := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	mapping := map[string]uint64{a: 0, b: 0, c: 1}
	input, err := openCommitteeInput("ignored", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	for i, want := range []uint64{45, 20} {
		tx, err := input.Next(uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		if err := priceCommitteeInput(tx, func(address string) uint64 { return mapping[address] }); err != nil {
			t.Fatal(err)
		}
		if tx.GasUsed != want || tx.SourceTxHash == "" || tx.Sender != a || tx.Recipient != b {
			t.Fatalf("wrong shared input record: %+v", tx)
		}
	}
	if _, err := input.Next(2); err != io.EOF {
		t.Fatalf("expected exact EOF: %v", err)
	}
}

func TestUnifiedInputFailsInsteadOfSilentlyShorteningWorkload(t *testing.T) {
	configureUnifiedInput(t)
	r, err := openCommitteeInput("ignored", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i := 0; i < 2; i++ {
		if _, err := r.Next(uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Next(2); err == nil || !strings.Contains(err.Error(), "requested 3") {
		t.Fatalf("short input hidden: %v", err)
	}

}

func TestLegacyInputAndNoopPricingRemainAvailable(t *testing.T) {
	old := params.UnifiedExecution
	params.UnifiedExecution = false
	t.Cleanup(func() { params.UnifiedExecution = old })
	p := filepath.Join(t.TempDir(), "legacy.csv")
	data := "x,x,x,0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,x,0,1,0,x,x,40\n" +
		"x,x,x,0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,x,0,0,0,x,x,40\n"
	if err := os.WriteFile(p, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := openCommitteeInput(p, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	tx, err := r.Next(0)
	if err != nil {
		t.Fatal(err)
	}
	if tx.GasUsed != 40 || tx.GLASBaseGas != 0 || tx.SourceTxHash != "" {
		t.Fatalf("legacy metadata changed: %+v", tx)
	}
	if err := priceCommitteeInput(tx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(1); err != io.EOF {
		t.Fatalf("contract filter changed: %v", err)
	}
}

func TestBaselineGraphUnitsDoNotInheritExecutionSurcharge(t *testing.T) {
	a, b, c := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	tx := &core.Transaction{Sender: a, Recipient: b, GasUsed: 90, GLASBaseGas: 40, GLASCallGasPerCall: 50, GLASAttachedGas: 50, GLASCallRelations: []core.GLASCallRelation{{Caller: b, Callee: c}}}
	for _, mode := range []string{"tx", "gas"} {
		graph := new(partition.CLPAState)
		graph.Init_CLPAState(0.5, 100, params.ShardNum)
		m := &CLPACommitteeModule{clpaGraph: graph, clpaWeightMode: mode}
		m.addCLPAEdge(tx)
		want := uint64(1)
		if mode == "gas" {
			want = 40
		}
		if got := graph.NetGraph.EdgeWeight[partition.Vertex{Addr: a}][partition.Vertex{Addr: b}]; got != want {
			t.Fatalf("CLPA %s weight=%d want %d", mode, got, want)
		}
		if len(graph.NetGraph.VertexSet) != 2 {
			t.Fatal("internal call leaked into CLPA graph")
		}
	}
	gas := &PLouvainGasCommitteeModule{graph: plouvain.NewGraph()}
	gas.addGraphEdge(tx)
	count := &PLouvainContribCommitteeModule{graph: plouvaincontrib.NewGraph()}
	count.addGraphEdge(tx)
	txa := &TxAlloContribCommitteeModule{graph: txallo.NewGraph()}
	txa.addGraphEdge(tx)
	if gas.graph.LogicalGas() != 40 || gas.graph.VertexCount() != 2 || count.graph.LogicalTransactions() != 1 || count.graph.VertexCount() != 2 || txa.graph.LogicalTransactions() != 1 || txa.graph.VertexCount() != 2 {
		t.Fatal("execution calls or charges changed baseline graph units")
	}
}
