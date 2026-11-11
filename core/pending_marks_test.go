package core

import "testing"

func TestFullPendingWindowMarksStayDormantThroughForecastBlock(t *testing.T) {
	pool := NewTxPool()
	top := windowPoolTx("top", "sender", 30, false)
	relay := windowPoolTx("relay", "relay-sender", 30, true)
	tail := windowPoolTx("tail", "tail-sender", 30, false)
	top.Nonce = 42
	relay.GLASCallRelations = []GLASCallRelation{{Caller: "caller", Callee: "callee"}}
	pool.AddTxs2Pool([]*Transaction{relay, top, tail})
	if err := pool.MergePendingMarks([]*Transaction{top}, &PendingMarks{Flags: []byte{pendingMarkNext}}); err != nil {
		t.Fatal(err)
	}
	if err := pool.ActivatePendingMarks(1); err != nil {
		t.Fatal(err)
	}
	resolve := func(address string) uint64 {
		if address == top.Recipient {
			return 1
		}
		return 0
	}
	forecastCrossTOP, window, forecast, err := pool.CaptureFullPendingWindow(2, 30, 30, 3, resolve)
	if err != nil {
		t.Fatal(err)
	}
	assertWindowSources(t, forecastCrossTOP, "top")
	assertWindowSources(t, window.Records, "relay")
	if len(forecast) != 1 || forecast[0].Nonce != top.Nonce || window.FullRecords != 2 || window.FullCallRelations != 1 {
		t.Fatalf("bad capture metadata: forecast=%v window=%+v", forecast, window)
	}
	if forecastCrossTOP[0] == top || window.Records[0] == relay ||
		&window.Records[0].GLASCallRelations[0] == &relay.GLASCallRelations[0] {
		t.Fatal("capture shared selected transaction bodies with live pool")
	}
	batch := []*Transaction{relay, top, tail}
	marks := pool.ExportPendingMarks(batch)
	if marks == nil || marks.Epoch != 1 || marks.Flags[0] != pendingMarkNext ||
		marks.Flags[1] != pendingMarkCurrent|pendingMarkNext || marks.Flags[2] != 0 {
		t.Fatalf("wrong next/current marks: %+v", marks)
	}
	block := pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 30})
	assertWindowSources(t, block, "top")
	if err := pool.CommitUnifiedExecutionRecords(block, 30); err != nil {
		t.Fatal(err)
	}
	descendant := windowPoolTx(top.SourceTxHash, top.Sender, 30, true)
	marks = pool.ExportPendingMarks([]*Transaction{descendant})
	if marks == nil || marks.Flags[0] != pendingMarkCurrent|pendingMarkNext {
		t.Fatalf("committed TOP's real Relay2 lost its marks: %+v", marks)
	}
}

func TestPendingMarksPromoteDeduplicatedAndLateRecordsInPlace(t *testing.T) {
	pool := NewTxPool()
	ordinary := windowPoolTx("ordinary", "ordinary-sender", 30, true)
	migrated := windowPoolTx("migrated", "migrated-sender", 30, false)
	txs := []*Transaction{ordinary, migrated}
	pool.AddTxs2Pool(txs)
	if err := pool.ActivatePendingMarks(1); err != nil {
		t.Fatal(err)
	}
	index := pool.planningWindow
	duplicate := *migrated
	if added, err := pool.AddTxs2PoolUniqueByExecutionID([]*Transaction{&duplicate}); err != nil || added != 0 {
		t.Fatalf("duplicate migration admitted: added=%d err=%v", added, err)
	}
	// Sender is still on epoch zero. Its next-epoch mark belongs to our
	// already active epoch one and must promote the existing pool record.
	marks := &PendingMarks{Flags: []byte{pendingMarkNext}}
	if err := pool.MergePendingMarks([]*Transaction{&duplicate}, marks); err != nil {
		t.Fatal(err)
	}
	if err := pool.MergePendingMarks([]*Transaction{&duplicate}, marks); err != nil {
		t.Fatal(err)
	}
	assertWindowSources(t, pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 60}), "migrated", "ordinary")
	if pool.planningWindow != index || index.dirty || pool.ActivePendingMarkCount() != 1 ||
		index.groups[planningWindowKey(migrated)].selected != 1 {
		t.Fatal("duplicate mark rebuilt or corrupted the live index")
	}
	if err := pool.ActivatePendingMarks(2); err != nil {
		t.Fatal(err)
	}
	if err := pool.MergePendingMarks([]*Transaction{&duplicate}, marks); err != nil {
		t.Fatal(err)
	}
	assertWindowSources(t, pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 60}), "ordinary", "migrated")
	if pool.ExportPendingMarks(txs) != nil {
		t.Fatal("retired marks survived activation")
	}
}
