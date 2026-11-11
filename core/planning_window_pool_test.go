package core

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func windowPoolTx(source, sender string, gas uint64, relayed bool) *Transaction {
	tx := testTxWithGas(1, gas)
	tx.SourceTxHash, tx.Sender, tx.Recipient = source, sender, "recipient-"+source
	tx.GLASBaseGas, tx.Relayed = gas, relayed
	return tx
}

func assertWindowSources(t *testing.T, got []*Transaction, want ...string) {
	t.Helper()
	sources := make([]string, len(got))
	for i, tx := range got {
		sources[i] = tx.SourceTxHash
	}
	if !reflect.DeepEqual(sources, want) {
		t.Fatalf("selected sources=%v want=%v", sources, want)
	}
}

func TestPreviewOmitRetainedPreservesSelection(t *testing.T) {
	queue := []*Transaction{windowPoolTx("a", "alice", 60, false), windowPoolTx("b", "alice", 40, false), windowPoolTx("c", "bob", 30, true)}
	options := TxSelectionOptions{MaxGas: 100, PriorityLevel: ExecutionPriority, LevelCount: 2, DependencyKey: ExecutionDependency}
	old := SelectTransactions(queue, options)
	options.OmitRetained = true
	fast := SelectTransactions(queue, options)
	if fast.Retained != nil || !reflect.DeepEqual(old.Packed, fast.Packed) || !reflect.DeepEqual(old.SelectedIndices, fast.SelectedIndices) || old.GasUsed != fast.GasUsed {
		t.Fatalf("preview changed selection: old=%+v fast=%+v", old, fast)
	}
	pool := NewTxPool()
	pool.AddTxs2Pool(queue)
	pool.PackTxsWithSelection(options)
	if !reflect.DeepEqual(pool.TxQueue, old.Retained) {
		t.Fatal("destructive packing incorrectly honored OmitRetained")
	}
}

func TestWindowPoolFourLevelsAndReusablePreview(t *testing.T) {
	pool := NewTxPool()
	pool.AddTxs2Pool([]*Transaction{
		windowPoolTx("planned-top", "alice", 10, false),
		windowPoolTx("other-top", "bob", 10, false),
		windowPoolTx("planned-relay", "carol", 10, true),
		windowPoolTx("other-relay", "dave", 10, true),
	})
	if err := pool.InstallPlanningWindow([]string{"planned-top", "planned-relay"}); err != nil {
		t.Fatal(err)
	}
	index := pool.planningWindow
	for i := 0; i < 2; i++ {
		assertWindowSources(t, pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 100}), "planned-relay", "planned-top", "other-relay", "other-top")
		if len(pool.TxQueue) != 4 || index != pool.planningWindow {
			t.Fatal("preview mutated pool or rebuilt reusable index")
		}
	}
	block := pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 20})
	assertWindowSources(t, block, "planned-relay", "planned-top")
	if err := pool.CommitUnifiedExecutionRecords(block, 20); err != nil {
		t.Fatal(err)
	}
	if pool.planningWindow != index || index.dirty || len(pool.TxQueue) != 2 {
		t.Fatal("ordinary prefix commit did not incrementally update index")
	}
	assertWindowSources(t, pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 100}), "other-relay", "other-top")
}

func TestWindowPoolGlobalSenderFIFOAndNormalFallback(t *testing.T) {
	pool := NewTxPool()
	pool.AddTxs2Pool([]*Transaction{
		windowPoolTx("predecessor", "alice", 30, false),
		windowPoolTx("planned", "alice", 40, false),
		windowPoolTx("relay", "bob", 10, true),
		windowPoolTx("ordinary", "carol", 10, false),
	})
	if err := pool.InstallPlanningWindow([]string{"planned"}); err != nil {
		t.Fatal(err)
	}
	// The ordinary predecessor first unlocks the selected TOP, which must then
	// be considered before the remaining ordinary TOP.
	assertWindowSources(t, pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 100}), "relay", "predecessor", "planned", "ordinary")
	if err := pool.InstallPlanningWindow([]string{"predecessor", "planned"}); err != nil {
		t.Fatal(err)
	}
	assertWindowSources(t, pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 60}), "predecessor", "relay", "ordinary")
}

func TestWindowPoolLateRelayAndNewEpochReplaceMembership(t *testing.T) {
	pool := NewTxPool()
	top := windowPoolTx("planned", "alice", 30, false)
	if _, err := pool.AddTxs2PoolUniqueByExecutionID([]*Transaction{top}); err != nil {
		t.Fatal(err)
	}
	if err := pool.InstallPlanningWindow([]string{"planned"}); err != nil {
		t.Fatal(err)
	}
	if err := pool.CommitUnifiedExecutionRecords([]*Transaction{top}, 100); err != nil {
		t.Fatal(err)
	}
	fresh, otherRelay := windowPoolTx("fresh", "bob", 10, false), windowPoolTx("other-relay", "carol", 10, true)
	relay := windowPoolTx("planned", "alice", 40, true)
	if _, err := pool.AddTxs2PoolUniqueByExecutionID([]*Transaction{fresh, otherRelay, relay}); err != nil {
		t.Fatal(err)
	}
	assertWindowSources(t, pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 100}), "planned", "other-relay", "fresh")
	if err := pool.InstallPlanningWindow([]string{"fresh"}); err != nil {
		t.Fatal(err)
	}
	assertWindowSources(t, pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 100}), "fresh", "other-relay", "planned")
	pool.ClearPlanningWindow()
	assertWindowSources(t, pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 100}), "other-relay", "planned", "fresh")
}

func TestWindowPoolReferencesRefreshAfterRepriceAndCatchUp(t *testing.T) {
	pool := NewTxPool()
	a, b := windowPoolTx("a", "alice", 10, false), windowPoolTx("b", "alice", 10, false)
	pool.AddTxs2Pool([]*Transaction{a, b})
	if err := pool.InstallPlanningWindow([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	// A catch-up block can contain a non-head record in this node's pool.
	if err := pool.CommitUnifiedExecutionRecords([]*Transaction{b}, 100); err != nil {
		t.Fatal(err)
	}
	if !pool.planningWindow.dirty {
		t.Fatal("non-prefix catch-up did not invalidate cached group")
	}
	assertWindowSources(t, pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 100}), "a")
	copyA := *a
	copyA.GasUsed = 80
	if err := pool.ReplaceTransactions(func(*Transaction) bool { return true }, []*Transaction{&copyA}, nil); err != nil {
		t.Fatal(err)
	}
	got := pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 100})
	if len(got) != 1 || got[0] != &copyA || got[0].GasUsed != 80 {
		t.Fatal("preview retained stale pre-repricing record")
	}
}

func TestUnifiedCommitMissingRecordsAndDelayedReplay(t *testing.T) {
	pool := NewTxPool()
	present, retained := windowPoolTx("present", "alice", 10, false), windowPoolTx("retained", "bob", 20, false)
	missing := windowPoolTx("missing", "carol", 30, true)
	pool.AddTxs2Pool([]*Transaction{present, retained})
	beforeMissing := *missing
	if err := pool.CommitUnifiedExecutionRecords([]*Transaction{present, missing}, 100); err != nil {
		t.Fatal(err)
	}
	assertWindowSources(t, pool.TxQueue, "retained")
	if !reflect.DeepEqual(*missing, beforeMissing) {
		t.Fatal("missing block record was enqueued or timestamps were changed")
	}
	if n, err := pool.AddTxs2PoolUniqueByExecutionID([]*Transaction{present, missing}); err != nil || n != 0 {
		t.Fatalf("delayed execution replay accepted: n=%d err=%v", n, err)
	}
	if err := pool.CommitUnifiedExecutionRecords([]*Transaction{missing}, 100); !errors.Is(err, ErrExecutionAlreadyCommitted) {
		t.Fatalf("repeated commit accepted: %v", err)
	}
}

func TestUnifiedCommitFailuresAreAtomic(t *testing.T) {
	for _, scenario := range []string{"conflict", "duplicate_block", "duplicate_pool", "gas"} {
		t.Run(scenario, func(t *testing.T) {
			pool := NewTxPool()
			a, b := windowPoolTx("a", "alice", 10, false), windowPoolTx("b", "bob", 20, false)
			pool.AddTxs2Pool([]*Transaction{a, b})
			block := []*Transaction{a, b}
			limit := uint64(100)
			switch scenario {
			case "conflict":
				changed := *b
				changed.GasUsed++
				block = []*Transaction{a, &changed}
			case "duplicate_block":
				block = []*Transaction{a, a}
			case "duplicate_pool":
				pool.AddTx2Pool(a)
			case "gas":
				limit = 25
			}
			before := append([]*Transaction(nil), pool.TxQueue...)
			beforeA, beforeB := *a, *b
			if err := pool.CommitUnifiedExecutionRecords(block, limit); err == nil {
				t.Fatal("invalid commit accepted")
			}
			if !reflect.DeepEqual(before, pool.TxQueue) || !reflect.DeepEqual(beforeA, *a) || !reflect.DeepEqual(beforeB, *b) || len(pool.committedExecutionIDs) != 0 {
				t.Fatal("failed commit changed queue, timestamps or tombstones")
			}
		})
	}
}

func TestWindowIndexMatchesFourTierSelectorAcrossBlocks(t *testing.T) {
	rng := rand.New(rand.NewSource(4411))
	for trial := 0; trial < 100; trial++ {
		pool := NewTxPool()
		sources := make([]string, 0)
		membership := make(map[string]bool)
		for i := 0; i < 100; i++ {
			source := fmt.Sprintf("t%d-%d", trial, i)
			tx := windowPoolTx(source, fmt.Sprintf("sender-%d", rng.Intn(15)), uint64(1+rng.Intn(70)), rng.Intn(4) == 0)
			pool.AddTx2Pool(tx)
			if rng.Intn(3) == 0 {
				sources = append(sources, source)
				membership[source] = true
			}
		}
		if err := pool.InstallPlanningWindow(sources); err != nil {
			t.Fatal(err)
		}
		for len(pool.TxQueue) != 0 {
			options := TxSelectionOptions{MaxGas: 100, LevelCount: 4, DependencyKey: ExecutionDependency, PriorityLevel: func(tx *Transaction) int {
				level := ExecutionPriority(tx)
				if !membership[tx.SourceTxHash] {
					level += 2
				}
				return level
			}}
			want := SelectTransactions(pool.TxQueue, options).Packed
			got := pool.PeekWindowTransactions(options)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("trial %d changed four-level selection", trial)
			}
			if len(got) == 0 {
				t.Fatal("positive valid records failed to make progress")
			}
			if err := pool.CommitUnifiedExecutionRecords(got, 100); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestPlanningWindowFailedBuildPreservesInstalledIndex(t *testing.T) {
	for _, scenario := range []string{"nil", "duplicate"} {
		t.Run(scenario, func(t *testing.T) {
			pool := NewTxPool()
			a := windowPoolTx("a", "alice", 10, false)
			pool.TxQueue = []*Transaction{a}
			if err := pool.InstallPlanningWindow([]string{"a"}); err != nil {
				t.Fatal(err)
			}
			previous := pool.planningWindow
			wantErr := ErrExecutionIDNotUnique
			bad := a
			if scenario == "nil" {
				bad, wantErr = nil, ErrExecutionIDConflict
			}
			pool.TxQueue = append(pool.TxQueue, windowPoolTx("b", "bob", 20, true), bad)
			if err := pool.InstallPlanningWindow([]string{"b"}); !errors.Is(err, wantErr) {
				t.Fatalf("invalid queue accepted: %v", err)
			}
			if pool.planningWindow != previous || previous.nextOrder != 1 || previous.dirty || len(previous.sources) != 1 {
				t.Fatal("partially built index replaced or changed the installed index")
			}
			if _, ok := previous.sources["a"]; !ok {
				t.Fatal("failed installation published new membership")
			}
		})
	}
}

func TestPlanningWindowRefreshesRepricedInstallationReferences(t *testing.T) {
	pool := NewTxPool()
	a, b := windowPoolTx("a", "alice", 10, false), windowPoolTx("b", "alice", 10, false)
	pool.TxQueue = []*Transaction{a, b}
	if err := pool.InstallPlanningWindow([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	previous := pool.planningWindow
	prepare := func(tx *Transaction) (*Transaction, error) {
		prepared, _ := copyMigrationPoolRecord(tx)
		if prepared.SourceTxHash == "a" {
			prepared.GasUsed = 80
		}
		return prepared, nil
	}
	if _, err := pool.InstallRepricedExecutionRecords(nil, prepare, copyMigrationPoolRecord); err != nil {
		t.Fatal(err)
	}
	if !previous.dirty || a.GasUsed != 10 || b.GasUsed != 10 {
		t.Fatal("repricing left a stale usable index or mutated receipt records")
	}
	if got := pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 20}); len(got) != 0 {
		t.Fatal("window selection used old prices or bypassed the sender predecessor")
	}
	got := pool.PeekWindowTransactions(TxSelectionOptions{MaxGas: 100})
	assertWindowSources(t, got, "a", "b")
	if got[0] != pool.TxQueue[0] || got[1] != pool.TxQueue[1] || got[0] == a || pool.planningWindow == previous {
		t.Fatal("window rebuild did not bind current queue objects in order")
	}
}
