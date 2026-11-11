package chain

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"blockEmulator/core"
)

func migrationTestTx(id string, relayed bool, base uint64) *core.Transaction {
	tx := unifiedTestTx(id, "a", base)
	tx.Relayed = relayed
	tx.GLASCallRelations = []core.GLASCallRelation{{Caller: "c", Callee: "d"}, {Caller: "c", Callee: "d"}}
	// Fixed lifecycle times let the test compare the entire priced record.
	tx.Broker1EnterPoolTime, tx.Broker2EnterPoolTime = time.Unix(2, 0), time.Unix(3, 0)
	tx.GLASStageEpoch, tx.GLASStageEpochSet = 7, true
	return tx
}

func TestUnifiedMigrationPricesEachRecordOnceUsingSharedSnapshot(t *testing.T) {
	enableUnifiedTest(t)
	for _, tc := range []struct {
		name                                string
		relayed, crossTop, callsBecomeLocal bool
		base                                uint64
	}{
		{name: "local TOP", base: 40},
		{name: "cross TOP", crossTop: true, base: 40},
		{name: "local RELAY2", relayed: true, base: 40},
		{name: "cross RELAY2", relayed: true, crossTop: true, base: 40},
		{name: "internal callee moves", callsBecomeLocal: true, base: 40},
		{name: "surcharge waits above capacity", base: 80},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bc := unifiedTestChain()
			if tc.crossTop {
				bc.PartitionMap["recipient"] = 1
			}
			if tc.callsBecomeLocal {
				bc.PartitionMap["d"] = 1
			}
			existing, err := bc.PrepareUnifiedTransactions([]*core.Transaction{migrationTestTx("existing", tc.relayed, tc.base)})
			if err != nil {
				t.Fatal(err)
			}
			bc.Txpool.AddTxs2Pool(existing)
			oldReceipt := *existing[0]
			oldCalls := append([]core.GLASCallRelation(nil), existing[0].GLASCallRelations...)
			if tc.callsBecomeLocal {
				bc.Update_PartitionMap("d", 0)
			} else {
				bc.Update_PartitionMap("d", 1)
			}
			snapshot := bc.ExecutionShardResolver()
			incoming := migrationTestTx("incoming", tc.relayed, tc.base)
			incomingBefore := *incoming
			want, err := prepareUnifiedTransactions([]*core.Transaction{existing[0], incoming}, snapshot, 100)
			if err != nil {
				t.Fatal(err)
			}
			// The supplied snapshot must be used by retained pricing, incoming
			// pricing, and forwarding even if a newer map is now available.
			bc.Update_PartitionMap("d", 1-snapshot("d"))
			lookups := 0
			resolve := func(address string) uint64 {
				lookups++
				if !bc.pmlock.TryLock() {
					t.Error("pricing callback held the chain map lock")
				} else {
					bc.pmlock.Unlock()
				}
				return snapshot(address)
			}
			if err := bc.InstallUnifiedMigrationWithResolver([]*core.Transaction{incoming}, resolve, true); err != nil {
				t.Fatal(err)
			}
			perRecord := 2 * len(incoming.GLASCallRelations)
			if !tc.relayed {
				perRecord += 2
			}
			if lookups != 2*perRecord {
				t.Fatalf("pricing repeated resolver work: lookups=%d want=%d", lookups, 2*perRecord)
			}
			if !reflect.DeepEqual(bc.Txpool.TxQueue, want) {
				t.Fatalf("migration diverged from legacy pricing: got=%+v want=%+v", bc.Txpool.TxQueue, want)
			}
			for i, source := range []*core.Transaction{existing[0], incoming} {
				got := bc.Txpool.TxQueue[i]
				if got == source || &got.GLASCallRelations[0] == &source.GLASCallRelations[0] {
					t.Fatal("migration reused received or historical call storage")
				}
				if err := core.ValidateExecutionAdmissionGas(got, snapshot, 100); err != nil {
					t.Fatal(err)
				}
			}
			forwarded, err := bc.PrepareUnifiedTransactionsWithResolver([]*core.Transaction{incoming}, resolve)
			if err != nil || !reflect.DeepEqual(forwarded, want[1:]) || forwarded[0] == incoming {
				t.Fatalf("forwarded pricing differs from installed snapshot: %v %+v", err, forwarded)
			}
			if lookups != 3*perRecord {
				t.Fatalf("forwarding repeated resolver work: lookups=%d want=%d", lookups, 3*perRecord)
			}
			bc.Txpool.TxQueue[0].GLASCallRelations[0].Callee = "changed"
			if !reflect.DeepEqual(*existing[0], oldReceipt) || !reflect.DeepEqual(existing[0].GLASCallRelations, oldCalls) || !reflect.DeepEqual(*incoming, incomingBefore) {
				t.Fatal("migration changed an earlier receipt or received record")
			}
		})
	}
}

func TestUnifiedMigrationNoopRetainsObjectsButPricesIncoming(t *testing.T) {
	enableUnifiedTest(t)
	bc := unifiedTestChain()
	bc.PartitionMap["d"] = 1
	existing, err := bc.PrepareUnifiedTransactions([]*core.Transaction{migrationTestTx("existing", false, 40)})
	if err != nil {
		t.Fatal(err)
	}
	bc.Txpool.AddTxs2Pool(existing)
	before := *existing[0]
	if err := bc.InstallUnifiedMigrationWithResolver(nil, func(string) uint64 {
		t.Fatal("unchanged retained record was repriced")
		return 0
	}, false); err != nil {
		t.Fatal(err)
	}
	if bc.Txpool.TxQueue[0] != existing[0] || !reflect.DeepEqual(*existing[0], before) {
		t.Fatal("empty migration changed retained object identity or receipt")
	}
	lookups := 0
	snapshot := bc.ExecutionShardResolver()
	incoming := migrationTestTx("incoming", true, 40)
	if err := bc.InstallUnifiedMigrationWithResolver([]*core.Transaction{incoming}, func(address string) uint64 {
		lookups++
		return snapshot(address)
	}, false); err != nil {
		t.Fatal(err)
	}
	if lookups != 4 || bc.Txpool.TxQueue[0] != existing[0] || bc.Txpool.TxQueue[1] == incoming || bc.Txpool.TxQueue[1].GasUsed != 80 {
		t.Fatalf("no-op must only price an owned incoming copy: lookups=%d queue=%+v", lookups, bc.Txpool.TxQueue)
	}
	bc.Txpool.TxQueue[1].GLASCallRelations[0].Callee = "changed"
	if incoming.GasUsed != 0 || incoming.GLASCallRelations[0].Callee != "d" || !reflect.DeepEqual(*existing[0], before) {
		t.Fatal("no-op incoming preparation mutated input or retained receipt")
	}
}

func TestUnifiedMigrationPreparationErrorsMatchLegacyAndLeavePoolUntouched(t *testing.T) {
	enableUnifiedTest(t)
	for _, tc := range []struct {
		name   string
		change func(*core.Transaction) *core.Transaction
		text   string
	}{
		{"nil", func(*core.Transaction) *core.Transaction { return nil }, "invalid unified execution record"},
		{"empty endpoint", func(tx *core.Transaction) *core.Transaction { tx.GLASCallRelations[0].Caller = ""; return tx }, "empty call endpoint"},
		{"wrong unit", func(tx *core.Transaction) *core.Transaction { tx.GLASCallGasPerCall++; return tx }, "cross-call unit cost"},
		{"overflow before base cap", func(tx *core.Transaction) *core.Transaction { tx.GLASBaseGas = math.MaxUint64 - 10; return tx }, "attached gas overflow"},
		{"base cap", func(tx *core.Transaction) *core.Transaction { tx.GLASBaseGas = 101; return tx }, "baseGas=101 exceeds"},
	} {
		for _, position := range []string{"retained", "incoming", "forwarded"} {
			t.Run(tc.name+"/"+position, func(t *testing.T) {
				bc := unifiedTestChain()
				bc.PartitionMap["d"] = 1
				bad := tc.change(migrationTestTx("bad", false, 40))
				resolve := bc.ExecutionShardResolver()
				_, wantErr := prepareUnifiedTransactions([]*core.Transaction{bad}, resolve, 100)
				if wantErr == nil || !strings.Contains(wantErr.Error(), tc.text) {
					t.Fatalf("bad legacy fixture: %v", wantErr)
				}
				existing, err := bc.PrepareUnifiedTransactions([]*core.Transaction{migrationTestTx("existing", false, 40)})
				if err != nil {
					t.Fatal(err)
				}
				bc.Txpool.AddTxs2Pool(existing)
				incoming := []*core.Transaction{bad}
				if position == "retained" {
					bc.Txpool.TxQueue = append(bc.Txpool.TxQueue, bad)
					incoming = []*core.Transaction{migrationTestTx("incoming", false, 40)}
				}
				before := append([]*core.Transaction(nil), bc.Txpool.TxQueue...)
				receipt := *existing[0]
				if position == "forwarded" {
					_, err = bc.PrepareUnifiedTransactionsWithResolver(incoming, resolve)
				} else {
					err = bc.InstallUnifiedMigrationWithResolver(incoming, resolve, true)
				}
				if err == nil || err.Error() != wantErr.Error() {
					t.Fatalf("error order changed: got=%v want=%v", err, wantErr)
				}
				if !reflect.DeepEqual(bc.Txpool.TxQueue, before) || bc.Txpool.TxQueue[0] != existing[0] || !reflect.DeepEqual(*existing[0], receipt) {
					t.Fatal("failed preparation changed pool or previous receipt")
				}
			})
		}
	}
}

func TestUnifiedMigrationNoopStillChecksStructureAndBaseCapacity(t *testing.T) {
	enableUnifiedTest(t)
	for _, change := range []func(*core.Transaction){
		func(tx *core.Transaction) { tx.SourceTxHash = "" },
		func(tx *core.Transaction) { tx.GLASCallRelations[0].Callee = "" },
		func(tx *core.Transaction) { tx.GLASCallGasPerCall++ },
		func(tx *core.Transaction) { tx.GLASBaseGas = 101 },
	} {
		bc := unifiedTestChain()
		tx := migrationTestTx("existing", false, 40)
		change(tx)
		bc.Txpool.TxQueue = []*core.Transaction{tx}
		if err := bc.InstallUnifiedMigrationWithResolver(nil, func(string) uint64 {
			t.Fatal("no-op validation repriced a record")
			return 0
		}, false); err == nil {
			t.Fatal("no-op skipped structural or base-capacity validation")
		}
		if bc.Txpool.TxQueue[0] != tx {
			t.Fatal("no-op validation failure replaced input")
		}
	}
}

func TestGARETUnifiedMigrationUsesGroupSnapshotAndPreservesReceipts(t *testing.T) {
	enableUnifiedTest(t)
	bc, catalog := garetRoutingFixture(t)
	tx := unifiedTestTx("existing", garetAddress(10), 40)
	tx.Recipient = garetAddress(20)
	tx.GLASCallRelations = []core.GLASCallRelation{{Caller: garetAddress(20), Callee: garetAddress(35)}}
	existing, err := bc.PrepareUnifiedTransactions([]*core.Transaction{tx})
	if err != nil {
		t.Fatal(err)
	}
	bc.Txpool.AddTxs2Pool(existing)
	if err := bc.InstallGARETPlacement(catalog.Digest(), 1, []uint64{0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	snapshot := bc.ExecutionShardResolver()
	incoming := *tx
	incoming.SourceTxHash = "incoming"
	incoming.Relayed = true
	if err := bc.InstallUnifiedMigrationWithResolver([]*core.Transaction{&incoming}, snapshot, true); err != nil {
		t.Fatal(err)
	}
	if len(bc.PartitionMap) != 0 || len(bc.Txpool.TxQueue) != 2 || existing[0].GasUsed != 60 || incoming.GasUsed != 0 {
		t.Fatal("group migration changed account overrides or source receipts")
	}
	for _, priced := range bc.Txpool.TxQueue {
		if priced.GasUsed != 40 || priced.GLASCrossCallCount != 0 {
			t.Fatal("group-only migration did not reprice attached calls")
		}
		if err := core.ValidateExecutionAdmissionGas(priced, snapshot, 100); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGARETUnifiedMigrationValidatesWholeIncomingBatchBeforePricing(t *testing.T) {
	enableUnifiedTest(t)
	for _, malformed := range []string{"sender", "recipient", "caller", "callee", "nil"} {
		t.Run(malformed, func(t *testing.T) {
			bc, _ := garetRoutingFixture(t)
			tx := unifiedTestTx("existing", garetAddress(10), 40)
			tx.Recipient = garetAddress(20)
			tx.GLASCallRelations = []core.GLASCallRelation{{Caller: garetAddress(20), Callee: garetAddress(35)}}
			existing, err := bc.PrepareUnifiedTransactions([]*core.Transaction{tx})
			if err != nil {
				t.Fatal(err)
			}
			bc.Txpool.AddTxs2Pool(existing)
			bad := *tx
			bad.GLASCallRelations = append([]core.GLASCallRelation(nil), tx.GLASCallRelations...)
			malformedTx := &bad
			switch malformed {
			case "sender":
				bad.Sender = "malformed"
			case "recipient":
				bad.Recipient = "malformed"
			case "caller":
				bad.GLASCallRelations[0].Caller = "malformed"
			case "callee":
				bad.GLASCallRelations[0].Callee = "malformed"
			case "nil":
				malformedTx = nil
			}
			// The later malformed address must precede this earlier structure
			// error, matching the original batch preparation entry point.
			invalidStructure := *tx
			invalidStructure.GLASBaseGas = 0
			incoming := []*core.Transaction{&invalidStructure, malformedTx}
			_, wantErr := bc.PrepareUnifiedTransactions(incoming)
			if wantErr == nil {
				t.Fatal("invalid GARET fixture")
			}
			lookups := 0
			snapshot := bc.ExecutionShardResolver()
			resolve := func(address string) uint64 {
				lookups++
				return snapshot(address)
			}
			if err := bc.InstallUnifiedMigrationWithResolver(incoming, resolve, true); err == nil || err.Error() != wantErr.Error() {
				t.Fatalf("migration changed GARET batch validation: got=%v want=%v", err, wantErr)
			}
			if lookups != 4 || len(bc.Txpool.TxQueue) != 1 || bc.Txpool.TxQueue[0] != existing[0] {
				t.Fatalf("invalid incoming batch was priced or partially published: lookups=%d", lookups)
			}
			if _, err := bc.PrepareUnifiedTransactionsWithResolver(incoming, resolve); err == nil || err.Error() != wantErr.Error() || lookups != 4 {
				t.Fatalf("forwarding changed GARET validation: %v lookups=%d", err, lookups)
			}
		})
	}
}
