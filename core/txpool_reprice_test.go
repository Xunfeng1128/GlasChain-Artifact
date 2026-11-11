package core

import (
	"errors"
	"math/big"
	"reflect"
	"testing"
	"time"
)

func migrationPoolRecord(source string) *Transaction {
	stamp := time.Unix(1700000000, 0)
	return &Transaction{
		Sender: "alice", Recipient: "contract", Value: big.NewInt(1),
		TxHash: []byte(source), SourceTxHash: source, GasUsed: 10,
		GLASBaseGas: 10, GLASCallGasPerCall: 7,
		GLASCallRelations: []GLASCallRelation{{Caller: "contract", Callee: "remote"}},
		Time:              stamp, Broker1EnterPoolTime: stamp,
	}
}

func copyMigrationPoolRecord(tx *Transaction) (*Transaction, error) {
	if tx == nil {
		return nil, nil
	}
	copyTx := *tx
	copyTx.GLASCallRelations = append([]GLASCallRelation(nil), tx.GLASCallRelations...)
	return &copyTx, nil
}

func keepMigrationPoolRecord(tx *Transaction) (*Transaction, error) { return tx, nil }

func TestInstallRepricedExecutionRecordsMatchesSeparateRepriceAndAdmission(t *testing.T) {
	existing, relay := migrationPoolRecord("existing"), migrationPoolRecord("relay")
	relay.Relayed = true
	replay, _ := copyMigrationPoolRecord(existing)
	replay.GasUsed = 999 // Incoming prices are stale until preparation.
	fresh := migrationPoolRecord("fresh")
	freshReplay, _ := copyMigrationPoolRecord(fresh)
	freshReplay.Time = freshReplay.Time.Add(time.Second)
	committed := migrationPoolRecord("committed")
	committed.Sender = "stale-owner" // Tombstones win over stale semantics.
	incoming := []*Transaction{replay, fresh, committed, freshReplay}
	before := (&TxPool{TxQueue: append([]*Transaction{existing, relay}, incoming...)}).Snapshot()
	shardFor := func(address string) uint64 {
		if address == "remote" {
			return 1
		}
		return 0
	}
	prepare := func(tx *Transaction) (*Transaction, error) {
		if err := ValidateExecutionRecord(tx, 7); err != nil {
			return nil, err
		}
		copyTx, _ := copyMigrationPoolRecord(tx)
		if err := PriceExecutionStage(copyTx, shardFor); err != nil {
			return nil, err
		}
		if err := ValidateExecutionAdmissionGas(copyTx, shardFor, 100); err != nil {
			return nil, err
		}
		return copyTx, nil
	}
	baseline, fused := NewTxPool(), NewTxPool()
	for _, pool := range []*TxPool{baseline, fused} {
		pool.TxQueue = []*Transaction{existing, relay}
		pool.committedExecutionIDs = map[string]struct{}{TransactionExecutionID(committed): {}}
	}
	// The reference keeps the old two-pass reprice-then-admit sequence.
	for i, tx := range baseline.TxQueue {
		prepared, err := prepare(tx)
		if err != nil {
			t.Fatal(err)
		}
		baseline.TxQueue[i] = prepared
	}
	preparedIncoming := make([]*Transaction, len(incoming))
	for i, tx := range incoming {
		var err error
		preparedIncoming[i], err = prepare(tx)
		if err != nil {
			t.Fatal(err)
		}
	}
	wantAdded, err := baseline.AddTxs2PoolUniqueByExecutionID(preparedIncoming)
	if err != nil {
		t.Fatal(err)
	}
	gotAdded, err := fused.InstallRepricedExecutionRecords(incoming, prepare, prepare)
	if err != nil || gotAdded != wantAdded || gotAdded != 1 || !reflect.DeepEqual(fused.TxQueue, baseline.TxQueue) {
		t.Fatalf("fused installation differs: added=%d want=%d err=%v", gotAdded, wantAdded, err)
	}
	assertWindowSources(t, fused.TxQueue, "existing", "relay", "fresh")
	if fused.TxQueue[0] == existing || fused.TxQueue[1] == relay || fused.TxQueue[0].GasUsed != 17 {
		t.Fatal("repricing reused an old receipt reference or missed current pricing")
	}
	if !reflect.DeepEqual(append([]*Transaction{existing, relay}, incoming...), before.TxQueue) {
		t.Fatal("installation mutated proposal/receipt references or incoming records")
	}
}

func TestInstallRepricedExecutionRecordsNoopAndOwnedAdmission(t *testing.T) {
	pool := NewTxPool()
	existing := migrationPoolRecord("existing")
	pool.TxQueue = []*Transaction{existing}
	if added, err := pool.InstallRepricedExecutionRecords(nil, keepMigrationPoolRecord, copyMigrationPoolRecord); err != nil || added != 0 || pool.TxQueue[0] != existing {
		t.Fatalf("no-op changed live record: added=%d err=%v", added, err)
	}
	incoming := migrationPoolRecord("incoming")
	incoming.Time, incoming.Broker1EnterPoolTime = time.Time{}, time.Time{}
	before := *incoming
	added, err := pool.InstallRepricedExecutionRecords([]*Transaction{incoming}, keepMigrationPoolRecord, copyMigrationPoolRecord)
	if err != nil || added != 1 || len(pool.TxQueue) != 2 || pool.TxQueue[0] != existing {
		t.Fatalf("no-op admission failed: added=%d err=%v", added, err)
	}
	if pool.TxQueue[1] == incoming || pool.TxQueue[1].Time.IsZero() || pool.TxQueue[1].Broker1EnterPoolTime.IsZero() {
		t.Fatal("new record was not separately owned and timestamped")
	}
	if !reflect.DeepEqual(*incoming, before) {
		t.Fatal("incoming record received pool timestamps")
	}
}

func TestInstallRepricedExecutionRecordsRejectsAtomically(t *testing.T) {
	callbackErr := errors.New("preparation failed")
	for _, scenario := range []string{"existing-callback", "incoming-callback", "live-nil", "live-duplicate", "live-conflict", "incoming-nil", "incoming-conflict", "batch-conflict"} {
		t.Run(scenario, func(t *testing.T) {
			pool := NewTxPool()
			a, b := migrationPoolRecord("a"), migrationPoolRecord("b")
			pool.TxQueue = []*Transaction{a, b}
			if err := pool.InstallPlanningWindow([]string{"a"}); err != nil {
				t.Fatal(err)
			}
			oldWindow := pool.planningWindow
			fresh := migrationPoolRecord("fresh")
			fresh.Time, fresh.Broker1EnterPoolTime = time.Time{}, time.Time{}
			incoming := []*Transaction{fresh}
			existingPrepare, incomingPrepare := copyMigrationPoolRecord, copyMigrationPoolRecord
			wantErr := ErrExecutionIDConflict
			switch scenario {
			case "existing-callback":
				wantErr = callbackErr
				existingPrepare = func(tx *Transaction) (*Transaction, error) {
					if tx == b {
						return nil, callbackErr
					}
					prepared, _ := copyMigrationPoolRecord(tx)
					prepared.GasUsed++
					return prepared, nil
				}
			case "incoming-callback":
				wantErr = callbackErr
				incoming = append(incoming, migrationPoolRecord("bad"))
				incomingPrepare = func(tx *Transaction) (*Transaction, error) {
					if tx.SourceTxHash == "bad" {
						return nil, callbackErr
					}
					return copyMigrationPoolRecord(tx)
				}
			case "live-nil":
				pool.TxQueue[1] = nil
			case "live-duplicate":
				pool.TxQueue[1] = a
				wantErr = ErrExecutionIDNotUnique
			case "live-conflict":
				pool.TxQueue[1], _ = copyMigrationPoolRecord(a)
				pool.TxQueue[1].GasUsed++
			case "incoming-nil":
				incoming = append(incoming, nil)
			case "incoming-conflict":
				conflict, _ := copyMigrationPoolRecord(a)
				conflict.GasUsed++
				incoming = append(incoming, conflict)
			case "batch-conflict":
				conflict, _ := copyMigrationPoolRecord(fresh)
				conflict.Nonce++
				incoming = append(incoming, conflict)
			}
			queueBefore := append([]*Transaction(nil), pool.TxQueue...)
			bodiesBefore := (&TxPool{TxQueue: append(append([]*Transaction(nil), pool.TxQueue...), incoming...)}).Snapshot()
			added, err := pool.InstallRepricedExecutionRecords(incoming, existingPrepare, incomingPrepare)
			if added != 0 || !errors.Is(err, wantErr) {
				t.Fatalf("added=%d err=%v want=%v", added, err, wantErr)
			}
			if !reflect.DeepEqual(pool.TxQueue, queueBefore) || pool.planningWindow != oldWindow || oldWindow.dirty {
				t.Fatal("failure published queue or invalidated previous planning index")
			}
			if !reflect.DeepEqual(append(append([]*Transaction(nil), pool.TxQueue...), incoming...), bodiesBefore.TxQueue) {
				t.Fatal("failure changed record contents or timestamps")
			}
		})
	}
}

func TestInstallRepricedExecutionRecordsPreservesPreparationErrorPriority(t *testing.T) {
	existingErr, incomingErr := errors.New("later existing preparation"), errors.New("incoming preparation")
	for _, failExisting := range []bool{false, true} {
		pool := NewTxPool()
		a, b := migrationPoolRecord("a"), migrationPoolRecord("b")
		pool.TxQueue = []*Transaction{a, a, b} // Live-ID error occurs before b.
		incomingCalls := 0
		existingPrepare := func(tx *Transaction) (*Transaction, error) {
			if tx == b && failExisting {
				return nil, existingErr
			}
			return copyMigrationPoolRecord(tx)
		}
		incomingPrepare := func(tx *Transaction) (*Transaction, error) {
			incomingCalls++
			return nil, incomingErr
		}
		_, err := pool.InstallRepricedExecutionRecords([]*Transaction{b}, existingPrepare, incomingPrepare)
		want, wantCalls := incomingErr, 1
		if failExisting {
			want, wantCalls = existingErr, 0
		}
		if !errors.Is(err, want) || incomingCalls != wantCalls {
			t.Fatalf("error priority changed: err=%v calls=%d want=%v/%d", err, incomingCalls, want, wantCalls)
		}
	}
}

func TestEmptyUniqueAdmissionStillValidatesLiveRecords(t *testing.T) {
	a := migrationPoolRecord("a")
	conflict, _ := copyMigrationPoolRecord(a)
	conflict.GasUsed++
	for _, test := range []struct {
		name  string
		queue []*Transaction
		want  error
	}{
		{"nil", []*Transaction{nil}, ErrExecutionIDConflict},
		{"duplicate", []*Transaction{a, a}, ErrExecutionIDNotUnique},
		{"conflict", []*Transaction{a, conflict}, ErrExecutionIDConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, fused := range []bool{false, true} {
				pool := NewTxPool()
				pool.TxQueue = test.queue
				var added int
				var err error
				if fused {
					added, err = pool.InstallRepricedExecutionRecords(nil, keepMigrationPoolRecord, copyMigrationPoolRecord)
				} else {
					added, err = pool.AddTxs2PoolUniqueByExecutionID(nil)
				}
				if added != 0 || !errors.Is(err, test.want) || !reflect.DeepEqual(pool.TxQueue, test.queue) {
					t.Fatalf("empty batch lost live validation: fused=%v added=%d err=%v", fused, added, err)
				}
			}
		})
	}
}
