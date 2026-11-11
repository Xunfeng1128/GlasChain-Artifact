package core

import (
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestPriceGLASStageGas(t *testing.T) {
	tests := []struct {
		name                  string
		base, unit, count     uint64
		paysAttached          bool
		wantGas, wantAttached uint64
		wantOverflow          bool
	}{
		{name: "duplicate call occurrences", base: 100, unit: 5, count: 2, paysAttached: true, wantGas: 110, wantAttached: 10},
		{name: "zero unit", base: 100, count: math.MaxUint64, paysAttached: true, wantGas: 100},
		{name: "zero calls", base: math.MaxUint64, unit: math.MaxUint64, paysAttached: true, wantGas: math.MaxUint64},
		{name: "cross-shard TOP skips surcharge", base: math.MaxUint64, unit: math.MaxUint64, count: math.MaxUint64, wantGas: math.MaxUint64},
		{name: "zero base remains arithmetic", unit: 3, count: 2, paysAttached: true, wantGas: 6, wantAttached: 6},
		{name: "maximum attached gas", unit: 1, count: math.MaxUint64, paysAttached: true, wantGas: math.MaxUint64, wantAttached: math.MaxUint64},
		{name: "exact near-maximum boundary", base: math.MaxUint64 - 10, unit: 5, count: 2, paysAttached: true, wantGas: math.MaxUint64, wantAttached: 10},
		{name: "addition overflow", base: math.MaxUint64 - 1, unit: 1, count: 2, paysAttached: true, wantOverflow: true},
		{name: "multiplication overflow", unit: 2, count: math.MaxUint64, paysAttached: true, wantOverflow: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gas, attached, err := PriceGLASStageGas(tt.base, tt.unit, tt.count, tt.paysAttached)
			if tt.wantOverflow {
				if err == nil || !strings.Contains(err.Error(), "attached gas overflow") {
					t.Fatalf("expected overflow error, got %v", err)
				}
				if gas != 0 || attached != 0 {
					t.Fatalf("overflow returned partial pricing: gas=%d attached=%d", gas, attached)
				}
				return
			}
			if err != nil || gas != tt.wantGas || attached != tt.wantAttached {
				t.Fatalf("pricing = (%d, %d, %v), want (%d, %d, nil)", gas, attached, err, tt.wantGas, tt.wantAttached)
			}
		})
	}
}

func TestGLASAttachedGasByCallOccurrencesAndRelayStage(t *testing.T) {
	placement := map[string]uint64{"a": 0, "b": 0, "c": 1, "d": 0}
	resolve := func(a string) uint64 { return placement[a] }
	tx := &Transaction{Sender: "a", Recipient: "b", SourceTxHash: "t", GLASBaseGas: 100000, GLASCallGasPerCall: 5000,
		GLASCallRelations: []GLASCallRelation{{"b", "c"}, {"b", "c"}, {"b", "d"}}}
	if err := PriceGLASStage(tx, resolve); err != nil {
		t.Fatal(err)
	}
	if tx.GasUsed != 110000 || tx.GLASAttachedGas != 10000 || tx.GLASCrossCallCount != 2 {
		t.Fatalf("local TOP: %+v", tx)
	}
	if err := PriceGLASStage(tx, resolve); err != nil || tx.GasUsed != 110000 {
		t.Fatal("pricing not idempotent", err)
	}
	digest := TransactionExecutionSemanticDigest(tx)
	copyTx := *tx
	copyTx.GLASCallGasPerCall++
	if digest == TransactionExecutionSemanticDigest(&copyTx) {
		t.Fatal("unit cost missing from digest")
	}
	copyTx.GasUsed++
	if err := ValidateGLASStageGas(&copyTx, resolve, 200000); err == nil {
		t.Fatal("accepted tampered gas")
	}
	placement["a"] = 1
	if err := PriceGLASStage(tx, resolve); err != nil {
		t.Fatal(err)
	}
	if tx.GasUsed != 100000 || tx.GLASAttachedGas != 0 {
		t.Fatal("RELAY1 paid call cost", tx)
	}
	relay, err := NewGLASRelay2(tx, 3, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if relay.GasUsed != 110000 || !relay.Relayed || tx.Relayed || tx.GasUsed != 100000 {
		t.Fatal("relay duplicated/mutated TOP cost")
	}
	placement["c"] = 0
	if err := PriceGLASStage(relay, resolve); err != nil {
		t.Fatal(err)
	}
	if relay.GasUsed != 100000 || relay.GLASAttachedGas != 0 || relay.GLASStageEpoch != 3 {
		t.Fatal("carried RELAY2 was not repriced correctly")
	}
	if tx.GLASBaseGas != 100000 || relay.GLASBaseGas != 100000 {
		t.Fatal("base was overwritten")
	}
}

func TestGLASAttachedGasValidationAndOverflowDoNotMutate(t *testing.T) {
	resolve := func(a string) uint64 {
		if a == "c" {
			return 1
		}
		return 0
	}
	if err := PriceGLASStage(nil, nil); err != nil {
		t.Fatal("nil legacy record changed behavior", err)
	}
	legacy := &Transaction{GasUsed: 25, ExecDelayMs: 7, GLASAttachedGas: 5, IsGLASCallTask: true, HasBroker: true,
		GLASCallRelations: []GLASCallRelation{{Caller: "", Callee: ""}}}
	legacyBefore := *legacy
	if err := PriceGLASStage(legacy, nil); err != nil || !reflect.DeepEqual(*legacy, legacyBefore) {
		t.Fatal("zero-base legacy record changed", err)
	}
	tests := []struct {
		name  string
		tx    Transaction
		mapFn func(string) uint64
		want  string
	}{
		{name: "nil map", tx: Transaction{GLASBaseGas: 1}, want: "invalid attached-call gas record"},
		{name: "call task", tx: Transaction{GLASBaseGas: 1, IsGLASCallTask: true}, mapFn: resolve, want: "invalid attached-call gas record"},
		{name: "broker", tx: Transaction{GLASBaseGas: 1, HasBroker: true}, mapFn: resolve, want: "invalid attached-call gas record"},
		{name: "empty caller", tx: Transaction{GLASBaseGas: 1, GLASCallRelations: []GLASCallRelation{{Callee: "c"}}}, mapFn: resolve, want: "empty GLAS call endpoint"},
		{name: "empty callee", tx: Transaction{GLASBaseGas: 1, GLASCallRelations: []GLASCallRelation{{Caller: "b"}}}, mapFn: resolve, want: "empty GLAS call endpoint"},
		{name: "overflow", tx: Transaction{GLASBaseGas: math.MaxUint64, GLASCallGasPerCall: 1, GLASCallRelations: []GLASCallRelation{{Caller: "b", Callee: "c"}}}, mapFn: resolve, want: "attached gas overflow"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := tt.tx
			tx.SourceTxHash, tx.GasUsed, tx.GLASAttachedGas, tx.GLASCrossCallCount, tx.ExecDelayMs = "validation", 25, 5, 3, 7
			before := tx
			err := PriceGLASStage(&tx, tt.mapFn)
			if err == nil || !strings.Contains(err.Error(), tt.want) || !strings.Contains(err.Error(), TransactionExecutionID(&tx)) {
				t.Fatalf("expected %q and execution ID in error, got %v", tt.want, err)
			}
			if !reflect.DeepEqual(tx, before) {
				t.Fatal("failed pricing mutated record")
			}
		})
	}
}

func TestGLASNearMaximumPriceValidationAndRelay2(t *testing.T) {
	placement := map[string]uint64{"a": 0, "b": 0, "c": 1}
	resolve := func(a string) uint64 { return placement[a] }
	top := &Transaction{Sender: "a", Recipient: "b", SourceTxHash: "boundary", GLASBaseGas: math.MaxUint64 - 2,
		GLASCallGasPerCall: 1, GLASCallRelations: []GLASCallRelation{{"b", "c"}, {"b", "c"}}}
	if err := PriceGLASStage(top, resolve); err != nil {
		t.Fatal(err)
	}
	if top.GasUsed != math.MaxUint64 || top.GLASAttachedGas != 2 || top.GLASCrossCallCount != 2 || top.ExecDelayMs != ComputeTxExecDelayMs(top.GasUsed) {
		t.Fatalf("incorrect boundary pricing or delay: %+v", top)
	}
	if err := ValidateGLASStageGas(top, resolve, math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	tamper := []func(*Transaction){
		func(tx *Transaction) { tx.GasUsed-- },
		func(tx *Transaction) { tx.GLASAttachedGas-- },
		func(tx *Transaction) { tx.GLASCrossCallCount-- },
		func(tx *Transaction) { tx.ExecDelayMs = -1 },
	}
	for i, change := range tamper {
		bad := *top
		change(&bad)
		if err := ValidateGLASStageGas(&bad, resolve, 0); err == nil {
			t.Fatalf("accepted tampered priced field %d", i)
		}
	}
	placement["a"] = 1
	if err := PriceGLASStage(top, resolve); err != nil || top.GasUsed != top.GLASBaseGas || top.GLASAttachedGas != 0 || top.GLASCrossCallCount != 2 {
		t.Fatal("cross-shard TOP pricing changed", err)
	}
	before := *top
	relay, err := NewGLASRelay2(top, 9, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if relay.GasUsed != math.MaxUint64 || relay.GLASAttachedGas != 2 || relay.ExecDelayMs != ComputeTxExecDelayMs(relay.GasUsed) || !relay.Relayed || !relay.GLASStageEpochSet || relay.GLASStageEpoch != 9 {
		t.Fatalf("incorrect RELAY2 pricing: %+v", relay)
	}
	if !reflect.DeepEqual(*top, before) {
		t.Fatal("NewGLASRelay2 mutated committed TOP")
	}
	relay.GLASCallRelations[0].Callee = "changed"
	if top.GLASCallRelations[0].Callee != "c" {
		t.Fatal("RELAY2 shares call relations with committed TOP")
	}
	top.GLASBaseGas = math.MaxUint64
	before = *top
	if failedRelay, err := NewGLASRelay2(top, 10, resolve); failedRelay != nil || err == nil || !strings.Contains(err.Error(), "attached gas overflow for RELAY2:boundary") {
		t.Fatalf("expected RELAY2 overflow with execution ID, got (%v, %v)", failedRelay, err)
	}
	if !reflect.DeepEqual(*top, before) {
		t.Fatal("failed NewGLASRelay2 mutated committed TOP")
	}
	if _, err := NewGLASRelay2(nil, 1, resolve); err == nil {
		t.Fatal("accepted nil RELAY1")
	}
	if _, err := NewGLASRelay2(&Transaction{IsGLASCallTask: true}, 1, resolve); err == nil {
		t.Fatal("accepted call task as RELAY1")
	}
}

func TestGLASAttachedGasZeroRateOverflowAndCapacity(t *testing.T) {
	resolve := func(a string) uint64 {
		if a == "c" {
			return 1
		}
		return 0
	}
	tx := &Transaction{Sender: "a", Recipient: "b", GLASBaseGas: 100, GLASCallRelations: []GLASCallRelation{{"b", "c"}}}
	if err := PriceGLASStage(tx, resolve); err != nil || tx.GLASCrossCallCount != 1 || tx.GasUsed != 100 {
		t.Fatal("zero fee must preserve cross classification", err)
	}
	tx.GLASCallGasPerCall = math.MaxUint64
	if err := PriceGLASStage(tx, resolve); err == nil {
		t.Fatal("accepted gas overflow")
	}
	tx.GLASCallGasPerCall = 5
	if err := PriceGLASStage(tx, resolve); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGLASStageGas(tx, resolve, 104); err == nil {
		t.Fatal("accepted oversized stage")
	}
}

func TestGLASAttachedMetadataSnapshotDeepCopy(t *testing.T) {
	p := NewTxPool()
	p.TxQueue = []*Transaction{{GLASCallRelations: []GLASCallRelation{{"b", "c"}}}}
	s := p.Snapshot()
	s.TxQueue[0].GLASCallRelations[0].Callee = "changed"
	if p.TxQueue[0].GLASCallRelations[0].Callee != "c" {
		t.Fatal("snapshot shares call metadata")
	}
}

func TestGLASBaseAdmissionDoesNotPermitOversizedBlocks(t *testing.T) {
	resolve := func(a string) uint64 {
		if a == "c" {
			return 1
		}
		return 0
	}
	tx := &Transaction{Sender: "a", Recipient: "b", GLASBaseGas: 100, GLASCallGasPerCall: 5, GLASCallRelations: []GLASCallRelation{{"b", "c"}}}
	if err := PriceGLASStage(tx, resolve); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGLASAdmissionGas(tx, resolve, 100); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGLASStageGas(tx, resolve, 100); err == nil {
		t.Fatal("oversized block accepted")
	}
	if err := ValidateGLASAdmissionGas(tx, resolve, 99); err == nil {
		t.Fatal("base oversized admission accepted")
	}
	bad := *tx
	bad.GasUsed--
	if err := ValidateGLASAdmissionGas(&bad, resolve, 100); err == nil {
		t.Fatal("bad pricing accepted")
	}
	behind := &Transaction{Sender: "a", GasUsed: 1, GLASBaseGas: 1}
	independent := &Transaction{Sender: "x", GasUsed: 40, GLASBaseGas: 40}
	queue := []*Transaction{tx, behind, independent}
	opts := TxSelectionOptions{MaxGas: 100, LevelCount: 1, PriorityLevel: func(*Transaction) int { return 0 }, DependencyKey: func(tx *Transaction) string { return tx.Sender }}
	selected := SelectTransactions(queue, opts)
	if len(selected.Packed) != 1 || selected.Packed[0] != independent || len(selected.Retained) != 2 {
		t.Fatalf("bad selection: %+v", selected)
	}
	if err := PriceGLASStage(tx, func(string) uint64 { return 0 }); err != nil {
		t.Fatal(err)
	}
	selected = SelectTransactions(queue, opts)
	if len(selected.Packed) != 1 || selected.Packed[0] != tx || selected.GasUsed != 100 {
		t.Fatal("repriced stage not executable")
	}
}
