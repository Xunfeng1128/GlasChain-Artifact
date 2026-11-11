package core

import (
	"math"
	"reflect"
	"testing"
)

func localWindowTx(source, sender string, gas uint64, relay bool) *Transaction {
	return &Transaction{SourceTxHash: source, Sender: sender, Recipient: sender, GasUsed: gas, GLASBaseGas: gas, Relayed: relay}
}

func TestLocalGasWindowMatchesEligibleHeadsAndRetainsQueueOrder(t *testing.T) {
	a := localWindowTx("a", "same", 60, false)
	b := localWindowTx("b", "same", 40, false)
	c := localWindowTx("c", "other", 20, false)
	r := localWindowTx("r", "relay", 30, true)
	a.Nonce, b.Nonce = 99, 1 // Queue FIFO, not nonce sorting.
	a.GLASCallRelations = []GLASCallRelation{{Caller: "x", Callee: "y"}, {Caller: "x", Callee: "y"}}
	queue := []*Transaction{a, c, b, r}
	got, err := SelectLocalGasWindow(queue, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	// Eligible order r(30), a(60), c(20); final whole record overshoots.
	if !reflect.DeepEqual(got.Records, []*Transaction{a, c, r}) || got.SelectedGas != 110 || got.FullRecords != 4 || got.FullCallRelations != 2 {
		t.Fatalf("unexpected selection: %+v", got)
	}
	if queue[0] != a || queue[2] != b || len(a.GLASCallRelations) != 2 {
		t.Fatal("selector changed live records")
	}
	if got.Records[0] != a || &got.Records[0].GLASCallRelations[0] != &a.GLASCallRelations[0] {
		t.Fatal("local selection must reference records, not deep-copy the whole pool")
	}
}

func TestLocalGasWindowDoesNotPackBlocksOrApplyGlobalCaps(t *testing.T) {
	a, b := localWindowTx("a", "same", 60, false), localWindowTx("b", "same", 60, false)
	got, err := SelectLocalGasWindow([]*Transaction{a, b}, 100, 100)
	if err != nil || len(got.Records) != 2 || got.SelectedGas != 120 {
		t.Fatalf("soft accumulation must include final whole record: %+v %v", got, err)
	}
	// Calls do not have their own local cutoff; central quota decides them.
	a.GLASCallRelations = make([]GLASCallRelation, 10)
	got, err = SelectLocalGasWindow([]*Transaction{a, b}, 100, 100)
	if err != nil || len(got.Records) != 2 || got.FullCallRelations != 10 {
		t.Fatalf("unexpected local call truncation: %+v %v", got, err)
	}
}

func TestLocalGasWindowEmptyInvalidAndOverflow(t *testing.T) {
	if got, err := SelectLocalGasWindow(nil, 100, 100); err != nil || len(got.Records) != 0 || got.FullRecords != 0 {
		t.Fatalf("empty queue: %+v %v", got, err)
	}
	for _, queue := range [][]*Transaction{{nil}, {localWindowTx("x", "s", 0, false)}, {localWindowTx("x", "s", 101, false)}} {
		if _, err := SelectLocalGasWindow(queue, 100, 100); err == nil {
			t.Fatal("accepted invalid physical stage")
		}
	}
	if _, err := SelectLocalGasWindow(nil, 0, 100); err == nil {
		t.Fatal("accepted zero budget")
	}
	queue := []*Transaction{localWindowTx("x", "s", math.MaxUint64-1, false), localWindowTx("y", "s", 2, false)}
	if _, err := SelectLocalGasWindow(queue, math.MaxUint64, math.MaxUint64); err == nil {
		t.Fatal("accepted Gas overflow")
	}
}

func TestLocalGasWindowFollowsInstalledWindowWithoutBypassingSender(t *testing.T) {
	relay := localWindowTx("ordinary-relay", "remote", 40, true)
	top := localWindowTx("selected-top", "same", 40, false)
	pool := &TxPool{TxQueue: []*Transaction{relay, top}}
	if err := pool.InstallPlanningWindow([]string{"selected-top"}); err != nil {
		t.Fatal(err)
	}
	pool.GetLocked()
	got, err := pool.SelectLocalGasWindowLocked(40, 100)
	pool.GetUnlocked()
	if err != nil || !reflect.DeepEqual(got.Records, []*Transaction{top}) {
		t.Fatalf("unfinished selected TOP must precede ordinary Relay2: %+v %v", got, err)
	}
	// The original queue strategy remains Relay2-first.
	legacy, err := SelectLocalGasWindow(pool.TxQueue, 40, 100)
	if err != nil || !reflect.DeepEqual(legacy.Records, []*Transaction{relay}) {
		t.Fatalf("changed original selection: %+v %v", legacy, err)
	}
	predecessor := localWindowTx("ordinary-predecessor", "same", 40, false)
	pool = &TxPool{TxQueue: []*Transaction{relay, predecessor, top}}
	if err := pool.InstallPlanningWindow([]string{"selected-top"}); err != nil {
		t.Fatal(err)
	}
	pool.GetLocked()
	got, err = pool.SelectLocalGasWindowLocked(80, 100)
	pool.GetUnlocked()
	if err != nil || !reflect.DeepEqual(got.Records, []*Transaction{relay, predecessor}) {
		t.Fatalf("selected TOP bypassed ordinary same-sender predecessor: %+v %v", got, err)
	}
}
