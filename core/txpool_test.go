package core

import (
	"errors"
	"math/big"
	"testing"
	"time"
)

func testTxWithGas(nonce uint64, gasUsed uint64) *Transaction {
	tx := NewTransaction("00000000001", "00000000002", big.NewInt(1), nonce, time.Now())
	tx.GasUsed = gasUsed
	return tx
}

func TestPackTxsWithGasLimit(t *testing.T) {
	pool := NewTxPool()
	pool.AddTxs2Pool([]*Transaction{
		testTxWithGas(1, 21000),
		testTxWithGas(2, 50000),
		testTxWithGas(3, 50000),
	})

	packed := pool.PackTxsWithGasLimit(71000)
	if len(packed) != 2 {
		t.Fatalf("expected 2 packed txs, got %d", len(packed))
	}
	if pool.GetTxQueueLen() != 1 {
		t.Fatalf("expected 1 tx left in pool, got %d", pool.GetTxQueueLen())
	}
}

func TestPackTxsWithGasLimitUsesIntrinsicGasForZero(t *testing.T) {
	pool := NewTxPool()
	pool.AddTxs2Pool([]*Transaction{
		testTxWithGas(1, 0),
		testTxWithGas(2, 21000),
		testTxWithGas(3, 21000),
	})

	packed := pool.PackTxsWithGasLimit(42000)
	if len(packed) != 2 {
		t.Fatalf("expected 2 packed txs, got %d", len(packed))
	}
}

func TestPackTxsWithGasLimitAndCostUsesFullRelayStageGas(t *testing.T) {
	pool := NewTxPool()
	pool.AddTxs2Pool([]*Transaction{
		testTxWithGas(1, 10_000_000),
		testTxWithGas(2, 10_000_000),
		testTxWithGas(3, 10_000_000),
	})

	stageCost := func(tx *Transaction) uint64 {
		return RelayStageGasUsed(EffectiveGasUsed(tx))
	}
	packed := pool.PackTxsWithGasLimitAndCost(10_000_000, stageCost)
	if len(packed) != 1 {
		t.Fatalf("expected 1 full-gas relay stage, got %d", len(packed))
	}
	if pool.GetTxQueueLen() != 2 {
		t.Fatalf("expected 2 txs left in pool, got %d", pool.GetTxQueueLen())
	}
}

func TestPackTxsWithGasLimitPanicsOnOversizedTx(t *testing.T) {
	pool := NewTxPool()
	pool.AddTxs2Pool([]*Transaction{
		testTxWithGas(1, 100000),
		testTxWithGas(2, 21000),
	})

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for tx gasUsed exceeding block gas limit")
		}
		if pool.GetTxQueueLen() != 2 {
			t.Fatalf("expected txpool to remain unchanged, got %d txs", pool.GetTxQueueLen())
		}
	}()

	_ = pool.PackTxsWithGasLimit(50000)
}

func TestPackTxsWithGasLimitAndCostFilteredLeavesUntouchedTopLevelQueued(t *testing.T) {
	pool := NewTxPool()
	queued := testTxWithGas(1, 21_000)
	relay := testTxWithGas(2, 30_000)
	relay.Relayed = true
	callTask := testTxWithGas(3, 40_000)
	callTask.IsGLASCallTask = true
	queuedAfter := testTxWithGas(4, 21_000)
	pool.AddTxs2Pool([]*Transaction{queued, relay, callTask, queuedAfter})

	packed := pool.PackTxsWithGasLimitAndCostFiltered(70_000, nil, func(tx *Transaction) bool {
		return tx.Relayed || tx.IsGLASCallTask
	})
	if len(packed) != 2 || packed[0] != relay || packed[1] != callTask {
		t.Fatalf("unexpected filtered records: %#v", packed)
	}
	if len(pool.TxQueue) != 2 || pool.TxQueue[0] != queued || pool.TxQueue[1] != queuedAfter {
		t.Fatalf("untouched queue order changed: %#v", pool.TxQueue)
	}
}

func TestPackTxsWithGasLimitAndCostPriorityFillsAfterInFlightWork(t *testing.T) {
	pool := NewTxPool()
	topFiller := testTxWithGas(1, 5_000)
	relay := testTxWithGas(2, 15_000)
	relay.Relayed = true
	callTask := testTxWithGas(3, 10_000)
	callTask.IsGLASCallTask = true
	topRetained := testTxWithGas(4, 20_000)
	pool.AddTxs2Pool([]*Transaction{topFiller, relay, callTask, topRetained})

	packed := pool.PackTxsWithGasLimitAndCostPriority(30_000, nil, func(tx *Transaction) bool {
		return tx.Relayed || tx.IsGLASCallTask
	})
	if len(packed) != 3 || packed[0] != relay || packed[1] != callTask || packed[2] != topFiller {
		t.Fatalf("priority packing order=%#v", packed)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != topRetained {
		t.Fatalf("unexpected retained queue=%#v", pool.TxQueue)
	}
}

func TestPackTxsWithGasLimitAndCostPriorityLevels(t *testing.T) {
	pool := NewTxPool()
	ordinaryFirst := testTxWithGas(1, 5_000)
	snapshotFirst := testTxWithGas(2, 6_000)
	snapshotFirst.SourceTxHash = "snapshot-1"
	relay := testTxWithGas(3, 7_000)
	relay.Relayed = true
	ordinaryRetained := testTxWithGas(4, 10_000)
	snapshotSecond := testTxWithGas(5, 8_000)
	snapshotSecond.SourceTxHash = "snapshot-2"
	callTask := testTxWithGas(6, 9_000)
	callTask.IsGLASCallTask = true
	pool.AddTxs2Pool([]*Transaction{
		ordinaryFirst, snapshotFirst, relay, ordinaryRetained, snapshotSecond, callTask,
	})

	snapshot := map[string]bool{"snapshot-1": true, "snapshot-2": true}
	packed := pool.PackTxsWithGasLimitAndCostPriorityLevels(
		35_000,
		nil,
		func(tx *Transaction) int {
			if tx.Relayed || tx.IsGLASCallTask {
				return 0
			}
			if snapshot[tx.SourceTxHash] {
				return 1
			}
			return 2
		},
		3,
	)
	want := []*Transaction{relay, callTask, snapshotFirst, snapshotSecond, ordinaryFirst}
	if len(packed) != len(want) {
		t.Fatalf("packed %d records, want %d: %#v", len(packed), len(want), packed)
	}
	for index := range want {
		if packed[index] != want[index] {
			t.Fatalf("packed[%d]=%p want %p; order=%#v", index, packed[index], want[index], packed)
		}
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != ordinaryRetained {
		t.Fatalf("unexpected retained queue=%#v", pool.TxQueue)
	}
}

func TestDependencyAwarePriorityPackingSkipsIndependentOversizedRemainder(t *testing.T) {
	pool := NewTxPool()
	txs := []*Transaction{
		{Sender: "alice", SourceTxHash: "alice-1", GasUsed: 60},
		{Sender: "bob", SourceTxHash: "bob-1", GasUsed: 50},
		{Sender: "carol", SourceTxHash: "carol-1", GasUsed: 40},
	}
	pool.AddTxs2Pool(txs)
	packed := pool.PackTxsWithGasLimitAndCostPriorityLevelsDependencyAware(
		100, nil, func(*Transaction) int { return 0 }, 1,
		func(tx *Transaction) string { return tx.Sender },
	)
	if len(packed) != 2 || packed[0].SourceTxHash != "alice-1" || packed[1].SourceTxHash != "carol-1" {
		t.Fatalf("packed=%v; independent 40-gas record should fill the block", packed)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0].SourceTxHash != "bob-1" {
		t.Fatalf("retained=%v; oversized-for-remainder record should remain", pool.TxQueue)
	}
}

func TestDependencyAwarePriorityPackingPreservesSameKeyOrder(t *testing.T) {
	pool := NewTxPool()
	pool.AddTxs2Pool([]*Transaction{
		{Sender: "alice", SourceTxHash: "alice-1", GasUsed: 60},
		{Sender: "alice", SourceTxHash: "alice-2", GasUsed: 40},
		{Sender: "bob", SourceTxHash: "bob-1", GasUsed: 50},
	})
	packed := pool.PackTxsWithGasLimitAndCostPriorityLevelsDependencyAware(
		100, nil,
		func(tx *Transaction) int {
			if tx.SourceTxHash == "bob-1" {
				return 0
			}
			return 1
		},
		2, func(tx *Transaction) string { return tx.Sender },
	)
	if len(packed) != 1 || packed[0].SourceTxHash != "bob-1" {
		t.Fatalf("packed=%v; alice-2 bypassed earlier alice-1", packed)
	}
	if len(pool.TxQueue) != 2 || pool.TxQueue[0].SourceTxHash != "alice-1" || pool.TxQueue[1].SourceTxHash != "alice-2" {
		t.Fatalf("same-key FIFO was not preserved: %v", pool.TxQueue)
	}
}

func TestPackTxsWithGasLimitAndCostFilteredFillUsesSmallerLaterTask(t *testing.T) {
	pool := NewTxPool()
	first := testTxWithGas(1, 20_000)
	first.IsGLASCallTask = true
	tooLargeForRemainder := testTxWithGas(2, 15_000)
	tooLargeForRemainder.IsGLASCallTask = true
	filler := testTxWithGas(3, 10_000)
	filler.IsGLASCallTask = true
	untouched := testTxWithGas(4, 5_000)
	pool.AddTxs2Pool([]*Transaction{first, tooLargeForRemainder, filler, untouched})

	packed := pool.PackTxsWithGasLimitAndCostFilteredFill(30_000, nil, func(tx *Transaction) bool {
		return tx.IsGLASCallTask
	})
	if len(packed) != 2 || packed[0] != first || packed[1] != filler {
		t.Fatalf("filtered fill order=%#v", packed)
	}
	if len(pool.TxQueue) != 2 || pool.TxQueue[0] != tooLargeForRemainder || pool.TxQueue[1] != untouched {
		t.Fatalf("unexpected retained queue=%#v", pool.TxQueue)
	}
}

func TestTxPoolSnapshotIsCompleteDeepCopy(t *testing.T) {
	shared := &Transaction{
		Sender:       "alice",
		Recipient:    "contract",
		Nonce:        7,
		Signature:    []byte{1, 2, 3},
		Value:        big.NewInt(99),
		TxHash:       []byte{4, 5, 6},
		RawTxHash:    []byte{7, 8, 9},
		SourceTxHash: "source-a",
		GasUsed:      42,
	}
	second := &Transaction{
		Sender:       "bob",
		Recipient:    "callee",
		Nonce:        8,
		Signature:    []byte{},
		Value:        big.NewInt(100),
		SourceTxHash: "source-b",
	}
	pool := NewTxPool()
	pool.AddTxs2Pool([]*Transaction{shared, second})
	// A snapshot is a structural operation and must remain total even if a
	// legacy caller has left a nil queue slot.
	pool.TxQueue = []*Transaction{shared, nil, second}
	pool.AddRelayTx(shared, 3)
	pool.AddRelayTx(second, 3)
	pool.RelayPool[9] = nil

	queueBefore := append([]*Transaction(nil), pool.TxQueue...)
	relayBefore := append([]*Transaction(nil), pool.RelayPool[3]...)
	snapshot := pool.Snapshot()

	if len(pool.TxQueue) != 3 || pool.TxQueue[0] != queueBefore[0] || pool.TxQueue[1] != nil || pool.TxQueue[2] != queueBefore[2] {
		t.Fatalf("snapshot modified live queue: %#v", pool.TxQueue)
	}
	if len(pool.RelayPool[3]) != 2 || pool.RelayPool[3][0] != relayBefore[0] || pool.RelayPool[3][1] != relayBefore[1] {
		t.Fatalf("snapshot modified live relay queue: %#v", pool.RelayPool[3])
	}
	if len(snapshot.TxQueue) != 3 || snapshot.TxQueue[1] != nil {
		t.Fatalf("snapshot did not preserve queue shape: %#v", snapshot.TxQueue)
	}
	if snapshot.TxQueue[0] == shared || snapshot.TxQueue[2] == second {
		t.Fatal("snapshot retained live transaction pointers")
	}
	if snapshot.TxQueue[2].Signature == nil {
		t.Fatal("snapshot changed a non-nil empty byte slice into nil")
	}
	if snapshot.RelayPool[3][0] != snapshot.TxQueue[0] || snapshot.RelayPool[3][1] != snapshot.TxQueue[2] {
		t.Fatal("snapshot did not preserve shared transaction identity within the copied pool")
	}
	if relay, ok := snapshot.RelayPool[9]; !ok || relay != nil {
		t.Fatalf("nil relay queue was not preserved: present=%v queue=%#v", ok, relay)
	}

	snapshot.TxQueue[0].Signature[0] = 99
	snapshot.TxQueue[0].TxHash[0] = 98
	snapshot.TxQueue[0].RawTxHash[0] = 97
	snapshot.TxQueue[0].Value.SetUint64(1234)
	snapshot.TxQueue[0].Sender = "changed"
	snapshot.TxQueue[2] = nil
	snapshot.RelayPool[3] = snapshot.RelayPool[3][:1]
	delete(snapshot.RelayPool, 9)

	if shared.Signature[0] != 1 || shared.TxHash[0] != 4 || shared.RawTxHash[0] != 7 || shared.Value.Uint64() != 99 || shared.Sender != "alice" {
		t.Fatalf("mutating snapshot changed live transaction: %#v", shared)
	}
	if pool.TxQueue[2] != second || len(pool.RelayPool[3]) != 2 {
		t.Fatal("mutating snapshot changed live pool containers")
	}

	second.Value.SetUint64(5678)
	if snapshot.RelayPool[3][0].Value.Uint64() != 1234 {
		t.Fatal("mutating live pool changed snapshot")
	}
}

func TestTransactionExecutionIDSeparatesStagesAndUsesStableFallbacks(t *testing.T) {
	top := &Transaction{SourceTxHash: "0xsource"}
	relay := &Transaction{SourceTxHash: "0xsource", Relayed: true}
	call := &Transaction{SourceTxHash: "0xsource", IsGLASCallTask: true, GLASCallTaskID: "task-7"}
	callFallback := &Transaction{
		SourceTxHash:   "0xsource",
		Recipient:      "recipient",
		GLASCallCallee: "callee",
		Nonce:          11,
		IsGLASCallTask: true,
	}
	hashFallback := &Transaction{TxHash: []byte{0x0a, 0xff}}

	cases := []struct {
		tx   *Transaction
		want string
	}{
		{top, "TOP:0xsource"},
		{relay, "RELAY2:0xsource"},
		{call, "CALL:task-7"},
		{callFallback, "CALL:0xsource:callee:11"},
		{hashFallback, "TOP:0aff"},
		{nil, ""},
	}
	for _, test := range cases {
		if got := TransactionExecutionID(test.tx); got != test.want {
			t.Errorf("TransactionExecutionID(%#v)=%q, want %q", test.tx, got, test.want)
		}
	}
	if TransactionExecutionID(top) == TransactionExecutionID(relay) {
		t.Fatal("top and Relay2 execution stages share an ID")
	}
}

func TestTransactionExecutionSemanticDigestIgnoresLifecycleButBindsExecution(t *testing.T) {
	tx := &Transaction{
		Sender: "alice", Recipient: "contract", Nonce: 9, Value: big.NewInt(17),
		SourceTxHash: "source", TxCategory: "contract", GasUsed: 80_000,
		ExecDelayMs: 2.5,
	}
	clone := *tx
	clone.Time = time.Now()
	clone.Broker1EnterPoolTime = time.Now().Add(time.Second)
	clone.Signature = []byte("different transport signature")
	clone.TxHash = []byte("different transport hash")
	if left, right := TransactionExecutionSemanticDigest(tx), TransactionExecutionSemanticDigest(&clone); left == "" || left != right {
		t.Fatalf("lifecycle-only clone digest mismatch: %q != %q", left, right)
	}

	changed := clone
	changed.GasUsed++
	if TransactionExecutionSemanticDigest(tx) == TransactionExecutionSemanticDigest(&changed) {
		t.Fatal("gas change did not change semantic digest")
	}
	changed = clone
	changed.Relayed = true
	if TransactionExecutionSemanticDigest(tx) == TransactionExecutionSemanticDigest(&changed) {
		t.Fatal("execution-stage change did not change semantic digest")
	}

	call := &Transaction{
		Sender: "contract-a", Recipient: "contract-b", Nonce: 9, Value: big.NewInt(0),
		SourceTxHash: "source", TxCategory: "glas_call_task", GasUsed: 80_000,
		IsGLASCallTask: true, GLASCallTaskID: "source:1:contract-b",
		GLASCallOriginShard: 2, GLASCallTargetShard: 7, GLASCallCallee: "contract-b",
	}
	migratedCall := *call
	migratedCall.GLASCallOriginShard = 3
	migratedCall.GLASCallTargetShard = 4
	if left, right := TransactionExecutionSemanticDigest(call), TransactionExecutionSemanticDigest(&migratedCall); left != right {
		t.Fatalf("placement-only call migration changed semantic digest: %q != %q", left, right)
	}
	migratedCall.GLASCallCallee = "contract-c"
	if TransactionExecutionSemanticDigest(call) == TransactionExecutionSemanticDigest(&migratedCall) {
		t.Fatal("callee change did not change semantic digest")
	}
}

func TestSelectTransactionsIsPureAndReturnsExactPlan(t *testing.T) {
	queue := []*Transaction{
		{Sender: "alice", SourceTxHash: "alice-1", GasUsed: 60},
		{Sender: "bob", SourceTxHash: "bob-1", GasUsed: 50},
		{Sender: "carol", SourceTxHash: "carol-1", GasUsed: 40},
	}
	queueHeaderBefore := append([]*Transaction(nil), queue...)
	result := SelectTransactions(queue, TxSelectionOptions{
		MaxGas:        100,
		PriorityLevel: func(*Transaction) int { return 0 },
		LevelCount:    1,
		DependencyKey: func(tx *Transaction) string { return tx.Sender },
	})

	if result.GasUsed != 100 {
		t.Fatalf("GasUsed=%d, want 100", result.GasUsed)
	}
	if len(result.SelectedIndices) != 2 || result.SelectedIndices[0] != 0 || result.SelectedIndices[1] != 2 {
		t.Fatalf("SelectedIndices=%v, want [0 2]", result.SelectedIndices)
	}
	if len(result.Packed) != 2 || result.Packed[0] != queue[0] || result.Packed[1] != queue[2] {
		t.Fatalf("Packed=%#v, want alice then carol", result.Packed)
	}
	if len(result.Retained) != 1 || result.Retained[0] != queue[1] {
		t.Fatalf("Retained=%#v, want bob", result.Retained)
	}
	for index := range queueHeaderBefore {
		if queue[index] != queueHeaderBefore[index] {
			t.Fatalf("pure selection modified queue at %d", index)
		}
		if !queue[index].Broker1LeavePoolTime.IsZero() || !queue[index].Broker2LeavePoolTime.IsZero() {
			t.Fatalf("pure selection wrote lifecycle time at %d", index)
		}
	}
	result.Retained[0] = nil
	if queue[1] == nil {
		t.Fatal("result slice aliases input queue storage")
	}
}

func TestPackTxsWithSelectionHardWhitelistLeavesOtherRecords(t *testing.T) {
	pool := NewTxPool()
	disallowed := &Transaction{Sender: "outside", SourceTxHash: "outside", GasUsed: 10}
	allowedTop := &Transaction{Sender: "alice", SourceTxHash: "selected", GasUsed: 60}
	allowedCall := &Transaction{
		Sender:         "entry",
		Recipient:      "callee",
		SourceTxHash:   "selected",
		GasUsed:        40,
		IsGLASCallTask: true,
		GLASCallTaskID: "call-1",
		GLASCallCallee: "callee",
	}
	pool.AddTxs2Pool([]*Transaction{disallowed, allowedTop, allowedCall})
	allowed := map[string]struct{}{
		TransactionExecutionID(allowedTop):  {},
		TransactionExecutionID(allowedCall): {},
	}

	packed := pool.PackTxsWithSelection(TxSelectionOptions{
		MaxGas:              100,
		PriorityLevel:       func(*Transaction) int { return 0 },
		LevelCount:          1,
		DependencyKey:       func(tx *Transaction) string { return tx.Sender },
		AllowedExecutionIDs: allowed,
	})
	if len(packed) != 2 || packed[0] != allowedTop || packed[1] != allowedCall {
		t.Fatalf("packed=%#v, want exactly manifest records", packed)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != disallowed {
		t.Fatalf("non-manifest record did not remain queued: %#v", pool.TxQueue)
	}
	if disallowed.Broker1LeavePoolTime.IsZero() == false {
		t.Fatal("non-manifest record received a leave timestamp")
	}
	if allowedTop.Broker1LeavePoolTime.IsZero() || allowedCall.Broker1LeavePoolTime.IsZero() {
		t.Fatal("packed records did not receive leave timestamps")
	}
}

func TestSelectionWhitelistCannotBypassDisallowedDependencyHead(t *testing.T) {
	head := &Transaction{Sender: "alice", SourceTxHash: "head", GasUsed: 20}
	follower := &Transaction{Sender: "alice", SourceTxHash: "follower", GasUsed: 20}
	independent := &Transaction{Sender: "bob", SourceTxHash: "independent", GasUsed: 20}
	queue := []*Transaction{head, follower, independent}
	allowed := map[string]struct{}{
		TransactionExecutionID(follower):    {},
		TransactionExecutionID(independent): {},
	}
	result := SelectTransactions(queue, TxSelectionOptions{
		MaxGas:              100,
		PriorityLevel:       func(*Transaction) int { return 0 },
		LevelCount:          1,
		DependencyKey:       func(tx *Transaction) string { return tx.Sender },
		AllowedExecutionIDs: allowed,
	})
	if len(result.Packed) != 1 || result.Packed[0] != independent {
		t.Fatalf("packed=%#v; follower bypassed non-manifest dependency head", result.Packed)
	}
	if len(result.Retained) != 2 || result.Retained[0] != head || result.Retained[1] != follower {
		t.Fatalf("retained queue order=%#v", result.Retained)
	}
}

func TestSelectionEmptyWhitelistPacksNothing(t *testing.T) {
	tx := &Transaction{Sender: "alice", SourceTxHash: "tx", GasUsed: 200}
	result := SelectTransactions([]*Transaction{tx}, TxSelectionOptions{
		MaxGas:              100,
		PriorityLevel:       func(*Transaction) int { return 0 },
		LevelCount:          1,
		AllowedExecutionIDs: map[string]struct{}{},
	})
	if len(result.Packed) != 0 || result.GasUsed != 0 || len(result.Retained) != 1 || result.Retained[0] != tx {
		t.Fatalf("empty whitelist result=%#v", result)
	}
}

func TestPackExactExecutionIDsEmptyManifestIsReady(t *testing.T) {
	pool := NewTxPool()
	tx := &Transaction{SourceTxHash: "queued", GasUsed: 21_000}
	pool.AddTx2Pool(tx)

	packed, ready, err := pool.PackExactExecutionIDs(nil, 0, nil)
	if err != nil || !ready || len(packed) != 0 {
		t.Fatalf("empty manifest: packed=%#v ready=%v err=%v", packed, ready, err)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != tx || !tx.Broker1LeavePoolTime.IsZero() {
		t.Fatal("empty exact manifest modified pool")
	}
}

func TestPackExactExecutionIDsMissingRecordDoesNotPartiallyCommit(t *testing.T) {
	pool := NewTxPool()
	present := &Transaction{SourceTxHash: "present", GasUsed: 21_000}
	pool.AddTx2Pool(present)

	packed, ready, err := pool.PackExactExecutionIDs(
		[]string{TransactionExecutionID(present), "TOP:not-arrived"},
		100_000,
		nil,
	)
	if err != nil || ready || packed != nil {
		t.Fatalf("missing manifest record: packed=%#v ready=%v err=%v", packed, ready, err)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != present || !present.Broker1LeavePoolTime.IsZero() {
		t.Fatal("missing exact record partially committed present record")
	}
}

func TestPackExactExecutionIDsRejectsDuplicateManifestOrPoolID(t *testing.T) {
	t.Run("manifest", func(t *testing.T) {
		pool := NewTxPool()
		tx := &Transaction{SourceTxHash: "same", GasUsed: 21_000}
		pool.AddTx2Pool(tx)
		id := TransactionExecutionID(tx)

		packed, ready, err := pool.PackExactExecutionIDs([]string{id, id}, 100_000, nil)
		if packed != nil || ready || !errors.Is(err, ErrExecutionIDNotUnique) {
			t.Fatalf("duplicate manifest: packed=%#v ready=%v err=%v", packed, ready, err)
		}
		if len(pool.TxQueue) != 1 || pool.TxQueue[0] != tx || !tx.Broker1LeavePoolTime.IsZero() {
			t.Fatal("duplicate manifest modified pool")
		}
	})

	t.Run("pool", func(t *testing.T) {
		pool := NewTxPool()
		first := &Transaction{SourceTxHash: "same", GasUsed: 21_000}
		second := &Transaction{SourceTxHash: "same", GasUsed: 21_000}
		pool.AddTxs2Pool([]*Transaction{first, second})

		packed, ready, err := pool.PackExactExecutionIDs([]string{TransactionExecutionID(first)}, 100_000, nil)
		if packed != nil || ready || !errors.Is(err, ErrExecutionIDNotUnique) {
			t.Fatalf("duplicate pool ID: packed=%#v ready=%v err=%v", packed, ready, err)
		}
		if len(pool.TxQueue) != 2 || pool.TxQueue[0] != first || pool.TxQueue[1] != second ||
			!first.Broker1LeavePoolTime.IsZero() || !second.Broker1LeavePoolTime.IsZero() {
			t.Fatal("duplicate pool ID modified pool")
		}
	})
}

func TestPackExactExecutionIDsRejectsTotalGasAtomically(t *testing.T) {
	pool := NewTxPool()
	first := &Transaction{SourceTxHash: "first", GasUsed: 60}
	second := &Transaction{SourceTxHash: "second", GasUsed: 50}
	pool.AddTxs2Pool([]*Transaction{first, second})

	packed, ready, err := pool.PackExactExecutionIDs(
		[]string{TransactionExecutionID(first), TransactionExecutionID(second)},
		100,
		func(tx *Transaction) uint64 { return tx.GasUsed },
	)
	if packed != nil || ready || !errors.Is(err, ErrExecutionGasLimit) {
		t.Fatalf("over-gas manifest: packed=%#v ready=%v err=%v", packed, ready, err)
	}
	if len(pool.TxQueue) != 2 || pool.TxQueue[0] != first || pool.TxQueue[1] != second ||
		!first.Broker1LeavePoolTime.IsZero() || !second.Broker1LeavePoolTime.IsZero() {
		t.Fatal("over-gas manifest partially committed")
	}
}

func TestPackExactExecutionIDsCommitsOnceInManifestOrder(t *testing.T) {
	pool := NewTxPool()
	retainedFirst := &Transaction{SourceTxHash: "outside-1", GasUsed: 5}
	secondInManifest := &Transaction{SourceTxHash: "second", GasUsed: 40}
	firstInManifest := &Transaction{SourceTxHash: "first", GasUsed: 60}
	retainedSecond := &Transaction{SourceTxHash: "outside-2", GasUsed: 5}
	pool.AddTxs2Pool([]*Transaction{retainedFirst, secondInManifest, firstInManifest, retainedSecond})

	packed, ready, err := pool.PackExactExecutionIDs(
		[]string{TransactionExecutionID(firstInManifest), TransactionExecutionID(secondInManifest)},
		100,
		func(tx *Transaction) uint64 { return tx.GasUsed },
	)
	if err != nil || !ready {
		t.Fatalf("exact manifest was not ready: ready=%v err=%v", ready, err)
	}
	if len(packed) != 2 || packed[0] != firstInManifest || packed[1] != secondInManifest {
		t.Fatalf("packed order=%#v, want manifest order", packed)
	}
	if len(pool.TxQueue) != 2 || pool.TxQueue[0] != retainedFirst || pool.TxQueue[1] != retainedSecond {
		t.Fatalf("retained order=%#v", pool.TxQueue)
	}
	if firstInManifest.Broker1LeavePoolTime.IsZero() || secondInManifest.Broker1LeavePoolTime.IsZero() {
		t.Fatal("exactly packed records did not receive leave timestamps")
	}
	if !retainedFirst.Broker1LeavePoolTime.IsZero() || !retainedSecond.Broker1LeavePoolTime.IsZero() {
		t.Fatal("retained records received leave timestamps")
	}
}

func TestPeekExactExecutionIDsRepeatedAndFailedProposalsDoNotModifyPool(t *testing.T) {
	pool := NewTxPool()
	outside := &Transaction{SourceTxHash: "outside", GasUsed: 5}
	second := &Transaction{SourceTxHash: "second", GasUsed: 40}
	first := &Transaction{SourceTxHash: "first", GasUsed: 60}
	pool.AddTxs2Pool([]*Transaction{outside, second, first})
	ids := []string{TransactionExecutionID(first), TransactionExecutionID(second)}
	gasCost := func(tx *Transaction) uint64 { return tx.GasUsed }

	for proposal := 0; proposal < 2; proposal++ {
		packed, ready, err := pool.PeekExactExecutionIDs(ids, 100, gasCost)
		if err != nil || !ready {
			t.Fatalf("proposal %d not ready: ready=%v err=%v", proposal, ready, err)
		}
		if len(packed) != 2 || packed[0] != first || packed[1] != second {
			t.Fatalf("proposal %d order=%#v, want manifest order", proposal, packed)
		}
		if len(pool.TxQueue) != 3 || pool.TxQueue[0] != outside || pool.TxQueue[1] != second || pool.TxQueue[2] != first {
			t.Fatalf("proposal %d modified queue=%#v", proposal, pool.TxQueue)
		}
		if !outside.Broker1LeavePoolTime.IsZero() || !second.Broker1LeavePoolTime.IsZero() || !first.Broker1LeavePoolTime.IsZero() {
			t.Fatalf("proposal %d wrote leave-pool timestamp", proposal)
		}
	}

	packed, ready, err := pool.PeekExactExecutionIDs(
		[]string{TransactionExecutionID(first), "TOP:not-arrived"},
		100,
		gasCost,
	)
	if err != nil || ready || packed != nil {
		t.Fatalf("failed proposal: packed=%#v ready=%v err=%v", packed, ready, err)
	}
	if len(pool.TxQueue) != 3 || pool.TxQueue[0] != outside || pool.TxQueue[1] != second || pool.TxQueue[2] != first {
		t.Fatalf("failed proposal modified queue=%#v", pool.TxQueue)
	}
	if !outside.Broker1LeavePoolTime.IsZero() || !second.Broker1LeavePoolTime.IsZero() || !first.Broker1LeavePoolTime.IsZero() {
		t.Fatal("failed proposal wrote leave-pool timestamp")
	}
}

func TestCommitExactExecutionIDsDeletesOnceAndReportsRepeatedCommit(t *testing.T) {
	pool := NewTxPool()
	outside := &Transaction{SourceTxHash: "outside", GasUsed: 5}
	planned := &Transaction{SourceTxHash: "planned", GasUsed: 40}
	pool.AddTxs2Pool([]*Transaction{outside, planned})
	ids := []string{TransactionExecutionID(planned)}
	gasCost := func(tx *Transaction) uint64 { return tx.GasUsed }

	packed, committed, err := pool.CommitExactExecutionIDs(ids, 100, gasCost)
	if err != nil || !committed {
		t.Fatalf("first commit: committed=%v err=%v", committed, err)
	}
	if len(packed) != 1 || packed[0] != planned {
		t.Fatalf("first commit packed=%#v", packed)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != outside {
		t.Fatalf("first commit retained=%#v", pool.TxQueue)
	}
	if planned.Broker1LeavePoolTime.IsZero() {
		t.Fatal("first commit did not write leave-pool timestamp")
	}
	leaveTime := planned.Broker1LeavePoolTime

	packed, committed, err = pool.CommitExactExecutionIDs(ids, 100, gasCost)
	if err != nil || committed || packed != nil {
		t.Fatalf("repeated commit: packed=%#v committed=%v err=%v", packed, committed, err)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != outside {
		t.Fatalf("repeated commit modified retained queue=%#v", pool.TxQueue)
	}
	if !planned.Broker1LeavePoolTime.Equal(leaveTime) || !outside.Broker1LeavePoolTime.IsZero() {
		t.Fatal("repeated commit changed lifecycle timestamps")
	}
}

func TestPeekTxsWithSelectionMatchesPackingWithoutModifyingPool(t *testing.T) {
	pool := NewTxPool()
	first := &Transaction{Sender: "alice", SourceTxHash: "first", GasUsed: 60}
	blocked := &Transaction{Sender: "bob", SourceTxHash: "blocked", GasUsed: 50}
	filler := &Transaction{Sender: "carol", SourceTxHash: "filler", GasUsed: 40}
	pool.AddTxs2Pool([]*Transaction{first, blocked, filler})
	options := TxSelectionOptions{
		MaxGas:        100,
		GasCost:       func(tx *Transaction) uint64 { return tx.GasUsed },
		PriorityLevel: func(*Transaction) int { return 0 },
		LevelCount:    1,
		DependencyKey: func(tx *Transaction) string { return tx.Sender },
	}

	for preview := 0; preview < 2; preview++ {
		packed := pool.PeekTxsWithSelection(options)
		if len(packed) != 2 || packed[0] != first || packed[1] != filler {
			t.Fatalf("preview %d packed=%#v", preview, packed)
		}
		packed[0] = nil
		if len(pool.TxQueue) != 3 || pool.TxQueue[0] != first || pool.TxQueue[1] != blocked || pool.TxQueue[2] != filler {
			t.Fatalf("preview %d modified queue=%#v", preview, pool.TxQueue)
		}
		if !first.Broker1LeavePoolTime.IsZero() || !blocked.Broker1LeavePoolTime.IsZero() || !filler.Broker1LeavePoolTime.IsZero() {
			t.Fatalf("preview %d wrote leave-pool timestamp", preview)
		}
	}

	packed := pool.PackTxsWithSelection(options)
	if len(packed) != 2 || packed[0] != first || packed[1] != filler {
		t.Fatalf("real packing diverged from preview: %#v", packed)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != blocked {
		t.Fatalf("real packing retained=%#v", pool.TxQueue)
	}
}

func TestAddTxs2PoolUniqueByExecutionIDIsIdempotent(t *testing.T) {
	pool := NewTxPool()
	existing := &Transaction{
		Sender:       "alice",
		Recipient:    "contract",
		Nonce:        1,
		Value:        big.NewInt(7),
		SourceTxHash: "existing",
		GasUsed:      30,
	}
	pool.AddTx2Pool(existing)
	replay := *existing
	replay.Time = time.Time{}
	replay.Broker1EnterPoolTime = time.Time{}
	replay.Broker1LeavePoolTime = time.Time{}

	incoming := &Transaction{
		Sender:         "entry",
		Recipient:      "callee",
		Nonce:          2,
		Value:          big.NewInt(0),
		SourceTxHash:   "source",
		GasUsed:        40,
		IsGLASCallTask: true,
		GLASCallTaskID: "call-1",
		GLASCallCallee: "callee",
	}
	incomingReplay := *incoming

	added, err := pool.AddTxs2PoolUniqueByExecutionID([]*Transaction{&replay, incoming, &incomingReplay})
	if err != nil || added != 1 {
		t.Fatalf("unique add: added=%d err=%v", added, err)
	}
	if len(pool.TxQueue) != 2 || pool.TxQueue[0] != existing || pool.TxQueue[1] != incoming {
		t.Fatalf("unique add queue=%#v", pool.TxQueue)
	}
	if !replay.Time.IsZero() || !replay.Broker1EnterPoolTime.IsZero() {
		t.Fatal("idempotent replay received pool timestamps")
	}
	if incoming.Time.IsZero() || incoming.Broker1EnterPoolTime.IsZero() {
		t.Fatal("new unique record did not receive pool timestamps")
	}

	added, err = pool.AddTxs2PoolUniqueByExecutionID([]*Transaction{&replay, &incomingReplay})
	if err != nil || added != 0 || len(pool.TxQueue) != 2 {
		t.Fatalf("second replay: added=%d queue=%#v err=%v", added, pool.TxQueue, err)
	}
}

func TestAddTxs2PoolUniqueByExecutionIDRejectsConflictAtomically(t *testing.T) {
	pool := NewTxPool()
	existing := &Transaction{
		Sender:       "alice",
		Recipient:    "contract",
		Value:        big.NewInt(1),
		SourceTxHash: "same",
		GasUsed:      30,
	}
	pool.AddTx2Pool(existing)
	newBeforeConflict := &Transaction{Sender: "bob", Recipient: "local", SourceTxHash: "new", GasUsed: 10}
	conflict := *existing
	conflict.GasUsed++
	conflict.Time = time.Time{}
	conflict.Broker1EnterPoolTime = time.Time{}

	added, err := pool.AddTxs2PoolUniqueByExecutionID([]*Transaction{newBeforeConflict, &conflict})
	if added != 0 || !errors.Is(err, ErrExecutionIDConflict) {
		t.Fatalf("conflicting add: added=%d err=%v", added, err)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != existing {
		t.Fatalf("conflicting batch partially added queue=%#v", pool.TxQueue)
	}
	if !newBeforeConflict.Time.IsZero() || !newBeforeConflict.Broker1EnterPoolTime.IsZero() ||
		!conflict.Time.IsZero() || !conflict.Broker1EnterPoolTime.IsZero() {
		t.Fatal("rejected batch received lifecycle timestamps")
	}
}

func TestReplaceTransactionsAtomicallyMigratesDeduplicatesAndSorts(t *testing.T) {
	pool := NewTxPool()
	removed := &Transaction{Sender: "old-owner", SourceTxHash: "z-removed", GasUsed: 10}
	retained := &Transaction{Sender: "local-owner", SourceTxHash: "m-retained", GasUsed: 20}
	pool.AddTxs2Pool([]*Transaction{removed, retained})
	incoming := &Transaction{Sender: "new-owner", SourceTxHash: "a-incoming", GasUsed: 30}
	incomingReplay := *incoming

	err := pool.ReplaceTransactions(
		func(tx *Transaction) bool { return tx.Sender == "old-owner" },
		[]*Transaction{incoming, &incomingReplay},
		func(a, b *Transaction) bool { return a.SourceTxHash < b.SourceTxHash },
	)
	if err != nil {
		t.Fatalf("replace transactions: %v", err)
	}
	if len(pool.TxQueue) != 2 || pool.TxQueue[0] != incoming || pool.TxQueue[1] != retained {
		t.Fatalf("replaced queue=%#v", pool.TxQueue)
	}
	if !removed.Broker1LeavePoolTime.IsZero() {
		t.Fatal("migration removal was marked as executed")
	}
	if incoming.Time.IsZero() || incoming.Broker1EnterPoolTime.IsZero() {
		t.Fatal("migration addition did not receive entry timestamps")
	}

	err = pool.ReplaceTransactions(nil, []*Transaction{&incomingReplay}, func(a, b *Transaction) bool {
		return a.SourceTxHash < b.SourceTxHash
	})
	if err != nil || len(pool.TxQueue) != 2 || pool.TxQueue[0] != incoming || pool.TxQueue[1] != retained {
		t.Fatalf("idempotent replacement changed queue=%#v err=%v", pool.TxQueue, err)
	}
}

func TestReplaceTransactionsConflictLeavesRemovedRecordsInPlace(t *testing.T) {
	pool := NewTxPool()
	wouldBeRemoved := &Transaction{Sender: "old-owner", SourceTxHash: "removed", GasUsed: 10}
	retained := &Transaction{Sender: "local-owner", SourceTxHash: "same", GasUsed: 20}
	pool.AddTxs2Pool([]*Transaction{wouldBeRemoved, retained})
	conflict := *retained
	conflict.GasUsed++
	conflict.Time = time.Time{}
	conflict.Broker1EnterPoolTime = time.Time{}

	err := pool.ReplaceTransactions(
		func(tx *Transaction) bool { return tx == wouldBeRemoved },
		[]*Transaction{&conflict},
		nil,
	)
	if !errors.Is(err, ErrExecutionIDConflict) {
		t.Fatalf("replacement conflict err=%v", err)
	}
	if len(pool.TxQueue) != 2 || pool.TxQueue[0] != wouldBeRemoved || pool.TxQueue[1] != retained {
		t.Fatalf("failed replacement modified queue=%#v", pool.TxQueue)
	}
	if !conflict.Time.IsZero() || !conflict.Broker1EnterPoolTime.IsZero() {
		t.Fatal("failed replacement wrote addition timestamps")
	}
}

func TestCommittedExecutionIDTombstonePreventsDelayedReaddition(t *testing.T) {
	pool := NewTxPool()
	committed := &Transaction{
		Sender:       "alice",
		Recipient:    "contract",
		Nonce:        7,
		Value:        big.NewInt(3),
		SourceTxHash: "committed-source",
		GasUsed:      40,
	}
	retained := &Transaction{Sender: "bob", Recipient: "local", SourceTxHash: "retained", GasUsed: 5}
	pool.AddTxs2Pool([]*Transaction{committed, retained})
	id := TransactionExecutionID(committed)

	packed, committedNow, err := pool.CommitExactExecutionIDs(
		[]string{id},
		100,
		func(tx *Transaction) uint64 { return tx.GasUsed },
	)
	if err != nil || !committedNow || len(packed) != 1 || packed[0] != committed {
		t.Fatalf("commit: packed=%#v committed=%v err=%v", packed, committedNow, err)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != retained {
		t.Fatalf("commit retained=%#v", pool.TxQueue)
	}

	delayed := &Transaction{
		Sender:       committed.Sender,
		Recipient:    committed.Recipient,
		Nonce:        committed.Nonce,
		Value:        new(big.Int).Set(committed.Value),
		SourceTxHash: committed.SourceTxHash,
		GasUsed:      committed.GasUsed,
	}
	conflictingDelayed := *delayed
	conflictingDelayed.GasUsed++
	added, err := pool.AddTxs2PoolUniqueByExecutionID([]*Transaction{delayed, &conflictingDelayed})
	if err != nil || added != 0 {
		t.Fatalf("delayed unique add: added=%d err=%v", added, err)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != retained {
		t.Fatalf("delayed unique add resurrected record: %#v", pool.TxQueue)
	}
	if !delayed.Time.IsZero() || !delayed.Broker1EnterPoolTime.IsZero() ||
		!conflictingDelayed.Time.IsZero() || !conflictingDelayed.Broker1EnterPoolTime.IsZero() {
		t.Fatal("ignored tombstoned records received lifecycle timestamps")
	}

	err = pool.ReplaceTransactions(nil, []*Transaction{delayed}, nil)
	if err != nil {
		t.Fatalf("replacement with delayed record: %v", err)
	}
	if len(pool.TxQueue) != 1 || pool.TxQueue[0] != retained {
		t.Fatalf("replacement resurrected tombstoned record: %#v", pool.TxQueue)
	}
}

func TestFollowerCommitRecordsOnlyExecutionIDsWithoutPendingCopies(t *testing.T) {
	pool := NewTxPool()
	top := &Transaction{SourceTxHash: "top", GasUsed: 40}
	relay := &Transaction{SourceTxHash: "relay", GasUsed: 60, Relayed: true}
	block := []*Transaction{top, relay}
	if err := pool.ValidateUncommittedExecutionRecords(block, 100); err != nil {
		t.Fatal(err)
	}
	if len(pool.committedExecutionIDs) != 0 {
		t.Fatal("validation modified committed identities")
	}
	if err := pool.CommitExecutionRecordsWithoutPool(block, 100); err != nil {
		t.Fatal(err)
	}
	if len(pool.TxQueue) != 0 || len(pool.RelayPool) != 0 || len(pool.committedExecutionIDs) != 2 {
		t.Fatalf("follower retained pending data: queue=%d relay=%d ids=%d", len(pool.TxQueue), len(pool.RelayPool), len(pool.committedExecutionIDs))
	}
	for _, tx := range block {
		if !tx.Time.IsZero() || !tx.Broker1EnterPoolTime.IsZero() || !tx.Broker1LeavePoolTime.IsZero() ||
			!tx.Broker2EnterPoolTime.IsZero() || !tx.Broker2LeavePoolTime.IsZero() {
			t.Fatal("follower fabricated pool lifecycle timestamps")
		}
	}
	if err := pool.ValidateUncommittedExecutionRecords(block, 100); !errors.Is(err, ErrExecutionAlreadyCommitted) {
		t.Fatalf("duplicate block record was not rejected: %v", err)
	}
	if added, err := pool.AddTxs2PoolUniqueByExecutionID(block); err != nil || added != 0 || len(pool.TxQueue) != 0 {
		t.Fatalf("late delivery resurrected committed records: added=%d err=%v", added, err)
	}
}

func TestFollowerCommitValidationIsAtomic(t *testing.T) {
	for _, name := range []string{"nil", "duplicate", "oversized", "sum-overflow", "committed", "pending-pool", "relay-pool"} {
		t.Run(name, func(t *testing.T) {
			pool := NewTxPool()
			a := &Transaction{SourceTxHash: "a", GasUsed: 40}
			b := &Transaction{SourceTxHash: "b", GasUsed: 60}
			txs, limit := []*Transaction{a, b}, uint64(100)
			switch name {
			case "nil":
				txs[1] = nil
			case "duplicate":
				txs[1] = a
			case "oversized":
				b.GasUsed = 101
			case "sum-overflow":
				limit, a.GasUsed, b.GasUsed = ^uint64(0), ^uint64(0)-1, 2
			case "committed":
				pool.committedExecutionIDs = make(map[string]struct{})
				pool.committedExecutionIDs[TransactionExecutionID(b)] = struct{}{}
			case "pending-pool":
				pool.TxQueue = []*Transaction{b}
			case "relay-pool":
				pool.RelayPool[1] = []*Transaction{b}
			}
			initialIDs := len(pool.committedExecutionIDs)
			initialPending, initialRelay := len(pool.TxQueue), len(pool.RelayPool[1])
			if err := pool.CommitExecutionRecordsWithoutPool(txs, limit); err == nil {
				t.Fatal("invalid follower commit succeeded")
			}
			if len(pool.committedExecutionIDs) != initialIDs || len(pool.TxQueue) != initialPending || len(pool.RelayPool[1]) != initialRelay {
				t.Fatal("failed validation partially changed retained state")
			}
			if _, ok := pool.committedExecutionIDs[TransactionExecutionID(a)]; ok {
				t.Fatal("failed validation prematurely marked first record committed")
			}
		})
	}
}
