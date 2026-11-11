// the define and some operation of txpool

package core

import (
	"blockEmulator/utils"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"
	"unsafe"
)

var ErrExecutionIDConflict = errors.New("execution record ID has conflicting semantics")
var ErrExecutionAlreadyCommitted = errors.New("execution record was already committed")

type TxPool struct {
	TxQueue               []*Transaction            // transaction Queue
	RelayPool             map[uint64][]*Transaction //designed for sharded blockchain, from Monoxide
	committedExecutionIDs map[string]struct{}
	planningWindow        *planningWindowIndex
	pendingMarkEpoch      uint64
	pendingMarkedSources  map[string]struct{}
	lock                  sync.Mutex
	// The pending list is ignored
}

type priorityQueueItem struct {
	index int
	key   string
}

type priorityQueueItems []priorityQueueItem

func (items priorityQueueItems) Len() int           { return len(items) }
func (items priorityQueueItems) Less(i, j int) bool { return items[i].index < items[j].index }
func (items priorityQueueItems) Swap(i, j int)      { items[i], items[j] = items[j], items[i] }
func (items *priorityQueueItems) Push(value interface{}) {
	*items = append(*items, value.(priorityQueueItem))
}
func (items *priorityQueueItems) Pop() interface{} {
	old := *items
	last := old[len(old)-1]
	*items = old[:len(old)-1]
	return last
}

func NewTxPool() *TxPool {
	return &TxPool{
		TxQueue:   make([]*Transaction, 0),
		RelayPool: make(map[uint64][]*Transaction),
	}
}

// Snapshot returns a complete deep copy of the pool without changing it.
// Queue order and every RelayPool sub-queue order are preserved. Transaction
// pointers, mutable byte slices, and big integers are independent from the
// live pool, so shadow execution may safely mutate its snapshot.
func (txpool *TxPool) Snapshot() *TxPool {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	snapshot := &TxPool{}
	cloned := make(map[*Transaction]*Transaction)
	cloneBytes := func(value []byte) []byte {
		if value == nil {
			return nil
		}
		copied := make([]byte, len(value))
		copy(copied, value)
		return copied
	}
	clone := func(tx *Transaction) *Transaction {
		if tx == nil {
			return nil
		}
		if copied, ok := cloned[tx]; ok {
			return copied
		}
		copied := *tx
		copied.Signature = cloneBytes(tx.Signature)
		copied.TxHash = cloneBytes(tx.TxHash)
		copied.RawTxHash = cloneBytes(tx.RawTxHash)
		copied.GLASCallRelations = append([]GLASCallRelation(nil), tx.GLASCallRelations...)
		if tx.Value != nil {
			copied.Value = new(big.Int).Set(tx.Value)
		}
		cloned[tx] = &copied
		return &copied
	}

	if txpool.TxQueue != nil {
		snapshot.TxQueue = make([]*Transaction, len(txpool.TxQueue))
		for index, tx := range txpool.TxQueue {
			snapshot.TxQueue[index] = clone(tx)
		}
	}
	if txpool.RelayPool != nil {
		snapshot.RelayPool = make(map[uint64][]*Transaction, len(txpool.RelayPool))
		for shardID, relayQueue := range txpool.RelayPool {
			if relayQueue == nil {
				snapshot.RelayPool[shardID] = nil
				continue
			}
			copiedQueue := make([]*Transaction, len(relayQueue))
			for index, tx := range relayQueue {
				copiedQueue[index] = clone(tx)
			}
			snapshot.RelayPool[shardID] = copiedQueue
		}
	}
	return snapshot
}

// isBroker2Tx: tx is a Broker2 tx iff it carries a RawTxHash and its Recipient equals FinalRecipient.
func isBroker2Tx(tx *Transaction) bool {
	return tx.RawTxHash != nil && tx.Recipient == tx.FinalRecipient
}

func blockGasCost(tx *Transaction) uint64 {
	return EffectiveGasUsed(tx)
}

func markPackedLeaveTime(txs []*Transaction) {
	now := time.Now()
	for _, tx := range txs {
		if isBroker2Tx(tx) {
			if tx.Broker2LeavePoolTime.IsZero() {
				tx.Broker2LeavePoolTime = now
			}
		} else {
			if tx.Broker1LeavePoolTime.IsZero() {
				tx.Broker1LeavePoolTime = now
			}
		}
	}
}

func markEnteredPoolTime(tx *Transaction, now time.Time) {
	if tx.Time.IsZero() {
		tx.Time = now
	}
	if isBroker2Tx(tx) {
		if tx.Broker2EnterPoolTime.IsZero() {
			tx.Broker2EnterPoolTime = now
		}
	} else if tx.Broker1EnterPoolTime.IsZero() {
		tx.Broker1EnterPoolTime = now
	}
}

// Add a transaction to the pool (consider the queue only)
func (txpool *TxPool) AddTx2Pool(tx *Transaction) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	now := time.Now()
	markEnteredPoolTime(tx, now)
	txpool.TxQueue = append(txpool.TxQueue, tx)
	txpool.invalidatePlanningWindowLocked()
}

// Add a list of transactions to the pool
func (txpool *TxPool) AddTxs2Pool(txs []*Transaction) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	now := time.Now()
	for _, tx := range txs {
		markEnteredPoolTime(tx, now)
		txpool.TxQueue = append(txpool.TxQueue, tx)
		txpool.invalidatePlanningWindowLocked()
	}
}

func executionRecordStage(tx *Transaction) uint8 {
	if tx.IsGLASCallTask {
		return 2
	}
	if tx.Relayed {
		return 1
	}
	return 0
}

// sameExecutionRecordSemantics deliberately ignores wire/pool lifecycle
// timestamps. Those may legitimately differ between replicas and do not
// change what the execution record does.
func sameExecutionRecordSemantics(a, b *Transaction) bool {
	if a == nil || b == nil {
		return a == b
	}
	if executionRecordStage(a) != executionRecordStage(b) ||
		a.SourceTxHash != b.SourceTxHash ||
		a.Sender != b.Sender ||
		a.Recipient != b.Recipient ||
		a.Nonce != b.Nonce ||
		a.GasUsed != b.GasUsed ||
		a.GLASBaseGas != b.GLASBaseGas ||
		a.GLASCallGasPerCall != b.GLASCallGasPerCall ||
		a.GLASCrossCallCount != b.GLASCrossCallCount ||
		a.GLASAttachedGas != b.GLASAttachedGas ||
		a.GLASCallTaskID != b.GLASCallTaskID ||
		a.GLASCallCallee != b.GLASCallCallee ||
		a.GLASCallOriginShard != b.GLASCallOriginShard ||
		a.GLASCallTargetShard != b.GLASCallTargetShard ||
		a.GLASCallCount != b.GLASCallCount ||
		a.GLASStageEpoch != b.GLASStageEpoch ||
		a.GLASStageEpochSet != b.GLASStageEpochSet ||
		a.TxCategory != b.TxCategory ||
		a.ExecDelayMs != b.ExecDelayMs ||
		a.HasBroker != b.HasBroker ||
		a.SenderIsBroker != b.SenderIsBroker ||
		a.OriginalSender != b.OriginalSender ||
		a.FinalRecipient != b.FinalRecipient {
		return false
	}
	if len(a.GLASCallRelations) != len(b.GLASCallRelations) {
		return false
	}
	for index, call := range a.GLASCallRelations {
		if call != b.GLASCallRelations[index] {
			return false
		}
	}
	if a.Value == nil || b.Value == nil {
		return a.Value == nil && b.Value == nil
	}
	return a.Value.Cmp(b.Value) == 0
}

func addLiveExecutionRecord(known map[string]*Transaction, tx *Transaction) error {
	if tx == nil {
		return fmt.Errorf("%w: live pool contains a nil record", ErrExecutionIDConflict)
	}
	id := TransactionExecutionID(tx)
	if existing, exists := known[id]; exists {
		if !sameExecutionRecordSemantics(existing, tx) {
			return fmt.Errorf("%w: live pool records for %q differ", ErrExecutionIDConflict, id)
		}
		return fmt.Errorf("%w: live pool repeats %q", ErrExecutionIDNotUnique, id)
	}
	known[id] = tx
	return nil
}

// uniqueExecutionIDAdditionsWithKnown consumes a call-local ID map. On failure
// the caller discards it together with the unpublished additions.
func uniqueExecutionIDAdditionsWithKnown(
	known map[string]*Transaction,
	additions []*Transaction,
	committedExecutionIDs map[string]struct{},
) ([]*Transaction, error) {
	pending := make([]*Transaction, 0, len(additions))
	for _, tx := range additions {
		if tx == nil {
			return nil, fmt.Errorf("%w: incoming record is nil", ErrExecutionIDConflict)
		}
		id := TransactionExecutionID(tx)
		if _, committed := committedExecutionIDs[id]; committed {
			// A delayed injection/relay broadcast must not resurrect an execution
			// record already reconciled from a committed PBFT block. Tombstones
			// deliberately win even if the stale payload differs semantically.
			continue
		}
		if existing, exists := known[id]; exists {
			if !sameExecutionRecordSemantics(existing, tx) {
				return nil, fmt.Errorf("%w: records for %q differ", ErrExecutionIDConflict, id)
			}
			continue
		}
		known[id] = tx
		pending = append(pending, tx)
	}
	return pending, nil
}

// uniqueExecutionIDAdditions validates additions against queue and against
// earlier additions without changing either input. Equal records are
// idempotent; the first semantic conflict aborts the complete batch.
func uniqueExecutionIDAdditions(
	queue, additions []*Transaction,
	committedExecutionIDs map[string]struct{},
) ([]*Transaction, error) {
	known := make(map[string]*Transaction, len(queue)+len(additions))
	for _, tx := range queue {
		if err := addLiveExecutionRecord(known, tx); err != nil {
			return nil, err
		}
	}
	return uniqueExecutionIDAdditionsWithKnown(known, additions, committedExecutionIDs)
}

// AddTxs2PoolUniqueByExecutionID atomically appends records not already in the
// pool. An equal stable ID with equal execution semantics is an idempotent
// replay; a semantic mismatch rejects the complete batch without timestamps
// or partial additions. IDs already committed from a block are tombstoned and
// ignored so delayed network broadcasts cannot resurrect executed records.
func (txpool *TxPool) AddTxs2PoolUniqueByExecutionID(txs []*Transaction) (added int, err error) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	pending, err := uniqueExecutionIDAdditions(txpool.TxQueue, txs, txpool.committedExecutionIDs)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	for _, tx := range pending {
		markEnteredPoolTime(tx, now)
	}
	txpool.TxQueue = append(txpool.TxQueue, pending...)
	txpool.appendPlanningWindowLocked(pending)
	return len(pending), nil
}

// InstallRepricedExecutionRecords atomically prepares the live queue and an
// incoming migration batch, then admits unique non-tombstoned incoming records.
// Its call-local ID map is built while preparing the live queue and therefore
// compares the prepared prices without another full-pool scan. Queue order is
// preserved and any error leaves the pool and lifecycle timestamps unchanged.
//
// Both callbacks are required and run under the pool lock. They must not mutate
// their inputs or acquire chain/pool locks; capture the routing resolver before
// calling this method. prepareExisting may reuse an unchanged record, while
// prepareIncoming must return an owned copy for successful entry timestamps.
func (txpool *TxPool) InstallRepricedExecutionRecords(
	incoming []*Transaction,
	prepareExisting func(*Transaction) (*Transaction, error),
	prepareIncoming func(*Transaction) (*Transaction, error),
) (added int, err error) {
	if prepareExisting == nil || prepareIncoming == nil {
		return 0, fmt.Errorf("install repriced execution records requires both preparation callbacks")
	}
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	updated := make([]*Transaction, len(txpool.TxQueue), len(txpool.TxQueue)+len(incoming))
	known := make(map[string]*Transaction, cap(updated))
	var liveIDError error
	for index, tx := range txpool.TxQueue {
		prepared, err := prepareExisting(tx)
		if err != nil {
			return 0, err
		}
		updated[index] = prepared
		if liveIDError == nil {
			liveIDError = addLiveExecutionRecord(known, prepared)
		}
	}
	preparedIncoming := make([]*Transaction, len(incoming))
	for index, tx := range incoming {
		prepared, err := prepareIncoming(tx)
		if err != nil {
			return 0, err
		}
		preparedIncoming[index] = prepared
	}
	// Preparation historically preceded ID validation for both batches. Keep
	// that error precedence even though live IDs are now checked in the loop.
	if liveIDError != nil {
		return 0, liveIDError
	}
	pending, err := uniqueExecutionIDAdditionsWithKnown(known, preparedIncoming, txpool.committedExecutionIDs)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	for _, tx := range pending {
		markEnteredPoolTime(tx, now)
	}
	txpool.TxQueue = append(updated, pending...)
	txpool.invalidatePlanningWindowLocked()
	return len(pending), nil
}

// ReplaceTransactions applies one migration update under the pool lock:
// remove matching records, idempotently add non-tombstoned execution records,
// then stably sort the resulting queue. On validation failure the original
// queue and all lifecycle timestamps remain unchanged. Callbacks must not call
// TxPool methods because they execute while the pool lock is held.
func (txpool *TxPool) ReplaceTransactions(
	remove func(*Transaction) bool,
	additions []*Transaction,
	less func(a, b *Transaction) bool,
) error {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	retained := make([]*Transaction, 0, len(txpool.TxQueue))
	for _, tx := range txpool.TxQueue {
		if remove == nil || !remove(tx) {
			retained = append(retained, tx)
		}
	}
	pending, err := uniqueExecutionIDAdditions(retained, additions, txpool.committedExecutionIDs)
	if err != nil {
		return err
	}
	updated := append(retained, pending...)
	if less != nil {
		sort.SliceStable(updated, func(i, j int) bool { return less(updated[i], updated[j]) })
	}
	now := time.Now()
	for _, tx := range pending {
		markEnteredPoolTime(tx, now)
	}
	txpool.TxQueue = updated
	txpool.invalidatePlanningWindowLocked()
	return nil
}

// add transactions into the pool head
func (txpool *TxPool) AddTxs2Pool_Head(tx []*Transaction) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	txpool.TxQueue = append(tx, txpool.TxQueue...)
	txpool.invalidatePlanningWindowLocked()
}

// Pack transactions for a proposal
func (txpool *TxPool) PackTxs(max_txs uint64) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	txNum := max_txs
	if uint64(len(txpool.TxQueue)) < txNum {
		txNum = uint64(len(txpool.TxQueue))
	}
	txs_Packed := txpool.TxQueue[:txNum]
	txpool.TxQueue = txpool.TxQueue[txNum:]
	txpool.invalidatePlanningWindowLocked()
	markPackedLeaveTime(txs_Packed)
	return txs_Packed
}

// Pack transactions for a proposal using an Ethereum-style block gas limit.
func (txpool *TxPool) PackTxsWithGasLimit(maxGas uint64) []*Transaction {
	return txpool.PackTxsWithGasLimitAndCost(maxGas, blockGasCost)
}

// PackTxsWithGasLimitAndCost packs transactions using the supplied gas-cost
// function. This lets the blockchain charge relay-stage gas without mutating
// the transaction's original GasUsed value.
func (txpool *TxPool) PackTxsWithGasLimitAndCost(maxGas uint64, gasCost func(*Transaction) uint64) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	if maxGas == 0 || len(txpool.TxQueue) == 0 {
		return []*Transaction{}
	}
	if gasCost == nil {
		gasCost = blockGasCost
	}

	txNum := 0
	currentGas := uint64(0)
	for txNum < len(txpool.TxQueue) {
		txGas := gasCost(txpool.TxQueue[txNum])
		if txGas > maxGas {
			panic(fmt.Sprintf("tx gasUsed %d exceeds block gas limit %d", txGas, maxGas))
		}
		if txGas > maxGas-currentGas {
			break
		}
		currentGas += txGas
		txNum++
	}

	txs_Packed := txpool.TxQueue[:txNum]
	txpool.TxQueue = txpool.TxQueue[txNum:]
	txpool.invalidatePlanningWindowLocked()
	markPackedLeaveTime(txs_Packed)
	return txs_Packed
}

// PackTxsWithGasLimitAndCostPriority packs the priority class first and then
// fills the remaining gas with ordinary records. Relative order is preserved
// inside each class and among all retained records. GLAS-FREIGHT uses this to
// finish Relay2/call-task work before starting more top-level transactions.
func (txpool *TxPool) PackTxsWithGasLimitAndCostPriority(
	maxGas uint64,
	gasCost func(*Transaction) uint64,
	priority func(*Transaction) bool,
) []*Transaction {
	if priority == nil {
		return []*Transaction{}
	}
	return txpool.PackTxsWithGasLimitAndCostPriorityLevels(
		maxGas,
		gasCost,
		func(tx *Transaction) int {
			if priority(tx) {
				return 0
			}
			return 1
		},
		2,
	)
}

// PackTxsWithGasLimitAndCostPriorityLevels packs lower-numbered priority
// levels first, then uses lower-priority records to fill any remaining gas.
// Relative queue order is preserved within every level and among retained
// records. GLAS-FREIGHT classifies three levels:
//
//  0. Relay2 and remote call tasks
//  1. transactions selected by the active next-epoch workset
//  2. other old backlog and newly injected transactions
//
// GLAS-FREIGHT passes all three levels, so level 2 remains eligible filler;
// callers may still pass a smaller level count when they intentionally need a
// filtered priority prefix.
func (txpool *TxPool) PackTxsWithGasLimitAndCostPriorityLevels(
	maxGas uint64,
	gasCost func(*Transaction) uint64,
	priorityLevel func(*Transaction) int,
	levelCount int,
) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	if maxGas == 0 || len(txpool.TxQueue) == 0 || priorityLevel == nil || levelCount <= 0 {
		return []*Transaction{}
	}
	if gasCost == nil {
		gasCost = blockGasCost
	}

	selected := make([]bool, len(txpool.TxQueue))
	packed := make([]*Transaction, 0)
	currentGas := uint64(0)
	packLevel := func(wantLevel int) {
		capacityReached := false
		for index, tx := range txpool.TxQueue {
			if selected[index] || priorityLevel(tx) != wantLevel || capacityReached {
				continue
			}
			txGas := gasCost(tx)
			if txGas > maxGas {
				panic(fmt.Sprintf("tx gasUsed %d exceeds block gas limit %d", txGas, maxGas))
			}
			if txGas > maxGas-currentGas {
				// Do not reorder records inside a class merely to improve
				// packing. The other class may still use the remaining gas.
				capacityReached = true
				continue
			}
			currentGas += txGas
			selected[index] = true
			packed = append(packed, tx)
		}
	}
	for level := 0; level < levelCount; level++ {
		packLevel(level)
	}

	retained := make([]*Transaction, 0, len(txpool.TxQueue)-len(packed))
	for index, tx := range txpool.TxQueue {
		if !selected[index] {
			retained = append(retained, tx)
		}
	}
	txpool.TxQueue = retained
	txpool.invalidatePlanningWindowLocked()
	markPackedLeaveTime(packed)
	return packed
}

// PackTxsWithGasLimitAndCostPriorityLevelsDependencyAware is the
// work-conserving form used by GLAS-FREIGHT. It may skip a record that does
// not fit the remaining block gas and pack a later independent record, but it
// never skips past an earlier record with the same non-empty dependency key.
// This preserves per-sender/per-logical-transaction ordering while avoiding
// gas fragments caused by an unrelated oversized queue record.
func (txpool *TxPool) PackTxsWithGasLimitAndCostPriorityLevelsDependencyAware(
	maxGas uint64,
	gasCost func(*Transaction) uint64,
	priorityLevel func(*Transaction) int,
	levelCount int,
	dependencyKey func(*Transaction) string,
) []*Transaction {
	return txpool.PackTxsWithSelection(TxSelectionOptions{
		MaxGas:        maxGas,
		GasCost:       gasCost,
		PriorityLevel: priorityLevel,
		LevelCount:    levelCount,
		DependencyKey: dependencyKey,
	})
}

// PackTxsWithSelection commits the shared pure selection result to the live
// pool. This is the real-execution counterpart of SelectTransactions. In
// particular, AllowedExecutionIDs acts as a hard whitelist: records absent
// from it are retained in original queue order and receive no leave timestamp.
func (txpool *TxPool) PackTxsWithSelection(options TxSelectionOptions) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	if options.MaxGas == 0 || len(txpool.TxQueue) == 0 || options.PriorityLevel == nil || options.LevelCount <= 0 {
		return []*Transaction{}
	}
	options.OmitRetained = false // Destructive packing requires the remaining queue.
	result := SelectTransactions(txpool.TxQueue, options)
	txpool.TxQueue = result.Retained
	txpool.invalidatePlanningWindowLocked()
	markPackedLeaveTime(result.Packed)
	return result.Packed
}

// PeekTxsWithSelection previews the shared deterministic selection without
// removing records or writing leave-pool timestamps. The returned slice has
// independent backing storage and follows the same execution order as
// PackTxsWithSelection.
func (txpool *TxPool) PeekTxsWithSelection(options TxSelectionOptions) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	return SelectTransactions(txpool.TxQueue, options).Packed
}

type exactExecutionSelection struct {
	packed   []*Transaction
	selected []bool
}

// selectExactExecutionIDs validates and resolves one exact manifest while the
// caller holds txpool.lock. It never mutates the pool or transaction lifecycle
// timestamps. Missing records are an expected not-ready state; malformed
// manifests are errors.
func (txpool *TxPool) selectExactExecutionIDs(
	ids []string,
	maxGas uint64,
	gasCost func(*Transaction) uint64,
) (selection exactExecutionSelection, ready bool, err error) {
	if len(ids) == 0 {
		return exactExecutionSelection{
			packed:   []*Transaction{},
			selected: make([]bool, len(txpool.TxQueue)),
		}, true, nil
	}
	if gasCost == nil {
		gasCost = blockGasCost
	}

	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, exists := wanted[id]; exists {
			return exactExecutionSelection{}, false, fmt.Errorf("%w: manifest repeats %q", ErrExecutionIDNotUnique, id)
		}
		wanted[id] = struct{}{}
	}

	positions := make(map[string][]int, len(wanted))
	for index, tx := range txpool.TxQueue {
		id := TransactionExecutionID(tx)
		if _, requested := wanted[id]; requested {
			positions[id] = append(positions[id], index)
		}
	}

	packed := make([]*Transaction, len(ids))
	selected := make([]bool, len(txpool.TxQueue))
	totalGas := uint64(0)
	for manifestIndex, id := range ids {
		matches := positions[id]
		switch len(matches) {
		case 0:
			return exactExecutionSelection{}, false, nil
		case 1:
			// Continue below.
		default:
			return exactExecutionSelection{}, false, fmt.Errorf("%w: pool contains %d records for %q", ErrExecutionIDNotUnique, len(matches), id)
		}

		queueIndex := matches[0]
		tx := txpool.TxQueue[queueIndex]
		txGas := gasCost(tx)
		if txGas > maxGas || txGas > maxGas-totalGas {
			return exactExecutionSelection{}, false, fmt.Errorf("%w: adding %q costs %d after %d of %d", ErrExecutionGasLimit, id, txGas, totalGas, maxGas)
		}
		totalGas += txGas
		packed[manifestIndex] = tx
		selected[queueIndex] = true
	}
	return exactExecutionSelection{packed: packed, selected: selected}, true, nil
}

// PeekExactExecutionIDs resolves one exact manifest without removing any pool
// record or writing a leave-pool timestamp. The returned transactions follow
// manifest order. A missing record reports ready=false without an error, so a
// proposal can be retried after its descendants arrive.
func (txpool *TxPool) PeekExactExecutionIDs(
	ids []string,
	maxGas uint64,
	gasCost func(*Transaction) uint64,
) (packed []*Transaction, ready bool, err error) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	selection, ready, err := txpool.selectExactExecutionIDs(ids, maxGas, gasCost)
	if err != nil || !ready {
		return nil, ready, err
	}
	return selection.packed, true, nil
}

// validateUncommittedExecutionRecords is independent of pending-pool contents.
// GLAS followers validate the proposal itself and retain only committed-ID
// tombstones, not a replica of the leader's pending transactions. Caller holds
// txpool.lock. Validation must finish before any tombstone is installed.
func (txpool *TxPool) validateUncommittedExecutionRecords(txs []*Transaction, maxGas uint64) error {
	seen := make(map[string]struct{}, len(txs))
	var total uint64
	for _, tx := range txs {
		if tx == nil {
			return fmt.Errorf("%w: nil committed record", ErrExecutionIDConflict)
		}
		id := TransactionExecutionID(tx)
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("%w: block repeats %q", ErrExecutionIDNotUnique, id)
		}
		if _, committed := txpool.committedExecutionIDs[id]; committed {
			return fmt.Errorf("%w: %q", ErrExecutionAlreadyCommitted, id)
		}
		seen[id] = struct{}{}
		gas := blockGasCost(tx)
		if gas > maxGas || gas > maxGas-total {
			return fmt.Errorf("%w: adding %q costs %d after %d of %d", ErrExecutionGasLimit, id, gas, total, maxGas)
		}
		total += gas
	}
	return nil
}

func (txpool *TxPool) ValidateUncommittedExecutionRecords(txs []*Transaction, maxGas uint64) error {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	return txpool.validateUncommittedExecutionRecords(txs, maxGas)
}

// CommitExecutionRecordsWithoutPool is the follower commit path. It records
// replay protection without enqueuing transactions, changing pool timestamps,
// or retaining transaction/call objects. A nonempty follower queue is a role
// violation and must not be silently discarded or treated as a recovered pool.
func (txpool *TxPool) CommitExecutionRecordsWithoutPool(txs []*Transaction, maxGas uint64) error {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	if len(txpool.TxQueue) != 0 {
		return fmt.Errorf("GLAS follower unexpectedly owns %d pending records", len(txpool.TxQueue))
	}
	for _, records := range txpool.RelayPool {
		if len(records) != 0 {
			return fmt.Errorf("GLAS follower unexpectedly owns outgoing relay records")
		}
	}
	if err := txpool.validateUncommittedExecutionRecords(txs, maxGas); err != nil {
		return err
	}
	if len(txs) != 0 && txpool.committedExecutionIDs == nil {
		txpool.committedExecutionIDs = make(map[string]struct{}, len(txs))
	}
	for _, tx := range txs {
		txpool.committedExecutionIDs[TransactionExecutionID(tx)] = struct{}{}
	}
	return nil
}

// CommitExactExecutionIDs atomically removes one exact manifest from the pool
// and writes leave-pool timestamps. Every ID must still occur exactly once and
// the whole manifest must fit maxGas. If any ID is missing (including after a
// successful earlier commit), committed is false and the pool is unchanged.
func (txpool *TxPool) CommitExactExecutionIDs(
	ids []string,
	maxGas uint64,
	gasCost func(*Transaction) uint64,
) (packed []*Transaction, committed bool, err error) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	selection, ready, err := txpool.selectExactExecutionIDs(ids, maxGas, gasCost)
	if err != nil || !ready {
		return nil, false, err
	}

	retained := make([]*Transaction, 0, len(txpool.TxQueue)-len(selection.packed))
	for index, tx := range txpool.TxQueue {
		if !selection.selected[index] {
			retained = append(retained, tx)
		}
	}
	txpool.TxQueue = retained
	txpool.removePlanningWindowLocked(selection.packed)
	markPackedLeaveTime(selection.packed)
	if len(ids) > 0 {
		if txpool.committedExecutionIDs == nil {
			txpool.committedExecutionIDs = make(map[string]struct{}, len(ids))
		}
		for _, id := range ids {
			txpool.committedExecutionIDs[id] = struct{}{}
		}
	}
	return selection.packed, true, nil
}

// PackExactExecutionIDs is kept for compatibility with existing callers. It
// has the original destructive semantics and commits the manifest atomically.
// Proposal construction should call PeekExactExecutionIDs instead.
func (txpool *TxPool) PackExactExecutionIDs(
	ids []string,
	maxGas uint64,
	gasCost func(*Transaction) uint64,
) (packed []*Transaction, ready bool, err error) {
	return txpool.CommitExactExecutionIDs(ids, maxGas, gasCost)
}

// PackTxsWithGasLimitAndCostFiltered packs only transactions accepted by
// include while preserving the relative order of both packed and retained
// records. It is used by reconfiguration protocols that must finish already
// started execution stages without starting queued top-level transactions.
func (txpool *TxPool) PackTxsWithGasLimitAndCostFiltered(
	maxGas uint64,
	gasCost func(*Transaction) uint64,
	include func(*Transaction) bool,
) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	if maxGas == 0 || len(txpool.TxQueue) == 0 || include == nil {
		return []*Transaction{}
	}
	if gasCost == nil {
		gasCost = blockGasCost
	}

	packed := make([]*Transaction, 0)
	retained := make([]*Transaction, 0, len(txpool.TxQueue))
	currentGas := uint64(0)
	capacityReached := false
	for _, tx := range txpool.TxQueue {
		if capacityReached || !include(tx) {
			retained = append(retained, tx)
			continue
		}
		txGas := gasCost(tx)
		if txGas > maxGas {
			panic(fmt.Sprintf("tx gasUsed %d exceeds block gas limit %d", txGas, maxGas))
		}
		if txGas > maxGas-currentGas {
			capacityReached = true
			retained = append(retained, tx)
			continue
		}
		currentGas += txGas
		packed = append(packed, tx)
	}
	txpool.TxQueue = retained
	txpool.invalidatePlanningWindowLocked()
	markPackedLeaveTime(packed)
	return packed
}

// PackTxsWithGasLimitAndCostFilteredFill is the gas-filling variant for a set
// of independent execution tasks. Unlike the FIFO filtered packer, a task that
// does not fit does not prevent a later, smaller task from using the remaining
// gas. Packed and retained records each keep their original relative order.
func (txpool *TxPool) PackTxsWithGasLimitAndCostFilteredFill(
	maxGas uint64,
	gasCost func(*Transaction) uint64,
	include func(*Transaction) bool,
) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	if maxGas == 0 || len(txpool.TxQueue) == 0 || include == nil {
		return []*Transaction{}
	}
	if gasCost == nil {
		gasCost = blockGasCost
	}

	packed := make([]*Transaction, 0)
	retained := make([]*Transaction, 0, len(txpool.TxQueue))
	currentGas := uint64(0)
	for _, tx := range txpool.TxQueue {
		if !include(tx) {
			retained = append(retained, tx)
			continue
		}
		txGas := gasCost(tx)
		if txGas > maxGas {
			panic(fmt.Sprintf("tx gasUsed %d exceeds block gas limit %d", txGas, maxGas))
		}
		if txGas > maxGas-currentGas {
			retained = append(retained, tx)
			continue
		}
		currentGas += txGas
		packed = append(packed, tx)
	}
	txpool.TxQueue = retained
	txpool.invalidatePlanningWindowLocked()
	markPackedLeaveTime(packed)
	return packed
}

// Pack transaction for a proposal (use 'BlocksizeInBytes' to control)
func (txpool *TxPool) PackTxsWithBytes(max_bytes int) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	txNum := len(txpool.TxQueue)
	currentSize := 0
	for tx_idx, tx := range txpool.TxQueue {
		currentSize += int(unsafe.Sizeof(*tx))
		if currentSize > max_bytes {
			txNum = tx_idx
			break
		}
	}

	txs_Packed := txpool.TxQueue[:txNum]
	txpool.TxQueue = txpool.TxQueue[txNum:]
	txpool.invalidatePlanningWindowLocked()
	markPackedLeaveTime(txs_Packed)
	return txs_Packed
}

// Relay transactions
func (txpool *TxPool) AddRelayTx(tx *Transaction, shardID uint64) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	_, ok := txpool.RelayPool[shardID]
	if !ok {
		txpool.RelayPool[shardID] = make([]*Transaction, 0)
	}
	txpool.RelayPool[shardID] = append(txpool.RelayPool[shardID], tx)
}

// txpool get locked
func (txpool *TxPool) GetLocked() {
	txpool.lock.Lock()
}

// txpool get unlocked
func (txpool *TxPool) GetUnlocked() {
	txpool.lock.Unlock()
}

// get the length of tx queue
func (txpool *TxPool) GetTxQueueLen() int {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	return len(txpool.TxQueue)
}

// get the length of ClearRelayPool
func (txpool *TxPool) ClearRelayPool() {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	txpool.RelayPool = nil
}

// abort ! Pack relay transactions from relay pool
func (txpool *TxPool) PackRelayTxs(shardID, minRelaySize, maxRelaySize uint64) ([]*Transaction, bool) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	if _, ok := txpool.RelayPool[shardID]; !ok {
		return nil, false
	}
	if len(txpool.RelayPool[shardID]) < int(minRelaySize) {
		return nil, false
	}
	txNum := maxRelaySize
	if uint64(len(txpool.RelayPool[shardID])) < txNum {
		txNum = uint64(len(txpool.RelayPool[shardID]))
	}
	relayTxPacked := txpool.RelayPool[shardID][:txNum]
	txpool.RelayPool[shardID] = txpool.RelayPool[shardID][txNum:]
	return relayTxPacked, true
}

// abort ! Transfer transactions when re-sharding
func (txpool *TxPool) TransferTxs(addr utils.Address) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	txTransfered := make([]*Transaction, 0)
	newTxQueue := make([]*Transaction, 0)
	for _, tx := range txpool.TxQueue {
		if tx.Sender == addr {
			txTransfered = append(txTransfered, tx)
		} else {
			newTxQueue = append(newTxQueue, tx)
		}
	}
	newRelayPool := make(map[uint64][]*Transaction)
	for shardID, shardPool := range txpool.RelayPool {
		for _, tx := range shardPool {
			if tx.Sender == addr {
				txTransfered = append(txTransfered, tx)
			} else {
				if _, ok := newRelayPool[shardID]; !ok {
					newRelayPool[shardID] = make([]*Transaction, 0)
				}
				newRelayPool[shardID] = append(newRelayPool[shardID], tx)
			}
		}
	}
	txpool.TxQueue = newTxQueue
	txpool.invalidatePlanningWindowLocked()
	txpool.RelayPool = newRelayPool
	return txTransfered
}
