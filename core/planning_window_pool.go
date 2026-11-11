package core

import (
	"container/heap"
	"fmt"
)

// The index owns references, not transaction bodies. Dependency groups cover
// the real pool so a selected TOP cannot overtake an unselected predecessor.
// Groups are built at installation, then maintained on ordinary admission and
// commit. Unusual legacy queue mutations invalidate the index for lazy rebuild.
type planningWindowCandidate struct {
	tx    *Transaction
	order int
}

type planningWindowGroup struct {
	records  []planningWindowCandidate
	head     int
	selected int
}

type planningWindowIndex struct {
	sources      map[string]struct{}
	groups       map[string]*planningWindowGroup
	windowGroups map[string]struct{}
	nextOrder    int
	dirty        bool
}

func planningWindowKey(tx *Transaction) string {
	if key := ExecutionDependency(tx); key != "" {
		return key
	}
	return "\x00" + TransactionExecutionID(tx)
}

func (index *planningWindowIndex) selected(tx *Transaction) bool {
	_, ok := index.sources[tx.SourceTxHash]
	return ok
}

func (index *planningWindowIndex) appendRecords(txs []*Transaction) {
	if index.dirty {
		return
	}
	for _, tx := range txs {
		index.appendRecord(tx)
	}
}

func (index *planningWindowIndex) appendRecord(tx *Transaction) {
	key := planningWindowKey(tx)
	group := index.groups[key]
	if group == nil {
		group = &planningWindowGroup{}
		index.groups[key] = group
	}
	group.records = append(group.records, planningWindowCandidate{tx: tx, order: index.nextOrder})
	index.nextOrder++
	if index.selected(tx) {
		group.selected++
		index.windowGroups[key] = struct{}{}
	}
}

func buildPlanningWindowIndex(queue []*Transaction, sources map[string]struct{}) (*planningWindowIndex, error) {
	index := &planningWindowIndex{sources: sources, groups: make(map[string]*planningWindowGroup), windowGroups: make(map[string]struct{})}
	seen := make(map[string]struct{}, len(queue))
	for _, tx := range queue {
		if tx == nil {
			return nil, fmt.Errorf("%w: nil planning-window pool record", ErrExecutionIDConflict)
		}
		id := TransactionExecutionID(tx)
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("%w: planning-window pool repeats %q", ErrExecutionIDNotUnique, id)
		}
		seen[id] = struct{}{}
		index.appendRecord(tx)
	}
	return index, nil
}

// InstallPlanningWindow replaces the previous epoch's membership. The caller
// must install it after migration/repricing so all references point at current
// records. Source identity includes later Relay2 arrivals without precreating
// them or treating unrelated fresh TOPs as planned work.
func (txpool *TxPool) InstallPlanningWindow(sources []string) error {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	membership := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if source == "" {
			return fmt.Errorf("planning window has an empty source identity")
		}
		if _, duplicate := membership[source]; duplicate {
			return fmt.Errorf("planning window repeats source %q", source)
		}
		membership[source] = struct{}{}
	}
	index, err := buildPlanningWindowIndex(txpool.TxQueue, membership)
	if err != nil {
		return err
	}
	txpool.planningWindow = index
	return nil
}

func (txpool *TxPool) ClearPlanningWindow() {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	txpool.planningWindow = nil
}

func (txpool *TxPool) invalidatePlanningWindowLocked() {
	if txpool.planningWindow != nil {
		txpool.planningWindow.dirty = true
	}
}

func (txpool *TxPool) appendPlanningWindowLocked(txs []*Transaction) {
	if txpool.planningWindow != nil {
		txpool.planningWindow.appendRecords(txs)
	}
}

// Normal selection commits dependency prefixes, so deleting them only visits
// the committed records and their groups. Catch-up may delete a non-prefix;
// that uncommon path invalidates the index instead of retaining stale refs.
func (txpool *TxPool) removePlanningWindowLocked(records []*Transaction) {
	index := txpool.planningWindow
	if index == nil || index.dirty {
		return
	}
	wanted := make(map[string]struct{}, len(records))
	counts := make(map[string]int)
	for _, tx := range records {
		wanted[TransactionExecutionID(tx)] = struct{}{}
		counts[planningWindowKey(tx)]++
	}
	for key, count := range counts {
		group := index.groups[key]
		if group == nil {
			index.dirty = true
			return
		}
		removed := 0
		for group.head < len(group.records) {
			record := group.records[group.head]
			if _, ok := wanted[TransactionExecutionID(record.tx)]; !ok {
				break
			}
			if index.selected(record.tx) {
				group.selected--
			}
			group.records[group.head] = planningWindowCandidate{}
			group.head++
			removed++
		}
		if removed != count {
			index.dirty = true
			return
		}
		if group.selected == 0 {
			delete(index.windowGroups, key)
		}
		if group.head == len(group.records) {
			delete(index.groups, key)
		}
	}
}

type planningWindowHead struct {
	key    string
	record planningWindowCandidate
}

type planningWindowHeads []planningWindowHead

func (heads planningWindowHeads) Len() int { return len(heads) }
func (heads planningWindowHeads) Less(i, j int) bool {
	return heads[i].record.order < heads[j].record.order
}
func (heads planningWindowHeads) Swap(i, j int) { heads[i], heads[j] = heads[j], heads[i] }
func (heads *planningWindowHeads) Push(value interface{}) {
	*heads = append(*heads, value.(planningWindowHead))
}
func (heads *planningWindowHeads) Pop() interface{} {
	last := len(*heads) - 1
	record := (*heads)[last]
	*heads = (*heads)[:last]
	return record
}

// PeekWindowTransactions uses the fixed execution dependency definition and
// four levels: selected Relay2, selected TOP, other Relay2, other TOP. It is a
// read-only preview over positive-Gas execution stages. MaxGas, GasCost and an
// optional AllowedExecutionIDs whitelist are honored; PriorityLevel and
// DependencyKey are deliberately fixed to the shared execution semantics.
// Only window-bearing groups are visited initially. Ordinary groups are loaded
// lazily if the selected work cannot fill the block. No remaining list is made.
func (txpool *TxPool) PeekWindowTransactions(options TxSelectionOptions) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	return txpool.peekWindowTransactionsLocked(options)
}

func (txpool *TxPool) peekWindowTransactionsLocked(options TxSelectionOptions) []*Transaction {
	if options.MaxGas == 0 || len(txpool.TxQueue) == 0 {
		return []*Transaction{}
	}
	if txpool.planningWindow == nil {
		options.PriorityLevel, options.LevelCount, options.DependencyKey = ExecutionPriority, 2, ExecutionDependency
		options.OmitRetained = true
		return SelectTransactions(txpool.TxQueue, options).Packed
	}
	index := txpool.planningWindow
	if index.dirty {
		var err error
		index, err = buildPlanningWindowIndex(txpool.TxQueue, index.sources)
		if err != nil {
			panic(err)
		}
		txpool.planningWindow = index
	}
	gasCost := options.GasCost
	if gasCost == nil {
		gasCost = blockGasCost
	}
	positions := make(map[string]int)
	blocked := make(map[string]bool)
	queued := make(map[string]bool)
	var heads [4]planningWindowHeads
	ordinaryLoaded := false
	position := func(key string) int {
		if value, exists := positions[key]; exists {
			return value
		}
		return index.groups[key].head
	}
	push := func(key string) {
		group := index.groups[key]
		if group == nil || blocked[key] || queued[key] {
			return
		}
		at := position(key)
		if at >= len(group.records) {
			return
		}
		record := group.records[at]
		if options.AllowedExecutionIDs != nil {
			if _, allowed := options.AllowedExecutionIDs[TransactionExecutionID(record.tx)]; !allowed {
				blocked[key] = true
				return
			}
		}
		level := ExecutionPriority(record.tx)
		if !index.selected(record.tx) {
			level += 2
		}
		if level >= 2 && !ordinaryLoaded {
			return
		}
		heap.Push(&heads[level], planningWindowHead{key: key, record: record})
		queued[key] = true
	}
	for key := range index.windowGroups {
		push(key)
	}
	packed := make([]*Transaction, 0)
	var used uint64
	for used < options.MaxGas {
		level := -1
		for candidate := range heads {
			if heads[candidate].Len() != 0 {
				level = candidate
				break
			}
		}
		if level < 0 {
			if ordinaryLoaded {
				break
			}
			ordinaryLoaded = true
			for key := range index.groups {
				push(key)
			}
			continue
		}
		item := heap.Pop(&heads[level]).(planningWindowHead)
		queued[item.key] = false
		tx := item.record.tx
		gas := gasCost(tx)
		if gas > options.MaxGas {
			if tx.GLASBaseGas > 0 && tx.GLASBaseGas <= options.MaxGas {
				blocked[item.key] = true
				continue
			}
			panic(fmt.Sprintf("tx gasUsed %d exceeds block gas limit %d", gas, options.MaxGas))
		}
		if gas > options.MaxGas-used {
			blocked[item.key] = true
			continue
		}
		packed = append(packed, tx)
		used += gas
		positions[item.key] = position(item.key) + 1
		push(item.key)
	}
	return packed
}

// CommitUnifiedExecutionRecords reconciles an already accepted block with the
// pending queue in one pass. Missing records (e.g. catch-up) are never enqueued.
// All block and queue checks complete before queue, timestamps or tombstones
// change. This is opt-in: legacy exact-manifest commit remains unchanged.
func (txpool *TxPool) CommitUnifiedExecutionRecords(txs []*Transaction, maxGas uint64) error {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	if err := txpool.validateUncommittedExecutionRecords(txs, maxGas); err != nil {
		return err
	}
	wanted := make(map[string]*Transaction, len(txs))
	for _, tx := range txs {
		wanted[TransactionExecutionID(tx)] = tx
	}
	seen := make(map[string]struct{}, len(txpool.TxQueue))
	retained := make([]*Transaction, 0, len(txpool.TxQueue))
	matched := make([]*Transaction, 0, len(txs))
	for _, tx := range txpool.TxQueue {
		if tx == nil {
			return fmt.Errorf("%w: nil pool record during commit", ErrExecutionIDConflict)
		}
		id := TransactionExecutionID(tx)
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("%w: pool repeats %q", ErrExecutionIDNotUnique, id)
		}
		seen[id] = struct{}{}
		if committed, exists := wanted[id]; exists {
			if !sameExecutionRecordSemantics(tx, committed) {
				return fmt.Errorf("%w: committed record differs for %q", ErrExecutionIDConflict, id)
			}
			matched = append(matched, tx)
		} else {
			retained = append(retained, tx)
		}
	}
	txpool.TxQueue = retained
	txpool.removePlanningWindowLocked(matched)
	markPackedLeaveTime(matched)
	if len(txs) != 0 && txpool.committedExecutionIDs == nil {
		txpool.committedExecutionIDs = make(map[string]struct{}, len(txs))
	}
	for id := range wanted {
		txpool.committedExecutionIDs[id] = struct{}{}
	}
	return nil
}
