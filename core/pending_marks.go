package core

import (
	"fmt"
	"math"
)

const (
	pendingMarkCurrent byte = 1 << iota
	pendingMarkNext
)

// PendingMarks carries shard-local packing priorities alongside a batch of
// real transactions. Each flag byte corresponds to the transaction at the
// same index. Bits refer to Epoch and Epoch+1, the sender's two live plans.
type PendingMarks struct {
	Epoch uint64
	Flags []byte
}

// ValidatePendingMarks checks the optional batch metadata without changing
// the pool. A record's mark is independent of its execution identity.
func ValidatePendingMarks(txs []*Transaction, marks *PendingMarks) error {
	if marks == nil {
		return nil
	}
	if len(marks.Flags) != len(txs) {
		return fmt.Errorf("pending marks count %d does not match %d transactions", len(marks.Flags), len(txs))
	}
	for i, flags := range marks.Flags {
		if flags&^(pendingMarkCurrent|pendingMarkNext) != 0 ||
			(flags&pendingMarkCurrent != 0 && marks.Epoch == 0) ||
			(flags&pendingMarkNext != 0 && marks.Epoch == math.MaxUint64) ||
			(flags != 0 && (txs[i] == nil || txs[i].SourceTxHash == "")) {
			return fmt.Errorf("invalid pending marks at transaction %d", i)
		}
	}
	return nil
}

func (txpool *TxPool) ExportPendingMarks(txs []*Transaction) *PendingMarks {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	return txpool.ExportPendingMarksLocked(txs)
}

// ExportPendingMarksLocked reads both live rounds without removing marks.
// In particular, a committed TOP's mark is still available to its Relay2.
// The caller holds the pool lock.
func (txpool *TxPool) ExportPendingMarksLocked(txs []*Transaction) *PendingMarks {
	var marks *PendingMarks
	for i, tx := range txs {
		if tx == nil {
			continue
		}
		var flags byte
		if txpool.pendingMarkEpoch != 0 && txpool.planningWindow != nil && txpool.planningWindow.selected(tx) {
			flags |= pendingMarkCurrent
		}
		if _, ok := txpool.pendingMarkedSources[tx.SourceTxHash]; ok {
			flags |= pendingMarkNext
		}
		if flags == 0 {
			continue
		}
		if marks == nil {
			marks = &PendingMarks{Epoch: txpool.pendingMarkEpoch, Flags: make([]byte, len(txs))}
		}
		marks.Flags[i] = flags
	}
	return marks
}

// MergePendingMarks can be called after admitting a batch, including records
// deduplicated by admission. Late tags from a retired plan have no effect.
func (txpool *TxPool) MergePendingMarks(txs []*Transaction, marks *PendingMarks) error {
	if err := ValidatePendingMarks(txs, marks); err != nil || marks == nil {
		return err
	}
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	for i, flags := range marks.Flags {
		if flags&pendingMarkCurrent != 0 {
			txpool.mergePendingMarkLocked(txs[i], marks.Epoch)
		}
		if flags&pendingMarkNext != 0 {
			txpool.mergePendingMarkLocked(txs[i], marks.Epoch+1)
		}
	}
	return nil
}

func (txpool *TxPool) markNextSourceLocked(source string) {
	if txpool.pendingMarkedSources == nil {
		txpool.pendingMarkedSources = make(map[string]struct{})
	}
	txpool.pendingMarkedSources[source] = struct{}{}
}

func (txpool *TxPool) mergePendingMarkLocked(tx *Transaction, epoch uint64) {
	if epoch > txpool.pendingMarkEpoch && epoch-txpool.pendingMarkEpoch == 1 {
		txpool.markNextSourceLocked(tx.SourceTxHash)
		return
	}
	index := txpool.planningWindow
	if epoch != txpool.pendingMarkEpoch || index == nil || index.selected(tx) {
		return
	}
	index.sources[tx.SourceTxHash] = struct{}{}
	if index.dirty {
		return
	}
	// The common Relay2 case only visits that source's dependency group.
	// If a copy already exists in the pool, update its priority in place.
	key := planningWindowKey(tx)
	if group := index.groups[key]; group != nil {
		for _, record := range group.records[group.head:] {
			if record.tx.SourceTxHash == tx.SourceTxHash {
				group.selected++
				index.windowGroups[key] = struct{}{}
			}
		}
	}
}

// ActivatePendingMarks installs the next round after migration and repricing.
// The normal dependency index is rebuilt once over the post-migration queue.
func (txpool *TxPool) ActivatePendingMarks(epoch uint64) error {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	if epoch <= txpool.pendingMarkEpoch || epoch-txpool.pendingMarkEpoch != 1 {
		return fmt.Errorf("cannot activate pending marks for epoch %d from epoch %d", epoch, txpool.pendingMarkEpoch)
	}
	sources := txpool.pendingMarkedSources
	if sources == nil {
		sources = make(map[string]struct{})
	}
	index, err := buildPlanningWindowIndex(txpool.TxQueue, sources)
	if err != nil {
		return err
	}
	txpool.planningWindow = index
	txpool.pendingMarkEpoch = epoch
	txpool.pendingMarkedSources = nil
	return nil
}

func (txpool *TxPool) ActivePendingMarkCount() int {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	if txpool.pendingMarkEpoch == 0 || txpool.planningWindow == nil {
		return 0
	}
	return len(txpool.planningWindow.sources)
}
