package core

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
)

// LocalWindowSelection references live records; it never copies transaction
// bodies or call traces. The caller must hold the pool lock while selecting
// and copying Records for transport. The supervisor still applies global caps.
type LocalWindowSelection struct {
	Records           []*Transaction
	FullRecords       int
	FullCallRelations int
	SelectedGas       uint64
}

type localWindowHeads struct {
	queue   []*Transaction
	indices []int
	sources map[string]struct{}
}

func (h localWindowHeads) Len() int { return len(h.indices) }
func (h localWindowHeads) Less(i, j int) bool {
	a, b := h.indices[i], h.indices[j]
	pa, pb := ExecutionPriority(h.queue[a]), ExecutionPriority(h.queue[b])
	if h.sources != nil {
		if _, selected := h.sources[h.queue[a].SourceTxHash]; !selected {
			pa += 2
		}
		if _, selected := h.sources[h.queue[b].SourceTxHash]; !selected {
			pb += 2
		}
	}
	if pa != pb {
		return pa < pb
	}
	return a < b
}
func (h localWindowHeads) Swap(i, j int)       { h.indices[i], h.indices[j] = h.indices[j], h.indices[i] }
func (h *localWindowHeads) Push(v interface{}) { h.indices = append(h.indices, v.(int)) }
func (h *localWindowHeads) Pop() interface{} {
	last := len(h.indices) - 1
	index := h.indices[last]
	h.indices = h.indices[:last]
	return index
}

// SelectLocalGasWindow selects visible dependency prefixes using the installed
// map's already-priced physical stage Gas. A final whole record may cross the
// soft budget; this is not a per-block packing simulation. Relay2 precedes TOP,
// and equal-priority heads follow actual queue order. No global count/call cap
// is applied locally: the central selector decides the final window.
func SelectLocalGasWindow(queue []*Transaction, gasBudget, blockGasLimit uint64) (LocalWindowSelection, error) {
	return selectLocalGasWindowWithSources(queue, gasBudget, blockGasLimit, nil)
}

// SelectLocalGasWindowLocked follows the currently installed execution window
// while selecting the next window: selected Relay2, selected TOP, ordinary
// Relay2, ordinary TOP. Sender dependencies still gate eligibility. The caller
// must already hold the pool lock; the membership is read without copying it.
func (txpool *TxPool) SelectLocalGasWindowLocked(gasBudget, blockGasLimit uint64) (LocalWindowSelection, error) {
	var sources map[string]struct{}
	if txpool.planningWindow != nil {
		sources = txpool.planningWindow.sources
	}
	return selectLocalGasWindowWithSources(txpool.TxQueue, gasBudget, blockGasLimit, sources)
}

func selectLocalGasWindowWithSources(queue []*Transaction, gasBudget, blockGasLimit uint64, sources map[string]struct{}) (LocalWindowSelection, error) {
	result := LocalWindowSelection{FullRecords: len(queue)}
	if gasBudget == 0 || blockGasLimit == 0 {
		return result, fmt.Errorf("local window requires positive Gas budget and block limit")
	}
	state := &localWindowHeads{queue: queue, sources: sources}
	next := make([]int, len(queue))
	previous := make(map[string]int)
	for index, tx := range queue {
		if tx == nil || tx.SourceTxHash == "" || tx.Sender == "" || tx.Recipient == "" || tx.IsGLASCallTask {
			return result, fmt.Errorf("invalid local window record at position %d", index)
		}
		if tx.GasUsed == 0 || tx.GasUsed > blockGasLimit {
			return result, fmt.Errorf("local window record %s stage Gas %d exceeds block capacity %d", TransactionExecutionID(tx), tx.GasUsed, blockGasLimit)
		}
		if len(tx.GLASCallRelations) > math.MaxInt-result.FullCallRelations {
			return result, fmt.Errorf("local window call count overflow")
		}
		result.FullCallRelations += len(tx.GLASCallRelations)
		next[index] = -1
		key := ExecutionDependency(tx)
		if prev, ok := previous[key]; key != "" && ok {
			next[prev] = index
		} else {
			state.indices = append(state.indices, index)
		}
		if key != "" {
			previous[key] = index
		}
	}
	heap.Init(state)
	selected := make([]int, 0)
	for state.Len() > 0 && result.SelectedGas < gasBudget {
		index := heap.Pop(state).(int)
		gas := queue[index].GasUsed
		if gas > math.MaxUint64-result.SelectedGas {
			return result, fmt.Errorf("local window Gas overflow")
		}
		result.SelectedGas += gas
		selected = append(selected, index)
		if next[index] >= 0 {
			heap.Push(state, next[index])
		}
	}
	// Transport keeps physical queue order, not the head-selection order.
	sort.Ints(selected)
	result.Records = make([]*Transaction, 0, len(selected))
	for _, index := range selected {
		result.Records = append(result.Records, queue[index])
	}
	return result, nil
}
