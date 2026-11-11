package core

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
)

var (
	// ErrExecutionIDNotUnique means an exact manifest repeats an ID or the
	// live pool contains more than one record with a requested stable ID.
	ErrExecutionIDNotUnique = errors.New("execution record ID is not unique")
	// ErrExecutionGasLimit means an exact manifest cannot fit its block gas
	// limit. Exact packing never partially commits in this case.
	ErrExecutionGasLimit = errors.New("execution manifest exceeds block gas limit")
)

// TransactionExecutionID returns the stable identifier of one executable
// transaction-pool record. The execution stage is part of the identifier so
// the top-level and Relay2 stages of the same logical transaction cannot be
// confused by a manifest.
//
// The normal forms are:
//
//	TOP:<source hash or execution hash>
//	RELAY2:<source hash or execution hash>
//	CALL:<call-task id>
//
// Legacy records without either hash get a deterministic address/nonce
// fallback. A call task without GLASCallTaskID uses its source, callee, and
// nonce, matching the fields that identify the synthetic execution stage.
func TransactionExecutionID(tx *Transaction) string {
	if tx == nil {
		return ""
	}

	logicalID := transactionLogicalID(tx)
	if tx.IsGLASCallTask {
		if tx.GLASCallTaskID != "" {
			return "CALL:" + tx.GLASCallTaskID
		}
		callee := tx.GLASCallCallee
		if callee == "" {
			callee = tx.Recipient
		}
		return "CALL:" + logicalID + ":" + callee + ":" + strconv.FormatUint(tx.Nonce, 10)
	}
	if tx.Relayed {
		return "RELAY2:" + logicalID
	}
	return "TOP:" + logicalID
}

func transactionLogicalID(tx *Transaction) string {
	if tx.SourceTxHash != "" {
		return tx.SourceTxHash
	}
	if len(tx.TxHash) > 0 {
		return hex.EncodeToString(tx.TxHash)
	}
	// RawTxHash is retained across broker stages and is a better legacy
	// identity than addresses alone when TxHash was not populated.
	if len(tx.RawTxHash) > 0 {
		return hex.EncodeToString(tx.RawTxHash)
	}
	return tx.Sender + ":" + tx.Recipient + ":" + strconv.FormatUint(tx.Nonce, 10)
}

// TransactionExecutionSemanticDigest binds the immutable execution semantics
// of one stable TOP/RELAY2/CALL record. Pool lifecycle timestamps, signatures,
// and transport hashes are intentionally excluded: they do not affect gas,
// state transition, or simulated execution delay and may legitimately be
// materialized differently after serialization. The call origin/target shard
// fields are also excluded because account migration rewrites them from the
// installed partition map; the epoch manifest already binds both MapDigest and
// the shard that owns each execution record. TransactionExecutionID is
// included, so the digest is still bound to the physical execution stage.
func TransactionExecutionSemanticDigest(tx *Transaction) string {
	if tx == nil {
		return ""
	}
	hash := sha256.New()
	writeUint64 := func(value uint64) {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], value)
		hash.Write(encoded[:])
	}
	writeString := func(value string) {
		writeUint64(uint64(len(value)))
		hash.Write([]byte(value))
	}
	writeBool := func(value bool) {
		if value {
			hash.Write([]byte{1})
		} else {
			hash.Write([]byte{0})
		}
	}

	writeString(TransactionExecutionID(tx))
	writeString(tx.Sender)
	writeString(tx.Recipient)
	writeUint64(tx.Nonce)
	if tx.Value == nil {
		writeBool(false)
	} else {
		writeBool(true)
		writeString(tx.Value.String())
	}
	writeUint64(tx.GasUsed)
	if tx.GLASBaseGas > 0 {
		writeString("glas-attached-call-gas-v1")
		writeUint64(tx.GLASBaseGas)
		writeUint64(tx.GLASCallGasPerCall)
		writeUint64(tx.GLASCrossCallCount)
		writeUint64(tx.GLASAttachedGas)
		writeUint64(uint64(len(tx.GLASCallRelations)))
		for _, call := range tx.GLASCallRelations {
			writeString(call.Caller)
			writeString(call.Callee)
		}
	}
	writeString(tx.SourceTxHash)
	writeString(tx.TxCategory)
	writeBool(tx.IsGLASCallTask)
	writeString(tx.GLASCallTaskID)
	writeString(tx.GLASCallCallee)
	writeUint64(tx.GLASCallCount)
	writeUint64(tx.GLASStageEpoch)
	writeBool(tx.GLASStageEpochSet)
	writeUint64(math.Float64bits(tx.ExecDelayMs))
	writeBool(tx.Relayed)
	writeBool(tx.HasBroker)
	writeBool(tx.SenderIsBroker)
	writeString(tx.OriginalSender)
	writeString(tx.FinalRecipient)

	return hex.EncodeToString(hash.Sum(nil))
}

// TxSelectionOptions describes the deterministic gas/dependency selection
// shared by shadow planning and real transaction-pool packing.
type TxSelectionOptions struct {
	MaxGas uint64

	// GasCost defaults to EffectiveGasUsed when nil.
	GasCost func(*Transaction) uint64

	// PriorityLevel assigns lower numbers higher priority. Eligible levels are
	// [0, LevelCount). A record outside that range remains retained and blocks
	// later records with the same non-empty dependency key.
	PriorityLevel func(*Transaction) int
	LevelCount    int

	// DependencyKey defines FIFO sub-queues. Empty keys (and a nil function)
	// make records independent, allowing a later fitting record to fill gas.
	DependencyKey func(*Transaction) string

	// AllowedExecutionIDs is a hard manifest whitelist. Nil allows every
	// record; a non-nil empty map allows none. A non-whitelisted record always
	// remains retained. TransactionExecutionID supplies the lookup key.
	AllowedExecutionIDs map[string]struct{}

	// OmitRetained avoids allocating and constructing an unused remaining-queue
	// result for opt-in read-only previews. Destructive packers must leave false.
	OmitRetained bool
}

// TxSelectionResult is a read-only selection result over the input queue.
// Packed is in execution order, Retained is in original queue order, and
// SelectedIndices maps Packed entries back to the original queue.
type TxSelectionResult struct {
	SelectedIndices []int
	Packed          []*Transaction
	Retained        []*Transaction
	GasUsed         uint64
}

// SelectTransactions performs dependency-aware, work-conserving gas
// selection without modifying the queue slice or any transaction. It uses the
// same ordering rules as real GLAS packing, so a shadow planner can call this
// function directly and commit the resulting manifest without semantic drift.
//
// A base-valid GLAS record whose map-dependent surcharge exceeds MaxGas stays
// queued and blocks only its dependency group. Malformed/legacy oversized
// records retain the existing fail-fast behavior.
func SelectTransactions(queue []*Transaction, options TxSelectionOptions) TxSelectionResult {
	result := TxSelectionResult{
		SelectedIndices: make([]int, 0),
		Packed:          make([]*Transaction, 0),
	}
	if !options.OmitRetained {
		result.Retained = make([]*Transaction, 0, len(queue))
	}
	if len(queue) == 0 {
		return result
	}
	if options.MaxGas == 0 || options.PriorityLevel == nil || options.LevelCount <= 0 {
		if !options.OmitRetained {
			result.Retained = append(result.Retained, queue...)
		}
		return result
	}

	gasCost := options.GasCost
	if gasCost == nil {
		gasCost = blockGasCost
	}

	// Every non-empty dependency key is a FIFO sub-queue. Empty keys are
	// deliberately made unique per queue position.
	groups := make(map[string][]int)
	for index, tx := range queue {
		key := ""
		if options.DependencyKey != nil {
			key = options.DependencyKey(tx)
		}
		if key == "" {
			key = fmt.Sprintf("\x00%d", index)
		}
		groups[key] = append(groups[key], index)
	}

	positions := make(map[string]int, len(groups))
	blocked := make(map[string]bool, len(groups))
	queues := make([]priorityQueueItems, options.LevelCount)
	pushHead := func(key string) {
		position := positions[key]
		indices := groups[key]
		if position >= len(indices) || blocked[key] {
			return
		}
		index := indices[position]
		tx := queue[index]
		if options.AllowedExecutionIDs != nil {
			if _, allowed := options.AllowedExecutionIDs[TransactionExecutionID(tx)]; !allowed {
				// A disallowed head stays in the pool and must not be bypassed by
				// a later record in the same dependency FIFO.
				blocked[key] = true
				return
			}
		}
		level := options.PriorityLevel(tx)
		if level < 0 || level >= options.LevelCount {
			// A later record with the same dependency key cannot bypass this
			// excluded head record.
			blocked[key] = true
			return
		}
		heap.Push(&queues[level], priorityQueueItem{index: index, key: key})
	}
	for key := range groups {
		pushHead(key)
	}

	var selected []bool
	if !options.OmitRetained {
		selected = make([]bool, len(queue))
	}
	for {
		level := -1
		for candidate := 0; candidate < options.LevelCount; candidate++ {
			if len(queues[candidate]) > 0 {
				level = candidate
				break
			}
		}
		if level < 0 {
			break
		}

		item := heap.Pop(&queues[level]).(priorityQueueItem)
		if blocked[item.key] {
			continue
		}
		position := positions[item.key]
		indices := groups[item.key]
		if position >= len(indices) || indices[position] != item.index {
			continue
		}

		tx := queue[item.index]
		txGas := gasCost(tx)
		if txGas > options.MaxGas {
			if tx.GLASBaseGas > 0 && tx.GLASBaseGas <= options.MaxGas {
				blocked[item.key] = true
				continue
			}
			panic(fmt.Sprintf("tx gasUsed %d exceeds block gas limit %d", txGas, options.MaxGas))
		}
		if txGas > options.MaxGas-result.GasUsed {
			blocked[item.key] = true
			continue
		}

		result.GasUsed += txGas
		if selected != nil {
			selected[item.index] = true
		}
		result.SelectedIndices = append(result.SelectedIndices, item.index)
		result.Packed = append(result.Packed, tx)
		positions[item.key] = position + 1
		pushHead(item.key)
	}

	if !options.OmitRetained {
		for index, tx := range queue {
			if !selected[index] {
				result.Retained = append(result.Retained, tx)
			}
		}
	}
	return result
}
