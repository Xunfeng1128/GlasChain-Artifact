package committee

import (
	"fmt"
	"math"

	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
)

// The captures are local and do not pause ordinary execution. Prefer a real
// Relay2 if a fast shard has already executed a TOP still visible in an earlier
// capture. Synthetic relays only fill gaps in the observed workload.
func fullPendingEarlySnapshots(reports map[uint64]message.PendingEarlyReport, epoch, target uint64, resolve func(string) uint64) (map[uint64]message.PendingSnapshot, error) {
	type capturedRecord struct {
		shard     uint64
		tx        *core.Transaction
		predicted bool
	}
	shards := uint64(len(reports))
	snapshots := make(map[uint64]message.PendingSnapshot, shards)
	observed := make(map[string]capturedRecord)
	var superseded map[*core.Transaction]bool
	for shard := uint64(0); shard < shards; shard++ {
		report, ok := reports[shard]
		if !ok || report.FromShard != shard || report.FullCallRelations < 0 {
			return nil, fmt.Errorf("missing full-window report for shard %d", shard)
		}
		snapshots[shard] = message.PendingSnapshot{Epoch: epoch, TargetSeq: target, FromShard: shard, LocalWindow: true,
			Txs: report.FullWindow, TotalRecords: report.FullRecords, TotalCallRelations: report.FullCallRelations}
		for _, tx := range report.FullWindow {
			if tx == nil || tx.SourceTxHash == "" {
				return nil, fmt.Errorf("invalid full-window record on shard %d", shard)
			}
			previous, exists := observed[tx.SourceTxHash]
			if !exists {
				observed[tx.SourceTxHash] = capturedRecord{shard: shard, tx: tx}
				continue
			}
			if tx.Relayed == previous.tx.Relayed {
				return nil, fmt.Errorf("duplicate observed early source %s", tx.SourceTxHash)
			}
			top, relay := previous, capturedRecord{shard: shard, tx: tx}
			if top.tx.Relayed {
				top, relay = relay, top
			}
			expected, err := forecastPendingRelay(top.tx, top.shard, shards, epoch, resolve)
			if err != nil || resolve(relay.tx.Recipient) != relay.shard || !samePendingRelay(expected, relay.tx) {
				return nil, fmt.Errorf("conflicting observed early stages for source %s", tx.SourceTxHash)
			}
			if superseded == nil {
				superseded = make(map[*core.Transaction]bool)
			}
			superseded[top.tx] = true
			observed[tx.SourceTxHash] = relay
		}
	}
	if len(superseded) != 0 {
		for shard, snapshot := range snapshots {
			for i, tx := range snapshot.Txs {
				if superseded[tx] {
					// Preserve the raw per-shard inventory and never mutate the
					// retained report's slice.
					kept := append([]*core.Transaction(nil), snapshot.Txs[:i]...)
					for _, tail := range snapshot.Txs[i+1:] {
						if !superseded[tail] {
							kept = append(kept, tail)
						}
					}
					snapshot.Txs = kept
					snapshots[shard] = snapshot
					break
				}
			}
		}
	}
	for shard := uint64(0); shard < shards; shard++ {
		for _, top := range reports[shard].ForecastCrossTOP {
			relay, err := forecastPendingRelay(top, shard, shards, epoch, resolve)
			if err != nil {
				return nil, err
			}
			if record, exists := observed[top.SourceTxHash]; exists {
				if record.predicted || !samePendingRelay(relay, record.tx) {
					return nil, fmt.Errorf("conflicting forecast for early source %s", top.SourceTxHash)
				}
				continue
			}
			to := resolve(relay.Recipient)
			snapshot := snapshots[to]
			if snapshot.TotalRecords == math.MaxInt || len(relay.GLASCallRelations) > math.MaxInt-snapshot.TotalCallRelations {
				return nil, fmt.Errorf("forecast inventory overflows on shard %d", to)
			}
			snapshot.Txs = append(snapshot.Txs, relay)
			snapshot.TotalRecords++
			snapshot.TotalCallRelations += len(relay.GLASCallRelations)
			snapshots[to] = snapshot
			observed[top.SourceTxHash] = capturedRecord{shard: to, tx: relay, predicted: true}
		}
	}
	return snapshots, nil
}

func forecastPendingRelay(top *core.Transaction, shard, shards, epoch uint64, resolve func(string) uint64) (*core.Transaction, error) {
	if err := core.ValidateExecutionRecord(top, params.UnifiedCrossCallGas); err != nil {
		return nil, fmt.Errorf("forecast TOP from shard %d: %w", shard, err)
	}
	to := resolve(top.Recipient)
	if top.Relayed || resolve(top.Sender) != shard || to == shard || to >= shards || epoch == 0 {
		return nil, fmt.Errorf("invalid early cross-shard forecast %s from shard %d", top.SourceTxHash, shard)
	}
	if err := core.ValidateExecutionStageGas(top, resolve, params.BlockGasLimit); err != nil {
		return nil, fmt.Errorf("forecast TOP from shard %d: %w", shard, err)
	}
	return core.NewExecutionRelay2(top, epoch-1, resolve)
}

func samePendingRelay(expected, observed *core.Transaction) bool {
	return expected != nil && observed.Relayed && observed.GLASStageEpochSet &&
		expected.InputSenderIsContract == observed.InputSenderIsContract &&
		expected.InputRecipientIsContract == observed.InputRecipientIsContract &&
		core.TransactionExecutionSemanticDigest(expected) == core.TransactionExecutionSemanticDigest(observed)
}
