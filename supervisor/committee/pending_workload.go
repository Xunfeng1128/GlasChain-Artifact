package committee

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"

	"blockEmulator/core"
	"blockEmulator/glasfreight/model"
	gplanner "blockEmulator/glasfreight/planner"
	"blockEmulator/message"
	"blockEmulator/params"
)

// convertPendingPools freezes the early local windows and forecast Relay2 work.
func convertPendingPools(snapshots map[uint64]message.PendingSnapshot, epoch, target, shards, callGas uint64, resolve func(string) uint64, admitted *gplanner.WindowStats) (gplanner.FrozenPools, model.Placement, error) {
	pools := gplanner.FrozenPools{ShardCount: shards, Records: make(map[uint64][]gplanner.FrozenRecord)}
	placement := make(model.Placement)
	if uint64(len(snapshots)) != shards {
		return pools, nil, fmt.Errorf("pending snapshots: received %d/%d", len(snapshots), shards)
	}
	seen := make(map[string]bool)
	digest := sha256.New()
	// Reuse the ordered execution record digests without re-encoding transaction JSON.
	digest.Write([]byte("GLAS-Pending-full-early-window-v1"))
	for shard := uint64(0); shard < shards; shard++ {
		snapshot, ok := snapshots[shard]
		if !ok || snapshot.FromShard != shard || snapshot.Epoch != epoch || snapshot.TargetSeq != target || snapshot.Error != "" {
			return pools, nil, fmt.Errorf("invalid pending snapshot for shard %d epoch %d", shard, epoch)
		}
		if snapshot.LocalWindow && (snapshot.TotalRecords < len(snapshot.Txs) || snapshot.TotalCallRelations < 0) {
			return pools, nil, fmt.Errorf("invalid local-window counts for shard %d", shard)
		}
		var count [8]byte
		binary.BigEndian.PutUint64(count[:], uint64(len(snapshot.Txs)))
		digest.Write(count[:])
		sentCalls := 0
		for index, tx := range snapshot.Txs {
			if err := core.ValidateExecutionRecord(tx, callGas); err != nil {
				return pools, nil, fmt.Errorf("snapshot shard %d: %w", shard, err)
			}
			if err := core.ValidateExecutionStageGas(tx, resolve, params.BlockGasLimit); err != nil {
				return pools, nil, err
			}
			if len(tx.GLASCallRelations) > math.MaxInt-sentCalls {
				return pools, nil, fmt.Errorf("pending snapshot call count overflow")
			}
			sentCalls += len(tx.GLASCallRelations)
			id := core.TransactionExecutionID(tx)
			if seen[id] {
				return pools, nil, fmt.Errorf("duplicate pending stage %s", id)
			}
			seen[id] = true
			recordDigest := core.TransactionExecutionSemanticDigest(tx)
			if tx.GasUsed == 0 || tx.GasUsed > math.MaxUint64-admitted.PerShardGas[shard] {
				return pools, nil, fmt.Errorf("invalid local-window gas for pending stage %s", id)
			}
			admitted.PerShardGas[shard] += tx.GasUsed
			digest.Write([]byte(recordDigest))
			var contracts byte
			if tx.InputSenderIsContract {
				contracts |= 1
			}
			if tx.InputRecipientIsContract {
				contracts |= 2
			}
			digest.Write([]byte{contracts})
			stage, owner := gplanner.TopLevelStage, tx.Sender
			if tx.Relayed {
				stage, owner = gplanner.Relay2Stage, tx.Recipient
			}
			if resolve(owner) != shard {
				return pools, nil, fmt.Errorf("snapshot stage %s owner is not shard %d", id, shard)
			}
			calls := make([]model.Call, 0, len(tx.GLASCallRelations))
			for _, call := range tx.GLASCallRelations {
				calls = append(calls, model.Call{Caller: call.Caller, Callee: call.Callee, IsCCGEdge: true, CallerIsSCA: true, CalleeIsSCA: true})
				placement[call.Caller], placement[call.Callee] = resolve(call.Caller), resolve(call.Callee)
			}
			placement[tx.Sender], placement[tx.Recipient] = resolve(tx.Sender), resolve(tx.Recipient)
			pools.Records[shard] = append(pools.Records[shard], gplanner.FrozenRecord{
				ID: id, ExecutionDigest: recordDigest, SourceHash: tx.SourceTxHash,
				Stage: stage, From: tx.Sender, To: tx.Recipient, Owner: owner, GasUsed: tx.GasUsed,
				BaseGasUsed: tx.GLASBaseGas, AttachedCallGas: tx.GLASAttachedGas, CallGasPerCall: tx.GLASCallGasPerCall,
				StageEpoch: tx.GLASStageEpoch, StageEpochSet: tx.GLASStageEpochSet,
				FromIsContract: tx.InputSenderIsContract, ToIsContract: tx.InputRecipientIsContract,
				Calls: calls, Category: tx.TxCategory, DependencyKey: core.ExecutionDependency(tx), Priority: core.ExecutionPriority(tx),
				ArrivalSeq: tx.Nonce, OriginShard: shard, QueueIndex: uint64(index), Affinity: 1,
			})
		}
		if snapshot.LocalWindow && snapshot.TotalCallRelations < sentCalls {
			return pools, nil, fmt.Errorf("local-window call count smaller than transmitted calls for shard %d", shard)
		}
		// The scope totals already bound these smaller per-shard counts.
		admitted.SelectedRecords += len(snapshot.Txs)
		admitted.SelectedCallRelations += sentCalls
	}
	pools.Digest = hex.EncodeToString(digest.Sum(nil))
	return pools, placement, nil
}

// pendingStagePrice uses the SAME arithmetic as the live unified execution
// layer. It prices one remaining stage; no blocks or time are simulated.
func pendingStagePrice(record gplanner.FrozenRecord, placement model.Placement) (gplanner.FrozenRecord, error) {
	if record.Stage != gplanner.TopLevelStage && record.Stage != gplanner.Relay2Stage {
		return record, fmt.Errorf("unsupported pending stage %s", record.ID)
	}
	tx := &core.Transaction{Sender: record.From, Recipient: record.To, SourceTxHash: record.SourceHash,
		GLASBaseGas: record.BaseGasUsed, GLASCallGasPerCall: record.CallGasPerCall, Relayed: record.Stage == gplanner.Relay2Stage}
	for _, address := range []string{record.From, record.To} {
		if _, ok := placement[address]; !ok {
			return record, fmt.Errorf("pending placement missing %s", address)
		}
	}
	for _, call := range record.Calls {
		if !call.IsCCGEdge || !call.CallerIsSCA || !call.CalleeIsSCA {
			continue
		}
		for _, address := range []string{call.Caller, call.Callee} {
			if _, ok := placement[address]; !ok {
				return record, fmt.Errorf("pending placement missing %s", address)
			}
		}
		tx.GLASCallRelations = append(tx.GLASCallRelations, core.GLASCallRelation{Caller: call.Caller, Callee: call.Callee})
	}
	if err := core.PriceExecutionStage(tx, func(address string) uint64 { return placement[address] }); err != nil {
		return record, err
	}
	record.GasUsed, record.AttachedCallGas = tx.GasUsed, tx.GLASAttachedGas
	return record, nil
}

func pendingStageExpansion(epoch uint64) gplanner.ExpandFunc {
	return func(record gplanner.FrozenRecord, placement model.Placement) ([]gplanner.FrozenRecord, error) {
		if record.Stage != gplanner.TopLevelStage {
			return nil, nil
		}
		from, ok := placement[record.From]
		if !ok {
			return nil, fmt.Errorf("missing TOP sender")
		}
		to, ok := placement[record.To]
		if !ok {
			return nil, fmt.Errorf("missing TOP recipient")
		}
		if from == to {
			return nil, nil
		}
		child := record
		child.ID, child.Stage = "RELAY2:"+record.SourceHash, gplanner.Relay2Stage
		child.Owner, child.Priority = record.To, 0
		child.DependencyKey = "logical:" + record.SourceHash
		child.StageEpoch, child.StageEpochSet = epoch, true
		priced, err := pendingStagePrice(child, placement)
		if err != nil {
			return nil, err
		}
		return []gplanner.FrozenRecord{priced}, nil
	}
}
