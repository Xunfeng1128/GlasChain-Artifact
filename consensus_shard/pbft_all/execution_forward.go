package pbft_all

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"

	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
)

// GLAS_Pending deliberately snapshots without an injection drain. Common
// CLPA migration must therefore preserve stages arriving after the snapshot
// or after the source has already extracted its outgoing queue. Forwarding
// is separate from Relay: it changes ownership, never execution stage.
func (crom *CLPARelayOutsideModule) installCLPAExecutionTransactions(txs []*core.Transaction, relayed bool) error {
	return crom.installMarkedCLPAExecutionTransactions(txs, relayed, nil, 0)
}

func (crom *CLPARelayOutsideModule) installMarkedCLPAExecutionTransactions(txs []*core.Transaction, relayed bool, marks *core.PendingMarks, fromShard uint64) error {
	if !params.UnifiedExecution || crom.cdm == nil {
		return crom.pbftNode.installExecutionTransactions(txs, relayed)
	}
	for _, tx := range txs {
		if tx == nil || tx.Relayed != relayed {
			return fmt.Errorf("incoming execution stage does not match its message kind")
		}
	}
	crom.pbftNode.unifiedExecutionMu.Lock()
	var forwards map[uint64]*message.ExecutionForward
	var err error
	if params.GLASPendingFullWindowEarlyEnabled() && marks != nil {
		// A Relay from a shard already using a newer mapping may arrive
		// before our own installation. The existing forwarding buffer keeps
		// the payload and its epoch marks together until our map catches up.
		forwards, err = crom.acceptExecutionForwardLocked(&message.ExecutionForward{
			Epoch: marks.Epoch, FromShard: fromShard, Txs: txs, PendingMarks: marks,
		})
	} else {
		forwards, err = crom.acceptExecutionStagesLocked(txs, nil)
	}
	crom.pbftNode.unifiedExecutionMu.Unlock()
	if err != nil {
		return err
	}
	return crom.pbftNode.sendExecutionForwards(forwards)
}

// Caller holds unifiedExecutionMu. Every record is separately repriced before
// either admission or forwarding. Original receipts and source times survive.
func (crom *CLPARelayOutsideModule) acceptExecutionStagesLocked(txs []*core.Transaction, marks *core.PendingMarks) (map[uint64]*message.ExecutionForward, error) {
	priced, err := crom.pbftNode.CurChain.PrepareUnifiedTransactions(txs)
	if err != nil {
		return nil, err
	}
	forwards, err := crom.admitPricedExecutionStagesLocked(priced, crom.pbftNode.CurChain.ExecutionShardResolver(), marks)
	if err == nil && params.GLASPendingFullWindowEarlyEnabled() {
		crom.pbftNode.tagExecutionForwards(forwards)
	}
	return forwards, err
}

// Installation already holds the execution guard and owns one routing snapshot.
func (crom *CLPARelayOutsideModule) acceptExecutionStagesWithResolverLocked(txs []*core.Transaction, shardFor func(string) uint64, marks *core.PendingMarks) (map[uint64]*message.ExecutionForward, error) {
	priced, err := crom.pbftNode.CurChain.PrepareUnifiedTransactionsWithResolver(txs, shardFor)
	if err != nil {
		return nil, err
	}
	return crom.admitPricedExecutionStagesLocked(priced, shardFor, marks)
}

func (crom *CLPARelayOutsideModule) admitPricedExecutionStagesLocked(priced []*core.Transaction, shardFor func(string) uint64, marks *core.PendingMarks) (map[uint64]*message.ExecutionForward, error) {
	crom.cdm.PartitionStateLock.Lock()
	epoch := crom.cdm.AccountTransferRound
	crom.cdm.PartitionStateLock.Unlock()
	local := make([]*core.Transaction, 0, len(priced))
	forwards := make(map[uint64]*message.ExecutionForward)
	for _, tx := range priced {
		owner := executionStageOwner(tx)
		target := shardFor(owner)
		if target >= crom.pbftNode.pbftChainConfig.ShardNums {
			return nil, fmt.Errorf("execution owner shard %d out of range", target)
		}
		if target == crom.pbftNode.ShardID {
			local = append(local, tx)
		} else {
			appendExecutionForward(forwards, target, epoch, crom.pbftNode.ShardID, tx)
		}
	}
	pool := crom.pbftNode.CurChain.Txpool
	if _, err := pool.AddTxs2PoolUniqueByExecutionID(local); err != nil {
		return nil, err
	}
	if params.GLASPendingFullWindowEarlyEnabled() {
		// Admission may have deduplicated a record that lacked the mark.
		// Merge the metadata for the full batch, including records to forward.
		if err := pool.MergePendingMarks(priced, marks); err != nil {
			return nil, err
		}
	}
	return forwards, nil
}

func executionStageOwner(tx *core.Transaction) string {
	if tx.Relayed {
		return tx.Recipient
	}
	return tx.Sender
}

func appendExecutionForward(forwards map[uint64]*message.ExecutionForward, target, epoch, source uint64, tx *core.Transaction) {
	if forwards[target] == nil {
		forwards[target] = &message.ExecutionForward{Epoch: epoch, FromShard: source}
	}
	forwards[target].Txs = append(forwards[target].Txs, tx)
}

func (crom *CLPARelayOutsideModule) handleExecutionForward(content []byte) {
	var forward message.ExecutionForward
	if err := json.Unmarshal(content, &forward); err != nil {
		log.Panicf("invalid execution forward: %v", err)
	}
	crom.pbftNode.unifiedExecutionMu.Lock()
	out, err := crom.acceptExecutionForwardLocked(&forward)
	crom.pbftNode.unifiedExecutionMu.Unlock()
	if err != nil {
		log.Panicf("invalid execution forward: %v", err)
	}
	if err := crom.pbftNode.sendExecutionForwards(out); err != nil {
		log.Panicf("execution forward delivery failed: %v", err)
	}
}

// A future-epoch message is buffered, rather than sent back using an old map.
// This prevents ping-pong while independent shards finish PBFT commits.
func (crom *CLPARelayOutsideModule) acceptExecutionForwardLocked(forward *message.ExecutionForward) (map[uint64]*message.ExecutionForward, error) {
	if !params.UnifiedExecution || crom.cdm == nil || forward == nil || forward.FromShard >= crom.pbftNode.pbftChainConfig.ShardNums {
		return nil, fmt.Errorf("invalid forwarded execution envelope")
	}
	for _, tx := range forward.Txs {
		if err := core.ValidateExecutionRecord(tx, params.UnifiedCrossCallGas); err != nil {
			return nil, err
		}
	}
	if params.GLASPendingFullWindowEarlyEnabled() {
		if err := core.ValidatePendingMarks(forward.Txs, forward.PendingMarks); err != nil {
			return nil, err
		}
	}
	crom.cdm.PartitionStateLock.Lock()
	if forward.Epoch > crom.cdm.AccountTransferRound+1 {
		crom.cdm.PartitionStateLock.Unlock()
		return nil, fmt.Errorf("forwarded execution epoch %d is more than one ahead of committed epoch", forward.Epoch)
	}
	if forward.Epoch > crom.cdm.AccountTransferRound {
		copyForward := *forward
		copyForward.Txs = (&core.TxPool{TxQueue: forward.Txs}).Snapshot().TxQueue
		if params.GLASPendingFullWindowEarlyEnabled() && forward.PendingMarks != nil {
			marks := *forward.PendingMarks
			marks.Flags = append([]byte(nil), marks.Flags...)
			copyForward.PendingMarks = &marks
		}
		crom.cdm.FutureExecutionForwards = append(crom.cdm.FutureExecutionForwards, &copyForward)
		crom.cdm.PartitionStateLock.Unlock()
		return nil, nil
	}
	crom.cdm.PartitionStateLock.Unlock()
	return crom.acceptExecutionStagesLocked(forward.Txs, forward.PendingMarks)
}

// Called after installation of the complete committed map, with
// unifiedExecutionMu held. It also extracts late arrivals that missed the
// earlier source-pool migration extraction. Retained local order is stable.
func (cphm *CLPAPbftInsideExtraHandleMod) flushExecutionForwardsLocked() (map[uint64]*message.ExecutionForward, error) {
	return cphm.flushExecutionForwardsWithResolverLocked(cphm.pbftNode.CurChain.ExecutionShardResolver())
}

func (cphm *CLPAPbftInsideExtraHandleMod) flushExecutionForwardsWithResolverLocked(shardFor func(string) uint64) (map[uint64]*message.ExecutionForward, error) {
	cphm.cdm.PartitionStateLock.Lock()
	epoch := cphm.cdm.AccountTransferRound
	eligible := make([]*message.ExecutionForward, 0)
	future := make([]*message.ExecutionForward, 0)
	for _, forward := range cphm.cdm.FutureExecutionForwards {
		if forward.Epoch <= epoch {
			eligible = append(eligible, forward)
		} else {
			future = append(future, forward)
		}
	}
	cphm.cdm.FutureExecutionForwards = future
	cphm.cdm.PartitionStateLock.Unlock()
	outside := &CLPARelayOutsideModule{cdm: cphm.cdm, pbftNode: cphm.pbftNode}
	forwards := make(map[uint64]*message.ExecutionForward)
	var late map[uint64]*message.ExecutionForward
	if params.GLASPendingFullWindowEarlyEnabled() {
		late = make(map[uint64]*message.ExecutionForward)
		if err := cphm.extractLateExecutionForwardsLocked(late, epoch, shardFor); err != nil {
			return nil, err
		}
		// Extract the late movers before building the new index. Buffered
		// future batches may carry marks for this epoch and the next one.
		cphm.activateFullWindowMarks(epoch)
	}
	for _, forward := range eligible {
		out, err := outside.acceptExecutionStagesWithResolverLocked(forward.Txs, shardFor, forward.PendingMarks)
		if err != nil {
			return nil, err
		}
		mergeExecutionForwards(forwards, out, epoch, cphm.pbftNode.ShardID)
	}
	if !params.GLASPendingFullWindowEarlyEnabled() {
		if err := cphm.extractLateExecutionForwardsLocked(forwards, epoch, shardFor); err != nil {
			return nil, err
		}
		return forwards, nil
	}
	mergeExecutionForwards(forwards, late, epoch, cphm.pbftNode.ShardID)
	cphm.pbftNode.tagExecutionForwards(forwards)
	return forwards, nil
}

func (cphm *CLPAPbftInsideExtraHandleMod) extractLateExecutionForwardsLocked(forwards map[uint64]*message.ExecutionForward, epoch uint64, shardFor func(string) uint64) error {
	pool := cphm.pbftNode.CurChain.Txpool
	pool.GetLocked()
	defer pool.GetUnlocked()
	retained := make([]*core.Transaction, 0, len(pool.TxQueue))
	for _, tx := range pool.TxQueue {
		target := shardFor(executionStageOwner(tx))
		if target >= cphm.pbftNode.pbftChainConfig.ShardNums {
			return fmt.Errorf("late execution owner shard %d out of range", target)
		}
		if target == cphm.pbftNode.ShardID {
			retained = append(retained, tx)
		} else {
			appendExecutionForward(forwards, target, epoch, cphm.pbftNode.ShardID, tx)
		}
	}
	pool.TxQueue = retained
	return nil
}

func mergeExecutionForwards(dst, src map[uint64]*message.ExecutionForward, epoch, shard uint64) {
	for target, batch := range src {
		if dst[target] == nil {
			dst[target] = &message.ExecutionForward{Epoch: epoch, FromShard: shard}
		}
		dst[target].Txs = append(dst[target].Txs, batch.Txs...)
	}
}

func (node *PbftConsensusNode) tagExecutionForwards(forwards map[uint64]*message.ExecutionForward) {
	for _, batch := range forwards {
		batch.PendingMarks = node.CurChain.Txpool.ExportPendingMarks(batch.Txs)
	}
}

// No pool or execution lock is held while sending. A failed send fails the
// run explicitly; it cannot be counted as a successful migration.
func (node *PbftConsensusNode) sendExecutionForwards(forwards map[uint64]*message.ExecutionForward) error {
	targets := make([]uint64, 0, len(forwards))
	for target := range forwards {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
	for _, target := range targets {
		payload, err := json.Marshal(forwards[target])
		if err != nil {
			return err
		}
		parent := context.Background()
		if node.clpaControlEnabled() {
			parent = node.clpaControlContext()
		}
		ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
		err = networks.TcpDialContext(ctx, message.MergeMessage(message.CExecutionForward, payload), node.ip_nodeTable[target][0])
		cancel()
		if err != nil {
			if node.clpaControlEnabled() && node.stopSignal.Load() {
				return nil
			}
			return fmt.Errorf("forward execution to shard %d: %w", target, err)
		}
	}
	return nil
}
