package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/message"
	"blockEmulator/params"
	"encoding/json"
	"log"
	"reflect"
)

// This module used in the blockChain using transaction relaying mechanism.
// "CLPA" means that the blockChain use Account State Transfer protocal by clpa.
type CLPARelayOutsideModule struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
}

func (crom *CLPARelayOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CRelay:
		crom.handleRelay(content)
	case message.CRelayWithProof:
		crom.handleRelayWithProof(content)
	case message.CInject:
		crom.handleInjectTx(content)
	case message.CExecutionForward:
		crom.handleExecutionForward(content)

	// messages about CLPA
	case message.CPartitionPrepare:
		crom.handlePartitionPrepare(content)
	case message.CPartitionMsg:
		crom.handlePartitionMsg(content)
	case message.CPendingEarlyPlan:
		crom.handlePendingEarlyPlan(content)
	case message.CPendingEarlyActivate:
		crom.handlePendingEarlyActivate(content)
	case message.AccountState_and_TX:
		crom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		crom.handlePartitionReady(content)
	case message.CPartitionInstallQuery:
		crom.handlePartitionInstallQuery(content)
	default:
	}
	return true
}

// receive relay transaction, which is for cross shard txs
func (crom *CLPARelayOutsideModule) handleRelay(content []byte) {
	defer crom.pbftNode.wakeCLPAControl()
	relay := new(message.Relay)
	err := json.Unmarshal(content, relay)
	if err != nil {
		log.Panic(err)
	}
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs from shard %d, the senderSeq is %d\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, relay.SenderShardID, relay.SenderSeq)
	if err := crom.installMarkedCLPAExecutionTransactions(relay.Txs, true, relay.PendingMarks, relay.SenderShardID); err != nil {
		log.Panicf("invalid unified inbound execution: %v", err)
	}
	crom.pbftNode.seqMapLock.Lock()
	if !crom.cdm.ControlEventsEnabled || relay.SenderSeq > crom.pbftNode.seqIDMap[relay.SenderShardID] {
		crom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	}
	crom.pbftNode.seqMapLock.Unlock()
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
}

func (crom *CLPARelayOutsideModule) handleRelayWithProof(content []byte) {
	defer crom.pbftNode.wakeCLPAControl()
	rwp := new(message.RelayWithProof)
	err := json.Unmarshal(content, rwp)
	if err != nil {
		log.Panic(err)
	}
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs & proofs from shard %d, the senderSeq is %d\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, rwp.SenderShardID, rwp.SenderSeq)
	// validate the proofs of txs
	isAllCorrect := true
	for i, tx := range rwp.Txs {
		if ok, _ := chain.TxProofVerify(tx.TxHash, &rwp.TxProofs[i]); !ok {
			isAllCorrect = false
			break
		}
	}
	if isAllCorrect {
		if err := crom.installMarkedCLPAExecutionTransactions(rwp.Txs, true, rwp.PendingMarks, rwp.SenderShardID); err != nil {
			log.Panicf("invalid unified inbound execution: %v", err)
		}
	} else {
		crom.pbftNode.pl.Plog.Println("Err: wrong proof!")
	}

	crom.pbftNode.seqMapLock.Lock()
	if !crom.cdm.ControlEventsEnabled || (isAllCorrect && rwp.SenderSeq > crom.pbftNode.seqIDMap[rwp.SenderShardID]) {
		crom.pbftNode.seqIDMap[rwp.SenderShardID] = rwp.SenderSeq
	}
	crom.pbftNode.seqMapLock.Unlock()
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
}

func (crom *CLPARelayOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	if err := crom.installCLPAExecutionTransactions(it.Txs, false); err != nil {
		log.Panicf("invalid unified inbound execution: %v", err)
	}
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, len(it.Txs))
}

// handlePartitionPrepare 处理 supervisor 提前广播的 partition 预告消息。
// 这条消息携带 Epoch 和 TargetSeq，体积约 40 字节，正常会远早于大 map 到达。
// shard 单调更新 PendingEpoch 和 PendingTargetSeq；propose 入口据此决定：
//   - 本地 seq < TargetSeq → 继续出普通块
//   - 本地 seq >= TargetSeq → barrier 等大 map 后出 partition
//
// 从而强制所有 shard 在同一 TargetSeq 出 partition，消除本地节拍漂移引发的错位。
func (crom *CLPARelayOutsideModule) handlePartitionPrepare(content []byte) {
	defer crom.pbftNode.wakeCLPAControl()
	pp := new(message.PartitionPrepare)
	if err := json.Unmarshal(content, pp); err != nil {
		log.Panic(err)
	}
	crom.cdm.PartitionStateLock.Lock()
	defer crom.cdm.PartitionStateLock.Unlock()
	if crom.cdm.ControlEventsEnabled {
		if pp.Epoch <= crom.cdm.AccountTransferRound {
			return
		}
		if pp.Epoch > crom.cdm.AccountTransferRound+1 {
			return
		}
		if pp.Epoch != crom.cdm.AccountTransferRound+1 || pp.TargetSeq == 0 || (pp.Epoch == crom.cdm.PendingEpoch && pp.TargetSeq != crom.cdm.PendingTargetSeq) {
			log.Panic("invalid CLPA partition prepare epoch or target")
		}
	}
	if accepted, err := crom.acceptPendingSnapshotPrepare(pp); err != nil {
		log.Panic(err)
	} else if !accepted {
		return
	}
	if crom.pbftNode.CurChain.GARETEnabled() {
		committed := crom.pbftNode.CurChain.GARETEpoch()
		if pp.Epoch <= committed {
			return
		}
		if pp.Epoch != committed+1 || pp.TargetSeq == 0 {
			log.Panic("invalid GARET partition prepare epoch or target")
		}
		if pp.Epoch == crom.cdm.PendingEpoch && pp.TargetSeq != crom.cdm.PendingTargetSeq {
			log.Panic("conflicting GARET partition prepare")
		}
	}
	if crom.cdm.ControlEventsEnabled {
		crom.cdm.ReadySeqLock.Lock()
		for _, seq := range crom.cdm.ReadySeq {
			if seq < pp.TargetSeq {
				crom.cdm.ReadySeqLock.Unlock()
				log.Panic("buffered CLPA Ready precedes target")
			}
		}
		crom.cdm.ReadySeqLock.Unlock()
	}
	// 只接受单调向前的 Prepare（防重复 / 乱序）
	if pp.Epoch > crom.cdm.PendingEpoch {
		crom.cdm.PendingEpoch = pp.Epoch
		crom.cdm.PendingTargetSeq = pp.TargetSeq
	}
	if params.GLASPendingFullWindowEarlyEnabled() {
		crom.activatePendingEarlyPlanLocked()
	}
	crom.pbftNode.pl.Plog.Printf("DIAG CLPA prepare arrived: epoch=%d, targetSeq=%d, pendingEpoch=%d, pendingTargetSeq=%d, round=%d, localSeq=%d, partitionOn=%t\n",
		pp.Epoch, pp.TargetSeq, crom.cdm.PendingEpoch, crom.cdm.PendingTargetSeq, crom.cdm.AccountTransferRound, crom.cdm.PendingSnapshotObservedSeq, crom.cdm.PartitionOn)
}

// the leader received the partition message from listener/decider,
// it init the local variant and send the accout message to other leaders.
func (crom *CLPARelayOutsideModule) handlePartitionMsg(content []byte) {
	defer crom.pbftNode.wakeCLPAControl()
	crom.cdm.PartitionStateLock.Lock()
	crom.pbftNode.pl.Plog.Printf("DIAG CLPA partition msg handling start: bytes=%d, localSeq=%d, pbftStage=%d, partitionOn=%t\n", len(content), crom.cdm.PendingSnapshotObservedSeq, crom.pbftNode.pbftStage.Load(), crom.cdm.PartitionOn)
	crom.cdm.PartitionStateLock.Unlock()
	pm := new(message.PartitionModifiedMap)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}
	if crom.cdm.ControlEventsEnabled {
		if params.GLASPendingFullWindowEarlyEnabled() {
			return // A staged plan is activated only by the matching prepare.
		}
		crom.acceptCLPAEventMap(pm)
		return
	}
	accepted, validationErr := crom.acceptGARETPartition(pm)
	if validationErr != nil {
		log.Panicf("invalid partition metadata: %v", validationErr)
	}
	if !accepted {
		return
	}
	crom.cdm.PartitionStateLock.Lock()
	defer crom.cdm.PartitionStateLock.Unlock()
	if pm.GARET == nil {
		crom.cdm.ModifiedMap = append(crom.cdm.ModifiedMap, pm.PartitionModified)
	}
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has received partition message\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
	crom.cdm.PartitionOn = true
	crom.pbftNode.pl.Plog.Printf("DIAG CLPA partition msg handling done: entries=%d, localSeq=%d, pbftStage=%d, partitionOn=%t\n", len(pm.PartitionModified), crom.cdm.PendingSnapshotObservedSeq, crom.pbftNode.pbftStage.Load(), crom.cdm.PartitionOn)
}

// wait for other shards' last rounds are over
func (crom *CLPARelayOutsideModule) handlePartitionReady(content []byte) {
	defer crom.pbftNode.wakeCLPAControl()
	pr := new(message.PartitionReady)
	err := json.Unmarshal(content, pr)
	if err != nil {
		log.Panic()
	}
	if crom.cdm.ControlEventsEnabled {
		crom.acceptCLPAEventReady(pr)
		return
	}
	crom.cdm.PartitionStateLock.Lock()
	partitionOn := crom.cdm.PartitionOn
	crom.cdm.PartitionStateLock.Unlock()
	crom.pbftNode.pl.Plog.Printf("DIAG CLPA ready handling: fromShard=%d, readySeq=%d, localSeq=%d, partitionOn=%t, pbftStage=%d\n", pr.FromShard, pr.NowSeqID, crom.cdm.PendingSnapshotObservedSeq, partitionOn, crom.pbftNode.pbftStage.Load())
	crom.cdm.P_ReadyLock.Lock()
	crom.cdm.PartitionReady[pr.FromShard] = true
	crom.cdm.P_ReadyLock.Unlock()

	crom.cdm.ReadySeqLock.Lock()
	crom.cdm.ReadySeq[pr.FromShard] = pr.NowSeqID
	crom.cdm.ReadySeqLock.Unlock()

	crom.pbftNode.pl.Plog.Printf("ready message from shard %d, seqid is %d\n", pr.FromShard, pr.NowSeqID)
}

// when the message from other shard arriving, it should be added into the message pool
//
// 并发安全注意：每条入站 AccountStateAndTx 都在独立 goroutine 中被 handle
// (handleMessage 的 default 分支 go ohm.HandleMessageOutsidePBFT)，
// partition flow 期间 31 个 shard 的 accountStateAndTx 几乎同时到达，
// 因此 AccountStateTx[X] = at 这个 map 写**必须**在锁内完成，否则 Go runtime
// 检测到并发 map 写会直接 fatal-throw 整个进程。CollectLock 就是这个用途。
func (crom *CLPARelayOutsideModule) handleAccountStateAndTxMsg(content []byte) {
	defer crom.pbftNode.wakeCLPAControl()
	at := new(message.AccountStateAndTx)
	if err := json.Unmarshal(content, at); err != nil {
		log.Panic()
	}

	if crom.cdm.ControlEventsEnabled {
		crom.acceptCLPAEventMigration(at)
		return
	}

	// Keep validation and collection in the same epoch as the group commit.
	if crom.pbftNode.CurChain.GARETEnabled() {
		crom.pbftNode.unifiedExecutionMu.Lock()
		defer crom.pbftNode.unifiedExecutionMu.Unlock()
	}
	if err := crom.validateGARETBatch(at); err != nil {
		log.Panicf("invalid account migration batch: %v", err)
	}
	crom.cdm.CollectLock.Lock()
	if at.GARET != nil {
		if prior, exists := crom.cdm.AccountStateTx[at.FromShard]; exists {
			if !reflect.DeepEqual(prior, at) {
				crom.cdm.CollectLock.Unlock()
				log.Panic("conflicting GARET account migration batch")
			}
			crom.cdm.CollectLock.Unlock()
			return
		}
	}
	crom.cdm.AccountStateTx[at.FromShard] = at
	done := len(crom.cdm.AccountStateTx) == int(crom.pbftNode.pbftChainConfig.ShardNums)-1
	if done {
		crom.cdm.CollectOver = true
	}
	crom.cdm.CollectLock.Unlock()

	crom.pbftNode.pl.Plog.Printf("S%dN%d has added the accoutStateandTx from %d to pool\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, at.FromShard)
	if done {
		crom.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
	}
}
