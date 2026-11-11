package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
)

type CLPAPbftInsideExtraHandleMod struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
	// Accessed only by ordinary commit/catch-up while pbftLock is held.
	pendingEarlyReportedEpoch uint64
}

// propose request with different types
func (cphm *CLPAPbftInsideExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	if cphm.cdm.ControlEventsEnabled {
		return cphm.clpaEventPropose(false)
	}
	// CLPA targetSeq 对齐 barrier:
	// supervisor 通过 PartitionPrepare(epoch, targetSeq) 预告下一轮 partition 后，
	// 所有 shard 都必须在 sequenceID >= targetSeq 才能出 partition block；
	// 在那之前继续出普通块。这样消除了 shard 间本地节拍漂移导致的 partition 块高错位。
	//
	// 注意：即使 PartitionOn 已经是 true (大 map 已到)，只要 sequenceID < TargetSeq，
	// 也必须先出普通块。这一段用 doPartition 显式接管走向，不能让下面那个老的
	// "if PartitionOn" 兜底分支吃掉决策。
	doPartition := false
	pendingEpoch, target, round, partitionOn := cphm.partitionControlState()
	if pendingEpoch == round+1 {
		seq := cphm.pbftNode.sequenceID
		if seq < target {
			cphm.pbftNode.pl.Plog.Printf("DIAG CLPA pre-target normal: seq=%d < target=%d, pendingEpoch=%d, partitionOn=%t\n",
				seq, target, pendingEpoch, partitionOn)
			// 即使 PartitionOn=true 也强制走普通块路径
		} else {
			// seq >= target：到达 partition 目标高度
			if !partitionOn {
				cphm.pbftNode.pl.Plog.Printf("DIAG CLPA barrier wait: seq=%d, target=%d, pendingEpoch=%d, round=%d\n",
					seq, target, pendingEpoch, round)
				for !partitionOn && pendingEpoch == round+1 {
					if cphm.pbftNode.stopSignal.Load() {
						return false, nil
					}
					time.Sleep(50 * time.Millisecond)
					pendingEpoch, _, round, partitionOn = cphm.partitionControlState()
				}
				cphm.pbftNode.pl.Plog.Printf("DIAG CLPA barrier released: seq=%d, partitionOn=%t, round=%d, pendingEpoch=%d\n",
					cphm.pbftNode.sequenceID, partitionOn, round, pendingEpoch)
			}
			doPartition = partitionOn
		}
	}

	if doPartition {
		cphm.pbftNode.pl.Plog.Printf("DIAG CLPA propose enter: seq=%d, pbftStage=%d\n", cphm.pbftNode.sequenceID, cphm.pbftNode.pbftStage.Load())
		cphm.sendPartitionReady()
		for !cphm.getPartitionReady() {
			cphm.pbftNode.pl.Plog.Printf("DIAG CLPA waiting partition ready: seq=%d, pbftStage=%d\n", cphm.pbftNode.sequenceID, cphm.pbftNode.pbftStage.Load())
			time.Sleep(time.Second)
		}
		cphm.pbftNode.pl.Plog.Printf("DIAG CLPA partition ready done: seq=%d, pbftStage=%d\n", cphm.pbftNode.sequenceID, cphm.pbftNode.pbftStage.Load())
		// send accounts and txs
		cphm.sendAccounts_and_Txs()
		// propose a partition
		for !cphm.getCollectOver() {
			cphm.pbftNode.pl.Plog.Printf("DIAG CLPA waiting accountStateAndTx: seq=%d, pbftStage=%d\n", cphm.pbftNode.sequenceID, cphm.pbftNode.pbftStage.Load())
			time.Sleep(time.Second)
		}
		cphm.pbftNode.pl.Plog.Printf("DIAG CLPA collect over, begin proposePartition: seq=%d, pbftStage=%d\n", cphm.pbftNode.sequenceID, cphm.pbftNode.pbftStage.Load())
		return cphm.proposePartition()
	}

	return cphm.proposeCLPAOrdinaryBlock()
}

func (cphm *CLPAPbftInsideExtraHandleMod) proposeCLPAOrdinaryBlock() (bool, *message.Request) {
	if cphm.epochQuotaOrdinaryPaused() {
		return false, nil
	}
	block := cphm.pbftNode.generateExecutionBlock()
	if delayMs := core.ScaleDelayMs(core.ComputeBlockExecDelayMs(block.Body), params.Lambda1); delayMs > 0 {
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}
	r := &message.Request{
		RequestType: message.BlockRequest,
		ReqTime:     time.Now(),
	}
	r.Msg.Content = block.Encode()
	return true, r

}

// the diy operation in preprepare
func (cphm *CLPAPbftInsideExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	// judge whether it is a partitionRequest or not
	isPartitionReq := ppmsg.RequestMsg.RequestType == message.PartitionReq

	if isPartitionReq {
		atm := message.DecodeAccountTransferMsg(ppmsg.RequestMsg.Msg.Content)
		if err := cphm.validatePendingWindowTransfer(atm); err != nil {
			cphm.pbftNode.pl.Plog.Printf("invalid planning window: %v\n", err)
			return false
		}
		if err := cphm.validateGARETTransfer(atm); err != nil {
			cphm.pbftNode.pl.Plog.Printf("invalid partition metadata: %v\n", err)
			return false
		}
		// after some checking
		cphm.pbftNode.pl.Plog.Printf("S%dN%d : a partition block\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	} else {
		// the request is a block
		if cphm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) != nil {
			cphm.pbftNode.pl.Plog.Printf("S%dN%d : not a valid block\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
			return false
		}
	}
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	cphm.pbftNode.cacheRequest(ppmsg.SeqID, string(ppmsg.Digest), ppmsg.RequestMsg)
	// merge to be a prepare message
	return true
}

// the operation in prepare, and in pbft + tx relaying, this function does not need to do any.
func (cphm *CLPAPbftInsideExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

func (cphm *CLPAPbftInsideExtraHandleMod) applyPartitionRequest(request *message.Request) *message.AccountTransferMsg {
	atm := message.DecodeAccountTransferMsg(request.Msg.Content)
	cphm.accountTransfer_do(atm)
	return atm
}

// the operation in commit.
func (cphm *CLPAPbftInsideExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {
	r, _ := cphm.pbftNode.requestByDigest(string(cmsg.Digest))
	// requestType ...
	if r.RequestType == message.PartitionReq {
		cphm.applyPartitionRequest(r)
		return true
	}
	// if a block request ...
	block := core.DecodeB(r.Msg.Content)
	if err := cphm.pbftNode.commitUnifiedExecutionBlock(block); err != nil {
		log.Panicf("invalid unified execution commit: %v", err)
	}
	if delayMs := core.ScaleDelayMs(core.ComputeBlockExecDelayMs(block.Body), params.Lambda2); delayMs > 0 {
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID, block.Header.Number, cphm.pbftNode.CurChain.CurrentBlock.Header.Number)
	cphm.pbftNode.CurChain.AddBlock(block)
	cphm.observePendingSnapshotSequence(block.Header.Number + 1)
	cphm.observeCLPANormalCommit(block.Header.Number)
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID, block.Header.Number)
	cphm.pbftNode.CurChain.PrintBlockChain()

	// now try to relay txs to other shards (for main nodes)
	if cphm.pbftNode.NodeID == uint64(cphm.pbftNode.view.Load()) {
		cphm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send relay txs at height = %d \n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID, block.Header.Number)
		// generate relay pool and collect txs excuted
		cphm.pbftNode.CurChain.Txpool.RelayPool = make(map[uint64][]*core.Transaction)
		interShardTxs := make([]*core.Transaction, 0)
		relay1Txs := make([]*core.Transaction, 0)
		relay2Txs := make([]*core.Transaction, 0)

		for _, tx := range block.Body {
			ssid := cphm.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
			rsid := cphm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)
			if !tx.Relayed && ssid != cphm.pbftNode.ShardID {
				log.Panic("incorrect tx")
			}
			if tx.Relayed && rsid != cphm.pbftNode.ShardID {
				log.Panic("incorrect tx")
			}
			if rsid != cphm.pbftNode.ShardID {
				relay1Txs = append(relay1Txs, tx)
				relay, err := cphm.pbftNode.executionRelay2(tx, cphm.cdm.AccountTransferRound)
				if err != nil {
					log.Panicf("cannot price Relay2: %v", err)
				}
				cphm.pbftNode.CurChain.Txpool.AddRelayTx(relay, rsid)
			} else {
				if tx.Relayed {
					relay2Txs = append(relay2Txs, tx)
				} else {
					interShardTxs = append(interShardTxs, tx)
				}
			}
		}

		// send relay txs
		if params.RelayWithMerkleProof == 1 {
			cphm.pbftNode.RelayWithProofSend(block)
		} else {
			cphm.pbftNode.RelayMsgSend()
		}

		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   interShardTxs,
			Epoch:           int(cphm.cdm.AccountTransferRound),
			BlockHeight:     block.Header.Number,

			Relay1Txs: relay1Txs,
			Relay2Txs: relay2Txs,

			SenderShardID: cphm.pbftNode.ShardID,
			ProposeTime:   r.ReqTime,
			CommitTime:    time.Now(),
		}
		if params.UnifiedMetrics {
			bim.ExecutionBlockHash = fmt.Sprintf("%x", block.Hash)
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		go networks.TcpDial(msg_send, cphm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
		cphm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)

		cphm.pbftNode.CurChain.Txpool.GetLocked()

		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",
			"# of Relay1 Txs in this block",
			"# of Relay2 Txs in this block",
			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Relay1 Txs) (Duration: Relay1 proposed -> Relay1 Commit)",
			"SUM of confirm latency (ms, Relay2 Txs) (Duration: Relay1 proposed -> Relay2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(cphm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),
			strconv.Itoa(len(relay1Txs)),
			strconv.Itoa(len(relay2Txs)),
			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay2Txs, bim.CommitTime), 10),
		}
		cphm.pbftNode.writeCSVline(metricName, metricVal)
		cphm.pbftNode.CurChain.Txpool.GetUnlocked()
	}
	cphm.observePendingEarlyCommit(block)
	return true
}

func (cphm *CLPAPbftInsideExtraHandleMod) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (cphm *CLPAPbftInsideExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		cphm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				if err := cphm.pbftNode.commitUnifiedExecutionBlock(b); err != nil {
					log.Panicf("invalid unified catch-up block: %v", err)
				}
				cphm.pbftNode.CurChain.AddBlock(b)
				cphm.observePendingSnapshotSequence(b.Header.Number + 1)
				cphm.observeCLPANormalCommit(b.Header.Number)
			} else {
				atm := message.DecodeAccountTransferMsg(r.Msg.Content)
				cphm.accountTransfer_do(atm)
			}
		}
		cphm.pbftNode.sequenceID = som.SeqEndHeight + 1
		cphm.pbftNode.CurChain.PrintBlockChain()
		// Report only the final applied catch-up boundary, never an intermediate
		// fifth block immediately followed by its sixth block in the same batch.
		if len(som.OldRequest) > 0 && som.OldRequest[len(som.OldRequest)-1].RequestType == message.BlockRequest {
			cphm.observePendingEarlyCommit(cphm.pbftNode.CurChain.CurrentBlock)
		}
	}
	return true
}
