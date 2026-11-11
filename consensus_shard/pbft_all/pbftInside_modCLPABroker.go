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

type CLPAPbftInsideExtraHandleMod_forBroker struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
}

// propose request with different types
func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) HandleinPropose() (bool, *message.Request) {
	doPartition := false
	if cphm.cdm.PendingEpoch == cphm.cdm.AccountTransferRound+1 {
		seq := cphm.pbftNode.sequenceID
		target := cphm.cdm.PendingTargetSeq
		if seq < target {
			cphm.pbftNode.pl.Plog.Printf("DIAG CLPA_Broker pre-target normal: seq=%d < target=%d, pendingEpoch=%d, partitionOn=%t\n",
				seq, target, cphm.cdm.PendingEpoch, cphm.cdm.PartitionOn)
		} else {
			if !cphm.cdm.PartitionOn {
				cphm.pbftNode.pl.Plog.Printf("DIAG CLPA_Broker barrier wait: seq=%d, target=%d, pendingEpoch=%d, round=%d\n",
					seq, target, cphm.cdm.PendingEpoch, cphm.cdm.AccountTransferRound)
				for !cphm.cdm.PartitionOn &&
					cphm.cdm.PendingEpoch == cphm.cdm.AccountTransferRound+1 {
					time.Sleep(50 * time.Millisecond)
				}
				cphm.pbftNode.pl.Plog.Printf("DIAG CLPA_Broker barrier released: seq=%d, partitionOn=%t, round=%d, pendingEpoch=%d\n",
					cphm.pbftNode.sequenceID, cphm.cdm.PartitionOn, cphm.cdm.AccountTransferRound, cphm.cdm.PendingEpoch)
			}
			doPartition = cphm.cdm.PartitionOn
		}
	}

	if doPartition {
		cphm.pbftNode.pl.Plog.Printf("DIAG CLPA_Broker propose enter: seq=%d, pbftStage=%d\n", cphm.pbftNode.sequenceID, cphm.pbftNode.pbftStage.Load())
		cphm.sendPartitionReady()
		for !cphm.getPartitionReady() {
			cphm.pbftNode.pl.Plog.Printf("DIAG CLPA_Broker waiting partition ready: seq=%d, pbftStage=%d\n", cphm.pbftNode.sequenceID, cphm.pbftNode.pbftStage.Load())
			time.Sleep(time.Second)
		}
		cphm.pbftNode.pl.Plog.Printf("DIAG CLPA_Broker partition ready done: seq=%d, pbftStage=%d\n", cphm.pbftNode.sequenceID, cphm.pbftNode.pbftStage.Load())
		// send accounts and txs
		cphm.sendAccounts_and_Txs()
		// propose a partition
		for !cphm.getCollectOver() {
			cphm.pbftNode.pl.Plog.Printf("DIAG CLPA_Broker waiting accountStateAndTx: seq=%d, pbftStage=%d\n", cphm.pbftNode.sequenceID, cphm.pbftNode.pbftStage.Load())
			time.Sleep(time.Second)
		}
		cphm.pbftNode.pl.Plog.Printf("DIAG CLPA_Broker collect over, begin proposePartition: seq=%d, pbftStage=%d\n", cphm.pbftNode.sequenceID, cphm.pbftNode.pbftStage.Load())
		return cphm.proposePartition()
	}

	// ELSE: propose a block
	block := cphm.pbftNode.CurChain.GenerateBlock(int32(cphm.pbftNode.NodeID))
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
func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	// judge whether it is a partitionRequest or not
	isPartitionReq := ppmsg.RequestMsg.RequestType == message.PartitionReq

	if isPartitionReq {
		if message.DecodeAccountTransferMsg(ppmsg.RequestMsg.Msg.Content).GARET != nil {
			cphm.pbftNode.pl.Plog.Printf("GARET transfer metadata is not permitted in broker mode\n")
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
func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) HandleinPrepare(pmsg *message.Prepare) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation in commit.
func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) HandleinCommit(cmsg *message.Commit) bool {
	r, _ := cphm.pbftNode.requestByDigest(string(cmsg.Digest))
	// requestType ...
	if r.RequestType == message.PartitionReq {
		// if a partition Requst ...
		atm := message.DecodeAccountTransferMsg(r.Msg.Content)
		cphm.accountTransfer_do(atm)
		return true
	}
	// if a block request ...
	block := core.DecodeB(r.Msg.Content)
	if delayMs := core.ScaleDelayMs(core.ComputeBlockExecDelayMs(block.Body), params.Lambda2); delayMs > 0 {
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID, block.Header.Number, cphm.pbftNode.CurChain.CurrentBlock.Header.Number)
	cphm.pbftNode.CurChain.AddBlock(block)
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID, block.Header.Number)
	cphm.pbftNode.CurChain.PrintBlockChain()

	// now try to relay txs to other shards (for main nodes)
	if cphm.pbftNode.NodeID == uint64(cphm.pbftNode.view.Load()) {
		cphm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send broker_300k_BlockTransaction confirm txs at height = %d \n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID, block.Header.Number)
		// generate brokertxs and collect txs excuted
		innerShardTxs := make([]*core.Transaction, 0)
		broker1Txs := make([]*core.Transaction, 0)
		broker2Txs := make([]*core.Transaction, 0)

		// generate block infos
		for _, tx := range block.Body {
			isBroker1Tx := tx.Sender == tx.OriginalSender
			isBroker2Tx := tx.Recipient == tx.FinalRecipient

			senderIsInshard := cphm.pbftNode.CurChain.Get_PartitionMap(tx.Sender) == cphm.pbftNode.ShardID
			recipientIsInshard := cphm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient) == cphm.pbftNode.ShardID
			if isBroker1Tx && !senderIsInshard {
				log.Panic("Err tx1")
			}
			if isBroker2Tx && !recipientIsInshard {
				log.Panic("Err tx2")
			}
			if tx.RawTxHash == nil {
				if tx.HasBroker {
					if tx.SenderIsBroker && !recipientIsInshard {
						log.Panic("err tx 1 - recipient")
					}
					if !tx.SenderIsBroker && !senderIsInshard {
						log.Panic("err tx 1 - sender")
					}
				} else {
					if !senderIsInshard || !recipientIsInshard {
						log.Panic("err tx - without broker_300k_BlockTransaction")
					}
				}
			}

			if isBroker2Tx {
				broker2Txs = append(broker2Txs, tx)
			} else if isBroker1Tx {
				broker1Txs = append(broker1Txs, tx)
			} else {
				innerShardTxs = append(innerShardTxs, tx)
			}
		}
		// send seqID
		for sid := uint64(0); sid < cphm.pbftNode.pbftChainConfig.ShardNums; sid++ {
			if sid == cphm.pbftNode.ShardID {
				continue
			}
			sii := message.SeqIDinfo{
				SenderShardID: cphm.pbftNode.ShardID,
				SenderSeq:     cphm.pbftNode.sequenceID,
			}
			sByte, err := json.Marshal(sii)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CSeqIDinfo, sByte)
			networks.TcpDial(msg_send, cphm.pbftNode.ip_nodeTable[sid][0])
			cphm.pbftNode.pl.Plog.Printf("S%dN%d : sended sequence ids to %d\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID, sid)
		}
		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   innerShardTxs,
			Broker1Txs:      broker1Txs,
			Broker2Txs:      broker2Txs,
			Epoch:           int(cphm.cdm.AccountTransferRound),
			BlockHeight:     block.Header.Number,
			SenderShardID:   cphm.pbftNode.ShardID,
			ProposeTime:     r.ReqTime,
			CommitTime:      time.Now(),
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		networks.TcpDial(msg_send, cphm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
		cphm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
		cphm.pbftNode.CurChain.Txpool.GetLocked()
		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",
			"# of Broker1 Txs in this block",
			"# of Broker2 Txs in this block",
			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Broker1 Txs) (Duration: Broker1 proposed -> Broker1 Commit)",
			"SUM of confirm latency (ms, Broker2 Txs) (Duration: Broker2 proposed -> Broker2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(cphm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),
			strconv.Itoa(len(broker1Txs)),
			strconv.Itoa(len(broker2Txs)),
			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(broker1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(broker2Txs, bim.CommitTime), 10),
		}
		cphm.pbftNode.writeCSVline(metricName, metricVal)
		cphm.pbftNode.CurChain.Txpool.GetUnlocked()
	}
	return true
}

func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		cphm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				cphm.pbftNode.CurChain.AddBlock(b)
			} else {
				atm := message.DecodeAccountTransferMsg(r.Msg.Content)
				cphm.accountTransfer_do(atm)
			}
		}
		cphm.pbftNode.sequenceID = som.SeqEndHeight + 1
		cphm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}
