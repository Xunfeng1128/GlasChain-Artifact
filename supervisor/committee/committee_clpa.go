package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// CLPA committee operations
type CLPACommitteeModule struct {
	injectionObservation
	installation     clpaInstallationTracker
	formalRunContext context.Context
	csvPath          string
	dataTotalNum     int
	nowDataNum       int
	batchDataNum     int

	// additional variants
	curEpoch            int32
	clpaLock            sync.Mutex
	clpaGraph           *partition.CLPAState
	clpaWeightMode      string
	modifiedMap         map[string]uint64
	clpaLastRunningTime time.Time
	clpaFreq            int

	// Optional map generator; the shared injection, timer and migration path stays here.
	partitionPlanner func(uint64) (map[string]uint64, error)
	// Algorithm-specific metadata/routing only; pacing and reconfiguration stay shared.
	inputRouting           func(string) uint64
	partitionPublishStaged func(uint64) bool
	partitionRecoverStaged func(context.Context, uint64, []uint64)
	partitionInstalled     func(uint64) // Advance the formal round after matching installation receipts.
	dispatchObserved       func([]*core.Transaction, uint64, time.Time)
	partitionGARET         func(uint64) *message.GARETPlacement
	skipHistoryGraph       bool

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string

	// shardMaxSeq[shardID] 记录该 shard 上报过的最新 BlockHeight。
	// HandleBlockInfo 写入，partitionPrepareSend 读取以计算 TargetSeq。
	shardMaxSeq     map[uint64]uint64
	shardMaxSeqLock sync.Mutex
}

func NewCLPACommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, clpaFrequency int) *CLPACommitteeModule {
	return newCLPACommitteeModuleWithWeightMode(Ip_nodeTable, Ss, sl, csvFilePath, dataNum, batchNum, clpaFrequency, "tx")
}

func NewCLPAGasCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, clpaFrequency int) *CLPACommitteeModule {
	return newCLPACommitteeModuleWithWeightMode(Ip_nodeTable, Ss, sl, csvFilePath, dataNum, batchNum, clpaFrequency, "gas")
}

func newCLPACommitteeModuleWithWeightMode(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, clpaFrequency int, weightMode string) *CLPACommitteeModule {
	cg := new(partition.CLPAState)
	cg.Init_CLPAState(params.CLPAWeightPenalty, 100, params.ShardNum)
	return &CLPACommitteeModule{
		csvPath:             csvFilePath,
		dataTotalNum:        dataNum,
		batchDataNum:        batchNum,
		nowDataNum:          0,
		clpaGraph:           cg,
		clpaWeightMode:      weightMode,
		modifiedMap:         make(map[string]uint64),
		clpaFreq:            clpaFrequency,
		clpaLastRunningTime: time.Time{},
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,
		shardMaxSeq:         make(map[uint64]uint64),
	}
}

func (ccm *CLPACommitteeModule) HandleOtherMessage(raw []byte) {
	kind, body := message.SplitMessage(raw)
	if kind != message.CPartitionInstalled {
		return
	}
	var receipt message.PartitionInstalled
	if err := json.Unmarshal(body, &receipt); err != nil {
		ccm.installation.fail(fmt.Errorf("decode partition installation: %w", err))
		return
	}
	if started := ccm.installation.accept(receipt); !started.IsZero() && ccm.sl != nil &&
		params.CLPAReconfigurationProfile == params.CLPAReconfigurationProfileInstallClock {
		ccm.sl.Slog.Printf("DIAG CLPA reconfiguration interval started: epoch=%d, allInstalledUnixNS=%d, intervalSeconds=%d\n", receipt.Epoch, started.UnixNano(), ccm.clpaFreq)
	}
}

func (ccm *CLPACommitteeModule) fetchModifiedMap(key string) uint64 {
	if ccm.inputRouting != nil {
		return ccm.inputRouting(key)
	}
	if val, ok := ccm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (ccm *CLPACommitteeModule) addCLPAEdge(tx *core.Transaction) {
	sender := partition.Vertex{Addr: tx.Sender}
	recipient := partition.Vertex{Addr: tx.Recipient}
	if ccm.clpaWeightMode == "gas" {
		ccm.clpaGraph.AddWeightedEdge(sender, recipient, core.BaseTransactionGas(tx))
		return
	}
	ccm.clpaGraph.AddEdge(sender, recipient)
}

func (ccm *CLPACommitteeModule) txSending(txlist []*core.Transaction) {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// send to shard
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				at := time.Now()
				if ccm.formalRunStopped(at) {
					return
				}
				if ccm.dispatchObserved != nil {
					ccm.dispatchObserved(sendToShard[sid], sid, at)
				}
				ccm.observeInjectionAt(sendToShard[sid], sid, at)
				go networks.TcpDial(send_msg, ccm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			if !ccm.waitControlTick() {
				return
			}
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		if err := priceCommitteeInput(tx, ccm.fetchModifiedMap); err != nil {
			log.Panic(err)
		}
		sendersid := ccm.fetchModifiedMap(tx.Sender)
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

func (ccm *CLPACommitteeModule) MsgSendingControl() {
	reader, err := openCommitteeInput(ccm.csvPath, ccm.dataTotalNum)
	if err != nil {
		log.Panic(err)
	}
	defer reader.Close()
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)
	clpaCnt := 0
	var audit *formalMapSnapshot
	if ccm.formalRunContext != nil && params.FormalMapCheckpoint > 0 {
		defer func() {
			if ccm.recorder.CompletionProgress().Complete() {
				if err := audit.save(); err != nil {
					log.Panicf("E1 mapping checkpoint: %v", err)
				}
			}
		}()
	}
	for {
		if ccm.formalRunContext != nil && ccm.formalRunStopped(time.Now()) {
			return
		}
		tx, readErr := reader.Next(uint64(ccm.nowDataNum))
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			log.Panic(readErr)
		}
		txlist = append(txlist, tx)
		ccm.nowDataNum++

		// batch sending condition
		if len(txlist) == int(ccm.batchDataNum) || ccm.nowDataNum == ccm.dataTotalNum {
			if params.E2PrefixCount > 0 && ccm.Ss.E2StopRequested() {
				return
			}
			// set the algorithm timer begins
			if ccm.clpaLastRunningTime.IsZero() {
				ccm.clpaLastRunningTime = time.Now()
			}
			if position := params.FormalMapCheckpoint; position > 0 && audit == nil {
				audit = ccm.formalMapForBatch(position, len(txlist), uint64(clpaCnt))
			}
			ccm.txSending(txlist)

			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			ccm.Ss.StopGap_ResetAll()
		}

		if ccm.formalRunContext != nil && ccm.formalRunStopped(time.Now()) {
			return
		}
		if params.ShardNum > 1 && ccm.reconfigurationDue() {
			// Finish any round already entered, but do not begin a new
			// migration after this E2 observation has closed.
			if params.E2PrefixCount > 0 && ccm.Ss.E2StopRequested() {
				return
			}
			clpaCnt++
			if !ccm.controlReconfiguration(uint64(clpaCnt), ccm.nowDataNum < ccm.dataTotalNum) {
				return
			}
		}

		if ccm.nowDataNum == ccm.dataTotalNum {
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for ccm.formalRunContext != nil || !ccm.Ss.GapEnough() {
		if !ccm.waitControlTick() {
			return
		}
		if params.E2PrefixCount > 0 && ccm.Ss.E2StopRequested() {
			return
		}
		if params.ShardNum > 1 && ccm.reconfigurationDue() {
			clpaCnt++
			if !ccm.controlReconfiguration(uint64(clpaCnt), false) {
				return
			}
		}
	}
	if params.FormalMapCheckpoint > 0 {
		if err := audit.save(); err != nil {
			log.Panicf("E1 mapping checkpoint: %v", err)
		}
	}
}

// Called by the existing sending/draining loop, not the network handlers.
// Preserve the receive-time origin: observing a block must not reset it.
func (ccm *CLPACommitteeModule) reconfigurationDue() bool {
	if params.FixedBlockEpochBoundariesEnabled() {
		return ccm.formalReconfigurationDue()
	}
	if params.CLPAReconfigurationProfile != params.CLPAReconfigurationProfileInstallClock {
		return !ccm.clpaLastRunningTime.IsZero() &&
			time.Since(ccm.clpaLastRunningTime) >= time.Duration(ccm.clpaFreq)*time.Second
	}
	started, err := ccm.installation.intervalStart(ccm.clpaLastRunningTime)
	if err != nil {
		log.Panic(err)
	}
	if started.IsZero() {
		return false
	}
	ccm.clpaLastRunningTime = started
	return time.Since(started) >= time.Duration(ccm.clpaFreq)*time.Second
}

// Keep the existing one-block target offset. A lagging shard continues normal
// consensus until this target; a shard already there pauses at its next idle
// consensus boundary. Transport delay can let a shard pass the observed target,
// so this is not a promise that every shard stops at exactly the same height.
const partitionTargetBuffer uint64 = 1

func (ccm *CLPACommitteeModule) pickPartitionTargetSeq() uint64 {
	ccm.shardMaxSeqLock.Lock()
	defer ccm.shardMaxSeqLock.Unlock()
	var maxSeq uint64 = 0
	for _, h := range ccm.shardMaxSeq {
		if h > maxSeq {
			maxSeq = h
		}
	}
	return maxSeq + partitionTargetBuffer
}

// Notify all replicas of the next partition epoch before planning/map dispatch.
// Leaders drive migration; followers also need to recognize the intentional
// pause rather than triggering a view change for a missing ordinary block.
func (ccm *CLPACommitteeModule) partitionPrepareSend(epoch uint64) {
	targetSeq := ccm.pickPartitionTargetSeq()
	if params.FixedBlockEpochBoundariesEnabled() {
		targetSeq = params.ExperimentEpochTarget(epoch)
	}
	if err := ccm.installation.begin(epoch, targetSeq, params.ShardNum); err != nil {
		log.Panic(err)
	}
	pp := message.PartitionPrepare{Epoch: epoch, TargetSeq: targetSeq}
	ppByte, err := json.Marshal(pp)
	if err != nil {
		log.Panic(err)
	}
	send_msg := message.MergeMessage(message.CPartitionPrepare, ppByte)
	// Every replica must know this intentional pause; only leaders coordinate.
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		for _, address := range ccm.IpNodeTable[i] {
			if address == "" {
				log.Panicf("empty CLPA prepare address for shard %d", i)
			}
			ccm.sendPartitionControl(send_msg, address)
		}
	}
	ccm.sl.Slog.Printf("DIAG CLPA partition prepare dispatched: epoch=%d, targetSeq=%d, bytes=%d\n", epoch, targetSeq, len(send_msg))
}

func (ccm *CLPACommitteeModule) clpaMapSend(m map[string]uint64) {
	// Bind the expected delta before any worker can install it and reply.
	var groupPlacement *message.GARETPlacement
	if ccm.partitionGARET != nil {
		groupPlacement = ccm.partitionGARET(ccm.installation.preparedEpoch())
	}
	epoch, err := ccm.installation.bindPlan(m, groupPlacement)
	if err != nil {
		log.Panic(err)
	}
	pm := message.PartitionModifiedMap{Epoch: epoch, PartitionModified: m, GARET: groupPlacement}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
	ccm.sl.Slog.Printf("DIAG CLPA partition map built: entries=%d, bytes=%d\n", len(m), len(send_msg))
	// send to worker shards
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		targetShard := i
		targetAddr := ccm.IpNodeTable[i][0]
		traceName := fmt.Sprintf("partitionMap-shard-%d", targetShard)
		ccm.sl.Slog.Printf("DIAG CLPA partition send scheduled: targetShard=%d, addr=%s, bytes=%d\n", targetShard, targetAddr, len(send_msg))
		networks.TcpDialWithTrace(send_msg, targetAddr, traceName, func(err error, waitBeforeWrite, writeCost, totalCost time.Duration) {
			if err != nil {
				ccm.sl.Slog.Printf("DIAG CLPA partition send done: targetShard=%d, addr=%s, bytes=%d, waitBeforeWrite=%s, writeCost=%s, totalCost=%s, err=%v\n", targetShard, targetAddr, len(send_msg), waitBeforeWrite, writeCost, totalCost, err)
				return
			}
			ccm.sl.Slog.Printf("DIAG CLPA partition send done: targetShard=%d, addr=%s, bytes=%d, waitBeforeWrite=%s, writeCost=%s, totalCost=%s\n", targetShard, targetAddr, len(send_msg), waitBeforeWrite, writeCost, totalCost)
		})
	}
	ccm.sl.Slog.Println("Supervisor: all partition map send tasks scheduled. ")
}

func (ccm *CLPACommitteeModule) clpaReset() {
	ccm.clpaGraph = new(partition.CLPAState)
	ccm.clpaGraph.Init_CLPAState(params.CLPAWeightPenalty, 100, params.ShardNum)
	for key, val := range ccm.modifiedMap {
		ccm.clpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
}

func (ccm *CLPACommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	ccm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	// 记录该 shard 上报的最新 BlockHeight，用于 partitionPrepareSend 计算 TargetSeq
	ccm.shardMaxSeqLock.Lock()
	if b.BlockHeight > ccm.shardMaxSeq[b.SenderShardID] {
		ccm.shardMaxSeq[b.SenderShardID] = b.BlockHeight
	}
	ccm.shardMaxSeqLock.Unlock()
	if atomic.CompareAndSwapInt32(&ccm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		ccm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}
	if b.BlockBodyLength == 0 || ccm.skipHistoryGraph {
		return
	}
	ccm.clpaLock.Lock()
	for _, tx := range b.InnerShardTxs {
		ccm.addCLPAEdge(tx)
	}
	for _, r2tx := range b.Relay2Txs {
		ccm.addCLPAEdge(r2tx)
	}
	ccm.clpaLock.Unlock()
}

// Match the original CLPA supervisor: one new-epoch ordinary block report is
// sufficient, checked once per second. Installation receipts do not release it.
func (ccm *CLPACommitteeModule) waitNewEpochBlock(epoch uint64) error {
	for atomic.LoadInt32(&ccm.curEpoch) != int32(epoch) {
		if !ccm.waitControlTick() {
			return errFormalRunStopped
		}
	}
	if err := ccm.installation.finishObservedEpoch(epoch); err != nil {
		return err
	}
	ccm.sl.Slog.Printf("DIAG CLPA new epoch ordinary block observed: epoch=%d; injection may resume\n", epoch)
	return nil
}
