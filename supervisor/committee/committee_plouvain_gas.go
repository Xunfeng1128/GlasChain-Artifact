package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition/plouvain"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// PLouvainGasCommitteeModule is the direct-gasification P-Louvain baseline.
// It intentionally observes only completed top-level logical transactions,
// resets graph edges after every reconfiguration, and does not inspect pending
// work or internal calls. The worker-side account-transfer protocol is shared
// with CLPA so runtime execution differs only in the account map generator.
type PLouvainGasCommitteeModule struct {
	injectionObservation
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	curEpoch                 int32
	plouvainLock             sync.Mutex
	graph                    *plouvain.Graph
	modifiedMap              map[string]uint64
	plouvainLastRunningTime  time.Time
	plouvainFrequencySeconds int

	sl          *supervisor_log.SupervisorLog
	Ss          *signal.StopSignal
	IpNodeTable map[uint64]map[uint64]string
	metrics     *plouvainGasMetricsWriter

	shardMaxSeq     map[uint64]uint64
	shardMaxSeqLock sync.Mutex
}

func NewPLouvainGasCommitteeModule(
	IpNodeTable map[uint64]map[uint64]string,
	Ss *signal.StopSignal,
	sl *supervisor_log.SupervisorLog,
	csvFilePath string,
	dataNum, batchNum, frequencySeconds int,
) *PLouvainGasCommitteeModule {
	metrics, err := newPLouvainGasMetricsWriter(params.DataWrite_path)
	if err != nil {
		log.Panicf("create P_Louvain_Gas metrics: %v", err)
	}
	return &PLouvainGasCommitteeModule{
		csvPath:                  csvFilePath,
		dataTotalNum:             dataNum,
		batchDataNum:             batchNum,
		graph:                    plouvain.NewGraph(),
		modifiedMap:              make(map[string]uint64),
		plouvainFrequencySeconds: frequencySeconds,
		IpNodeTable:              IpNodeTable,
		Ss:                       Ss,
		sl:                       sl,
		metrics:                  metrics,
		shardMaxSeq:              make(map[uint64]uint64),
	}
}

func (module *PLouvainGasCommitteeModule) HandleOtherMessage([]byte) {}

func (module *PLouvainGasCommitteeModule) fetchModifiedMap(address string) uint64 {
	if shard, exists := module.modifiedMap[address]; exists {
		return shard
	}
	return uint64(utils.Addr2Shard(address))
}

func (module *PLouvainGasCommitteeModule) addGraphEdge(tx *core.Transaction) {
	if tx == nil {
		return
	}
	if err := module.graph.AddTransaction(tx.Sender, tx.Recipient, core.BaseTransactionGas(tx)); err != nil {
		log.Panicf("add P_Louvain_Gas edge: %v", err)
	}
}

func (module *PLouvainGasCommitteeModule) txSending(txlist []*core.Transaction) {
	sendToShard := make(map[uint64][]*core.Transaction)
	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
				injection := message.InjectTxs{Txs: sendToShard[shard], ToShardID: shard}
				payload, err := json.Marshal(injection)
				if err != nil {
					log.Panic(err)
				}
				module.observeInjection(sendToShard[shard], shard)
				go networks.TcpDial(message.MergeMessage(message.CInject, payload), module.IpNodeTable[shard][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		if err := priceCommitteeInput(tx, module.fetchModifiedMap); err != nil {
			log.Panic(err)
		}
		shard := module.fetchModifiedMap(tx.Sender)
		sendToShard[shard] = append(sendToShard[shard], tx)
	}
}

func (module *PLouvainGasCommitteeModule) MsgSendingControl() {
	defer func() {
		if err := module.metrics.Close(); err != nil {
			module.sl.Slog.Printf("close P_Louvain_Gas metrics: %v\n", err)
		}
	}()
	reader, err := openCommitteeInput(module.csvPath, module.dataTotalNum)
	if err != nil {
		log.Panic(err)
	}
	defer reader.Close()
	txlist := make([]*core.Transaction, 0)
	round := 0
	for {
		tx, readErr := reader.Next(uint64(module.nowDataNum))
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			log.Panic(readErr)
		}
		txlist = append(txlist, tx)
		module.nowDataNum++

		if len(txlist) == module.batchDataNum || module.nowDataNum == module.dataTotalNum {
			if module.plouvainLastRunningTime.IsZero() {
				module.plouvainLastRunningTime = time.Now()
			}
			module.txSending(txlist)
			txlist = make([]*core.Transaction, 0)
			module.Ss.StopGap_ResetAll()
		}

		if module.partitionDue() {
			round++
			observation := module.beginReconfiguration(uint64(round), module.nowDataNum < module.dataTotalNum, "observed_new_epoch_block")
			module.runPartition(uint64(round))
			module.waitForPartitionRound(round)
			observation.finish("completed")
		}
		if module.nowDataNum == module.dataTotalNum {
			break
		}
	}

	for !module.Ss.GapEnough() {
		time.Sleep(time.Second)
		if module.partitionDue() {
			round++
			observation := module.beginReconfiguration(uint64(round), false, "observed_new_epoch_block")
			module.runPartition(uint64(round))
			module.waitForPartitionRound(round)
			observation.finish("completed")
		}
	}
}

func (module *PLouvainGasCommitteeModule) partitionDue() bool {
	return params.ShardNum > 1 && !module.plouvainLastRunningTime.IsZero() &&
		time.Since(module.plouvainLastRunningTime) >= time.Duration(module.plouvainFrequencySeconds)*time.Second
}

func (module *PLouvainGasCommitteeModule) waitForPartitionRound(round int) {
	for atomic.LoadInt32(&module.curEpoch) != int32(round) {
		time.Sleep(time.Second)
	}
	module.plouvainLastRunningTime = time.Now()
	module.sl.Slog.Printf("Next P_Louvain_Gas epoch begins: %d\n", round)
}

func (module *PLouvainGasCommitteeModule) runPartition(round uint64) {
	module.plouvainLock.Lock()
	defer module.plouvainLock.Unlock()

	module.partitionPrepareSend(round)
	placement := module.optimizePartition(round)

	module.partitionMapSend(placement)
	for address, shard := range placement {
		module.modifiedMap[address] = shard
	}
	module.graph.Reset()
}

// optimizePartition is shared by the original lifecycle and the explicit CLPA adapter.
// Caller holds the method graph lock; this function performs no network operations.
func (module *PLouvainGasCommitteeModule) optimizePartition(round uint64) map[string]uint64 {
	current := make(map[string]uint64, module.graph.VertexCount())
	for _, address := range module.graph.Vertices() {
		current[address] = module.fetchModifiedMap(address)
	}
	result, err := plouvain.Optimize(module.graph, current, plouvain.Config{
		ShardCount:          params.ShardNum,
		Beta:                params.PLouvainGasBeta,
		MaxRefinementPasses: params.PLouvainGasMaxIterations,
		BlockGasLimit:       params.BlockGasLimit,
	})
	if err != nil {
		log.Panicf("P_Louvain_Gas round %d: %v", round, err)
	}
	if err := module.metrics.Write(round, params.PLouvainGasBeta, params.BlockGasLimit, result); err != nil {
		log.Panicf("write P_Louvain_Gas metrics for round %d: %v", round, err)
	}
	module.sl.Slog.Printf(
		"P_Louvain_Gas round=%d graphTxs=%d graphGas=%.0f communities=%d moves=%d beforeCrossGas=%.6f afterCrossGas=%.6f beforeMaxGas=%.0f afterMaxGas=%.0f beforeMaxBlocks=%d afterMaxBlocks=%d\n",
		round, result.GraphTransactions, result.GraphGas, len(result.Communities), result.MoveOperations,
		result.Before.CrossGasRatio, result.After.CrossGasRatio, result.Before.MaxPhysicalGas, result.After.MaxPhysicalGas,
		result.Before.MaxRequiredBlocks, result.After.MaxRequiredBlocks,
	)

	return result.Placement
}
func (module *PLouvainGasCommitteeModule) pickPartitionTargetSeq() uint64 {
	module.shardMaxSeqLock.Lock()
	defer module.shardMaxSeqLock.Unlock()
	maxSeq := uint64(0)
	for _, height := range module.shardMaxSeq {
		if height > maxSeq {
			maxSeq = height
		}
	}
	return maxSeq + partitionTargetBuffer
}

func (module *PLouvainGasCommitteeModule) partitionPrepareSend(round uint64) {
	prepare := message.PartitionPrepare{Epoch: round, TargetSeq: module.pickPartitionTargetSeq()}
	payload, err := json.Marshal(prepare)
	if err != nil {
		log.Panic(err)
	}
	msg := message.MergeMessage(message.CPartitionPrepare, payload)
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		go networks.TcpDial(msg, module.IpNodeTable[shard][0])
	}
	module.sl.Slog.Printf("DIAG P_Louvain_Gas prepare dispatched: round=%d targetSeq=%d bytes=%d\n", round, prepare.TargetSeq, len(msg))
}

func (module *PLouvainGasCommitteeModule) partitionMapSend(placement map[string]uint64) {
	payload, err := json.Marshal(message.PartitionModifiedMap{PartitionModified: placement})
	if err != nil {
		log.Panic(err)
	}
	msg := message.MergeMessage(message.CPartitionMsg, payload)
	module.sl.Slog.Printf("DIAG P_Louvain_Gas map built: entries=%d bytes=%d\n", len(placement), len(msg))
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		targetShard := shard
		targetAddress := module.IpNodeTable[shard][0]
		traceName := fmt.Sprintf("plouvainGasMap-shard-%d", targetShard)
		networks.TcpDialWithTrace(msg, targetAddress, traceName, func(err error, waitBeforeWrite, writeCost, totalCost time.Duration) {
			if err != nil {
				module.sl.Slog.Printf("DIAG P_Louvain_Gas map send: shard=%d bytes=%d wait=%s write=%s total=%s err=%v\n", targetShard, len(msg), waitBeforeWrite, writeCost, totalCost, err)
				return
			}
			module.sl.Slog.Printf("DIAG P_Louvain_Gas map send: shard=%d bytes=%d wait=%s write=%s total=%s\n", targetShard, len(msg), waitBeforeWrite, writeCost, totalCost)
		})
	}
}

func (module *PLouvainGasCommitteeModule) HandleBlockInfo(block *message.BlockInfoMsg) {
	module.shardMaxSeqLock.Lock()
	if block.BlockHeight > module.shardMaxSeq[block.SenderShardID] {
		module.shardMaxSeq[block.SenderShardID] = block.BlockHeight
	}
	module.shardMaxSeqLock.Unlock()
	if atomic.CompareAndSwapInt32(&module.curEpoch, int32(block.Epoch-1), int32(block.Epoch)) {
		module.sl.Slog.Printf("P_Louvain_Gas current epoch updated: %d\n", block.Epoch)
	}
	if block.BlockBodyLength == 0 {
		return
	}
	module.plouvainLock.Lock()
	for _, tx := range block.InnerShardTxs {
		module.addGraphEdge(tx)
	}
	// Relay2 is the single completed logical record for a cross-shard
	// transaction. Relay1 is deliberately excluded to avoid double-counting.
	for _, tx := range block.Relay2Txs {
		module.addGraphEdge(tx)
	}
	module.plouvainLock.Unlock()
}
