package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	plouvaincontrib "blockEmulator/partition/plouvain_contrib"
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

// PLouvainContribCommitteeModule uses the original transaction-count P-Louvain branch from
// ContribChain (isDeterministic=true, isTxAllo=false). It is separate from
// the existing Gas-weighted P-Louvain and TxAllo modes.
// It intentionally observes only completed top-level logical transactions,
// resets graph edges after every reconfiguration, and does not inspect pending
// work or internal calls. The worker-side account-transfer protocol is shared
// with CLPA so runtime execution differs only in the account map generator.
type PLouvainContribCommitteeModule struct {
	injectionObservation
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	curEpoch                        int32
	plouvainContribLock             sync.Mutex
	graph                           *plouvaincontrib.Graph
	modifiedMap                     map[string]uint64
	plouvainContribLastRunningTime  time.Time
	plouvainContribFrequencySeconds int

	sl          *supervisor_log.SupervisorLog
	Ss          *signal.StopSignal
	IpNodeTable map[uint64]map[uint64]string
	metrics     *plouvainContribMetricsWriter

	shardMaxSeq     map[uint64]uint64
	shardMaxSeqLock sync.Mutex
}

func NewPLouvainContribCommitteeModule(
	IpNodeTable map[uint64]map[uint64]string,
	Ss *signal.StopSignal,
	sl *supervisor_log.SupervisorLog,
	csvFilePath string,
	dataNum, batchNum, frequencySeconds int,
) *PLouvainContribCommitteeModule {
	metrics, err := newPLouvainContribMetricsWriter(params.DataWrite_path)
	if err != nil {
		log.Panicf("create P_Louvain_Contrib metrics: %v", err)
	}
	return &PLouvainContribCommitteeModule{
		csvPath:                         csvFilePath,
		dataTotalNum:                    dataNum,
		batchDataNum:                    batchNum,
		graph:                           plouvaincontrib.NewGraph(),
		modifiedMap:                     make(map[string]uint64),
		plouvainContribFrequencySeconds: frequencySeconds,
		IpNodeTable:                     IpNodeTable,
		Ss:                              Ss,
		sl:                              sl,
		metrics:                         metrics,
		shardMaxSeq:                     make(map[uint64]uint64),
	}
}

func (module *PLouvainContribCommitteeModule) HandleOtherMessage([]byte) {}

func (module *PLouvainContribCommitteeModule) fetchModifiedMap(address string) uint64 {
	if shard, exists := module.modifiedMap[address]; exists {
		return shard
	}
	return uint64(utils.Addr2Shard(address))
}

func (module *PLouvainContribCommitteeModule) addGraphEdge(tx *core.Transaction) {
	if tx == nil {
		return
	}
	if err := module.graph.AddTransaction(tx.Sender, tx.Recipient); err != nil {
		log.Panicf("add P_Louvain_Contrib edge: %v", err)
	}
}

func (module *PLouvainContribCommitteeModule) txSending(txlist []*core.Transaction) {
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

func (module *PLouvainContribCommitteeModule) MsgSendingControl() {
	defer func() {
		if err := module.metrics.Close(); err != nil {
			module.sl.Slog.Printf("close P_Louvain_Contrib metrics: %v\n", err)
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
			log.Panicf("P_Louvain_Contrib input ended after %d accepted transactions; requested %d", module.nowDataNum, module.dataTotalNum)
		}
		if readErr != nil {
			log.Panic(readErr)
		}
		txlist = append(txlist, tx)
		module.nowDataNum++

		if len(txlist) == module.batchDataNum || module.nowDataNum == module.dataTotalNum {
			if module.plouvainContribLastRunningTime.IsZero() {
				module.plouvainContribLastRunningTime = time.Now()
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

func (module *PLouvainContribCommitteeModule) partitionDue() bool {
	return params.ShardNum > 1 && !module.plouvainContribLastRunningTime.IsZero() &&
		time.Since(module.plouvainContribLastRunningTime) >= time.Duration(module.plouvainContribFrequencySeconds)*time.Second
}

func (module *PLouvainContribCommitteeModule) waitForPartitionRound(round int) {
	for atomic.LoadInt32(&module.curEpoch) != int32(round) {
		time.Sleep(time.Second)
	}
	module.plouvainContribLastRunningTime = time.Now()
	module.sl.Slog.Printf("Next P_Louvain_Contrib epoch begins: %d\n", round)
}

func (module *PLouvainContribCommitteeModule) runPartition(round uint64) {
	module.plouvainContribLock.Lock()
	defer module.plouvainContribLock.Unlock()

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
func (module *PLouvainContribCommitteeModule) optimizePartition(round uint64) map[string]uint64 {
	current := make(map[string]uint64, module.graph.VertexCount())
	for _, address := range module.graph.Vertices() {
		current[address] = module.fetchModifiedMap(address)
	}
	result, err := plouvaincontrib.Optimize(module.graph, current, plouvaincontrib.Config{
		ShardCount:    params.ShardNum,
		Beta:          params.PLouvainContribBeta,
		MaxIterations: params.PLouvainContribMaxIterations,
	})
	if err != nil {
		log.Panicf("P_Louvain_Contrib round %d: %v", round, err)
	}
	if err := module.metrics.Write(round, params.PLouvainContribBeta, result); err != nil {
		log.Panicf("write P_Louvain_Contrib metrics for round %d: %v", round, err)
	}
	module.sl.Slog.Printf(
		"P_Louvain_Contrib round=%d graphTxs=%d communities=%d movedAccounts=%d beforeCrossRate=%.6f afterCrossRate=%.6f beforeMaxCountLoad=%.6f afterMaxCountLoad=%.6f\n",
		round, result.GraphTransactions, result.CommunityCount, result.MovedAccounts,
		result.Before.CrossTransactionRate, result.After.CrossTransactionRate,
		result.Before.MaxModeledLoad, result.After.MaxModeledLoad,
	)

	return result.Placement
}
func (module *PLouvainContribCommitteeModule) pickPartitionTargetSeq() uint64 {
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

func (module *PLouvainContribCommitteeModule) partitionPrepareSend(round uint64) {
	prepare := message.PartitionPrepare{Epoch: round, TargetSeq: module.pickPartitionTargetSeq()}
	payload, err := json.Marshal(prepare)
	if err != nil {
		log.Panic(err)
	}
	msg := message.MergeMessage(message.CPartitionPrepare, payload)
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		go networks.TcpDial(msg, module.IpNodeTable[shard][0])
	}
	module.sl.Slog.Printf("DIAG P_Louvain_Contrib prepare dispatched: round=%d targetSeq=%d bytes=%d\n", round, prepare.TargetSeq, len(msg))
}

func (module *PLouvainContribCommitteeModule) partitionMapSend(placement map[string]uint64) {
	payload, err := json.Marshal(message.PartitionModifiedMap{PartitionModified: placement})
	if err != nil {
		log.Panic(err)
	}
	msg := message.MergeMessage(message.CPartitionMsg, payload)
	module.sl.Slog.Printf("DIAG P_Louvain_Contrib map built: entries=%d bytes=%d\n", len(placement), len(msg))
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		targetShard := shard
		targetAddress := module.IpNodeTable[shard][0]
		traceName := fmt.Sprintf("plouvainContribMap-shard-%d", targetShard)
		networks.TcpDialWithTrace(msg, targetAddress, traceName, func(err error, waitBeforeWrite, writeCost, totalCost time.Duration) {
			if err != nil {
				module.sl.Slog.Printf("DIAG P_Louvain_Contrib map send: shard=%d bytes=%d wait=%s write=%s total=%s err=%v\n", targetShard, len(msg), waitBeforeWrite, writeCost, totalCost, err)
				return
			}
			module.sl.Slog.Printf("DIAG P_Louvain_Contrib map send: shard=%d bytes=%d wait=%s write=%s total=%s\n", targetShard, len(msg), waitBeforeWrite, writeCost, totalCost)
		})
	}
}

func (module *PLouvainContribCommitteeModule) HandleBlockInfo(block *message.BlockInfoMsg) {
	if block == nil {
		return
	}
	module.shardMaxSeqLock.Lock()
	if block.BlockHeight > module.shardMaxSeq[block.SenderShardID] {
		module.shardMaxSeq[block.SenderShardID] = block.BlockHeight
	}
	module.shardMaxSeqLock.Unlock()
	if atomic.CompareAndSwapInt32(&module.curEpoch, int32(block.Epoch-1), int32(block.Epoch)) {
		module.sl.Slog.Printf("P_Louvain_Contrib current epoch updated: %d\n", block.Epoch)
	}
	if block.BlockBodyLength == 0 {
		return
	}
	module.plouvainContribLock.Lock()
	for _, tx := range block.InnerShardTxs {
		module.addGraphEdge(tx)
	}
	// Relay2 is the single completed logical record for a cross-shard
	// transaction. Relay1 is deliberately excluded to avoid double-counting.
	for _, tx := range block.Relay2Txs {
		module.addGraphEdge(tx)
	}
	module.plouvainContribLock.Unlock()
}
