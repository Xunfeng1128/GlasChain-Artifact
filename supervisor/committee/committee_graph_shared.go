package committee

import (
	"fmt"
	"sync"

	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
)

// GraphSharedCommitteeModule changes only the lifecycle around an existing
// graph planner. All input, prepare, installation, interval and resume logic is
// inherited from CLPA; the original constructors remain available unchanged.
type GraphSharedCommitteeModule struct {
	*CLPACommitteeModule
	historyLock  *sync.Mutex
	addHistory   func(*core.Transaction)
	closeMetrics func() error
}

func NewGraphSharedCommitteeModule(method string, table map[uint64]map[uint64]string,
	ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, path string, count, batch, frequency int,
) *GraphSharedCommitteeModule {
	base := NewCLPACommitteeModule(table, ss, sl, path, count, batch, frequency)
	base.skipHistoryGraph = true
	module := &GraphSharedCommitteeModule{CLPACommitteeModule: base}
	var optimize func(uint64) map[string]uint64
	var reset func()
	switch method {
	case "TxAllo_Contrib":
		planner := NewTxAlloContribCommitteeModule(table, ss, sl, path, count, batch, frequency)
		planner.modifiedMap = base.modifiedMap
		module.historyLock, module.addHistory = &planner.txalloLock, planner.addGraphEdge
		optimize, reset = planner.optimizePartition, planner.graph.Reset
		module.closeMetrics = planner.metrics.Close
	case "P_Louvain_Contrib":
		planner := NewPLouvainContribCommitteeModule(table, ss, sl, path, count, batch, frequency)
		planner.modifiedMap = base.modifiedMap
		module.historyLock, module.addHistory = &planner.plouvainContribLock, planner.addGraphEdge
		optimize, reset = planner.optimizePartition, planner.graph.Reset
		module.closeMetrics = planner.metrics.Close
	case "P_Louvain_Gas":
		planner := NewPLouvainGasCommitteeModule(table, ss, sl, path, count, batch, frequency)
		planner.modifiedMap = base.modifiedMap
		module.historyLock, module.addHistory = &planner.plouvainLock, planner.addGraphEdge
		optimize, reset = planner.optimizePartition, planner.graph.Reset
		module.closeMetrics = planner.metrics.Close
	default:
		panic(fmt.Errorf("unsupported shared graph planner %q", method))
	}
	base.partitionPlanner = func(epoch uint64) (map[string]uint64, error) {
		module.historyLock.Lock()
		defer module.historyLock.Unlock()
		base.partitionPrepareSend(epoch)
		mapping := optimize(epoch)
		reset()
		return mapping, nil
	}
	return module
}

func (module *GraphSharedCommitteeModule) MsgSendingControl() {
	defer func() {
		if module.closeMetrics != nil {
			if err := module.closeMetrics(); err != nil {
				panic(fmt.Errorf("close graph planner metrics: %w", err))
			}
		}
	}()
	module.CLPACommitteeModule.MsgSendingControl()
}

func (module *GraphSharedCommitteeModule) HandleBlockInfo(block *message.BlockInfoMsg) {
	if block == nil {
		return
	}
	module.CLPACommitteeModule.HandleBlockInfo(block)
	if block.BlockBodyLength == 0 {
		return
	}
	module.historyLock.Lock()
	defer module.historyLock.Unlock()
	// Retain the original methods' completed logical-transaction population.
	// Relay1 is not a completion and internal call records are not graph edges.
	for _, tx := range block.InnerShardTxs {
		module.addHistory(tx)
	}
	for _, tx := range block.Relay2Txs {
		module.addHistory(tx)
	}
}
