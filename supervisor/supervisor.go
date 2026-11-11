// Supervisor is an abstract role in this simulator that may read txs, generate partition infos,
// and handle history data.

package supervisor

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/committee"
	"blockEmulator/supervisor/measure"
	"blockEmulator/supervisor/observability"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/supervisor/unifiedmetrics"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type Supervisor struct {
	// basic infos
	IPaddr       string // ip address of this Supervisor
	ChainConfig  *params.ChainConfig
	Ip_nodeTable map[uint64]map[uint64]string

	// tcp control
	listenStop bool
	tcpLn      net.Listener
	tcpLock    sync.Mutex
	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss *signal.StopSignal // to control the stop message sending

	// supervisor and committee components
	comMod committee.CommitteeModule

	// measure components
	testMeasureMods []measure.MeasureModule

	blockInfoReceiptLock sync.Mutex
	stopLock             sync.Mutex
	stopToken            string
	stopAcks             map[[2]uint64]bool
	stopInFlight         map[[2]uint64]bool
	stopWake             chan struct{}

	// Unified observation is opt-in and does not replace legacy measurements.
	unifiedRecorder      *unifiedmetrics.Recorder
	unifiedMetricsClosed bool
	formalRunCancel      context.CancelFunc
	e2ReceiptFence       *sync.RWMutex // E2 seal waits for timestamped receipts queued behind tcpLock
	unifiedBlockDigests  map[[2]uint64][32]byte

	// diy, add more structures or classes here ...
}

func (d *Supervisor) NewSupervisor(ip string, pcc *params.ChainConfig, committeeMethod string, measureModNames ...string) {
	d.IPaddr = ip
	d.ChainConfig = pcc
	d.Ip_nodeTable = params.IPmap_nodeTable

	d.sl = supervisor_log.NewSupervisorLog()

	d.Ss = signal.NewStopSignal(int(pcc.ShardNums))

	switch committeeMethod {
	case "CLPA_Broker":
		d.comMod = committee.NewCLPACommitteeMod_Broker(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "CLPA":
		d.comMod = committee.NewCLPACommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "CLPA_Gas":
		d.comMod = committee.NewCLPAGasCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "P_Louvain_Gas":
		if params.CLPAReconfigurationScheduler == params.CLPAReconfigurationSchedulerPoll {
			d.comMod = committee.NewGraphSharedCommitteeModule(committeeMethod, d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
		} else {
			d.comMod = committee.NewPLouvainGasCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
		}
	case "TxAllo_Contrib":
		if params.CLPAReconfigurationScheduler == params.CLPAReconfigurationSchedulerPoll {
			d.comMod = committee.NewGraphSharedCommitteeModule(committeeMethod, d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
		} else {
			d.comMod = committee.NewTxAlloContribCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
		}
	case "P_Louvain_Contrib":
		if params.CLPAReconfigurationScheduler == params.CLPAReconfigurationSchedulerPoll {
			d.comMod = committee.NewGraphSharedCommitteeModule(committeeMethod, d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
		} else {
			d.comMod = committee.NewPLouvainContribCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
		}
	case "GARET":
		if params.CLPAReconfigurationScheduler == params.CLPAReconfigurationSchedulerPoll {
			d.comMod = committee.NewGARETSharedCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
		} else {
			d.comMod = committee.NewGARETCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
		}
	case "GLAS_Pending":
		d.comMod = committee.NewGLASPendingCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "Broker":
		d.comMod = committee.NewBrokerCommitteeMod(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	default:
		d.comMod = committee.NewRelayCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	}

	d.initUnifiedMetrics(committeeMethod)

	d.testMeasureMods = make([]measure.MeasureModule, 0)
	for _, mModName := range measureModNames {
		switch mModName {
		case "TPS_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Relay())
		case "TPS_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Broker())
		case "TCL_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Relay())
		case "TCL_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Broker())
		case "CrossTxRate_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Relay())
		case "CrossTxRate_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Broker())
		case "TxNumberCount_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Relay())
		case "TxNumberCount_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Broker())
		case "Tx_Details":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxDetail())
		case "Tx_Execution_Details":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxExecutionDetails())
		default:
		}
	}
}

// Supervisor received the block information from the leaders, and handle these
// message to measure the performances.
func (d *Supervisor) handleBlockInfos(content []byte) {
	d.handleBlockInfosAt(content, time.Now())
}

func (d *Supervisor) handleBlockInfosAt(content []byte, observedAt time.Time) {
	bim := new(message.BlockInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}
	if d.unifiedRecorder != nil {
		duplicate, err := d.deduplicateUnifiedBlockInfo(bim)
		if err != nil {
			log.Panicf("unified BlockInfo deduplication: %v", err)
		}
		if duplicate {
			// Preserve raw replay timestamps for offline earliest-receipt
			// aggregation, but never repeat graph or stop-signal side effects.
			if !d.unifiedMetricsClosed {
				if err := d.unifiedRecorder.RecordBlock(bim, observedAt); err != nil {
					log.Panicf("record unified replay metrics: %v", err)
				}
			}
			return
		}
	}
	// Record before committee handlers mutate derived counters. The timestamp
	// was captured at receipt, before waiting for the supervisor dispatch lock.
	if d.unifiedRecorder != nil && !d.unifiedMetricsClosed {
		if err := d.unifiedRecorder.RecordBlock(bim, observedAt); err != nil {
			log.Panicf("record unified block metrics: %v", err)
		}
	}
	// StopSignal check
	if bim.BlockBodyLength == 0 {
		d.Ss.StopGap_Inc(bim.SenderShardID)
	} else {
		d.Ss.StopGap_Reset(bim.SenderShardID)
	}

	if timed, ok := d.comMod.(interface {
		HandleBlockInfoAt(*message.BlockInfoMsg, time.Time)
	}); ok {
		timed.HandleBlockInfoAt(bim, observedAt)
	} else {
		d.comMod.HandleBlockInfo(bim)
	}

	// measure update
	for _, measureMod := range d.testMeasureMods {
		measureMod.UpdateMeasureRecord(bim)
	}
	// add codes here ...
}

// read transactions from dataFile. When the number of data is enough,
// the Supervisor will do re-partition and send partitionMSG and txs to leaders.
func (d *Supervisor) SupervisorTxHandling() {
	// Read-only observation; the existing control loop owns all experiment timing.
	if d.sl != nil {
		stopMemoryObservation := observability.StartMemoryObserver(d.sl.Slog, 30*time.Second)
		defer stopMemoryObservation()
	}
	var runErr error
	grace := time.Duration(0)
	if d.unifiedRecorder == nil {
		d.comMod.MsgSendingControl()
		for !d.Ss.GapEnough() {
			time.Sleep(time.Second)
		}
	} else {
		grace = unifiedCompletionGrace()
		d.sl.Slog.Printf("Supervisor: unified completion guard enabled; idle no-progress grace=%s\n", grace)
		if d.formalRunCancel != nil {
			runErr = d.waitGLASFormalCompletion(grace)
		} else {
			runErr = d.waitUnifiedCompletion(startCommitteeControl(d.comMod), grace)
		}
	}
	if runErr == nil {
		// A verified complete run excludes stop-ACK time from its observation.
		runErr = d.finishUnifiedMetrics(nil, grace)
	} else {
		// Keep accepting late receipts during bounded worker shutdown. Preserve
		// a diagnostic immediately as well, in case shutdown itself fails.
		d.sl.Slog.Printf("Supervisor: refusing successful completion: %v\n", runErr)
		d.tcpLock.Lock()
		diagnosticErr := d.unifiedRecorder.WriteCompletionDiagnostic(runErr.Error(), grace)
		d.tcpLock.Unlock()
		if diagnosticErr != nil {
			d.sl.Slog.Printf("Supervisor: completion diagnostic error: %v\n", diagnosticErr)
		}
	}
	d.sl.Slog.Println("Supervisor: stopping all nodes and waiting for acknowledgements")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	err := d.stopWorkers(ctx)
	cancel()
	if err == nil {
		d.sl.Slog.Printf("Supervisor: stop acknowledgements complete %d/%d; launcher will verify process exit", d.ChainConfig.ShardNums*d.ChainConfig.Nodes_perShard, d.ChainConfig.ShardNums*d.ChainConfig.Nodes_perShard)
	}
	if err != nil {
		if runErr == nil {
			runErr = err
		} else {
			runErr = fmt.Errorf("%v; worker shutdown: %w", runErr, err)
		}
	}
	if d.unifiedRecorder != nil && !d.unifiedMetricsClosed {
		runErr = d.finishUnifiedMetrics(runErr, grace)
	}

	d.sl.Slog.Println("Supervisor: now Closing")
	d.listenStop = true
	d.CloseSupervisor()
	if runErr != nil {
		// A failed stop barrier must not look like a successful run. The launcher
		// will terminate any remaining PIDs owned by this experiment.
		log.Panicf("incomplete experiment: %v", runErr)
	}
}

// A committee may retain control while draining EOF (notably GLAS). Funnel
// its failures to the supervisor so worker shutdown and log sealing still run.
func startCommitteeControl(module committee.CommitteeModule) <-chan error {
	result := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			if value := recover(); value != nil {
				err = fmt.Errorf("committee control failed: %v", value)
			}
			result <- err
		}()
		module.MsgSendingControl()
	}()
	return result
}

func (d *Supervisor) acceptStopAck(ack message.StopAck) {
	d.stopLock.Lock()
	defer d.stopLock.Unlock()
	if d.stopToken == "" || ack.Token != d.stopToken || ack.ShardID >= d.ChainConfig.ShardNums || ack.NodeID >= d.ChainConfig.Nodes_perShard {
		return
	}
	d.stopAcks[[2]uint64{ack.ShardID, ack.NodeID}] = true
	select {
	case d.stopWake <- struct{}{}:
	default:
	}
}

func (d *Supervisor) stopWorkers(ctx context.Context) error {
	d.stopLock.Lock()
	d.stopToken = fmt.Sprintf("%s/%d", d.IPaddr, time.Now().UnixNano())
	d.stopAcks = make(map[[2]uint64]bool)
	d.stopInFlight = make(map[[2]uint64]bool)
	d.stopWake = make(chan struct{}, 1)
	payload, _ := json.Marshal(message.StopRequest{Token: d.stopToken})
	wake := d.stopWake
	d.stopLock.Unlock()
	wire := message.MergeMessage(message.CStop, payload)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		d.stopLock.Lock()
		if len(d.stopAcks) == int(d.ChainConfig.ShardNums*d.ChainConfig.Nodes_perShard) {
			d.stopLock.Unlock()
			return nil
		}
		for sid := uint64(0); sid < d.ChainConfig.ShardNums; sid++ {
			for nid := uint64(0); nid < d.ChainConfig.Nodes_perShard; nid++ {
				key := [2]uint64{sid, nid}
				if d.stopAcks[key] || d.stopInFlight[key] {
					continue
				}
				d.stopInFlight[key] = true
				go func(key [2]uint64, address string) {
					sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					_ = networks.TcpDialContext(sendCtx, wire, address)
					d.stopLock.Lock()
					delete(d.stopInFlight, key)
					d.stopLock.Unlock()
				}(key, d.Ip_nodeTable[sid][nid])
			}
		}
		d.stopLock.Unlock()
		select {
		case <-ctx.Done():
			d.stopLock.Lock()
			count := len(d.stopAcks)
			d.stopLock.Unlock()
			return fmt.Errorf("stop acknowledgements %d/%d: %w", count, d.ChainConfig.ShardNums*d.ChainConfig.Nodes_perShard, ctx.Err())
		case <-wake:
		case <-ticker.C:
		}
	}
}

// handle message. only one message to be handled now
func (d *Supervisor) handleMessage(msg []byte) {
	d.handleMessageAt(msg, time.Now())
}

func (d *Supervisor) handleMessageAt(msg []byte, observedAt time.Time) {
	msgType, content := message.SplitMessage(msg)
	if d.e2ReceiptFence != nil && msgType != message.CStopAck {
		if d.unifiedMetricsClosed {
			return
		}
		if d.formalRunCancel != nil {
			if deadline := d.unifiedRecorder.E2Deadline(); !deadline.IsZero() && !observedAt.Before(deadline) {
				return
			}
		}
	}
	switch msgType {
	case message.CStopAck:
		var ack message.StopAck
		if json.Unmarshal(content, &ack) == nil {
			d.acceptStopAck(ack)
		}
	case message.CBlockInfo:
		d.handleBlockInfosAt(content, observedAt)
		// add codes for more functionality
	default:
		d.comMod.HandleOtherMessage(msg)
		for _, mm := range d.testMeasureMods {
			mm.HandleExtraMessage(msg)
		}
	}
}

func (d *Supervisor) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		switch err {
		case nil:
			if d.e2ReceiptFence != nil {
				d.e2ReceiptFence.RLock()
			}
			observedAt := time.Now()
			d.tcpLock.Lock()
			d.handleMessageAt(clientRequest, observedAt)
			d.tcpLock.Unlock()
			if d.e2ReceiptFence != nil {
				d.e2ReceiptFence.RUnlock()
			}
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

func (d *Supervisor) TcpListen() {
	ln, err := net.Listen("tcp", d.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	d.tcpLn = ln
	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			return
		}
		go d.handleClientRequest(conn)
	}
}

// close Supervisor, and record the data in .csv file
func (d *Supervisor) CloseSupervisor() {
	d.closeUnifiedMetrics()
	d.sl.Slog.Println("Closing...")
	for _, measureMod := range d.testMeasureMods {
		d.sl.Slog.Println(measureMod.OutputMetricName())
		d.sl.Slog.Println(measureMod.OutputRecord())
		println()
	}
	networks.CloseAllConnInPool()
	if d.tcpLn != nil {
		d.tcpLn.Close()
	}
}
