// The pbft consensus process

package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

type PbftConsensusNode struct {
	// the local config about pbft
	RunningNode *shard.Node // the node information
	ShardID     uint64      // denote the ID of the shard (or pbft), only one pbft consensus in a shard
	NodeID      uint64      // denote the ID of the node in the pbft (shard)

	// the data structure for blockchain
	CurChain *chain.BlockChain // all node in the shard maintain the same blockchain
	// Serializes opt-in baseline admission/proposal with an execution-map change.
	unifiedExecutionMu sync.Mutex
	db                 ethdb.Database // to save the mpt

	// the global config about pbft
	pbftChainConfig *params.ChainConfig          // the chain config in this pbft
	ip_nodeTable    map[uint64]map[uint64]string // denote the ip of the specific node
	node_nums       uint64                       // the number of nodes in this pfbt, denoted by N
	malicious_nums  uint64                       // f, 3f + 1 = N

	// view change
	view                    atomic.Int32 // denote the view of this pbft, the main node can be inferred from this variant
	clpaStep4               *clpaStep4Timing
	clpaWakeOnce            sync.Once
	clpaControlWake         chan struct{}
	clpaContextOnce         sync.Once
	clpaContext             context.Context
	clpaCancel              context.CancelFunc
	lastCommitTime          atomic.Int64 // the time since last commit.
	lastPartitionCommitTime atomic.Int64 // reset the next local tick after a partition block commits.
	viewChangeMap           map[ViewChangeData]map[uint64]bool
	newViewMap              map[ViewChangeData]map[uint64]bool

	// the control message and message checking utils in pbft
	sequenceID         uint64                          // the message sequence id of the pbft
	stopSignal         atomic.Bool                     // send stop signal
	pStop              chan uint64                     // channle for stopping consensus
	requestPool        map[string]*message.Request     // RequestHash to Request
	cntPrepareConfirm  map[string]map[*shard.Node]bool // count the prepare confirm message, [messageHash][Node]bool
	cntCommitConfirm   map[string]map[*shard.Node]bool // count the commit confirm message, [messageHash][Node]bool
	isCommitBordcast   map[string]bool                 // denote whether the commit is broadcast
	isReply            map[string]bool                 // denote whether the message is reply
	height2Digest      map[uint64]string               // sequence (block height) -> request, fast read
	committedRequests  map[uint64]bool                 // cached sequence has actually completed local execution
	supersededRequests map[string]bool                 // replaced proposal vote indexes to retire under pbftLock
	requestMu          sync.RWMutex                    // protects the bounded request/index cache
	stopOnce           sync.Once                       // shutdown and database close happen once

	// pbft stage wait
	pbftStage              atomic.Int32 // 1->Preprepare, 2->Prepare, 3->Commit, 4->Done
	prePrepareAccepted     bool         // current proposal validated; pbftLock guards this (leader reserves stage 2 before its self-message)
	pbftLock               sync.Mutex
	conditionalVarpbftLock sync.Cond

	// locks about pbft
	sequenceLock   sync.Mutex // the lock of sequence
	lock           sync.Mutex // lock the stage
	catchUpPending bool       // one outstanding range, protected by pbftLock
	catchUpStart   uint64
	catchUpEnd     uint64

	// seqID of other Shards, to synchronize
	seqIDMap   map[uint64]uint64
	seqMapLock sync.Mutex

	// logger
	pl *pbft_log.PbftLog
	// tcp control
	tcpln       net.Listener
	tcpPoolLock sync.Mutex

	// to handle the message in the pbft
	ihm ExtraOpInConsensus

	// to handle the message outside of pbft
	ohm OpInterShards
}

// generate a pbft consensus for a node
func NewPbftNode(shardID, nodeID uint64, pcc *params.ChainConfig, messageHandleType string) *PbftConsensusNode {
	p := new(PbftConsensusNode)
	garetCatalog, garetErr := loadGARETWorkerCatalog(messageHandleType)
	if garetErr != nil {
		log.Panic(garetErr)
	}
	p.ip_nodeTable = params.IPmap_nodeTable
	p.node_nums = pcc.Nodes_perShard
	p.ShardID = shardID
	p.NodeID = nodeID
	p.pbftChainConfig = pcc
	if err := validateGLASPoolFreshStart(messageHandleType, params.DatabaseWrite_path, shardID, nodeID); err != nil {
		log.Panic(err)
	}
	fp := params.DatabaseWrite_path + "mptDB/ldb/s" + strconv.FormatUint(shardID, 10) + "/n" + strconv.FormatUint(nodeID, 10)
	var err error
	p.db, err = rawdb.NewLevelDBDatabase(fp, 0, 1, "accountState", false)
	if err != nil {
		log.Panic(err)
	}
	p.CurChain, err = chain.NewBlockChain(pcc, p.db)
	if err != nil {
		log.Panic("cannot new a blockchain")
	}

	if garetCatalog != nil {
		if err := p.CurChain.InitGARETRouting(garetCatalog); err != nil {
			log.Panic(err)
		}
	}

	p.RunningNode = &shard.Node{
		NodeID:  nodeID,
		ShardID: shardID,
		IPaddr:  p.ip_nodeTable[shardID][nodeID],
	}

	p.stopSignal.Store(false)
	p.sequenceID = p.CurChain.CurrentBlock.Header.Number + 1
	p.pStop = make(chan uint64)
	p.requestPool = make(map[string]*message.Request)
	p.cntPrepareConfirm = make(map[string]map[*shard.Node]bool)
	p.cntCommitConfirm = make(map[string]map[*shard.Node]bool)
	p.isCommitBordcast = make(map[string]bool)
	p.isReply = make(map[string]bool)
	p.height2Digest = make(map[uint64]string)
	p.malicious_nums = (p.node_nums - 1) / 3

	// init view & last commit time
	p.view.Store(0)
	p.lastCommitTime.Store(time.Now().Add(time.Second * 5).UnixMilli())
	p.viewChangeMap = make(map[ViewChangeData]map[uint64]bool)
	p.newViewMap = make(map[ViewChangeData]map[uint64]bool)

	p.seqIDMap = make(map[uint64]uint64)

	p.pl = pbft_log.NewPbftLog(shardID, nodeID)

	// choose how to handle the messages in pbft or beyond pbft
	switch string(messageHandleType) {
	case "CLPA_Broker":
		ncdm := dataSupport.NewCLPADataSupport()
		p.ihm = &CLPAPbftInsideExtraHandleMod_forBroker{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPABrokerOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "CLPA", "CLPA_Gas", "P_Louvain_Gas", "TxAllo_Contrib", "P_Louvain_Contrib", "GARET", "GLAS_Pending":
		ncdm := dataSupport.NewCLPADataSupport()
		ncdm.ControlEventsEnabled = clpaControlEventsEnabled(messageHandleType)
		ncdm.ControlPolling = ncdm.ControlEventsEnabled && params.CLPAReconfigurationScheduler == params.CLPAReconfigurationSchedulerPoll
		ncdm.PendingSnapshotMode = messageHandleType == "GLAS_Pending"
		ncdm.PendingSnapshotObservedSeq = p.sequenceID
		p.ihm = &CLPAPbftInsideExtraHandleMod{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPARelayOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "Broker":
		p.ihm = &RawBrokerPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawBrokerOutsideModule{
			pbftNode: p,
		}
	default:
		p.ihm = &RawRelayPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawRelayOutsideModule{
			pbftNode: p,
		}
	}

	// set pbft stage now
	p.conditionalVarpbftLock = *sync.NewCond(&p.pbftLock)
	p.pbftStage.Store(1)
	go p.observeMemory()

	return p
}

// A restarted GLAS worker has neither its leader's uncommitted pool nor the
// run's pool-owner bootstrap in memory. Reusing its ledger would therefore not
// be recovery, even if the persisted chain is still at genesis. Inspect only
// this worker's existing database paths before opening/creating either one;
// sibling workers may have already created the shared parent directories.
func validateGLASPoolFreshStart(mode, databaseRoot string, shardID, nodeID uint64) error {
	if !params.UnifiedExecution {
		return nil
	}
	paths := []string{
		filepath.Join(databaseRoot, "mptDB", "ldb", fmt.Sprintf("s%d", shardID), fmt.Sprintf("n%d", nodeID)),
		filepath.Join(databaseRoot, "chainDB", fmt.Sprintf("S%d_N%d", shardID, nodeID)),
	}
	for _, path := range paths {
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("execution cannot inspect worker database %q: %w", path, err)
		}
		nonempty := info.Size() != 0
		if info.IsDir() {
			entries, err := os.ReadDir(path)
			if err != nil {
				return fmt.Errorf("execution cannot inspect worker database %q: %w", path, err)
			}
			nonempty = len(entries) != 0
		}
		if nonempty {
			return fmt.Errorf("execution pool cannot recover existing worker database %q; use a new ExpDataRootDir for a fresh experiment; existing data was not deleted", path)
		}
	}
	return nil
}

// Low-frequency diagnostics use the existing node log, never copy a pool, and
// never force GC. HeapIdle minus HeapReleased helps distinguish retained runtime
// pages from live heap objects; these counters are not process RSS.
func (p *PbftConsensusNode) observeMemory() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		p.logMemory("periodic", 0)
		select {
		case <-p.pStop:
			return
		case <-ticker.C:
		}
	}
}

// Phase samples share the existing log and only count owned objects. Each
// component takes its own lock; this is diagnostic, not an atomic system snapshot.
// Call outside pool/data-support locks and never force a GC to obtain a sample.
func (p *PbftConsensusNode) logMemory(stage string, epoch uint64) {
	if p == nil || p.pl == nil || p.pl.Plog == nil {
		return
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	requests, requestBytes := p.requestCacheStats()
	queued := 0
	if p.CurChain != nil && p.CurChain.Txpool != nil {
		queued = p.CurChain.Txpool.GetTxQueueLen()
	}
	prefix := "GLAS-MEMORY"
	if stage == "periodic" {
		prefix = "MEMORY"
	}
	p.pl.Plog.Printf("%s stage=%s epoch=%d heap_alloc=%d heap_inuse=%d heap_idle=%d heap_released=%d runtime_sys=%d gc=%d goroutines=%d request_cache=%d request_payload_bytes=%d queued_records=%d\n",
		prefix, stage, epoch, memory.HeapAlloc, memory.HeapInuse, memory.HeapIdle, memory.HeapReleased, memory.Sys,
		memory.NumGC, runtime.NumGoroutine(), requests, requestBytes, queued)
}

// handle the raw message, send it to corresponded interfaces
func (p *PbftConsensusNode) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	// pbft inside message type
	case message.CPrePrepare:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handlePrePrepare(content)
	case message.CPrepare:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handlePrepare(content)
	case message.CCommit:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handleCommit(content)

	case message.ViewChangePropose:
		p.handleViewChangeMsg(content)
	case message.NewChange:
		p.handleNewViewMsg(content)

	case message.CRequestOldrequest:
		p.handleRequestOldSeq(content)
	case message.CSendOldrequest:
		p.handleSendOldSeq(content)

	case message.CStop:
		var stop message.StopRequest
		_ = json.Unmarshal(content, &stop) // Older senders did not include a token.
		p.waitToStop(stop.Token)

	// handle the message from outside
	default:
		go p.ohm.HandleMessageOutsidePBFT(msgType, content)
	}
}

func (p *PbftConsensusNode) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		if p.stopSignal.Load() {
			return
		}
		switch err {
		case nil:
			p.tcpPoolLock.Lock()
			p.handleMessage(clientRequest)
			p.tcpPoolLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

// A consensus node starts tcp-listen.
func (p *PbftConsensusNode) TcpListen() {
	ln, err := net.Listen("tcp", p.RunningNode.IPaddr)
	p.tcpln = ln
	if err != nil {
		log.Panic(err)
	}
	for {
		conn, err := p.tcpln.Accept()
		if err != nil {
			return
		}
		go p.handleClientRequest(conn)
	}
}

// When receiving a stop message, this node try to stop.
func (p *PbftConsensusNode) WaitToStop() {
	p.waitToStop("")
}

func (p *PbftConsensusNode) waitToStop(token string) {
	p.stopOnce.Do(func() {
		p.pl.Plog.Println("handling stop message")
		p.stopSignal.Store(true)
		p.cancelCLPAControl()
		// Acknowledge entering shutdown, not completed shutdown. The launcher
		// still waits for actual process exit. Send before cancelling the pool.
		ack, err := json.Marshal(message.StopAck{Token: token, ShardID: p.ShardID, NodeID: p.NodeID})
		if err == nil && p.ip_nodeTable != nil {
			addr := p.ip_nodeTable[params.SupervisorShard][0]
			if addr != "" {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err = networks.TcpDialContext(ctx, message.MergeMessage(message.CStopAck, ack), addr)
				cancel()
			}
		}
		if err != nil {
			p.pl.Plog.Printf("stop acknowledgement failed: %v", err)
		}
		networks.CloseAllConnInPool()
		if p.tcpln != nil {
			_ = p.tcpln.Close()
		}
		// Wake PBFT stage waiters so they do not retain decoded requests while
		// stopped. Serialize database close against an already-running commit.
		p.pbftLock.Lock()
		p.conditionalVarpbftLock.Broadcast()
		p.closePbft()
		p.pbftLock.Unlock()
		p.pl.Plog.Println("handled stop message in TCPListen Routine")
		if p.pStop != nil {
			close(p.pStop)
		}
	})
}

// close the pbft
func (p *PbftConsensusNode) closePbft() {
	if p.CurChain != nil {
		p.CurChain.CloseBlockChain()
	}
}
