package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"blockEmulator/storage"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

func assertPBFTBroadcastTargets(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("broadcast targets=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("broadcast targets=%v want=%v", got, want)
		}
	}
}

func TestPBFTBroadcastTargetsRespectConfiguredMembership(t *testing.T) {
	sharedTable := make(map[uint64]map[uint64]string)
	originalTable := make(map[uint64]map[uint64]string)
	for _, shardID := range []uint64{2, 7} {
		sharedTable[shardID] = make(map[uint64]string)
		originalTable[shardID] = make(map[uint64]string)
		// Insert in descending order so table traversal cannot define the order.
		for nodeID := uint64(16); nodeID > 0; nodeID-- {
			ip := fmt.Sprintf("shard-%d-node-%d", shardID, nodeID-1)
			sharedTable[shardID][nodeID-1] = ip
			originalTable[shardID][nodeID-1] = ip
		}
	}
	for _, test := range []struct {
		name      string
		nodes     uint64
		malicious uint64
	}{{"N4", 4, 1}, {"N16", 16, 5}, {"N1", 1, 0}, {"N0", 0, 0}} {
		t.Run(test.name, func(t *testing.T) {
			node := &PbftConsensusNode{ShardID: 2, node_nums: test.nodes,
				malicious_nums: test.malicious, ip_nodeTable: sharedTable}
			wantLocal, wantOther := make([]string, 0), make([]string, 0)
			for nodeID := uint64(0); nodeID < test.nodes; nodeID++ {
				wantLocal = append(wantLocal, originalTable[2][nodeID])
				wantOther = append(wantOther, originalTable[7][nodeID])
			}
			for repeat := 0; repeat < 20; repeat++ {
				assertPBFTBroadcastTargets(t, node.getNeighborNodes(), wantLocal)
				assertPBFTBroadcastTargets(t, node.getNodeIpsWithinShard(2), wantLocal)
				assertPBFTBroadcastTargets(t, node.getNodeIpsWithinShard(7), wantOther)
			}
			if node.node_nums != test.nodes || node.malicious_nums != test.malicious {
				t.Fatal("broadcast target lookup changed PBFT membership or fault threshold")
			}
			if !reflect.DeepEqual(sharedTable, originalTable) {
				t.Fatal("broadcast target lookup changed the shared IP table")
			}
		})
	}
}

func TestPBFTBroadcastTargetsSkipMissingAndEmptyAddresses(t *testing.T) {
	sharedTable := map[uint64]map[uint64]string{
		2: {0: "local-0", 1: "", 3: "local-3", 4: "inactive-4", 15: "inactive-15"},
		7: {1: "other-1", 2: "", 3: "other-3", 12: "inactive-12"},
		8: nil,
	}
	originalTable := map[uint64]map[uint64]string{
		2: {0: "local-0", 1: "", 3: "local-3", 4: "inactive-4", 15: "inactive-15"},
		7: {1: "other-1", 2: "", 3: "other-3", 12: "inactive-12"},
		8: nil,
	}
	node := &PbftConsensusNode{ShardID: 2, node_nums: 4, malicious_nums: 1, ip_nodeTable: sharedTable}
	assertPBFTBroadcastTargets(t, node.getNeighborNodes(), []string{"local-0", "local-3"})
	assertPBFTBroadcastTargets(t, node.getNodeIpsWithinShard(7), []string{"other-1", "other-3"})
	assertPBFTBroadcastTargets(t, node.getNodeIpsWithinShard(8), nil)
	assertPBFTBroadcastTargets(t, node.getNodeIpsWithinShard(99), nil)
	if node.node_nums != 4 || node.malicious_nums != 1 {
		t.Fatal("sparse IP table changed PBFT membership or fault threshold")
	}
	if !reflect.DeepEqual(sharedTable, originalTable) {
		t.Fatal("sparse target lookup changed the shared IP table")
	}
	empty := &PbftConsensusNode{ShardID: 2, node_nums: 4, malicious_nums: 1}
	assertPBFTBroadcastTargets(t, empty.getNeighborNodes(), nil)
	assertPBFTBroadcastTargets(t, empty.getNodeIpsWithinShard(7), nil)
}

func requestHistoryNode(t *testing.T, path string) *PbftConsensusNode {
	t.Helper()
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	node := &PbftConsensusNode{
		CurChain:          &chain.BlockChain{Storage: &storage.Storage{DataBase: db}},
		cntPrepareConfirm: make(map[string]map[*shard.Node]bool),
		cntCommitConfirm:  make(map[string]map[*shard.Node]bool),
		isCommitBordcast:  make(map[string]bool),
		isReply:           make(map[string]bool),
		pl:                &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)},
	}
	node.conditionalVarpbftLock = *sync.NewCond(&node.pbftLock)
	return node
}

func TestPBFTRecentBlockAndPartitionCacheWithoutDiskHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chain.db")
	node := requestHistoryNode(t, path)
	requests := make([]*message.Request, 0, pbftRequestCacheCount+4)
	types := []message.RequestType{message.BlockRequest, message.PartitionReq}
	for i := 0; i < pbftRequestCacheCount+4; i++ {
		typ := types[i%len(types)]
		request := &message.Request{RequestType: typ,
			ReqTime: time.Unix(100+int64(i), 123456).UTC(),
			Msg:     message.RawMessage{Content: []byte(fmt.Sprintf("raw-payload-%d", i))}}
		digest := string(getDigest(request))
		sequence := uint64(i + 1)
		node.cacheRequest(sequence, digest, request)
		node.markRequestCommitted(sequence, digest)
		node.isReply[digest] = true
		node.cntPrepareConfirm[digest] = map[*shard.Node]bool{{NodeID: 0}: true}
		node.pruneCommittedRequests(sequence + 1)
		requests = append(requests, request)
	}
	if count, _ := node.requestCacheStats(); count > pbftRequestCacheCount {
		t.Fatalf("request cache grew beyond bound: %d", count)
	}
	if len(node.height2Digest) > pbftRequestCacheCount || len(node.committedRequests) > pbftRequestCacheCount || len(node.isReply) != 0 || len(node.cntPrepareConfirm) != 0 {
		t.Fatalf("settled indexes/confirmations were retained: indexes=%d reply=%d prepare=%d", len(node.height2Digest), len(node.isReply), len(node.cntPrepareConfirm))
	}
	for i := 0; i < 4; i++ {
		if _, ok := node.requestPool[string(getDigest(requests[i]))]; ok {
			t.Fatal("old block/partition payload still cached")
		}
		if _, ok := node.requestAtSequence(uint64(i + 1)); ok {
			t.Fatalf("evicted request %d unexpectedly recoverable", i)
		}
	}
	if partial, err := node.cachedRequestRange(1, uint64(len(requests))); err == nil || partial != nil {
		t.Fatal("cache miss returned partial historical range")
	}
	if recent, err := node.cachedRequestRange(5, uint64(len(requests))); err != nil || len(recent) != pbftRequestCacheCount {
		t.Fatalf("recent complete range unavailable: %v", err)
	}
	if err := node.CurChain.Storage.DataBase.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("pbftRequests")) != nil {
			t.Fatal("cache operation created a permanent request archive")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count, _ := node.requestCacheStats(); count > pbftRequestCacheCount {
		t.Fatal("catch-up lookup refilled historical cache")
	}
	if err := node.CurChain.Storage.DataBase.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := requestHistoryNode(t, path)
	for i, request := range requests {
		if _, ok := reopened.requestAtSequence(uint64(i + 1)); ok {
			t.Fatalf("request %d unexpectedly recovered after restart", i)
		}
		if _, ok := reopened.requestByDigest(string(getDigest(request))); ok {
			t.Fatalf("request digest %d unexpectedly recovered after restart", i)
		}
	}
	if count, _ := reopened.requestCacheStats(); count != 0 {
		t.Fatalf("disk reads unexpectedly repopulated cache: %d", count)
	}
}

func TestPBFTRecentCacheProtectsPendingAndLimitsLargePayloads(t *testing.T) {
	node := requestHistoryNode(t, filepath.Join(t.TempDir(), "chain.db"))
	pending := &message.Request{RequestType: message.PartitionReq, Msg: message.RawMessage{Content: []byte("pending")}}
	pendingDigest := string(getDigest(pending))
	node.cacheRequest(100, pendingDigest, pending)
	for i := 1; i <= 10; i++ {
		request := &message.Request{RequestType: message.BlockRequest,
			ReqTime: time.Unix(int64(i), 0).UTC(), Msg: message.RawMessage{Content: bytes.Repeat([]byte{byte(i)}, 4<<20)}}
		digest := string(getDigest(request))
		node.cacheRequest(uint64(i), digest, request)
		node.markRequestCommitted(uint64(i), digest)
		node.pruneCommittedRequests(uint64(i + 1))
	}
	if _, payloadBytes := node.requestCacheStats(); payloadBytes > pbftRequestCacheBytes+len(pending.Msg.Content) {
		t.Fatalf("large payload cache is unbounded: %d", payloadBytes)
	}
	if got, ok := node.requestByDigest(pendingDigest); !ok || got != pending {
		t.Fatal("eviction removed an uncommitted request")
	}
	if _, ok := node.requestAtSequence(1); ok {
		t.Fatal("evicted large request still retained")
	}
	oversized := &message.Request{RequestType: message.PartitionReq,
		Msg: message.RawMessage{Content: make([]byte, pbftRequestCacheBytes+1)}}
	oversizedDigest := string(getDigest(oversized))
	node.cacheRequest(11, oversizedDigest, oversized)
	node.markRequestCommitted(11, oversizedDigest)
	node.pruneCommittedRequests(12)
	if _, ok := node.requestAtSequence(11); ok {
		t.Fatal("single oversized committed payload bypassed the byte bound")
	}
	if _, ok := node.requestAtSequence(100); !ok {
		t.Fatal("oversized committed eviction removed pending request")
	}
}

func TestPBFTRecentCacheRejectsConflictingCommittedSequence(t *testing.T) {
	node := requestHistoryNode(t, filepath.Join(t.TempDir(), "chain.db"))
	first := &message.Request{RequestType: message.BlockRequest, Msg: message.RawMessage{Content: []byte("first")}}
	other := &message.Request{RequestType: message.PartitionReq, Msg: message.RawMessage{Content: []byte("other")}}
	node.cacheRequest(1, string(getDigest(first)), first)
	node.markRequestCommitted(1, string(getDigest(first)))
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("conflicting committed sequence silently replaced cached request")
			}
		}()
		node.cacheRequest(1, string(getDigest(other)), other)
	}()
	got, ok := node.requestAtSequence(1)
	if !ok || string(got.Msg.Content) != "first" {
		t.Fatal("original history damaged by rejected conflict")
	}
}

func TestPBFTPruneNeverDropsUncommittedPayload(t *testing.T) {
	node := requestHistoryNode(t, filepath.Join(t.TempDir(), "chain.db"))
	request := &message.Request{RequestType: message.PartitionReq, Msg: message.RawMessage{Content: []byte("uncommitted")}}
	digest := string(getDigest(request))
	node.cacheRequest(1, digest, request)
	// A view change can expose a newer sequence before catch-up. A numeric
	// watermark alone is not proof that an older payload actually committed.
	node.pruneCommittedRequests(100)
	if got, ok := node.requestByDigest(digest); !ok || got != request {
		t.Fatal("uncommitted request was evicted")
	}
	if node.committedRequests[1] {
		t.Fatal("numeric sequence advance incorrectly marked pending request committed")
	}
}

func TestPBFTStaleViewCannotReopenPrunedConsensusSequence(t *testing.T) {
	node := &PbftConsensusNode{sequenceID: 20}
	old, _ := json.Marshal(message.NewViewMsg{NewSeqID: 1, NextView: 1})
	node.handleNewViewMsg(old)
	change, _ := json.Marshal(message.ViewChangeMsg{SeqID: 1, NextView: 1})
	node.handleViewChangeMsg(change)
	if node.sequenceID != 20 || node.view.Load() != 0 || len(node.newViewMap) != 0 || len(node.viewChangeMap) != 0 {
		t.Fatal("stale view message changed settled consensus state")
	}
}

func TestPBFTStopIsIdempotentAndWakesWaiters(t *testing.T) {
	node := &PbftConsensusNode{pStop: make(chan uint64), pl: &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)}}
	node.conditionalVarpbftLock = *sync.NewCond(&node.pbftLock)
	woke := make(chan struct{})
	go func() {
		node.pbftLock.Lock()
		defer node.pbftLock.Unlock()
		for !node.stopSignal.Load() {
			node.conditionalVarpbftLock.Wait()
		}
		close(woke)
	}()
	node.WaitToStop()
	node.WaitToStop()
	select {
	case <-node.pStop:
	default:
		t.Fatal("shutdown did not close the proposal stop channel")
	}
	select {
	case <-woke:
	case <-time.After(time.Second):
		t.Fatal("PBFT condition waiter retained after shutdown")
	}
}

func TestPBFTRecentCatchUpAcrossExistingModes(t *testing.T) {
	oldPath, oldCapacity := params.DatabaseWrite_path, params.BlockCapacityMode
	t.Cleanup(func() {
		params.DatabaseWrite_path, params.BlockCapacityMode = oldPath, oldCapacity
	})
	params.DatabaseWrite_path = t.TempDir() + "/"
	params.BlockCapacityMode = "gas"
	newNode := func(id uint64) *PbftConsensusNode {
		config := &params.ChainConfig{ShardID: 0, NodeID: id, ShardNums: 1, Nodes_perShard: 4, BlockGasLimit: 30_000_000}
		bc, err := chain.NewBlockChain(config, rawdb.NewMemoryDatabase())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(bc.CloseBlockChain)
		node := &PbftConsensusNode{CurChain: bc, pbftChainConfig: config, sequenceID: 1, NodeID: id,
			isReply: make(map[string]bool), pl: &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)}}
		node.conditionalVarpbftLock = *sync.NewCond(&node.pbftLock)
		return node
	}
	source := newNode(0)
	count := uint64(pbftRequestCacheCount - 4)
	for seq := uint64(1); seq <= count; seq++ {
		block := source.CurChain.GenerateBlock(0)
		request := &message.Request{RequestType: message.BlockRequest, Msg: message.RawMessage{Content: block.Encode()}, ReqTime: block.Header.Time}
		digest := string(getDigest(request))
		source.cacheRequest(seq, digest, request)
		source.CurChain.AddBlock(block)
		source.markRequestCommitted(seq, digest)
		source.pruneCommittedRequests(seq + 1)
	}
	response := message.SendOldMessage{SeqStartHeight: 1, SeqEndHeight: count}
	for seq := uint64(1); seq <= count; seq++ {
		request, ok := source.requestAtSequence(seq)
		if !ok {
			t.Fatalf("source cannot supply recent sequence %d", seq)
		}
		response.OldRequest = append(response.OldRequest, request)
	}
	payload, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	for index, mode := range []string{"Relay", "Broker", "CLPA", "CLPA_Broker"} {
		t.Run(mode, func(t *testing.T) {
			node := newNode(uint64(index + 1))
			switch mode {
			case "Relay":
				node.ihm = &RawRelayPbftExtraHandleMod{pbftNode: node}
			case "Broker":
				node.ihm = &RawBrokerPbftExtraHandleMod{pbftNode: node}
			case "CLPA":
				node.ihm = &CLPAPbftInsideExtraHandleMod{pbftNode: node, cdm: dataSupport.NewCLPADataSupport()}
			case "CLPA_Broker":
				node.ihm = &CLPAPbftInsideExtraHandleMod_forBroker{pbftNode: node, cdm: dataSupport.NewCLPADataSupport()}
			}
			node.catchUpPending, node.catchUpStart, node.catchUpEnd = true, 1, count
			node.handleSendOldSeq(payload)
			if node.sequenceID != count+1 || node.CurChain.CurrentBlock.Header.Number != count || node.catchUpPending {
				t.Fatalf("catch-up incomplete: sequence=%d block=%d pending=%t", node.sequenceID, node.CurChain.CurrentBlock.Header.Number, node.catchUpPending)
			}
			node.handleSendOldSeq(payload) // duplicate response must not reapply.
			if node.sequenceID != count+1 {
				t.Fatal("duplicate response moved sequence backwards")
			}
			if cached, _ := node.requestCacheStats(); cached > pbftRequestCacheCount {
				t.Fatalf("catch-up left %d full requests resident", cached)
			}
			if err := node.CurChain.Storage.DataBase.View(func(tx *bolt.Tx) error {
				if tx.Bucket([]byte("pbftRequests")) != nil {
					t.Fatal("catch-up created a request archive")
				}
				if bucket := tx.Bucket([]byte("block")); bucket == nil || bucket.Stats().KeyN != int(count)+1 {
					t.Fatal("cache-only request handling damaged ordinary block persistence")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPBFTRecentCacheSharedPendingDigestAndReplacement(t *testing.T) {
	node := requestHistoryNode(t, filepath.Join(t.TempDir(), "chain.db"))
	shared := &message.Request{RequestType: message.PartitionReq, Msg: message.RawMessage{Content: []byte("shared")}}
	digest := string(getDigest(shared))
	node.cacheRequest(1, digest, shared)
	node.markRequestCommitted(1, digest)
	node.cacheRequest(100, digest, shared)
	node.cntPrepareConfirm[digest] = map[*shard.Node]bool{{NodeID: 1}: true}
	for seq := uint64(2); seq <= 20; seq++ {
		request := &message.Request{RequestType: message.BlockRequest, ReqTime: time.Unix(int64(seq), 0)}
		key := string(getDigest(request))
		node.cacheRequest(seq, key, request)
		node.markRequestCommitted(seq, key)
		node.pruneCommittedRequests(seq + 1)
	}
	if _, old := node.height2Digest[1]; old {
		t.Fatal("settled shared sequence was not evicted")
	}
	if got, ok := node.requestAtSequence(100); !ok || got != shared || len(node.cntPrepareConfirm[digest]) != 1 {
		t.Fatal("pruning a settled sequence removed shared pending payload or votes")
	}
	replacement := &message.Request{RequestType: message.PartitionReq, Msg: message.RawMessage{Content: []byte("replacement")}}
	node.cacheRequest(100, string(getDigest(replacement)), replacement)
	if _, ok := node.requestByDigest(digest); ok {
		t.Fatal("superseded pending payload retained without a sequence reference")
	}
	node.pruneCommittedRequests(21)
	if len(node.cntPrepareConfirm[digest]) != 0 {
		t.Fatal("superseded proposal votes retained after pruning")
	}
}

func assertPBFTCatchUpFailure(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if failure := recover(); failure == nil || !strings.Contains(fmt.Sprint(failure), "PBFT-CATCHUP-FAILED") {
			t.Fatalf("expected explicit catch-up failure, got %v", failure)
		}
	}()
	fn()
}

func TestPBFTMissingCatchUpFailsWithoutPartialReplay(t *testing.T) {
	node := requestHistoryNode(t, filepath.Join(t.TempDir(), "chain.db"))
	node.sequenceID = 2
	node.catchUpPending, node.catchUpStart, node.catchUpEnd = true, 2, 4
	node.ihm = new(glasSchedulerTestInside)
	for _, response := range []message.SendOldMessage{
		{SeqStartHeight: 2, SeqEndHeight: 3, OldRequest: []*message.Request{{}, {}}},
		{SeqStartHeight: 2, SeqEndHeight: 4, OldRequest: []*message.Request{{}, {}}},
		{SeqStartHeight: 2, SeqEndHeight: 4, OldRequest: []*message.Request{{}, nil, {}}},
	} {
		payload, _ := json.Marshal(response)
		assertPBFTCatchUpFailure(t, func() { node.handleSendOldSeq(payload) })
		if node.sequenceID != 2 || len(node.requestPool) != 0 || !node.catchUpPending {
			t.Fatal("invalid response partially changed catch-up state")
		}
	}
	request := message.RequestOldMessage{SeqStartHeight: 1, SeqEndHeight: 2, SenderNode: &shard.Node{}}
	payload, _ := json.Marshal(request)
	assertPBFTCatchUpFailure(t, func() { node.handleRequestOldSeq(payload) })
}

func TestPBFTPrepareMissingDigestDoesNotTriggerCatchUpFailure(t *testing.T) {
	node := requestHistoryNode(t, filepath.Join(t.TempDir(), "chain.db"))
	node.sequenceID = 1
	node.prePrepareAccepted = true
	node.pbftStage.Store(2)
	payload, _ := json.Marshal(message.Prepare{SeqID: 1, Digest: []byte("not-arrived-yet"), SenderNode: &shard.Node{NodeID: 0}})
	node.handlePrepare(payload)
	if node.catchUpPending || node.sequenceID != 1 {
		t.Fatal("ordinary out-of-order Prepare incorrectly entered catch-up")
	}
}

func TestPBFTCachedRangeRejectsGapsAndOverflow(t *testing.T) {
	node := requestHistoryNode(t, filepath.Join(t.TempDir(), "chain.db"))
	for _, sequence := range []uint64{1, 3, 4} {
		request := &message.Request{RequestType: message.BlockRequest, ReqTime: time.Unix(int64(sequence), 0)}
		node.cacheRequest(sequence, string(getDigest(request)), request)
	}
	for _, interval := range [][2]uint64{{1, 3}, {4, 3}, {0, ^uint64(0)}} {
		if partial, err := node.cachedRequestRange(interval[0], interval[1]); err == nil || partial != nil {
			t.Fatalf("invalid range returned a partial payload: %v", interval)
		}
	}
}

// Exercise the real message handlers without ledger execution or an experiment.
// The listener records self-votes and catch-up requests, but deliberately does
// not deliver them until the test chooses their arrival order.
type pbftOrderingTestInside struct {
	glasSchedulerTestInside
	validated atomic.Int64
	prepared  atomic.Int64
	committed atomic.Int64
	replayed  atomic.Int64
	reject    atomic.Bool
}

func (inside *pbftOrderingTestInside) HandleinPrePrepare(*message.PrePrepare) bool {
	inside.validated.Add(1)
	return !inside.reject.Load()
}

func (inside *pbftOrderingTestInside) HandleinPrepare(*message.Prepare) bool {
	inside.prepared.Add(1)
	return true
}

func (inside *pbftOrderingTestInside) HandleinCommit(*message.Commit) bool {
	inside.committed.Add(1)
	return true
}

func (inside *pbftOrderingTestInside) HandleforSequentialRequest(*message.SendOldMessage) bool {
	inside.replayed.Add(1)
	return true
}

func newPBFTOrderingTestNode(t *testing.T) (*PbftConsensusNode, *pbftOrderingTestInside, <-chan []byte) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	oldDelay, oldJitter, oldBandwidth := params.Delay, params.JitterRange, params.Bandwidth
	params.Delay, params.JitterRange, params.Bandwidth = 0, 0, 1<<25
	networks.InitNetworkTools()
	wires := make(chan []byte, 32)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			readers.Add(1)
			go func() {
				defer readers.Done()
				defer conn.Close()
				scanner := bufio.NewScanner(conn)
				for scanner.Scan() {
					wires <- append([]byte(nil), scanner.Bytes()...)
				}
			}()
		}
	}()
	node := requestHistoryNode(t, filepath.Join(t.TempDir(), "chain.db"))
	inside := new(pbftOrderingTestInside)
	node.ihm, node.sequenceID, node.NodeID = inside, 17, 3
	node.node_nums, node.malicious_nums = 4, 1
	node.RunningNode = &shard.Node{NodeID: 3, ShardID: 0, IPaddr: listener.Addr().String()}
	// Only the self address is exposed to Broadcast. The same capture endpoint
	// stands in for the leader when requestCatchUp writes its existing request.
	node.ip_nodeTable = map[uint64]map[uint64]string{0: {0: listener.Addr().String(), 3: listener.Addr().String()}}
	node.pbftStage.Store(1)
	t.Cleanup(func() {
		node.stopSignal.Store(true)
		node.pbftLock.Lock()
		node.conditionalVarpbftLock.Broadcast()
		node.pbftLock.Unlock()
		networks.CloseAllConnInPool()
		_ = listener.Close()
		readers.Wait()
		params.Delay, params.JitterRange, params.Bandwidth = oldDelay, oldJitter, oldBandwidth
	})
	return node, inside, wires
}

func pbftOrderingRequest(sequence uint64) *message.Request {
	return &message.Request{RequestType: message.BlockRequest,
		ReqTime: time.Unix(int64(sequence), 0).UTC(),
		Msg:     message.RawMessage{Content: []byte(fmt.Sprintf("sequence-%d", sequence))}}
}

func pbftOrderingJSON(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func pbftOrderingCall(handler func([]byte), payload []byte) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler(payload)
	}()
	return done
}

func awaitPBFTOrderingCall(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("PBFT message handler remained blocked")
	}
}

func awaitPBFTOrderingWire(t *testing.T, wires <-chan []byte, want message.MessageType) []byte {
	t.Helper()
	select {
	case wire := <-wires:
		kind, content := message.SplitMessage(wire)
		if kind != want {
			t.Fatalf("message kind=%s want=%s", kind, want)
		}
		return content
	case <-time.After(2 * time.Second):
		t.Fatalf("missing outgoing %s", want)
		return nil
	}
}

func TestPBFTFuturePrePrepareAndPrepareActivateAfterNormalCommit(t *testing.T) {
	node, inside, wires := newPBFTOrderingTestNode(t)
	current, next := pbftOrderingRequest(17), pbftOrderingRequest(18)
	node.cacheRequest(17, string(getDigest(current)), current)
	node.prePrepareAccepted = true
	node.pbftStage.Store(3)
	proposal := pbftOrderingJSON(t, message.PrePrepare{SeqID: 18, RequestMsg: next, Digest: getDigest(next)})
	node.handlePrePrepare(proposal)
	if inside.validated.Load() != 0 {
		t.Fatal("future PrePrepare executed before the preceding request committed")
	}
	// A future Prepare must wait even while the preceding sequence is already
	// in stage 3; checking only the stage used to drop this vote immediately.
	futurePrepare := pbftOrderingCall(node.handlePrepare, pbftOrderingJSON(t, message.Prepare{
		SeqID: 18, Digest: getDigest(next), SenderNode: &shard.Node{NodeID: 0},
	}))
	select {
	case <-futurePrepare:
		t.Fatal("future Prepare was discarded before its sequence became current")
	case <-time.After(30 * time.Millisecond):
	}
	for sender := uint64(0); sender < 3; sender++ {
		awaitPBFTOrderingCall(t, pbftOrderingCall(node.handleCommit, pbftOrderingJSON(t, message.Commit{
			SeqID: 17, Digest: getDigest(current), SenderNode: &shard.Node{NodeID: sender},
		})))
	}
	awaitPBFTOrderingCall(t, futurePrepare)
	if node.sequenceID != 18 || node.pbftStage.Load() != 2 || inside.validated.Load() != 1 || inside.prepared.Load() != 1 || inside.committed.Load() != 1 {
		t.Fatalf("cached next request was not activated: sequence=%d stage=%d validations=%d prepares=%d commits=%d",
			node.sequenceID, node.pbftStage.Load(), inside.validated.Load(), inside.prepared.Load(), inside.committed.Load())
	}
	selfPayload := awaitPBFTOrderingWire(t, wires, message.CPrepare)
	var self message.Prepare
	if err := json.Unmarshal(selfPayload, &self); err != nil || self.SeqID != 18 || self.SenderNode == nil || self.SenderNode.NodeID != node.NodeID {
		t.Fatalf("wrong cached-request self-Prepare: %+v err=%v", self, err)
	}
	node.handlePrepare(selfPayload)
	if inside.prepared.Load() != 2 || node.pbftStage.Load() != 2 {
		t.Fatal("cached-request self-Prepare did not enter the normal prepare path")
	}
	node.handlePrePrepare(proposal)
	node.handlePrePrepare(proposal)
	if inside.validated.Load() != 1 || node.pbftStage.Load() != 2 {
		t.Fatal("duplicate PrePrepare repeated validation or advanced the stage")
	}
	select {
	case <-wires:
		t.Fatal("duplicate PrePrepare emitted another vote")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestPBFTFutureCommitQuorumCatchesUpAndActivatesCachedNext(t *testing.T) {
	node, inside, wires := newPBFTOrderingTestNode(t)
	next := pbftOrderingRequest(20)
	node.handlePrePrepare(pbftOrderingJSON(t, message.PrePrepare{SeqID: 20, RequestMsg: next, Digest: getDigest(next)}))
	last := pbftOrderingRequest(19)
	for sender := uint64(0); sender < 3; sender++ {
		awaitPBFTOrderingCall(t, pbftOrderingCall(node.handleCommit, pbftOrderingJSON(t, message.Commit{
			SeqID: 19, Digest: getDigest(last), SenderNode: &shard.Node{NodeID: sender},
		})))
		if sender < 2 && node.catchUpPending {
			t.Fatalf("catch-up started without 2f+1 commits: votes=%d", sender+1)
		}
		if sender == 0 {
			// A fresh JSON decode yields a new SenderNode pointer, not a new
			// voter. Repeated delivery must never manufacture a quorum.
			for repeat := 0; repeat < 3; repeat++ {
				awaitPBFTOrderingCall(t, pbftOrderingCall(node.handleCommit, pbftOrderingJSON(t, message.Commit{
					SeqID: 19, Digest: getDigest(last), SenderNode: &shard.Node{NodeID: sender},
				})))
			}
			if node.catchUpPending {
				t.Fatal("repeated commits from one node manufactured a catch-up quorum")
			}
		}
	}
	if !node.catchUpPending || node.catchUpStart != 17 || node.catchUpEnd != 19 || node.sequenceID != 17 || inside.committed.Load() != 0 {
		t.Fatal("future commit quorum did not request history without prematurely executing it")
	}
	requestPayload := awaitPBFTOrderingWire(t, wires, message.CRequestOldrequest)
	var request message.RequestOldMessage
	if err := json.Unmarshal(requestPayload, &request); err != nil || request.SeqStartHeight != 17 || request.SeqEndHeight != 19 {
		t.Fatalf("wrong catch-up request: %+v err=%v", request, err)
	}
	response := pbftOrderingJSON(t, message.SendOldMessage{SeqStartHeight: 17, SeqEndHeight: 19,
		OldRequest: []*message.Request{pbftOrderingRequest(17), pbftOrderingRequest(18), last}})
	awaitPBFTOrderingCall(t, pbftOrderingCall(node.handleSendOldSeq, response))
	if node.sequenceID != 20 || node.catchUpPending || node.pbftStage.Load() != 2 || inside.validated.Load() != 1 || inside.replayed.Load() != 1 {
		t.Fatalf("catch-up did not activate cached next request: sequence=%d stage=%d pending=%t validations=%d replays=%d",
			node.sequenceID, node.pbftStage.Load(), node.catchUpPending, inside.validated.Load(), inside.replayed.Load())
	}
	selfPayload := awaitPBFTOrderingWire(t, wires, message.CPrepare)
	var self message.Prepare
	if err := json.Unmarshal(selfPayload, &self); err != nil || self.SeqID != 20 || self.SenderNode == nil || self.SenderNode.NodeID != node.NodeID {
		t.Fatalf("wrong catch-up self-Prepare: %+v err=%v", self, err)
	}
	awaitPBFTOrderingCall(t, pbftOrderingCall(node.handlePrepare, selfPayload))
	if inside.prepared.Load() != 1 {
		t.Fatal("catch-up self-Prepare remained blocked behind the old stage")
	}
	node.handleSendOldSeq(response)
	if inside.replayed.Load() != 1 || inside.validated.Load() != 1 || node.sequenceID != 20 || node.pbftStage.Load() != 2 {
		t.Fatal("duplicate catch-up response replayed or reactivated the current request")
	}
}

func TestPBFTLeaderReservationNeedsValidationAndDistinctPrepareQuorum(t *testing.T) {
	node, inside, wires := newPBFTOrderingTestNode(t)
	node.NodeID, node.RunningNode.NodeID = 0, 0
	request := pbftOrderingRequest(17)
	digest := getDigest(request)
	node.cacheRequest(17, string(digest), request)
	// The proposer reserves stage 2 before its own PrePrepare arrives. That
	// reservation is neither local validation nor a Prepare quorum.
	node.pbftStage.Store(2)
	node.sequenceLock.Lock()
	for sender := uint64(0); sender < 2; sender++ {
		awaitPBFTOrderingCall(t, pbftOrderingCall(node.handleCommit, pbftOrderingJSON(t, message.Commit{
			SeqID: 17, Digest: digest, SenderNode: &shard.Node{NodeID: sender},
		})))
	}
	quorumCommit := pbftOrderingCall(node.handleCommit, pbftOrderingJSON(t, message.Commit{
		SeqID: 17, Digest: digest, SenderNode: &shard.Node{NodeID: 2},
	}))
	select {
	case <-quorumCommit:
		t.Fatal("current Commit quorum bypassed local PrePrepare validation")
	case <-time.After(30 * time.Millisecond):
	}
	if inside.committed.Load() != 0 || inside.validated.Load() != 0 {
		t.Fatal("unvalidated cached request was executed")
	}
	proposal := pbftOrderingJSON(t, message.PrePrepare{SeqID: 17, RequestMsg: request, Digest: digest})
	node.handlePrePrepare(proposal)
	if inside.validated.Load() != 1 || node.pbftStage.Load() != 2 {
		t.Fatal("leader reservation skipped validation or advanced directly to Commit stage")
	}
	selfPrepare := awaitPBFTOrderingWire(t, wires, message.CPrepare)
	for repeat := 0; repeat < 3; repeat++ {
		node.handlePrepare(selfPrepare)
	}
	if len(node.cntPrepareConfirm[string(digest)]) != 1 || node.isCommitBordcast[string(digest)] || node.pbftStage.Load() != 2 {
		t.Fatal("repeated Prepare from one replica manufactured a quorum")
	}
	node.handlePrePrepare(proposal)
	if inside.validated.Load() != 1 || inside.committed.Load() != 0 {
		t.Fatal("duplicate leader PrePrepare repeated validation or bypassed Prepare quorum")
	}
	for sender := uint64(1); sender < 3; sender++ {
		node.handlePrepare(pbftOrderingJSON(t, message.Prepare{
			SeqID: 17, Digest: digest, SenderNode: &shard.Node{NodeID: sender},
		}))
	}
	awaitPBFTOrderingCall(t, quorumCommit)
	if inside.committed.Load() != 1 || node.sequenceID != 18 || node.pbftStage.Load() != 1 {
		t.Fatal("validated current quorum did not commit exactly once")
	}
	commitPayload := awaitPBFTOrderingWire(t, wires, message.CCommit)
	var selfCommit message.Commit
	if err := json.Unmarshal(commitPayload, &selfCommit); err != nil || selfCommit.SeqID != 17 || selfCommit.SenderNode == nil || selfCommit.SenderNode.NodeID != node.NodeID {
		t.Fatalf("wrong leader self-Commit: %+v err=%v", selfCommit, err)
	}
}

func TestPBFTRejectedCurrentProposalCannotCommitFromEarlyQuorum(t *testing.T) {
	node, inside, wires := newPBFTOrderingTestNode(t)
	request := pbftOrderingRequest(17)
	digest := getDigest(request)
	node.handlePrePrepare(pbftOrderingJSON(t, message.PrePrepare{SeqID: 17, RequestMsg: request, Digest: []byte("wrong")}))
	if inside.validated.Load() != 0 {
		t.Fatal("invalid wire digest reached proposal validation")
	}
	inside.reject.Store(true)
	node.handlePrePrepare(pbftOrderingJSON(t, message.PrePrepare{SeqID: 17, RequestMsg: request, Digest: digest}))
	if inside.validated.Load() != 1 || node.prePrepareAccepted || node.pbftStage.Load() != 1 {
		t.Fatal("rejected proposal was marked accepted or advanced the stage")
	}
	for sender := uint64(0); sender < 2; sender++ {
		awaitPBFTOrderingCall(t, pbftOrderingCall(node.handleCommit, pbftOrderingJSON(t, message.Commit{
			SeqID: 17, Digest: digest, SenderNode: &shard.Node{NodeID: sender},
		})))
	}
	quorumCommit := pbftOrderingCall(node.handleCommit, pbftOrderingJSON(t, message.Commit{
		SeqID: 17, Digest: digest, SenderNode: &shard.Node{NodeID: 2},
	}))
	select {
	case <-quorumCommit:
		t.Fatal("current quorum bypassed a rejected cached proposal")
	case wire := <-wires:
		kind, _ := message.SplitMessage(wire)
		t.Fatalf("rejected proposal emitted %s", kind)
	case <-time.After(30 * time.Millisecond):
	}
	node.pbftLock.Lock()
	if inside.committed.Load() != 0 || node.catchUpPending || node.sequenceID != 17 {
		node.pbftLock.Unlock()
		t.Fatal("rejected current request was committed or bypassed through catch-up")
	}
	node.stopSignal.Store(true)
	node.conditionalVarpbftLock.Broadcast()
	node.pbftLock.Unlock()
	awaitPBFTOrderingCall(t, quorumCommit)
}
