package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition/garet"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/supervisor/unifiedmetrics"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// GARETCommitteeModule reproduces FS and its lagged Gas prediction. Only the
// identity catalog is prepared ahead of injection. Planner weights come from
// committed physical execution stages, never from pending or future records.
type GARETCommitteeModule struct {
	injectionObservation
	csvPath                                string
	dataTotalNum, batchDataNum, nowDataNum int
	frequency                              time.Duration
	shards                                 int
	catalog                                *garet.Catalog
	placement                              []uint64            // injection/control goroutine owns this slice
	known                                  map[string]struct{} // identities already dispatched, including calls
	lastCheck                              time.Time
	lastPredictionCycle                    int64
	mu                                     sync.Mutex
	start                                  time.Time
	history                                *garet.History
	blocks                                 map[[2]uint64]string
	stages                                 map[[32]byte]uint8
	lateStages                             uint64
	lateGas                                uint64
	heights                                map[uint64]uint64
	epochs                                 map[uint64]int
	metrics                                *os.File
	sl                                     *supervisor_log.SupervisorLog
	Ss                                     *signal.StopSignal
	IpNodeTable                            map[uint64]map[uint64]string
}

func NewGARETCommitteeModule(ip map[uint64]map[uint64]string, ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csv string, count, batch, frequency int) *GARETCommitteeModule {
	if err := params.ValidateGARETConfig(); err != nil {
		log.Panic(err)
	}
	catalog, err := garet.LoadCatalog(params.GARETGroupCatalogFile)
	if err != nil {
		log.Panicf("load GARET catalog: %v", err)
	}
	if catalog.GroupCount() != params.GARETGroupCount || params.ShardNum <= 0 {
		log.Panic("GARET catalog group count or shard count mismatch")
	}
	history, err := garet.NewHistory(catalog.GroupCount(), int64(time.Duration(params.GARETSampleSeconds)*time.Second), frequency/params.GARETSampleSeconds)
	if err != nil {
		log.Panic(err)
	}
	dir := filepath.Join(params.DataWrite_path, "supervisor_measureOutput")
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Panic(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "GARET_Rounds.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		log.Panicf("create GARET diagnostics: %v", err)
	}
	m := &GARETCommitteeModule{csvPath: csv, dataTotalNum: count, batchDataNum: batch, frequency: time.Duration(frequency) * time.Second,
		shards: params.ShardNum, catalog: catalog, placement: catalog.InitialPlacement(params.ShardNum), known: make(map[string]struct{}),
		lastPredictionCycle: -1, history: history, blocks: make(map[[2]uint64]string), stages: make(map[[32]byte]uint8),
		heights: make(map[uint64]uint64), epochs: make(map[uint64]int), metrics: f, sl: sl, Ss: ss, IpNodeTable: ip}
	m.writeDiagnostic(map[string]interface{}{"type": "initial", "catalog_digest": catalog.Digest(), "placement": m.placement,
		"groups": catalog.GroupCount(), "sample_seconds": params.GARETSampleSeconds, "cycle_seconds": frequency,
		"history_clock": "supervisor_receipt_monotonic", "history_gas": "physical_stage", "history_lag_cycles": 1})
	return m
}

func (m *GARETCommitteeModule) HandleOtherMessage([]byte) {}
func (m *GARETCommitteeModule) writeDiagnostic(row interface{}) {
	if m.metrics == nil {
		return
	}
	if err := json.NewEncoder(m.metrics).Encode(row); err != nil {
		panic(fmt.Errorf("GARET diagnostics: %w", err))
	}
}
func (m *GARETCommitteeModule) fetchModifiedMap(address string) uint64 {
	group, err := m.catalog.GroupFor(address)
	if err != nil {
		panic(err)
	}
	return m.placement[group]
}

func (m *GARETCommitteeModule) observeDispatch(txs []*core.Transaction, shard uint64, at time.Time) {
	if len(txs) == 0 {
		return
	}
	m.mu.Lock()
	if m.start.IsZero() {
		m.start = at
		m.lastCheck = at
	}
	m.mu.Unlock()
	for _, tx := range txs {
		m.known[tx.Sender] = struct{}{}
		m.known[tx.Recipient] = struct{}{}
		for _, call := range tx.GLASCallRelations {
			m.known[call.Caller] = struct{}{}
			m.known[call.Callee] = struct{}{}
		}
	}
	if m.recorder != nil {
		if err := m.recorder.RecordInjection(txs, shard, at); err != nil {
			panic(err)
		}
	}
}

// Same batch pacing and input/price functions as the dynamic count baselines.
func (m *GARETCommitteeModule) txSending(txs []*core.Transaction) {
	toShard := make(map[uint64][]*core.Transaction)
	for i := 0; i <= len(txs); i++ {
		if i > 0 && (i%params.InjectSpeed == 0 || i == len(txs)) {
			for s := uint64(0); s < uint64(m.shards); s++ {
				payload, err := json.Marshal(message.InjectTxs{Txs: toShard[s], ToShardID: s})
				if err != nil {
					panic(err)
				}
				m.observeDispatch(toShard[s], s, time.Now())
				go networks.TcpDial(message.MergeMessage(message.CInject, payload), m.IpNodeTable[s][0])
			}
			toShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if i == len(txs) {
			break
		}
		if err := priceCommitteeInput(txs[i], m.fetchModifiedMap); err != nil {
			panic(err)
		}
		s := m.fetchModifiedMap(txs[i].Sender)
		toShard[s] = append(toShard[s], txs[i])
	}
}

func (m *GARETCommitteeModule) MsgSendingControl() {
	defer func() {
		if m.metrics != nil {
			m.mu.Lock()
			terminal := map[string]interface{}{"type": "controller_end", "late_history_stages": m.lateStages,
				"late_history_gas": m.lateGas, "observed_blocks": len(m.blocks), "observed_sources": len(m.stages),
				"last_prediction_cycle": m.lastPredictionCycle}
			m.mu.Unlock()
			m.writeDiagnostic(terminal)
			if err := m.metrics.Close(); err != nil {
				panic(err)
			}
		}
	}()
	reader, err := openCommitteeInput(m.csvPath, m.dataTotalNum)
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	round := uint64(0)
	batch := make([]*core.Transaction, 0, m.batchDataNum)
	for m.nowDataNum < m.dataTotalNum {
		tx, err := reader.Next(uint64(m.nowDataNum))
		if err == io.EOF {
			panic(fmt.Errorf("GARET input ended after %d accepted transactions; requested %d", m.nowDataNum, m.dataTotalNum))
		}
		if err != nil {
			panic(err)
		}
		batch = append(batch, tx)
		m.nowDataNum++
		if len(batch) == m.batchDataNum || m.nowDataNum == m.dataTotalNum {
			m.txSending(batch)
			batch = make([]*core.Transaction, 0, m.batchDataNum)
			m.Ss.StopGap_ResetAll()
		}
		m.maybePartition(&round)
	}
	for !m.Ss.GapEnough() {
		time.Sleep(time.Second)
		m.maybePartition(&round)
	}
}

func (m *GARETCommitteeModule) maybePartition(round *uint64) {
	now := time.Now()
	if m.shards <= 1 || m.lastCheck.IsZero() || now.Sub(m.lastCheck) < m.frequency {
		return
	}
	m.mu.Lock()
	// Re-read after acquiring the lock: a receipt may have advanced and pruned
	// the history while this control goroutine was waiting at a cycle boundary.
	now = time.Now()
	prediction, ready, err := m.history.Predict(now.Sub(m.start).Nanoseconds())
	lateStages, lateGas := m.lateStages, m.lateGas
	m.mu.Unlock()
	if err != nil {
		panic(err)
	}
	m.lastCheck = now
	if !ready {
		m.writeDiagnostic(map[string]interface{}{"type": "cold_start", "elapsed_ns": now.Sub(m.start).Nanoseconds()})
		return
	}
	if prediction.Cycle <= m.lastPredictionCycle {
		return
	}
	observation := m.beginReconfiguration(*round+1, m.nowDataNum < m.dataTotalNum, unifiedmetrics.CompletionBasisAllShards)
	newPlacement, err := garet.FullShuffle(prediction.WeightedGas, m.shards)
	if err != nil {
		panic(err)
	}
	changed := make(map[string]uint64)
	for address := range m.known {
		group, err := m.catalog.GroupFor(address)
		if err != nil {
			panic(err)
		}
		if newPlacement[group] != m.placement[group] {
			changed[address] = newPlacement[group]
		}
	}
	movedGroups := 0
	for group, old := range m.placement {
		if newPlacement[group] != old {
			movedGroups++
		}
	}
	*round++
	planningEnded := time.Now()
	m.writeDiagnostic(map[string]interface{}{"type": "plan", "round": *round, "prediction_cycle": prediction.Cycle,
		"history_start_ns": prediction.Cycle * int64(m.frequency), "history_end_ns": (prediction.Cycle + 1) * int64(m.frequency),
		"trigger_elapsed_ns": now.Sub(m.start).Nanoseconds(), "planning_ns": planningEnded.Sub(now).Nanoseconds(),
		"weighted_gas_numerators": prediction.WeightedGas, "weight_denominator": prediction.WeightDenominator,
		"late_history_stages": lateStages, "late_history_gas": lateGas,
		"before_placement": m.placement, "placement": newPlacement, "moved_groups": movedGroups, "changed_observed_accounts": len(changed)})
	m.partitionPrepareSend(*round)
	payload, err := json.Marshal(message.PartitionModifiedMap{PartitionModified: changed,
		GARET: &message.GARETPlacement{CatalogDigest: m.catalog.Digest(), Epoch: *round, Placement: newPlacement}})
	if err != nil {
		panic(err)
	}
	msg := message.MergeMessage(message.CPartitionMsg, payload)
	for s := uint64(0); s < uint64(m.shards); s++ {
		go networks.TcpDial(msg, m.IpNodeTable[s][0])
	}
	if err := m.waitForPartitionRound(*round, time.Duration(params.GARETReconfigurationTimeoutSeconds)*time.Second); err != nil {
		m.writeDiagnostic(map[string]interface{}{"type": "failed", "round": *round, "error": err.Error()})
		observation.finish("failed")
		panic(err)
	}
	m.placement = newPlacement
	m.lastPredictionCycle = prediction.Cycle
	m.lastCheck = time.Now() // shared baseline check cadence; history slots stay fixed
	m.writeDiagnostic(map[string]interface{}{"type": "installed", "round": *round, "elapsed_ns": m.lastCheck.Sub(m.start).Nanoseconds()})
	observation.finish("completed")
}

// Missing control delivery or a stalled shard must fail the run rather than
// hang injection forever. The common supervisor catches the error and stops
// its own workers while retaining incomplete results.
func (m *GARETCommitteeModule) waitForPartitionRound(round uint64, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for !m.allShardsAt(round) {
		select {
		case <-tick.C:
		case <-deadline.C:
			m.mu.Lock()
			missing := make([]string, 0)
			for s := uint64(0); s < uint64(m.shards); s++ {
				if m.epochs[s] < int(round) {
					missing = append(missing, fmt.Sprintf("shard=%d epoch=%d height=%d", s, m.epochs[s], m.heights[s]))
				}
			}
			m.mu.Unlock()
			if len(missing) == 0 {
				return nil
			}
			return fmt.Errorf("GARET reconfiguration epoch=%d timed out after %s: %s", round, timeout, strings.Join(missing, "; "))
		}
	}
	return nil
}

func (m *GARETCommitteeModule) allShardsAt(round uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for s := uint64(0); s < uint64(m.shards); s++ {
		if m.epochs[s] < int(round) {
			return false
		}
	}
	return true
}
func (m *GARETCommitteeModule) partitionPrepareSend(round uint64) {
	m.mu.Lock()
	var max uint64
	for _, h := range m.heights {
		if h > max {
			max = h
		}
	}
	m.mu.Unlock()
	payload, err := json.Marshal(message.PartitionPrepare{Epoch: round, TargetSeq: max + partitionTargetBuffer})
	if err != nil {
		panic(err)
	}
	msg := message.MergeMessage(message.CPartitionPrepare, payload)
	for s := uint64(0); s < uint64(m.shards); s++ {
		go networks.TcpDial(msg, m.IpNodeTable[s][0])
	}
}

func (m *GARETCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	m.HandleBlockInfoAt(b, time.Now())
}

// The supervisor supplies the same receipt timestamp used by common metrics.
// Replays are filtered upstream; this local block guard also protects direct use.
func (m *GARETCommitteeModule) HandleBlockInfoAt(b *message.BlockInfoMsg, at time.Time) {
	if err := m.recordBlockAt(b, at); err != nil {
		panic(fmt.Errorf("GARET history: %w", err))
	}
}
func (m *GARETCommitteeModule) recordBlockAt(b *message.BlockInfoMsg, at time.Time) error {
	if b == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if b.SenderShardID >= uint64(m.shards) || b.Epoch < 0 {
		return fmt.Errorf("invalid block shard or epoch")
	}
	key := [2]uint64{b.SenderShardID, b.BlockHeight}
	if previous, ok := m.blocks[key]; ok {
		if previous != b.ExecutionBlockHash {
			return fmt.Errorf("conflicting block identity")
		}
		return nil
	}
	if b.ExecutionBlockHash == "" {
		return fmt.Errorf("missing committed block hash")
	}
	if b.BlockHeight > m.heights[b.SenderShardID] {
		m.heights[b.SenderShardID] = b.BlockHeight
	}
	if b.Epoch > m.epochs[b.SenderShardID] {
		m.epochs[b.SenderShardID] = b.Epoch
	}
	m.blocks[key] = b.ExecutionBlockHash
	if b.BlockBodyLength == 0 {
		return nil
	}
	if m.start.IsZero() {
		return fmt.Errorf("execution before first injection")
	}
	relative := at.Sub(m.start).Nanoseconds()
	if relative < 0 {
		return fmt.Errorf("execution precedes injection clock")
	}
	type observation struct {
		tx    *core.Transaction
		relay bool
	}
	rows := make([]observation, 0, len(b.InnerShardTxs)+len(b.Relay1Txs)+len(b.Relay2Txs))
	for _, tx := range b.InnerShardTxs {
		rows = append(rows, observation{tx, false})
	}
	for _, tx := range b.Relay1Txs {
		rows = append(rows, observation{tx, false})
	}
	for _, tx := range b.Relay2Txs {
		rows = append(rows, observation{tx, true})
	}
	if len(rows) != b.BlockBodyLength {
		return fmt.Errorf("block body and physical stages differ")
	}
	for _, row := range rows {
		tx := row.tx
		if tx == nil || tx.GasUsed == 0 || tx.Relayed != row.relay {
			return fmt.Errorf("invalid physical stage")
		}
		var source [32]byte
		hash := strings.TrimPrefix(tx.SourceTxHash, "0x")
		if len(hash) != 64 {
			return fmt.Errorf("invalid source hash")
		}
		if _, err := hex.Decode(source[:], []byte(hash)); err != nil {
			return err
		}
		bit := uint8(1)
		owner := tx.Sender
		if row.relay {
			bit = 2
			owner = tx.Recipient
		}
		if m.stages[source]&bit != 0 {
			return fmt.Errorf("physical stage committed more than once: %s relay=%v", tx.SourceTxHash, row.relay)
		}
		group, err := m.catalog.GroupFor(owner)
		if err != nil {
			return err
		}
		if err := m.history.Record(relative, group, tx.GasUsed); err != nil {
			if !errors.Is(err, garet.ErrLateSample) {
				return err
			}
			// Common execution metrics already retain this receipt. An old
			// observation cannot revise a prediction that has been published.
			m.lateStages++
			m.lateGas += tx.GasUsed
		}
		m.stages[source] |= bit
	}
	return nil
}
