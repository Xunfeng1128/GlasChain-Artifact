package committee

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/partition/garet"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/supervisor/unifiedmetrics"
)

func garetSharedFixture(t *testing.T) *GARETSharedCommitteeModule {
	t.Helper()
	m := garetTestModule(t)
	m.lastPredictionCycle = -1
	oldShards := params.ShardNum
	params.ShardNum = 2
	t.Cleanup(func() { params.ShardNum = oldShards })
	f, err := os.Create(filepath.Join(t.TempDir(), "rounds.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	m.metrics = f
	base := &CLPACommitteeModule{sl: &supervisor_log.SupervisorLog{Slog: log.New(io.Discard, "", 0)},
		skipHistoryGraph: true, shardMaxSeq: make(map[uint64]uint64), modifiedMap: make(map[string]uint64), IpNodeTable: map[uint64]map[uint64]string{}}
	shared := &GARETSharedCommitteeModule{CLPACommitteeModule: base, algorithm: m}
	base.inputRouting, base.dispatchObserved = m.fetchModifiedMap, m.observeDispatch
	base.partitionPlanner, base.partitionGARET = shared.planGARET, shared.groupPayload
	return shared
}

func closeGARETSharedTestRound(t *testing.T, m *GARETSharedCommitteeModule, round uint64, delta map[string]uint64) {
	t.Helper()
	// No sockets are needed: run the real common binding/serialization with an
	// empty destination table, then close the same observed-block gate.
	m.clpaMapSend(delta)
	if err := m.installation.finishObservedEpoch(round); err != nil {
		t.Fatal(err)
	}
}

func TestGARETSharedColdStartAndLagUseUnchangedFS(t *testing.T) {
	m := garetSharedFixture(t)
	start := m.algorithm.start
	initial := append([]uint64(nil), m.algorithm.placement...)
	if delta, err := m.planGARETAt(1, start.Add(48*time.Second)); err != nil || len(delta) != 0 {
		t.Fatalf("cold start %v %v", delta, err)
	} else {
		closeGARETSharedTestRound(t, m, 1, delta)
	}
	if !reflect.DeepEqual(initial, m.algorithm.placement) || m.algorithm.lastPredictionCycle != -1 {
		t.Fatal("cold control round ran FS or changed group placement")
	}
	// Physical TOP/RELAY1 belongs to sender, RELAY2 to recipient, including the
	// original measured stage gas. No pending/future record supplies weights.
	block := &message.BlockInfoMsg{SenderShardID: 0, BlockHeight: 1, Epoch: 0, ExecutionBlockHash: "physical", BlockBodyLength: 2,
		Relay1Txs: []*core.Transaction{garetStage(1, 1, 3, 13, false)}, Relay2Txs: []*core.Transaction{garetStage(1, 1, 3, 41, true)}}
	m.HandleBlockInfoAt(block, start.Add(8*time.Second))
	newer := &message.BlockInfoMsg{SenderShardID: 0, BlockHeight: 2, Epoch: 1, ExecutionBlockHash: "newer", BlockBodyLength: 1,
		InnerShardTxs: []*core.Transaction{garetStage(2, 1, 2, 1000000, false)}}
	m.HandleBlockInfoAt(newer, start.Add(70*time.Second))
	if m.shardMaxSeq[0] != 2 || m.curEpoch != 1 {
		t.Fatal("shared height/epoch observations lost")
	}
	m.algorithm.observeDispatch([]*core.Transaction{{Sender: fmt.Sprintf("%040x", 1), Recipient: fmt.Sprintf("%040x", 3)}}, 0, start.Add(50*time.Second))
	delta, err := m.planGARETAt(2, start.Add(96*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	want, err := garet.FullShuffle([]uint64{26, 82}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.algorithm.placement, want) || m.algorithm.lastPredictionCycle != 0 {
		t.Fatalf("FS/lag changed: got %v want %v", m.algorithm.placement, want)
	}
	closeGARETSharedTestRound(t, m, 2, delta)
	if m.algorithm.start != start {
		t.Fatal("install reset fixed history origin")
	}
	// Calling the common planner again in the same prediction cycle must keep
	// the selected groups and avoid presenting cached weights as a new sample.
	if delta, err = m.planGARETAt(3, start.Add(110*time.Second)); err != nil || len(delta) != 0 {
		t.Fatalf("cached prediction %v %v", delta, err)
	}
	closeGARETSharedTestRound(t, m, 3, delta)
	data, err := os.ReadFile(m.algorithm.metrics.Name())
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	for decoder.More() {
		var row map[string]interface{}
		if err := decoder.Decode(&row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	if len(rows) != 3 || rows[0]["fs_ran"] != false || rows[1]["fs_ran"] != true || rows[2]["type"] != "unchanged_prediction" {
		t.Fatalf("audit %v", rows)
	}
}

func TestGARETSharedConstructorWiresOnlyActualDispatch(t *testing.T) {
	configureUnifiedInput(t)
	oldMethod, oldMetrics, oldMode, oldDir := params.ConsensusMethod, params.UnifiedMetrics, params.BlockCapacityMode, params.DataWrite_path
	oldGroups, oldSample, oldGap, oldCatalog := params.GARETGroupCount, params.GARETSampleSeconds, params.ReconfigTimeGap, params.GARETGroupCatalogFile
	oldShards, oldScheduler := params.ShardNum, params.CLPAReconfigurationScheduler
	t.Cleanup(func() {
		params.ConsensusMethod, params.UnifiedMetrics, params.BlockCapacityMode, params.DataWrite_path = oldMethod, oldMetrics, oldMode, oldDir
		params.GARETGroupCount, params.GARETSampleSeconds, params.ReconfigTimeGap, params.GARETGroupCatalogFile = oldGroups, oldSample, oldGap, oldCatalog
		params.ShardNum, params.CLPAReconfigurationScheduler = oldShards, oldScheduler
	})
	params.ConsensusMethod, params.UnifiedMetrics, params.BlockCapacityMode, params.DataWrite_path = 10, true, "gas", t.TempDir()
	params.GARETGroupCount, params.GARETSampleSeconds, params.ReconfigTimeGap = 2, 8, 48
	params.ShardNum, params.CLPAReconfigurationScheduler = 2, "poll"
	catalog := garetTestModule(t).catalog
	params.GARETGroupCatalogFile = filepath.Join(t.TempDir(), "groups.json")
	if err := garet.SaveCatalog(params.GARETGroupCatalogFile, catalog); err != nil {
		t.Fatal(err)
	}
	m := NewGARETSharedCommitteeModule(nil, nil, &supervisor_log.SupervisorLog{Slog: log.New(io.Discard, "", 0)}, "unused", 2, 2, 48)
	t.Cleanup(func() { m.algorithm.metrics.Close() })
	if !m.skipHistoryGraph || m.partitionPlanner == nil || m.partitionGARET == nil || m.inputRouting == nil || m.dispatchObserved == nil {
		t.Fatal("incorrect shared hooks")
	}
	if len(m.algorithm.known) != 0 || !m.algorithm.start.IsZero() {
		t.Fatal("constructor observed future input")
	}
	recorder, err := unifiedmetrics.New(filepath.Join(t.TempDir(), "unified"), unifiedmetrics.RunConfig{Method: "GARET", ConsensusMethod: 10, ShardCount: 2, ExpectedTransactions: 1, InjectSpeed: 2, BlockGasLimit: 100, CrossCallGas: 5})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recorder.Close(time.Now()); err != nil {
			t.Error(err)
		}
	})
	m.SetUnifiedRecorder(recorder)
	if m.recorder != recorder || m.algorithm.recorder != nil {
		t.Fatal("shared/private recorder wiring duplicates metrics")
	}
	tx := garetStage(1, 1, 2, 1, false)
	tx.GLASCallRelations = []core.GLASCallRelation{{Caller: fmt.Sprintf("%040x", 3), Callee: fmt.Sprintf("%040x", 4)}}
	at := time.Now()
	m.dispatchObserved([]*core.Transaction{tx}, 0, at)
	if m.algorithm.start != at || len(m.algorithm.known) != 4 {
		t.Fatal("actual dispatch did not establish clock/call identities")
	}
	if recorder.CompletionProgress().Injected != 0 {
		t.Fatal("private dispatch observer also wrote unified metrics")
	}
}

func TestGARETSharedRouteUsesGroupsAndPayloadIsImmutable(t *testing.T) {
	m := garetSharedFixture(t)
	m.algorithm.placement = []uint64{1, 0}
	address := fmt.Sprintf("%040x", 2)
	m.modifiedMap[address] = 0 // An address override cannot supersede a group.
	if m.fetchModifiedMap(address) != 1 {
		t.Fatal("shared route lost group placement")
	}
	payload := m.groupPayload(9)
	payload.Placement[0] = 0
	if m.algorithm.placement[0] != 1 || payload.Epoch != 9 {
		t.Fatal("payload aliases live groups")
	}
	if m.algorithm.recorder != nil {
		t.Fatal("private observer must not duplicate common metrics")
	}
}
