package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/partition/garet"
	"blockEmulator/supervisor/unifiedmetrics"
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func garetTestModule(t *testing.T) *GARETCommitteeModule {
	t.Helper()
	addresses := []string{fmt.Sprintf("%040x", 1), fmt.Sprintf("%040x", 2), fmt.Sprintf("%040x", 3), fmt.Sprintf("%040x", 4)}
	catalog, err := garet.NewCatalog(addresses, 2)
	if err != nil {
		t.Fatal(err)
	}
	history, err := garet.NewHistory(2, int64(8*time.Second), 6)
	if err != nil {
		t.Fatal(err)
	}
	return &GARETCommitteeModule{catalog: catalog, shards: 2, history: history, start: time.Unix(1000, 0), frequency: 48 * time.Second,
		placement: catalog.InitialPlacement(2), known: make(map[string]struct{}), blocks: make(map[[2]uint64]string),
		stages: make(map[[32]byte]uint8), heights: make(map[uint64]uint64), epochs: make(map[uint64]int)}
}
func garetStage(source int, sender, recipient int, gas uint64, relay bool) *core.Transaction {
	return &core.Transaction{SourceTxHash: fmt.Sprintf("0x%064x", source), Sender: fmt.Sprintf("%040x", sender),
		Recipient: fmt.Sprintf("%040x", recipient), GasUsed: gas, GLASBaseGas: 1, Relayed: relay}
}
func TestGARETHistoryUsesPhysicalGasAndOwnersOnce(t *testing.T) {
	m := garetTestModule(t)
	b := &message.BlockInfoMsg{SenderShardID: 0, BlockHeight: 1, Epoch: 0, ExecutionBlockHash: "first", BlockBodyLength: 3,
		InnerShardTxs: []*core.Transaction{garetStage(1, 1, 2, 11, false)}, Relay1Txs: []*core.Transaction{garetStage(2, 2, 4, 13, false)},
		Relay2Txs: []*core.Transaction{garetStage(2, 2, 4, 17, true)}}
	if err := m.recordBlockAt(b, m.start.Add(8*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := m.recordBlockAt(b, m.start.Add(9*time.Second)); err != nil {
		t.Fatal(err)
	}
	p, ready, err := m.history.Predict(int64(96 * time.Second))
	if err != nil || !ready {
		t.Fatalf("%v %v", ready, err)
	}
	if !reflect.DeepEqual(p.WeightedGas, []uint64{48, 34}) || p.WeightDenominator != 21 || p.Cycle != 0 {
		t.Fatalf("wrong physical stage attribution or duplicate counted: %+v", p)
	}
	if b.Relay2Txs[0].GasUsed != 17 {
		t.Fatal("observer mutated execution price")
	}
}
func TestGARETRejectsDoubleExecutionAndConflictingBlock(t *testing.T) {
	m := garetTestModule(t)
	b := &message.BlockInfoMsg{SenderShardID: 0, BlockHeight: 1, ExecutionBlockHash: "first", BlockBodyLength: 1, InnerShardTxs: []*core.Transaction{garetStage(1, 1, 2, 11, false)}}
	if err := m.recordBlockAt(b, m.start); err != nil {
		t.Fatal(err)
	}
	conflict := *b
	conflict.ExecutionBlockHash = "other"
	if err := m.recordBlockAt(&conflict, m.start); err == nil {
		t.Fatal("conflicting block accepted")
	}
	duplicate := *b
	duplicate.BlockHeight = 2
	duplicate.ExecutionBlockHash = "second"
	if err := m.recordBlockAt(&duplicate, m.start.Add(time.Second)); err == nil {
		t.Fatal("same physical stage committed in two blocks")
	}
}
func TestGARETColdStartFixedSlotsAndAllShardBarrier(t *testing.T) {
	m := garetTestModule(t)
	p, ready, err := m.history.Predict(int64(48 * time.Second))
	if ready || err != nil {
		t.Fatalf("cold start %+v %v %v", p, ready, err)
	}
	for shard := uint64(0); shard < 2; shard++ {
		b := &message.BlockInfoMsg{SenderShardID: shard, BlockHeight: 1, Epoch: 1, ExecutionBlockHash: fmt.Sprint(shard)}
		if err := m.recordBlockAt(b, m.start.Add(50*time.Second)); err != nil {
			t.Fatal(err)
		}
		if m.allShardsAt(1) != (shard == 1) {
			t.Fatal("mapping activated before all shards acknowledged new epoch")
		}
	}
}
func TestGARETFutureAddressUsesCurrentGroup(t *testing.T) {
	m := garetTestModule(t)
	m.placement = []uint64{1, 0}
	address := fmt.Sprintf("%040x", 2)
	if _, known := m.known[address]; known {
		t.Fatal("fixture should not have injected account")
	}
	if got := m.fetchModifiedMap(address); got != 1 {
		t.Fatalf("unseen account ignored moved group: %d", got)
	}
	m.observeDispatch([]*core.Transaction{{Sender: address, Recipient: fmt.Sprintf("%040x", 3), GLASCallRelations: []core.GLASCallRelation{{Caller: fmt.Sprintf("%040x", 1), Callee: fmt.Sprintf("%040x", 4)}}}}, 1, m.start)
	if len(m.known) != 4 {
		t.Fatalf("migration identity set missed call participants: %v", m.known)
	}
}

func TestGARETLateHistoryDoesNotFailExecutionOrRevisePrediction(t *testing.T) {
	m := garetTestModule(t)
	p, ready, err := m.history.Predict(int64(96 * time.Second))
	if err != nil || !ready {
		t.Fatal(err)
	}
	b := &message.BlockInfoMsg{SenderShardID: 0, BlockHeight: 1, ExecutionBlockHash: "late", BlockBodyLength: 1, InnerShardTxs: []*core.Transaction{garetStage(1, 1, 2, 11, false)}}
	if err := m.recordBlockAt(b, m.start.Add(40*time.Second)); err != nil {
		t.Fatal(err)
	}
	if m.lateStages != 1 || m.lateGas != 11 {
		t.Fatal("late stage not diagnosed")
	}
	again, _, err := m.history.Predict(int64(96 * time.Second))
	if err != nil || !reflect.DeepEqual(p, again) {
		t.Fatal("late receipt changed a published prediction")
	}
	// A receipt in the newest closed cycle is retained even after the tick took
	// the history lock first. It becomes the next prediction, not the old one.
	b.BlockHeight = 2
	b.ExecutionBlockHash = "recent"
	b.InnerShardTxs = []*core.Transaction{garetStage(2, 1, 2, 7, false)}
	if err := m.recordBlockAt(b, m.start.Add(70*time.Second)); err != nil {
		t.Fatal(err)
	}
	next, _, err := m.history.Predict(int64(144 * time.Second))
	if err != nil || next.WeightedGas[0] != 21 {
		t.Fatalf("recent out-of-order receipt lost: %+v %v", next, err)
	}
}

func TestGARETPartitionBarrierHasBoundedFailure(t *testing.T) {
	m := garetTestModule(t)
	m.epochs[0] = 1
	m.heights[1] = 7
	if err := m.waitForPartitionRound(1, time.Millisecond); err == nil {
		t.Fatal("missing shard did not stop the run")
	}
	m.epochs[1] = 1
	if err := m.waitForPartitionRound(1, time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// Exercise the real recorder at the same first-reconfiguration boundary as
// GARET. The history-only fixtures have no recorder and cannot catch a producer
// completion basis that the shared metrics writer rejects.
func TestGARETReconfigurationWithUnifiedRecorder(t *testing.T) {
	for _, tc := range []struct {
		name      string
		injecting bool
		failed    bool
	}{
		{"injecting_completed", true, false},
		{"draining_completed", false, false},
		{"injecting_failed", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "unified")
			r, err := unifiedmetrics.New(dir, unifiedmetrics.RunConfig{
				Method: "GARET", ConsensusMethod: 10, ShardCount: 2,
				ExpectedTransactions: 1, InjectSpeed: 1000, BlockGasLimit: 30000000, CrossCallGas: 3000,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := r.Close(time.Now()); err != nil {
					t.Error(err)
				}
			})
			m := garetTestModule(t)
			m.SetUnifiedRecorder(r)
			m.start = time.Time{}
			tx := core.NewTransaction(fmt.Sprintf("%040x", 1), fmt.Sprintf("%040x", 2), big.NewInt(0), 0, time.Now())
			tx.SourceTxHash, tx.GLASBaseGas, tx.GasUsed = fmt.Sprintf("0x%064x", 1), 1, 1
			tx.GLASCallGasPerCall = 3000
			m.observeDispatch([]*core.Transaction{tx}, 0, time.Now().Add(-96*time.Second))
			span := m.beginReconfiguration(1, tc.injecting, unifiedmetrics.CompletionBasisAllShards)
			m.epochs[0] = 1
			if m.allShardsAt(1) {
				t.Fatal("one shard cannot complete the all-shard barrier")
			}
			status := "completed"
			if tc.failed {
				if err := m.waitForPartitionRound(1, time.Millisecond); err == nil {
					t.Fatal("missing shard did not fail the barrier")
				}
				status = "failed"
			} else {
				m.epochs[1] = 1
				if err := m.waitForPartitionRound(1, time.Millisecond); err != nil {
					t.Fatal(err)
				}
			}
			span.finish(status)
			f, err := os.Open(filepath.Join(dir, "reconfiguration.csv"))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			rows, err := csv.NewReader(f).ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			want, phase := []string{"trigger"}, "draining"
			if tc.injecting {
				want, phase = append(want, "injection_pause_begin"), "injecting"
			}
			want = append(want, "finish")
			if tc.injecting && !tc.failed {
				want = append(want, "injection_pause_end")
			}
			if len(rows) != len(want)+1 {
				t.Fatalf("wrong control rows: %v", rows)
			}
			for i, event := range want {
				row := rows[i+1]
				elapsed, err := strconv.ParseInt(row[5], 10, 64)
				if err != nil || elapsed < int64(96*time.Second) {
					t.Fatalf("control event lost actual injection origin: %v", row)
				}
				if row[0] != event || row[1] != "1" || row[2] != "1" || row[3] != phase ||
					row[4] != "supervisor_control" || row[7] != "relative_to_first_injection" ||
					row[9] != unifiedmetrics.CompletionBasisAllShards {
					t.Fatalf("wrong GARET control observation: %v", row)
				}
				if (event == "finish" || event == "injection_pause_end") && row[8] != status {
					t.Fatalf("wrong completion status: %v", row)
				}
			}
			if len(injectionTestRows(t, dir)) != 2 {
				t.Fatal("control events polluted transaction statistics")
			}
		})
	}
}
