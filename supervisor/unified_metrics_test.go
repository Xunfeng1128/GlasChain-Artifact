package supervisor

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/unifiedmetrics"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type metricsTestCommittee struct{ blocks int }

type completionTestCommittee struct{ control func() }

func (m *completionTestCommittee) MsgSendingControl()                  { m.control() }
func (*completionTestCommittee) HandleBlockInfo(*message.BlockInfoMsg) {}
func (*completionTestCommittee) HandleOtherMessage([]byte)             {}

func (m *metricsTestCommittee) HandleBlockInfo(b *message.BlockInfoMsg) {
	m.blocks++
	// Exercise raw recording before committee-side mutation.
	b.InnerShardTxs = nil
}
func (*metricsTestCommittee) MsgSendingControl()        {}
func (*metricsTestCommittee) HandleOtherMessage([]byte) {}

func metricsTestTx(id string, stage string) *core.Transaction {
	tx := core.NewTransaction("a", "b", big.NewInt(1), 0, time.Now())
	tx.SourceTxHash, tx.GLASBaseGas, tx.GasUsed = fmt.Sprintf("0x%x", sha256.Sum256([]byte(id))), 100, 100
	tx.Relayed = stage == "RELAY2"
	return tx
}
func metricsTestRecorder(t *testing.T, n uint64) (*unifiedmetrics.Recorder, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "run")
	r, err := unifiedmetrics.New(dir, unifiedmetrics.RunConfig{Method: "Relay", ConsensusMethod: 3, ShardCount: 2,
		ExpectedTransactions: n, InjectSpeed: 2000, BlockGasLimit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return r, dir
}
func metricsRows(t *testing.T, dir string) [][]string {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, "events.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestUnifiedReceiptRecordsBeforeMutationAndDeduplicates(t *testing.T) {
	r, dir := metricsTestRecorder(t, 1)
	origin := time.Now().Add(-time.Minute)
	tx := metricsTestTx("source", "LOCAL")
	if err := r.RecordInjection([]*core.Transaction{tx}, 0, origin); err != nil {
		t.Fatal(err)
	}
	cm := &metricsTestCommittee{}
	d := &Supervisor{unifiedRecorder: r, comMod: cm, Ss: signal.NewStopSignal(2)}
	b := message.BlockInfoMsg{SenderShardID: 0, BlockHeight: 1, ExecutionBlockHash: "hash", BlockBodyLength: 1,
		InnerShardTxs: []*core.Transaction{tx}, CommitTime: origin.Add(9 * time.Second)}
	data, _ := json.Marshal(b)
	d.handleBlockInfosAt(data, origin.Add(10*time.Second))
	b.CommitTime = origin.Add(4 * time.Second)
	retry, _ := json.Marshal(b)
	d.handleBlockInfosAt(retry, origin.Add(5*time.Second))
	if cm.blocks != 1 {
		t.Fatal("duplicate receipt changed committee graph")
	}
	rows := metricsRows(t, dir)
	if len(rows) != 6 || rows[3][0] != "stage" || rows[3][3] != "10000000000" || rows[5][3] != "5000000000" {
		t.Fatalf("receipt time/stage lost: %v", rows)
	}
	d.closeUnifiedMetrics()
	// Later protocol receipts remain processable, but are outside sealed metrics.
	b.BlockHeight, b.ExecutionBlockHash = 2, "late"
	data, _ = json.Marshal(b)
	d.handleBlockInfosAt(data, time.Now())
	if cm.blocks != 2 || len(metricsRows(t, dir)) != 6 {
		t.Fatal("late protocol receipt altered sealed metrics")
	}
	d.closeUnifiedMetrics()
}

func TestUnifiedReceiptRejectsSemanticConflicts(t *testing.T) {
	tx := metricsTestTx("source", "LOCAL")
	original := message.BlockInfoMsg{SenderShardID: 0, BlockHeight: 1, ExecutionBlockHash: "hash", BlockBodyLength: 1, InnerShardTxs: []*core.Transaction{tx}}
	for _, kind := range []string{"hash", "stage", "gas", "call", "epoch"} {
		t.Run(kind, func(t *testing.T) {
			d := &Supervisor{}
			if duplicate, err := d.deduplicateUnifiedBlockInfo(&original); duplicate || err != nil {
				t.Fatal(err)
			}
			b := original
			copyTx := *tx
			b.InnerShardTxs = []*core.Transaction{&copyTx}
			switch kind {
			case "hash":
				b.ExecutionBlockHash = "other"
			case "stage":
				b.Relay1Txs = b.InnerShardTxs
				b.InnerShardTxs = nil
			case "gas":
				copyTx.GasUsed++
			case "call":
				copyTx.GLASCallRelations = []core.GLASCallRelation{{Caller: "a", Callee: "c"}}
			case "epoch":
				b.Epoch++
			}
			if duplicate, err := d.deduplicateUnifiedBlockInfo(&b); duplicate || err == nil {
				t.Fatal("conflicting block accepted")
			}
		})
	}
}

// A tiny synthetic fixture verifies the production Go schema and Python reader
// together. No network, workload data or simulated experiment is launched.
func TestUnifiedProducerToPythonSummary(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python3 not installed")
	}
	r, dir := metricsTestRecorder(t, 2)
	origin := time.Now().Add(-time.Minute)
	local, first, final := metricsTestTx("local", "LOCAL"), metricsTestTx("cross", "RELAY1"), metricsTestTx("cross", "RELAY2")
	if err := r.RecordInjection([]*core.Transaction{local, first}, 0, origin); err != nil {
		t.Fatal(err)
	}
	blocks := []message.BlockInfoMsg{
		{SenderShardID: 0, BlockHeight: 1, ExecutionBlockHash: "a", BlockBodyLength: 1, InnerShardTxs: []*core.Transaction{local}},
		{SenderShardID: 0, BlockHeight: 2, ExecutionBlockHash: "b", BlockBodyLength: 1, Relay1Txs: []*core.Transaction{first}},
		{SenderShardID: 1, BlockHeight: 1, ExecutionBlockHash: "c", BlockBodyLength: 1, Relay2Txs: []*core.Transaction{final}},
	}
	for i, sec := range []int{10, 11, 20} {
		at := origin.Add(time.Duration(sec) * time.Second)
		blocks[i].CommitTime = at.Add(-time.Millisecond)
		if err := r.RecordBlock(&blocks[i], at); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Close(origin.Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	outDir := filepath.Join(t.TempDir(), "summary")
	cmd := exec.Command(python, "../scripts/unified_metrics/summarize.py", "--run-dir", dir, "--output-dir", outDir, "--window-seconds", "10", "--duration-seconds", "30")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Go->Python integration: %v %s", err, out)
	}
	data, err := os.ReadFile(filepath.Join(outDir, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Valid   bool `json:"valid_for_publication"`
		Primary struct {
			Completed int     `json:"completed"`
			TPS       float64 `json:"tps"`
			Top       float64 `json:"top_cross_ratio"`
			Union     float64 `json:"union_cross_ratio"`
		} `json:"primary"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if !result.Valid || result.Primary.Completed != 2 || result.Primary.TPS != 2.0/30 || result.Primary.Top != .5 || result.Primary.Union != .5 {
		t.Fatalf("incorrect aggregation: %s", data)
	}
	f, err := os.Open(filepath.Join(outDir, "windows.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 || rows[1][3] != "0" || rows[2][3] != "1" || rows[3][3] != "1" {
		t.Fatalf("half-open boundary error: %v", rows)
	}
}

func TestCompletionStopGuardWaitsForExpectedSourcesAndNewProgress(t *testing.T) {
	now := time.Now()
	guard := completionStopGuard{grace: 10 * time.Second}
	p := unifiedmetrics.CompletionProgress{Expected: 3, Injected: 2}
	for _, at := range []time.Time{now, now.Add(time.Hour)} {
		if ready, err := guard.observe(at, p, false, true); ready || err != nil {
			t.Fatalf("stopped during injection: %v %v", ready, err)
		}
	}
	p.Injected = 3
	if ready, err := guard.observe(now, p, false, true); ready || err != nil {
		t.Fatal("idle threshold must start a grace period")
	}
	p.Completed, p.FinalSources = 1, 1
	if _, err := guard.observe(now.Add(9*time.Second), p, false, true); err != nil {
		t.Fatal(err)
	}
	if _, err := guard.observe(now.Add(18*time.Second), p, false, true); err != nil {
		t.Fatal("new completion did not reset grace")
	}
	if _, err := guard.observe(now.Add(19*time.Second), p, false, true); err == nil {
		t.Fatal("duplicate progress kept stalled run alive")
	}
	// The final recheck wins over an expired candidate timer.
	p.Completed, p.FinalSources = 3, 3
	if ready, err := guard.observe(now.Add(20*time.Second), p, false, true); ready || err != nil {
		t.Fatal("stopped before committee returned")
	}
	if ready, err := guard.observe(now.Add(20*time.Second), p, true, true); !ready || err != nil {
		t.Fatalf("rejected fully reconciled completion: %v", err)
	}
}

func TestCompletionStopGuardRejectsShortInputAndResetsOnActiveBlocks(t *testing.T) {
	guard := completionStopGuard{grace: time.Second}
	now := time.Now()
	p := unifiedmetrics.CompletionProgress{Expected: 2, Injected: 1, Completed: 1, FinalSources: 1}
	if _, err := guard.observe(now, p, true, false); err == nil {
		t.Fatal("short completed injector silently hung")
	}
	p.Injected = 2
	guard.observe(now, p, true, true)
	guard.observe(now.Add(time.Second), p, true, false)
	if _, err := guard.observe(now.Add(2*time.Second), p, true, true); err != nil {
		t.Fatal("active blocks did not reset idle candidate")
	}
}

func TestCommitteeControlPanicReturnsToShutdownCoordinator(t *testing.T) {
	result := startCommitteeControl(&completionTestCommittee{control: func() { panic("input failed") }})
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "input failed") {
			t.Fatalf("lost panic: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("committee panic did not return")
	}
}

func TestCompletionMonitorDoesNotWaitForBlockedEOFControl(t *testing.T) {
	r, _ := metricsTestRecorder(t, 1)
	defer func() { r.CloseIncomplete(time.Now(), "test") }()
	if err := r.RecordInjection([]*core.Transaction{metricsTestTx("unfinished", "LOCAL")}, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	d := &Supervisor{unifiedRecorder: r, Ss: signal.NewStopSignal(2)}
	d.Ss.MarkComplete()
	blocked := make(chan struct{})
	defer close(blocked)
	sender := startCommitteeControl(&completionTestCommittee{control: func() { <-blocked }})
	finished := make(chan error, 1)
	go func() { finished <- d.waitUnifiedCompletion(sender, 0) }()
	select {
	case err := <-finished:
		if err == nil || !strings.Contains(err.Error(), "stalled") {
			t.Fatalf("blocked EOF did not fail: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("EOF drain blocked completion monitor")
	}
}

func TestCompletionMonitorWaitsForLastReceiptSideEffects(t *testing.T) {
	r, _ := metricsTestRecorder(t, 1)
	defer func() { r.Close(time.Now()) }()
	tx := metricsTestTx("last", "LOCAL")
	now := time.Now()
	if err := r.RecordInjection([]*core.Transaction{tx}, 0, now); err != nil {
		t.Fatal(err)
	}
	d := &Supervisor{unifiedRecorder: r, Ss: signal.NewStopSignal(2)}
	d.Ss.MarkComplete()
	// Model RecordBlock finishing while the committee finalizer still holds tcpLock.
	d.tcpLock.Lock()
	if err := r.RecordBlock(&message.BlockInfoMsg{SenderShardID: 0, BlockHeight: 1, ExecutionBlockHash: "final", BlockBodyLength: 1, InnerShardTxs: []*core.Transaction{tx}, CommitTime: now}, now); err != nil {
		d.tcpLock.Unlock()
		t.Fatal(err)
	}
	sender := make(chan error, 1)
	sender <- nil
	finished := make(chan error, 1)
	go func() { finished <- d.waitUnifiedCompletion(sender, 0) }()
	select {
	case err := <-finished:
		d.tcpLock.Unlock()
		t.Fatalf("sealed before receipt side effects: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	d.tcpLock.Unlock()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("complete receipt was not accepted")
	}
}

func TestIncompleteSupervisorSealPreservesDiagnosticAndRecordingStatus(t *testing.T) {
	r, dir := metricsTestRecorder(t, 1)
	tx := metricsTestTx("unfinished", "LOCAL")
	if err := r.RecordInjection([]*core.Transaction{tx}, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	d := &Supervisor{unifiedRecorder: r}
	if err := d.finishUnifiedMetrics(nil, time.Minute); err == nil {
		t.Fatal("incomplete run sealed successfully")
	}
	data, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta struct {
		Status  string `json:"status"`
		Failure string `json:"completion_failure"`
	}
	if err := json.Unmarshal(data, &meta); err != nil || meta.Status != "recording" || meta.Failure == "" {
		t.Fatalf("wrong failure metadata: %s %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "completion_check.json")); err != nil {
		t.Fatal(err)
	}
}
