package supervisor

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/unifiedmetrics"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestE2StopWaitsForBothGoalsAndSafeControllerReturn(t *testing.T) {
	st := signal.NewStopSignal(1)
	g := completionStopGuard{grace: time.Minute}
	at := time.Now()
	p := unifiedmetrics.CompletionProgress{Expected: 100, Injected: 50, Completed: 50, FinalSources: 50, E2PrefixCount: 10, E2PrefixInjected: 10, E2PrefixCompleted: 10}
	if ok, err := g.observeE2(at, p, false, false, true, st); ok || err != nil || st.E2StopRequested() {
		t.Fatalf("stopped with only cohort complete: %v %v", ok, err)
	}
	if ok, err := g.observeE2(at, p, true, false, false, st); ok || err != nil || !st.E2StopRequested() {
		t.Fatalf("stopped active controller: %v %v", ok, err)
	}
	st.StopGap_ResetAll()
	if !st.E2StopRequested() || st.GapEnough() {
		t.Fatal("E2 request and empty-block counters must be independent")
	}
	if ok, err := g.observeE2(at, p, true, true, false, st); !ok || err != nil {
		t.Fatalf("safe shutdown failed: %v %v", ok, err)
	}
}

func TestE2StopDistinguishesNaturalEOFAndMissingSources(t *testing.T) {
	at := time.Now()
	healthy := unifiedmetrics.CompletionProgress{Expected: 10, Injected: 10, Completed: 10, FinalSources: 10, E2PrefixCount: 4, E2PrefixInjected: 4, E2PrefixCompleted: 4}
	st := signal.NewStopSignal(1)
	g := completionStopGuard{grace: time.Second}
	if ok, err := g.observeE2(at, healthy, false, true, true, st); ok || err != nil || st.E2StopRequested() {
		t.Fatalf("short full workload was not held until the window: %v %v", ok, err)
	}
	if ok, err := g.observeE2(at, healthy, true, true, true, st); !ok || err != nil {
		t.Fatalf("full workload plus window: %v %v", ok, err)
	}
	missing := healthy
	missing.E2PrefixInjected--
	if _, err := g.observeE2(at, missing, false, true, true, st); err == nil {
		t.Fatal("missing prefix source accepted")
	}
	missing = healthy
	missing.E2PrefixCompleted--
	missing.Completed--
	missing.FinalSources--
	g = completionStopGuard{grace: time.Second}
	if ok, err := g.observeE2(at, missing, false, true, true, st); ok || err != nil {
		t.Fatal("did not wait for possible last receipt")
	}
	if _, err := g.observeE2(at.Add(time.Second), missing, false, true, true, st); err == nil {
		t.Fatal("did not diagnose missing final receipt after idle grace")
	}
}

func TestE2SealDrainsAlreadyTimestampedReceipts(t *testing.T) {
	target, background := metricsTestTx("target", "LOCAL"), metricsTestTx("background", "LOCAL")
	tmp := t.TempDir()
	prefix := filepath.Join(tmp, "prefix.csv")
	dir := filepath.Join(tmp, "metrics")
	if err := os.WriteFile(prefix, []byte("source_tx_hash\n"+target.SourceTxHash+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := unifiedmetrics.New(dir, unifiedmetrics.RunConfig{Method: "Relay", ConsensusMethod: 3, ShardCount: 1, ExpectedTransactions: 3, InjectSpeed: 1, BlockGasLimit: 1000, E2ObservationSeconds: 1, E2PrefixCount: 1, E2PrefixFile: prefix})
	if err != nil {
		t.Fatal(err)
	}
	d := &Supervisor{unifiedRecorder: r, e2ReceiptFence: new(sync.RWMutex)}
	start := time.Now().Add(-3 * time.Second)
	if err = r.RecordInjection([]*core.Transaction{target, background}, 0, start); err != nil {
		t.Fatal(err)
	}
	block := func(tx *core.Transaction, height uint64, hash string) *message.BlockInfoMsg {
		return &message.BlockInfoMsg{BlockBodyLength: 1, InnerShardTxs: []*core.Transaction{tx}, BlockHeight: height, ExecutionBlockHash: hash, CommitTime: start.Add(time.Millisecond)}
	}
	if err = r.RecordBlock(block(target, 1, "target"), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	// Reproduce a handler that timestamped a second report inside the window
	// but cannot finish its TCP-serialized work until after the cutoff.
	d.e2ReceiptFence.RLock()
	done := make(chan error, 1)
	go func() { done <- d.finishUnifiedMetrics(nil, time.Minute) }()
	select {
	case err := <-done:
		t.Fatalf("sealed over pending receipt: %v", err)
	case <-time.After(15 * time.Millisecond):
	}
	err = r.RecordBlock(block(background, 2, "bg"), start.Add(500*time.Millisecond))
	d.e2ReceiptFence.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Status    string
		StageRows int    `json:"stage_rows"`
		Scope     string `json:"completion_scope"`
	}
	if err = json.Unmarshal(data, &m); err != nil || m.StageRows != 2 || m.Scope != "e2" || m.Status != "closed" {
		t.Fatalf("bad seal: %s err=%v", data, err)
	}
}

func TestE2SupervisorEndsPartialWorkloadViaCooperativeController(t *testing.T) {
	target := metricsTestTx("target", "LOCAL")
	dir := t.TempDir()
	prefix := filepath.Join(dir, "prefix.csv")
	if err := os.WriteFile(prefix, []byte("source_tx_hash\n"+target.SourceTxHash+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := unifiedmetrics.New(filepath.Join(dir, "metrics"), unifiedmetrics.RunConfig{Method: "Relay", ConsensusMethod: 3, ShardCount: 1, ExpectedTransactions: 2, InjectSpeed: 1, BlockGasLimit: 1000, E2ObservationSeconds: 1, E2PrefixCount: 1, E2PrefixFile: prefix})
	if err != nil {
		t.Fatal(err)
	}
	d := &Supervisor{Ss: signal.NewStopSignal(1), unifiedRecorder: r}
	start := time.Now().Add(-2 * time.Second)
	if err = r.RecordInjection([]*core.Transaction{target}, 0, start); err != nil {
		t.Fatal(err)
	}
	b := &message.BlockInfoMsg{BlockBodyLength: 1, InnerShardTxs: []*core.Transaction{target}, BlockHeight: 1, ExecutionBlockHash: "done", CommitTime: start}
	if err = r.RecordBlock(b, start.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	sender, exit := make(chan error, 1), make(chan error, 1)
	go func() { exit <- d.waitUnifiedCompletion(sender, time.Minute) }()
	deadline := time.Now().Add(4 * time.Second)
	for !d.Ss.E2StopRequested() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !d.Ss.E2StopRequested() {
		sender <- nil
		t.Fatal("complete E2 goals did not request sender return")
	}
	select {
	case err = <-exit:
		t.Fatalf("terminated before sender accepted stop: %v", err)
	default:
	}
	sender <- nil
	select {
	case err = <-exit:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("goals and sender completion did not finish")
	}
	if err = d.finishUnifiedMetrics(nil, time.Minute); err != nil {
		t.Fatal(err)
	}
}
