package unifiedmetrics

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func testConfig() RunConfig {
	return RunConfig{Method: "CLPA", ConsensusMethod: 1, ShardCount: 4, ExpectedTransactions: 2,
		InjectSpeed: 2000, BlockGasLimit: 1000, CrossCallGas: 20,
		TransactionsFile: "transactions.csv", CallsFile: "contract_calls.csv", ConfigPath: "config.json", ConfigSHA256: "test-config"}
}

func testTx(hash string, calls uint64, stage string, unit uint64) *core.Transaction {
	tx := core.NewTransaction("sender", "recipient", big.NewInt(1), 0, time.Now())
	tx.SourceTxHash, tx.GLASBaseGas, tx.GLASCallGasPerCall = fmt.Sprintf("0x%x", sha256.Sum256([]byte(hash))), 100, unit
	tx.GLASCrossCallCount = calls
	for i := uint64(0); i < calls; i++ {
		tx.GLASCallRelations = append(tx.GLASCallRelations, core.GLASCallRelation{Caller: "caller", Callee: "callee"})
	}
	tx.Relayed = stage == "RELAY2"
	tx.GasUsed, tx.GLASAttachedGas, _ = core.PriceGLASStageGas(100, unit, calls, stage != "RELAY1")
	return tx
}

func testBlock(at time.Time, local, relay1, relay2 []*core.Transaction) *message.BlockInfoMsg {
	return &message.BlockInfoMsg{BlockBodyLength: len(local) + len(relay1) + len(relay2),
		InnerShardTxs: local, Relay1Txs: relay1, Relay2Txs: relay2, SenderShardID: 0,
		ExecutionBlockHash: "block-hash", BlockHeight: 1, CommitTime: at.Add(-time.Millisecond)}
}

func readRows(t *testing.T, dir string) [][]string {
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

func readMetadata(t *testing.T, dir string) metadata {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var value metadata
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestRecorderLogicalCompletionAndRawRetries(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	r, err := New(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	local, top := testTx("local", 2, "LOCAL", 20), testTx("cross", 2, "RELAY1", 20)
	if err := r.RecordBlock(testBlock(start, nil, nil, nil), start.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r.RecordInjection(nil, 0, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if !r.start.IsZero() {
		t.Fatal("empty injection started observation")
	}
	if err := r.RecordInjection([]*core.Transaction{local, top}, 0, start); err != nil {
		t.Fatal(err)
	}
	if err := r.RecordBlock(testBlock(start, nil, nil, nil), start.Add(-time.Millisecond)); err != nil {
		t.Fatalf("delayed empty pre-injection receipt should be ignored: %v", err)
	}
	// A duplicate send is preserved in the raw ledger for offline deduplication.
	if err := r.RecordInjection([]*core.Transaction{top}, 0, start.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	b := testBlock(start, []*core.Transaction{local}, []*core.Transaction{top}, nil)
	if err := r.RecordBlock(b, start.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	// Network handlers may process an earlier captured receipt later.
	if err := r.RecordBlock(b, start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	relay := testTx("cross", 2, "RELAY2", 20)
	b2 := testBlock(start, nil, nil, []*core.Transaction{relay})
	b2.ExecutionBlockHash, b2.BlockHeight = "block-hash-2", 2
	if err := r.RecordBlock(b2, start.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	empty := testBlock(start, nil, nil, nil)
	empty.ExecutionBlockHash, empty.BlockHeight = "empty-block", 3
	if err := r.RecordBlock(empty, start.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(start.Add(60 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(start.Add(61 * time.Second)); err != nil {
		t.Fatal(err)
	}
	rows := readRows(t, dir)
	if !reflect.DeepEqual(rows[0], Header) || len(rows) != 1+3+4+5 {
		t.Fatalf("unexpected raw event schema/count: %v", rows)
	}
	if rows[1][3] != "0" || rows[1][10] != "100" || rows[1][14] != "" {
		t.Fatalf("invalid first injection: %v", rows[1])
	}
	if got := rows[5]; got[2] != "LOCAL" || got[11] != "140" || got[13] != "40" || got[14] != "0" || got[15] != "1" || got[16] != "1" {
		t.Fatalf("wrong local final stage: %v", got)
	}
	if got := rows[6]; got[2] != "RELAY1" || got[11] != "100" || got[12] != "2" || got[13] != "0" || got[14] != "1" || got[15] != "0" {
		t.Fatalf("wrong relay1 physical stage: %v", got)
	}
	if got := rows[11]; got[2] != "RELAY2" || got[11] != "140" || got[14] != "1" || got[15] != "1" || got[16] != "1" {
		t.Fatalf("wrong relay2 final stage: %v", got)
	}
	meta := readMetadata(t, dir)
	if meta.Status != "closed" || meta.SchemaVersion != 1 || meta.Clock != "supervisor_receipt_monotonic" ||
		meta.InjectionRows != 3 || meta.BlockRows != 4 || meta.StageRows != 5 ||
		meta.FirstInjectionUnixNS != start.UnixNano() || meta.ObservedDurationNS != int64(60*time.Second) {
		t.Fatalf("unexpected run metadata: %+v", meta)
	}
	if err := r.RecordInjection([]*core.Transaction{top}, 0, start.Add(70*time.Second)); err == nil {
		t.Fatal("accepted an event after close")
	}
}

func TestRecorderZeroFeeStillReportsInternalCrossing(t *testing.T) {
	config := testConfig()
	config.CrossCallGas = 0
	dir := filepath.Join(t.TempDir(), "run")
	r, err := New(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	tx := testTx("zero-fee", 1, "LOCAL", 0)
	if err := r.RecordInjection([]*core.Transaction{tx}, 0, start); err != nil {
		t.Fatal(err)
	}
	if err := r.RecordBlock(testBlock(start, []*core.Transaction{tx}, nil, nil), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(start.Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	stage := readRows(t, dir)[3]
	if stage[13] != "0" || stage[15] != "1" || stage[16] != "1" {
		t.Fatalf("free cross-call lost its cross-shard identity: %v", stage)
	}
}

func TestRejectInvalidWholeReceiptBeforeAnyWrite(t *testing.T) {
	cases := map[string]func(*message.BlockInfoMsg){
		"wrong body length":      func(b *message.BlockInfoMsg) { b.BlockBodyLength++ },
		"missing block identity": func(b *message.BlockInfoMsg) { b.ExecutionBlockHash = "" },
		"missing commit time":    func(b *message.BlockInfoMsg) { b.CommitTime = time.Time{} },
		"unknown shard":          func(b *message.BlockInfoMsg) { b.SenderShardID = 4 },
		"unknown source":         func(b *message.BlockInfoMsg) { b.InnerShardTxs[1].SourceTxHash = "" },
		"zero base":              func(b *message.BlockInfoMsg) { b.InnerShardTxs[1].GLASBaseGas = 0 },
		"synthetic call":         func(b *message.BlockInfoMsg) { b.InnerShardTxs[1].IsGLASCallTask = true },
		"broker":                 func(b *message.BlockInfoMsg) { b.InnerShardTxs[1].HasBroker = true },
		"broker class":           func(b *message.BlockInfoMsg) { b.Broker1Txs = b.InnerShardTxs[:1] },
		"wrong relay flag":       func(b *message.BlockInfoMsg) { b.InnerShardTxs[1].Relayed = true },
		"wrong call unit":        func(b *message.BlockInfoMsg) { b.InnerShardTxs[1].GLASCallGasPerCall++ },
		"wrong charged gas":      func(b *message.BlockInfoMsg) { b.InnerShardTxs[1].GasUsed++ },
		"wrong attached gas":     func(b *message.BlockInfoMsg) { b.InnerShardTxs[1].GLASAttachedGas++ },
		"impossible call count":  func(b *message.BlockInfoMsg) { b.InnerShardTxs[1].GLASCrossCallCount = 2 },
		"empty call endpoint":    func(b *message.BlockInfoMsg) { b.InnerShardTxs[1].GLASCallRelations[0].Callee = "" },
		"duplicate stage":        func(b *message.BlockInfoMsg) { b.InnerShardTxs[1] = b.InnerShardTxs[0] },
		"overfull block": func(b *message.BlockInfoMsg) {
			b.InnerShardTxs[1].GLASBaseGas = 900
			b.InnerShardTxs[1].GasUsed = 920
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "run")
			r, err := New(dir, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			first, second := testTx("first", 1, "LOCAL", 20), testTx("second", 1, "LOCAL", 20)
			if err := r.RecordInjection([]*core.Transaction{first, second}, 0, start); err != nil {
				t.Fatal(err)
			}
			b := testBlock(start, []*core.Transaction{first, second}, nil, nil)
			mutate(b)
			if err := r.RecordBlock(b, start.Add(time.Second)); err == nil {
				t.Fatal("accepted invalid receipt")
			}
			if err := r.Close(start.Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if rows := readRows(t, dir); len(rows) != 3 {
				t.Fatalf("invalid receipt partially logged: %v", rows)
			}
		})
	}
}

func TestTimingAndBatchValidation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	r, err := New(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	valid := testTx("valid", 0, "LOCAL", 20)
	invalid := testTx("invalid", 0, "LOCAL", 20)
	invalid.SourceTxHash = ""
	if err := r.RecordBlock(testBlock(start, []*core.Transaction{valid}, nil, nil), start); err == nil {
		t.Fatal("accepted nonempty block before injection")
	}
	if err := r.RecordInjection([]*core.Transaction{valid, invalid}, 0, start); err == nil {
		t.Fatal("accepted partial injection batch")
	}
	if !r.start.IsZero() || len(readRows(t, dir)) != 1 {
		t.Fatal("invalid batch changed origin or wrote rows")
	}
	if err := r.RecordInjection([]*core.Transaction{valid}, 0, start); err != nil {
		t.Fatal(err)
	}
	if err := r.RecordInjection([]*core.Transaction{valid}, 0, start.Add(-time.Millisecond)); err == nil {
		t.Fatal("accepted negative elapsed time")
	}
	if err := r.RecordBlock(testBlock(start, []*core.Transaction{valid}, nil, nil), start.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(start.Add(time.Second)); err == nil {
		t.Fatal("close discarded recorded coverage")
	}
	if err := r.Close(start.Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestFreshDirectoryAndEmptyRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	r, err := New(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(dir, testConfig()); err == nil {
		t.Fatal("new recorder overwrote existing run")
	}
	if err := r.Close(time.Now()); err != nil {
		t.Fatal(err)
	}
	meta := readMetadata(t, dir)
	if meta.Status != "closed" || meta.FirstInjectionUnixNS != 0 || meta.ObservedDurationNS != 0 || len(readRows(t, dir)) != 1 {
		t.Fatalf("empty run fabricated coverage: %+v", meta)
	}
	config := testConfig()
	config.ConsensusMethod = 0
	if _, err := New(filepath.Join(t.TempDir(), "unsupported"), config); err == nil {
		t.Fatal("accepted broker configuration")
	}
}

func TestConcurrentRawWrites(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	r, err := New(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	tx := testTx("retry", 0, "LOCAL", 20)
	if err := r.RecordInjection([]*core.Transaction{tx}, 0, start); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := r.RecordInjection([]*core.Transaction{tx}, 0, start.Add(time.Duration(i+1)*time.Millisecond)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if err := r.Close(start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if meta := readMetadata(t, dir); meta.InjectionRows != 21 || len(readRows(t, dir)) != 22 {
		t.Fatalf("lost/interleaved concurrent rows: %+v", meta)
	}
}

func TestCompletionProgressCountsOnlyUniqueInjectedFinalSources(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	r, err := New(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	local, top := testTx("local", 0, "LOCAL", 20), testTx("cross", 0, "RELAY1", 20)
	if err := r.RecordInjection([]*core.Transaction{local, top, top}, 0, start); err != nil {
		t.Fatal(err)
	}
	if err := r.RecordBlock(testBlock(start, []*core.Transaction{local}, []*core.Transaction{top}, nil), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	p := r.CompletionProgress()
	if p.Injected != 2 || p.Completed != 1 || p.FinalSources != 1 || p.Complete() {
		t.Fatalf("incorrect progress: %+v", p)
	}
	// Neither retransmission nor a first-stage Relay can count as new completion.
	if err := r.RecordBlock(testBlock(start, []*core.Transaction{local}, []*core.Transaction{top}, nil), start.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if r.CompletionProgress() != p {
		t.Fatal("replay advanced logical completion")
	}
	relay := testTx("cross", 0, "RELAY2", 20)
	if err := r.RecordBlock(testBlock(start, nil, nil, []*core.Transaction{relay}), start.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !r.CompletionProgress().Complete() {
		t.Fatal("final relay did not finish known source")
	}
	if err := r.Close(start.Add(4 * time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownFinalCannotReplaceAnUnfinishedInjectedSource(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	r, err := New(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	known, missing, unknown := testTx("known", 0, "LOCAL", 20), testTx("missing", 0, "LOCAL", 20), testTx("unknown", 0, "LOCAL", 20)
	if err := r.RecordInjection([]*core.Transaction{known, missing}, 0, start); err != nil {
		t.Fatal(err)
	}
	if err := r.RecordBlock(testBlock(start, []*core.Transaction{known, unknown}, nil, nil), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	p := r.CompletionProgress()
	if p.Injected != 2 || p.FinalSources != 2 || p.Completed != 1 || p.Complete() {
		t.Fatalf("unmatched final counted as completed: %+v", p)
	}
	if err := r.WriteCompletionDiagnostic("test unfinished", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := r.CloseIncomplete(start.Add(2*time.Second), "test unfinished"); err != nil {
		t.Fatal(err)
	}
	meta := readMetadata(t, dir)
	if meta.Status != "recording" || meta.CompletionFailure != "test unfinished" {
		t.Fatalf("incomplete run sealed as success: %+v", meta)
	}
	data, err := os.ReadFile(filepath.Join(dir, "completion_check.json"))
	if err != nil || !strings.Contains(string(data), missing.SourceTxHash) {
		t.Fatalf("missing diagnostic sample: %s %v", data, err)
	}
}

func TestInvalidHashAndFailedFlushDoNotAdvanceCompletion(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	r, err := New(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	good, bad := testTx("good", 0, "LOCAL", 20), testTx("bad", 0, "LOCAL", 20)
	bad.SourceTxHash = "noncanonical"
	if err := r.RecordInjection([]*core.Transaction{good, bad}, 0, start); err == nil {
		t.Fatal("accepted noncanonical source")
	}
	if r.CompletionProgress().Injected != 0 {
		t.Fatal("invalid batch partially advanced progress")
	}
	if err := r.RecordInjection([]*core.Transaction{good}, 0, start); err != nil {
		t.Fatal(err)
	}
	if err := r.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.RecordBlock(testBlock(start, []*core.Transaction{good}, nil, nil), start.Add(time.Second)); err == nil {
		t.Fatal("ignored failed raw-event flush")
	}
	if r.CompletionProgress().Completed != 0 {
		t.Fatal("counted a final receipt that was not written")
	}
	if err := r.CloseIncomplete(start.Add(2*time.Second), "failed flush"); err == nil {
		t.Fatal("forgot recorder failure")
	}
}
