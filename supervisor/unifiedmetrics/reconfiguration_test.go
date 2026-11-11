package unifiedmetrics

import (
	"blockEmulator/core"
	"encoding/csv"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func readReconfigurationRows(t *testing.T, directory string) [][]string {
	t.Helper()
	f, err := os.Open(filepath.Join(directory, "reconfiguration.csv"))
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

func testReconfigurationEvent() ReconfigurationEvent {
	return ReconfigurationEvent{Event: "trigger", ID: 1, TargetEpoch: 1,
		Phase: "injecting", CompletionBasis: "observed_new_epoch_block"}
}

func TestReconfigurationUsesActualInjectionOrigin(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	r, err := New(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	event := testReconfigurationEvent()
	if err := r.RecordReconfiguration(event, start.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if !r.start.IsZero() {
		t.Fatal("control event changed transaction origin")
	}
	if err := r.RecordInjection([]*core.Transaction{testTx("first", 0, "LOCAL", 20)}, 0, start); err != nil {
		t.Fatal(err)
	}
	event.Event = "finish"
	event.Status = "completed"
	if err := r.RecordReconfiguration(event, start.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(start.Add(time.Second)); err == nil {
		t.Fatal("close discarded control-event coverage")
	}
	if err := r.Close(start.Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	rows := readReconfigurationRows(t, dir)
	if !reflect.DeepEqual(rows[0], ReconfigurationHeader) || len(rows) != 3 {
		t.Fatalf("wrong control schema/rows: %v", rows)
	}
	if rows[1][5] != "" || rows[1][7] != "before_first_injection" {
		t.Fatalf("pre-origin event fabricated elapsed: %v", rows[1])
	}
	if rows[2][5] != "2000000000" || rows[2][7] != "relative_to_first_injection" {
		t.Fatalf("wrong shared clock: %v", rows[2])
	}
	meta := readMetadata(t, dir)
	if meta.SchemaVersion != 1 || meta.ReconfigurationSchemaVersion != 1 || meta.ReconfigurationRows != 2 || meta.FirstInjectionUnixNS != start.UnixNano() {
		t.Fatalf("wrong additive metadata: %+v", meta)
	}
	if len(readRows(t, dir)) != 2 {
		t.Fatal("control events polluted transaction raw ledger")
	}
}

func TestReconfigurationBeforeAnyInjectionAndEmptyHash(t *testing.T) {
	for _, hasEvent := range []bool{false, true} {
		dir := filepath.Join(t.TempDir(), "run")
		config := testConfig()
		config.Method, config.ConsensusMethod = "Relay", 3
		r, err := New(dir, config)
		if err != nil {
			t.Fatal(err)
		}
		at := time.Now()
		if hasEvent {
			if err := r.RecordReconfiguration(testReconfigurationEvent(), at); err != nil {
				t.Fatal(err)
			}
		}
		if err := r.Close(at.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		wantRows := 1
		if hasEvent {
			wantRows++
		}
		if len(readReconfigurationRows(t, dir)) != wantRows {
			t.Fatal("missing empty/control log")
		}
		meta := readMetadata(t, dir)
		if meta.FirstInjectionUnixNS != 0 || meta.ObservedDurationNS != 0 {
			t.Fatal("control-only run fabricated transaction coverage")
		}
	}
}

func TestReconfigurationValidationAndWriteFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	r, err := New(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	invalid := testReconfigurationEvent()
	invalid.Event, invalid.Phase = "injection_pause_begin", "draining"
	if err := r.RecordReconfiguration(invalid, at); err == nil {
		t.Fatal("draining reported as injection pause")
	}
	invalid.Event, invalid.Phase, invalid.Status = "injection_pause_end", "injecting", "failed"
	if err := r.RecordReconfiguration(invalid, at); err == nil {
		t.Fatal("failed run reported as resumed")
	}
	invalid = testReconfigurationEvent()
	invalid.CompletionBasis = "unknown_completion_basis"
	if err := r.RecordReconfiguration(invalid, at); err == nil {
		t.Fatal("unknown completion basis accepted")
	}
	if err := r.reconfigurationFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.RecordReconfiguration(testReconfigurationEvent(), at); err == nil {
		t.Fatal("ignored control log write failure")
	}
	if err := r.RecordInjection([]*core.Transaction{testTx("first", 0, "LOCAL", 20)}, 0, at); err == nil {
		t.Fatal("continued after logging failure")
	}
	if err := r.Close(at.Add(time.Second)); err == nil {
		t.Fatal("close hid control log failure")
	}
	if meta := readMetadata(t, dir); meta.Status != "recording" {
		t.Fatal("failed logger marked cleanly closed")
	}
}

func TestInstalledAllShardsBasisKeepsActualTransactionClock(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	r, err := New(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := r.RecordInjection([]*core.Transaction{testTx("first", 0, "LOCAL", 20)}, 0, start); err != nil {
		t.Fatal(err)
	}
	event := testReconfigurationEvent()
	event.CompletionBasis = CompletionBasisInstalledAllShards
	if err := r.RecordReconfiguration(event, start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	event.Event = "finish"
	event.Status = "completed"
	if err := r.RecordReconfiguration(event, start.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(start.Add(12 * time.Second)); err != nil {
		t.Fatal(err)
	}
	rows := readReconfigurationRows(t, dir)
	if rows[2][5] != "3000000000" || rows[2][9] != CompletionBasisInstalledAllShards {
		t.Fatalf("wrong install observation: %v", rows[2])
	}
	if readMetadata(t, dir).FirstInjectionUnixNS != start.UnixNano() {
		t.Fatal("installation moved actual-injection origin")
	}
}
