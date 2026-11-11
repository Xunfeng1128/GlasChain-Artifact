package unifiedmetrics

import (
	"blockEmulator/core"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestE2FixedWindowClosesWithOutstandingAndUnsentSources(t *testing.T) {
	cfg := testConfig()
	cfg.Method, cfg.ConsensusMethod = "GLAS_Pending", 12
	cfg.E2FixedWindow = true
	cfg.E2ObservationSeconds = 960
	dir := filepath.Join(t.TempDir(), "metrics")
	r, err := New(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if !r.E2Deadline().IsZero() || !r.FirstInjectionTime().IsZero() {
		t.Fatal("unexpected fixed-window timestamps before first dispatch")
	}
	if p, ready := r.StopProgress(start.Add(time.Hour)); ready || !p.IsE2() || !p.E2FixedWindow {
		t.Fatalf("unexpected pre-dispatch progress: %+v ready=%v", p, ready)
	}
	tx := testTx("pending", 0, "LOCAL", 20)
	if err := r.RecordInjection([]*core.Transaction{tx}, 0, start); err != nil {
		t.Fatal(err)
	}
	deadline := start.Add(960 * time.Second)
	if r.FirstInjectionTime() != start || r.E2Deadline() != deadline {
		t.Fatalf("incorrect observation bounds: %v through %v", r.FirstInjectionTime(), r.E2Deadline())
	}
	if _, ready := r.StopProgress(deadline.Add(-time.Nanosecond)); ready {
		t.Fatal("window completed before 960 seconds from dispatch")
	}
	if err := r.RecordInjection([]*core.Transaction{tx}, 0, deadline); err == nil {
		t.Fatal("accepted dispatch at the cutoff")
	}
	if err := r.RecordBlock(testBlock(deadline, []*core.Transaction{tx}, nil, nil), deadline); err == nil {
		t.Fatal("accepted receipt at the cutoff")
	}
	if p, ready := r.StopProgress(deadline); !ready || p.Complete() || p.Injected != 1 || p.Completed != 0 {
		t.Fatalf("unexpected deadline progress: %+v ready=%v", p, ready)
	}
	if err := r.Close(deadline.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	m := readMetadata(t, dir)
	if m.Status != "closed" || m.CompletionScope != "e2" || m.E2StopPolicy != "fixed_window" ||
		m.ObservedDurationNS != int64(960*time.Second) || m.E2PrefixCount != 0 || m.E2PrefixSHA256 != "" ||
		m.Completion.Injected != 1 || m.Completion.Completed != 0 || m.Completion.Expected != 2 {
		t.Fatalf("incorrect fixed-window metadata: %+v, progress %+v", m, m.Completion)
	}
}

func TestE2TracksFixedHashesThroughFinalReceiptAndCoverage(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprintf("prefixFinishesAfterWindow_%v", late), func(t *testing.T) {
			config := testConfig()
			config.ExpectedTransactions = 4
			config.E2ObservationSeconds, config.E2PrefixCount = 10, 2
			config.E2PrefixFile = filepath.Join(t.TempDir(), "prefix.csv")
			a, b, other := testTx("a", 0, "LOCAL", 20), testTx("b", 0, "RELAY1", 20), testTx("not-in-prefix", 0, "LOCAL", 20)
			prefix := "source_tx_hash\n" + a.SourceTxHash + "\n" + b.SourceTxHash + "\n"
			if err := os.WriteFile(config.E2PrefixFile, []byte(prefix), 0600); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(t.TempDir(), "metrics")
			r, err := New(dir, config)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			if _, reached := r.StopProgress(start.Add(time.Hour)); reached {
				t.Fatal("observation started without dispatch")
			}
			// Input prefix order differs from the per-shard dispatch order.
			if err = r.RecordInjection([]*core.Transaction{other, b}, 1, start); err != nil {
				t.Fatal(err)
			}
			if err = r.RecordInjection([]*core.Transaction{a}, 0, start.Add(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if err = r.RecordInjection([]*core.Transaction{a}, 0, start.Add(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			at := start.Add(time.Second)
			bb := testBlock(at, []*core.Transaction{a}, []*core.Transaction{b}, nil)
			if err = r.RecordBlock(bb, at); err != nil {
				t.Fatal(err)
			}
			if err = r.RecordBlock(bb, at); err != nil {
				t.Fatal(err)
			}
			p, reached := r.StopProgress(start.Add(10 * time.Second))
			if reached || p.Injected != 3 || p.E2PrefixInjected != 2 || p.Completed != 1 || p.E2PrefixCompleted != 1 {
				t.Fatalf("wrong progress: %+v %v", p, reached)
			}
			second := start.Add(2 * time.Second)
			if late {
				second = start.Add(11 * time.Second)
			}
			finalB := testTx("b", 0, "RELAY2", 20)
			if err = r.RecordBlock(testBlock(second, nil, nil, []*core.Transaction{finalB}), second); err != nil {
				t.Fatal(err)
			}
			if !late {
				if _, ready := r.StopProgress(start.Add(10*time.Second - time.Nanosecond)); ready {
					t.Fatal("early close before wall window")
				}
			}
			closeAt := start.Add(12 * time.Second)
			p, reached = r.StopProgress(closeAt)
			if !reached || p.Complete() || p.E2PrefixCompleted != 2 {
				t.Fatalf("both goals incomplete: %+v", p)
			}
			if err = r.Close(closeAt); err != nil {
				t.Fatal(err)
			}
			m := readMetadata(t, dir)
			if m.Status != "closed" || m.CompletionScope != "e2" || m.E2PrefixSHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte(prefix))) || m.E2ObservationSeconds != 10 || m.E2PrefixCount != 2 || m.ObservedDurationNS != int64(12*time.Second) || m.Completion.Complete() {
				t.Fatalf("E2 scope or coverage differs: %+v", m)
			}
		})
	}
}

func TestE2RejectsBadInputPrefixes(t *testing.T) {
	a, b := testTx("a", 0, "LOCAL", 20).SourceTxHash, testTx("b", 0, "LOCAL", 20).SourceTxHash
	for name, data := range map[string]string{"empty": "", "header": "hash\n" + a + "\n" + b + "\n", "truncated": "source_tx_hash\n" + a + "\n", "duplicate": "source_tx_hash\n" + a + "\n" + a + "\n", "extra": "source_tx_hash\n" + a + "\n" + b + "\n" + a + "\n", "invalid": "source_tx_hash\ninvalid\n" + b + "\n"} {
		t.Run(name, func(t *testing.T) {
			config := testConfig()
			config.E2ObservationSeconds, config.E2PrefixCount, config.E2PrefixFile = 10, 2, filepath.Join(t.TempDir(), "prefix.csv")
			if err := os.WriteFile(config.E2PrefixFile, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(t.TempDir(), "metrics")
			if _, err := New(dir, config); err == nil {
				t.Fatal("accepted invalid prefix")
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatal("created partial run before validating prefix")
			}
		})
	}
}

func TestE2CloseBeforeGoalKeepsRunUnverified(t *testing.T) {
	cfg := testConfig()
	missing := testTx("missing", 0, "LOCAL", 20)
	cfg.E2ObservationSeconds, cfg.E2PrefixCount, cfg.E2PrefixFile = 5, 1, filepath.Join(t.TempDir(), "prefix.csv")
	if err := os.WriteFile(cfg.E2PrefixFile, []byte("source_tx_hash\n"+missing.SourceTxHash+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "metrics")
	r, err := New(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err = r.RecordInjection([]*core.Transaction{missing}, 0, start); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(start.Add(time.Minute)); err == nil {
		t.Fatal("allowed E2 close with unfinished prefix")
	}
	meta := readMetadata(t, dir)
	if meta.Status == "closed" || meta.CompletionFailure == "" || meta.Completion.E2PrefixCompleted != 0 {
		t.Fatalf("missing-target run marked successful: %+v", meta)
	}
}
