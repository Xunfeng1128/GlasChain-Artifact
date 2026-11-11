package committee

import (
	"blockEmulator/core"
	"blockEmulator/supervisor/unifiedmetrics"
	"encoding/csv"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newInjectionTestRecorder(t *testing.T) (*unifiedmetrics.Recorder, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "unified")
	r, err := unifiedmetrics.New(dir, unifiedmetrics.RunConfig{
		Method: "test", ConsensusMethod: 6, ShardCount: 2, ExpectedTransactions: 1,
		InjectSpeed: 2000, BlockGasLimit: 1000, CrossCallGas: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(time.Now()); err != nil {
			t.Error(err)
		}
	})
	return r, dir
}

func injectionTestTransaction() *core.Transaction {
	tx := core.NewTransaction("sender", "receiver", big.NewInt(0), 0, time.Now())
	tx.SourceTxHash, tx.GLASBaseGas, tx.GLASCallGasPerCall, tx.GasUsed = "0x"+strings.Repeat("a", 64), 100, 20, 100
	return tx
}

func injectionTestRows(t *testing.T, dir string) [][]string {
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

func TestComparisonCommitteesShareInjectionObserver(t *testing.T) {
	type observer interface {
		SetUnifiedRecorder(*unifiedmetrics.Recorder)
		observeInjection([]*core.Transaction, uint64)
	}
	modules := map[string]observer{
		"Hash": &RelayCommitteeModule{}, "CLPA and CLPA-Gas": &CLPACommitteeModule{},
		"P-Louvain-Gas": &PLouvainGasCommitteeModule{}, "P-Louvain-Contrib": &PLouvainContribCommitteeModule{},
		"TxAllo-Contrib": &TxAlloContribCommitteeModule{}, "GLAS": &GLASPendingCommitteeModule{CLPACommitteeModule: &CLPACommitteeModule{}},
	}
	for name, module := range modules {
		t.Run(name, func(t *testing.T) {
			// Even a legacy record is untouched while logging is disabled.
			module.observeInjection([]*core.Transaction{{}}, 0)
			r, dir := newInjectionTestRecorder(t)
			module.SetUnifiedRecorder(r)
			module.observeInjection(nil, 0)
			if len(injectionTestRows(t, dir)) != 1 {
				t.Fatal("empty shard sub-batch started observation")
			}
			tx := injectionTestTransaction()
			module.observeInjection([]*core.Transaction{tx}, 1)
			rows := injectionTestRows(t, dir)
			if len(rows) != 2 || rows[1][0] != "inject" || rows[1][1] != tx.SourceTxHash || rows[1][2] != "TOP" || rows[1][3] != "0" || rows[1][6] != "1" {
				t.Fatalf("wrong injection observation: %v", rows)
			}
		})
	}
}

func TestInjectionObserverFailsClosedOnMissingIdentity(t *testing.T) {
	r, dir := newInjectionTestRecorder(t)
	module := &RelayCommitteeModule{}
	module.SetUnifiedRecorder(r)
	defer func() {
		if recover() == nil {
			t.Error("invalid unidentifiable injection did not stop dispatch")
		}
		if len(injectionTestRows(t, dir)) != 1 {
			t.Error("invalid injection was partially recorded")
		}
	}()
	module.observeInjection([]*core.Transaction{{}}, 0)
}
