package committee

import (
	"blockEmulator/supervisor/unifiedmetrics"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReconfigurationObservationDoesNotPauseDraining(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "run")
	r, err := unifiedmetrics.New(directory, unifiedmetrics.RunConfig{
		Method: "CLPA", ConsensusMethod: 1, ShardCount: 4,
		ExpectedTransactions: 10, InjectSpeed: 10, BlockGasLimit: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	o := injectionObservation{}
	o.SetUnifiedRecorder(r)
	o.beginReconfiguration(1, true, "observed_new_epoch_block").finish("completed")
	o.beginReconfiguration(2, false, "observed_new_epoch_block").finish("completed")
	o.beginReconfiguration(3, true, "glas_start_barrier").finish("failed")
	if err := r.Close(time.Now()); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(directory, "reconfiguration.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"trigger", "injection_pause_begin", "finish", "injection_pause_end", "trigger", "finish", "trigger", "injection_pause_begin", "finish"}
	if len(rows) != len(want)+1 {
		t.Fatalf("wrong event count: %v", rows)
	}
	for index, event := range want {
		if rows[index+1][0] != event {
			t.Fatalf("wrong event sequence: %v", rows)
		}
	}
	if rows[5][3] != "draining" || rows[6][3] != "draining" {
		t.Fatalf("incorrect EOF scope: %v", rows)
	}
	if rows[9][8] != "failed" {
		t.Fatalf("failed attempt was reported completed: %v", rows[9])
	}
}

func TestReconfigurationObservationDisabled(t *testing.T) {
	o := injectionObservation{}
	o.beginReconfiguration(1, true, "observed_new_epoch_block").finish("completed")
	if o.reconfigurationID != 0 {
		t.Fatal("disabled logging changed observation state")
	}
}
