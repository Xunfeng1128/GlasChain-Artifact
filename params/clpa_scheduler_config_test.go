package params

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLPASchedulerConfigLoad(t *testing.T) {
	if os.Getenv("CLPA_SCHEDULER_CONFIG_TEST_CHILD") == "1" {
		ReadConfigFile()
		fmt.Printf("loadedScheduler=%s\n", CLPAReconfigurationScheduler)
		return
	}
	for _, tc := range []struct {
		name    string
		method  int
		value   interface{}
		include bool
		invalid bool
		want    string
	}{
		{"default_clpa", 1, nil, false, false, "loadedScheduler=event"},
		{"event_clpa", 1, "event", true, false, "loadedScheduler=event"},
		{"poll_clpa", 1, "poll", true, false, "loadedScheduler=poll"},
		{"poll_clpa_gas", 4, "poll", true, false, "loadedScheduler=poll"},
		{"unknown", 1, "legacy", true, true, "must be event or poll"},
		{"empty", 1, "", true, true, "must be event or poll"},
		{"case", 1, "Poll", true, true, "must be event or poll"},
		{"whitespace", 1, "poll ", true, true, "must be event or poll"},
		{"nonstring", 1, true, true, true, "cannot unmarshal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]interface{}{
				"ConsensusMethod": tc.method, "UnifiedExecution": true, "UnifiedMetrics": true,
				"UnifiedTransactionsFile": "canonical.csv", "UnifiedCallsFile": "calls.csv", "UnifiedCrossCallGas": 3000,
				"BlockCapacityMode": "gas", "BlockGasLimit": 30000000, "Block_Interval": 8000,
				"InjectSpeed": 1000, "TxBatchSize": 2000, "TotalDataSize": 200000, "ReconfigTimeGap": 48,
			}
			if tc.include {
				cfg["CLPAReconfigurationScheduler"] = tc.value
			}
			b, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestCLPASchedulerConfigLoad$")
			cmd.Env = append(os.Environ(), "CLPA_SCHEDULER_CONFIG_TEST_CHILD=1", "BLOCKEMULATOR_CONFIG="+path)
			out, err := cmd.CombinedOutput()
			if (err != nil) != tc.invalid || !strings.Contains(string(out), tc.want) {
				t.Fatalf("error=%v invalid=%v output=%s", err, tc.invalid, out)
			}
		})
	}
}

func TestSharedCLPAReconfigurationDispatchMatrix(t *testing.T) {
	for method := -1; method <= 14; method++ {
		for _, scheduler := range []string{"event", "poll", "", "legacy"} {
			legacyShared := method == 1 || method == 4 || method == 12
			optIn := method >= 7 && method <= 10
			want := (scheduler == "event" || scheduler == "poll") && (legacyShared || scheduler == "poll" && optIn)
			if got := UsesSharedCLPAReconfiguration(method, scheduler); got != want {
				t.Errorf("method=%d scheduler=%q got=%t want=%t", method, scheduler, got, want)
			}
		}
	}
}

func TestCLPASchedulerOmittedConfigResetsPriorLoad(t *testing.T) {
	beforeMode, beforeScheduler := ConsensusMethod, CLPAReconfigurationScheduler
	beforeProfile := CLPAReconfigurationProfile
	t.Cleanup(func() {
		ConsensusMethod, CLPAReconfigurationScheduler = beforeMode, beforeScheduler
		CLPAReconfigurationProfile = beforeProfile
	})
	ConsensusMethod = 1
	poll := CLPAReconfigurationSchedulerPoll
	if err := loadCLPASchedulerConfig(CLPASchedulerConfigFields{CLPAReconfigurationScheduler: &poll}); err != nil {
		t.Fatal(err)
	}
	if CLPAReconfigurationScheduler != "poll" {
		t.Fatal("explicit poll selection was lost")
	}
	if err := loadCLPASchedulerConfig(CLPASchedulerConfigFields{}); err != nil {
		t.Fatal(err)
	}
	if CLPAReconfigurationScheduler != "event" {
		t.Fatalf("omitted field retained previous scheduler %q", CLPAReconfigurationScheduler)
	}
	// A later load of an unrelated legacy mode also restores the existing
	// default and never selects the new poll implementation for that mode.
	CLPAReconfigurationScheduler = "poll"
	ConsensusMethod = 3
	if err := loadCLPASchedulerConfig(CLPASchedulerConfigFields{}); err != nil || CLPAReconfigurationScheduler != "event" {
		t.Fatalf("legacy default: scheduler=%q err=%v", CLPAReconfigurationScheduler, err)
	}
}
