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

func TestUnifiedConfigLoad(t *testing.T) {
	if os.Getenv("UNIFIED_CONFIG_TEST_CHILD") == "1" {
		ReadConfigFile()
		fmt.Printf("unified=%v metrics=%v charge=%d\n", UnifiedExecution, UnifiedMetrics, UnifiedCrossCallGas)
		return
	}
	tests := []struct {
		name    string
		mode    int
		fields  map[string]interface{}
		want    string
		invalid bool
	}{
		{"legacy", 3, map[string]interface{}{"UnifiedExecution": false}, "unified=false", false},
		{"metrics_opt_in", 3, map[string]interface{}{"UnifiedMetrics": true}, "unified=true metrics=true", false},
		{"e1_mid_batch", 1, map[string]interface{}{"UnifiedMetrics": true, "FormalMapCheckpoint": 5}, "unified=true metrics=true", false},
		{"e1_reject_nonshared_router", 9, map[string]interface{}{"UnifiedMetrics": true, "FormalMapCheckpoint": 5}, "FormalMapCheckpoint requires", true},
		{"e1_reject_end", 1, map[string]interface{}{"UnifiedMetrics": true, "FormalMapCheckpoint": 10}, "FormalMapCheckpoint requires", true},
		{"e1_reject_negative", 1, map[string]interface{}{"UnifiedMetrics": true, "FormalMapCheckpoint": -1}, "FormalMapCheckpoint requires", true},
		{"e1_reject_early_stop", 1, map[string]interface{}{"UnifiedMetrics": true, "FormalMapCheckpoint": 5, "E2ObservationSeconds": 960, "E2PrefixCount": 9, "E2PrefixFile": "prefix.csv"}, "FormalMapCheckpoint requires", true},
		{"metrics_require_shared_execution", 3, map[string]interface{}{"UnifiedExecution": false, "UnifiedMetrics": true}, "UnifiedMetrics requires UnifiedExecution=true", true},
		{"e2_hash", 3, map[string]interface{}{"UnifiedMetrics": true, "E2ObservationSeconds": 960, "E2PrefixCount": 9, "E2PrefixFile": "prefix.csv"}, "unified=true metrics=true", false},
		{"e2_needs_metrics", 3, map[string]interface{}{"E2ObservationSeconds": 960, "E2PrefixCount": 9, "E2PrefixFile": "prefix.csv"}, "E2 observation requires UnifiedMetrics", true},
		{"e2_missing_list", 3, map[string]interface{}{"UnifiedMetrics": true, "E2ObservationSeconds": 960, "E2PrefixCount": 9}, "E2 observation requires positive", true},
		{"e2_prefix_exceeds_cap", 3, map[string]interface{}{"UnifiedMetrics": true, "E2ObservationSeconds": 960, "E2PrefixCount": 11, "E2PrefixFile": "prefix.csv"}, "E2 observation requires positive", true},
		{"default_charge", 3, nil, "unified=true metrics=false charge=5000", false},
		{"zero_charge", 3, map[string]interface{}{"UnifiedCrossCallGas": 0}, "unified=true metrics=false charge=0", false},
		{"missing_tables", 1, map[string]interface{}{"UnifiedCallsFile": ""}, "UnifiedTransactionsFile and UnifiedCallsFile", true},
		{"unsupported_broker", 2, nil, "supports only", true},
		{"wrong_capacity", 3, map[string]interface{}{"BlockCapacityMode": "tx"}, "requires BlockCapacityMode=gas", true},
		{"missing_limit", 3, map[string]interface{}{"BlockGasLimit": 0}, "positive", true},
		{"zero_total", 3, map[string]interface{}{"TotalDataSize": 0}, "positive", true},
	}
	for _, mode := range []int{1, 4, 7, 8, 9} {
		tests = append(tests, struct {
			name    string
			mode    int
			fields  map[string]interface{}
			want    string
			invalid bool
		}{fmt.Sprintf("mode_%d", mode), mode, nil, "unified=true", false})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]interface{}{"ConsensusMethod": tc.mode, "UnifiedExecution": true, "UnifiedTransactionsFile": "canonical.csv", "UnifiedCallsFile": "calls.csv", "BlockCapacityMode": "gas", "BlockGasLimit": 100, "InjectSpeed": 2, "TxBatchSize": 2, "TotalDataSize": 10, "ReconfigTimeGap": 48, "GLASExecutionMode": "map_only"}
			for k, v := range tc.fields {
				cfg[k] = v
			}
			b, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestUnifiedConfigLoad$")
			cmd.Env = append(os.Environ(), "UNIFIED_CONFIG_TEST_CHILD=1", "BLOCKEMULATOR_CONFIG="+path)
			out, err := cmd.CombinedOutput()
			if (err != nil) != tc.invalid || !strings.Contains(string(out), tc.want) {
				t.Fatalf("error=%v invalid=%v output=%s", err, tc.invalid, out)
			}
		})
	}
}
