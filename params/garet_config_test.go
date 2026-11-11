package params

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestGARETModePreservesExistingIDs(t *testing.T) {
	want := []string{"CLPA_Broker", "CLPA", "Broker", "Relay", "CLPA_Gas", "", "", "P_Louvain_Gas", "TxAllo_Contrib", "P_Louvain_Contrib", "GARET"}
	if len(CommitteeMethod) < len(want) || !reflect.DeepEqual(CommitteeMethod[:len(want)], want) {
		t.Fatalf("mode IDs changed: %v", CommitteeMethod)
	}
}
func TestGARETConfigLoad(t *testing.T) {
	if os.Getenv("GARET_CONFIG_TEST_CHILD") == "1" {
		ReadConfigFile()
		fmt.Printf("garet=%d/%d/%s\n", GARETGroupCount, GARETSampleSeconds, GARETGroupCatalogFile)
		return
	}
	for _, tc := range []struct {
		name    string
		fields  map[string]interface{}
		want    string
		invalid bool
	}{
		{"defaults", nil, "garet=100/8/catalog.json", false},
		{"override", map[string]interface{}{"GARETGroupCount": 80, "GARETSampleSeconds": 4}, "garet=80/4/catalog.json", false},
		{"zero_groups", map[string]interface{}{"GARETGroupCount": 0}, "GARETGroupCount", true},
		{"zero_sample", map[string]interface{}{"GARETSampleSeconds": 0}, "positive multiple", true},
		{"unequal_slots", map[string]interface{}{"GARETSampleSeconds": 7}, "positive multiple", true},
		{"missing_catalog", map[string]interface{}{"GARETGroupCatalogFile": ""}, "GARETGroupCatalogFile", true},
		{"zero_timeout", map[string]interface{}{"GARETReconfigurationTimeoutSeconds": 0}, "GARETReconfigurationTimeoutSeconds", true},
		{"no_metrics", map[string]interface{}{"UnifiedMetrics": false}, "requires UnifiedExecution", true},
		{"no_common_execution", map[string]interface{}{"UnifiedExecution": false}, "requires UnifiedExecution", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]interface{}{"ConsensusMethod": 10, "UnifiedExecution": true, "UnifiedMetrics": true,
				"UnifiedTransactionsFile": "canonical.csv", "UnifiedCallsFile": "calls.csv", "BlockCapacityMode": "gas", "BlockGasLimit": 30000000,
				"InjectSpeed": 2000, "TxBatchSize": 2000, "TotalDataSize": 3000000, "ReconfigTimeGap": 48, "GARETGroupCatalogFile": "catalog.json"}
			for key, val := range tc.fields {
				cfg[key] = val
			}
			b, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestGARETConfigLoad$")
			cmd.Env = append(os.Environ(), "GARET_CONFIG_TEST_CHILD=1", "BLOCKEMULATOR_CONFIG="+path)
			out, err := cmd.CombinedOutput()
			if (err != nil) != tc.invalid || !strings.Contains(string(out), tc.want) {
				t.Fatalf("err=%v output=%s", err, out)
			}
		})
	}
}
