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

func TestPLouvainContribModePreservesExistingIDs(t *testing.T) {
	previous := []string{"CLPA_Broker", "CLPA", "Broker", "Relay", "CLPA_Gas", "", "", "P_Louvain_Gas", "TxAllo_Contrib"}
	if len(CommitteeMethod) < 10 || !reflect.DeepEqual(CommitteeMethod[:9], previous) || CommitteeMethod[9] != "P_Louvain_Contrib" {
		t.Fatalf("algorithm IDs changed: %v", CommitteeMethod)
	}
}

func TestPLouvainContribConfigLoad(t *testing.T) {
	if os.Getenv("PLOUVAIN_CONTRIB_CONFIG_TEST_CHILD") == "1" {
		ReadConfigFile()
		fmt.Printf("count=%g/%d legacyBeta=%g gas=%g/%d txallo=%g/%d\n", PLouvainContribBeta, PLouvainContribMaxIterations, Beta, PLouvainGasBeta, PLouvainGasMaxIterations, TxAlloBeta, TxAlloMaxIterations)
		return
	}
	for _, tc := range []struct {
		name    string
		fields  map[string]interface{}
		want    string
		invalid bool
	}{
		{name: "independent_defaults", fields: map[string]interface{}{"Beta": 0.25, "PLouvainGasBeta": 3.5, "PLouvainGasMaxIterations": 12, "TxAlloBeta": 4, "TxAlloMaxIterations": 10}, want: "count=2/70 legacyBeta=0.25 gas=3.5/12 txallo=4/10"},
		{name: "explicit_zero", fields: map[string]interface{}{"PLouvainContribBeta": 0, "PLouvainContribMaxIterations": 0}, want: "count=0/0"},
		{name: "override", fields: map[string]interface{}{"PLouvainContribBeta": 2.5, "PLouvainContribMaxIterations": 10}, want: "count=2.5/10"},
		{name: "negative_beta", fields: map[string]interface{}{"PLouvainContribBeta": -1}, want: "PLouvainContribBeta", invalid: true},
		{name: "float32_overflow", fields: map[string]interface{}{"PLouvainContribBeta": 1e40}, want: "PLouvainContribBeta", invalid: true},
		{name: "negative_iterations", fields: map[string]interface{}{"PLouvainContribMaxIterations": -1}, want: "PLouvainContribMaxIterations", invalid: true},
		{name: "tx_capacity", fields: map[string]interface{}{"BlockCapacityMode": "tx"}, want: "requires BlockCapacityMode=gas", invalid: true},
		{name: "zero_rate", fields: map[string]interface{}{"InjectSpeed": 0}, want: "must be positive", invalid: true},
		{name: "zero_batch", fields: map[string]interface{}{"TxBatchSize": 0}, want: "must be positive", invalid: true},
		{name: "zero_interval", fields: map[string]interface{}{"ReconfigTimeGap": 0}, want: "ReconfigTimeGap", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]interface{}{"ConsensusMethod": 9, "BlockCapacityMode": "gas", "BlockGasLimit": 30000000,
				"DatasetFile": "not-read-by-this-test.csv", "ReconfigTimeGap": 48, "InjectSpeed": 2000, "TxBatchSize": 2000, "TotalDataSize": 5000000}
			for key, value := range tc.fields {
				cfg[key] = value
			}
			data, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestPLouvainContribConfigLoad$")
			cmd.Env = append(os.Environ(), "PLOUVAIN_CONTRIB_CONFIG_TEST_CHILD=1", "BLOCKEMULATOR_CONFIG="+path)
			output, err := cmd.CombinedOutput()
			if (err != nil) != tc.invalid || !strings.Contains(string(output), tc.want) {
				t.Fatalf("error=%v want invalid=%v output=%s", err, tc.invalid, output)
			}
		})
	}
}
