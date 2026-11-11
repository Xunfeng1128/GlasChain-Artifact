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

func TestTxAlloModePreservesExistingIDs(t *testing.T) {
	previous := []string{"CLPA_Broker", "CLPA", "Broker", "Relay", "CLPA_Gas", "", "", "P_Louvain_Gas"}
	if len(CommitteeMethod) < 9 || !reflect.DeepEqual(CommitteeMethod[:8], previous) || CommitteeMethod[8] != "TxAllo_Contrib" {
		t.Fatalf("algorithm IDs changed: %v", CommitteeMethod)
	}
}

func TestTxAlloConfigLoad(t *testing.T) {
	if os.Getenv("TXALLO_CONFIG_TEST_CHILD") == "1" {
		ReadConfigFile()
		fmt.Printf("txallo=%g/%d legacyBeta=%g plouvain=%g/%d\n", TxAlloBeta, TxAlloMaxIterations, Beta, PLouvainGasBeta, PLouvainGasMaxIterations)
		return
	}
	for _, tc := range []struct {
		name    string
		fields  map[string]interface{}
		want    string
		invalid bool
	}{
		{name: "independent_defaults", fields: map[string]interface{}{"Beta": 0.25, "PLouvainGasBeta": 3.5, "PLouvainGasMaxIterations": 12}, want: "txallo=2/70 legacyBeta=0.25 plouvain=3.5/12"},
		{name: "explicit_zero", fields: map[string]interface{}{"TxAlloBeta": 0, "TxAlloMaxIterations": 0}, want: "txallo=0/0"},
		{name: "override", fields: map[string]interface{}{"TxAlloBeta": 2.5, "TxAlloMaxIterations": 10}, want: "txallo=2.5/10"},
		{name: "negative_beta", fields: map[string]interface{}{"TxAlloBeta": -1}, want: "TxAlloBeta", invalid: true},
		{name: "float32_overflow", fields: map[string]interface{}{"TxAlloBeta": 1e40}, want: "TxAlloBeta", invalid: true},
		{name: "negative_iterations", fields: map[string]interface{}{"TxAlloMaxIterations": -1}, want: "TxAlloMaxIterations", invalid: true},
		{name: "tx_capacity", fields: map[string]interface{}{"BlockCapacityMode": "tx"}, want: "requires BlockCapacityMode=gas", invalid: true},
		{name: "zero_rate", fields: map[string]interface{}{"InjectSpeed": 0}, want: "must be positive", invalid: true},
		{name: "zero_batch", fields: map[string]interface{}{"TxBatchSize": 0}, want: "must be positive", invalid: true},
		{name: "zero_interval", fields: map[string]interface{}{"ReconfigTimeGap": 0}, want: "ReconfigTimeGap", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]interface{}{"ConsensusMethod": 8, "BlockCapacityMode": "gas", "BlockGasLimit": 30000000,
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
			cmd := exec.Command(os.Args[0], "-test.run=^TestTxAlloConfigLoad$")
			cmd.Env = append(os.Environ(), "TXALLO_CONFIG_TEST_CHILD=1", "BLOCKEMULATOR_CONFIG="+path)
			output, err := cmd.CombinedOutput()
			if (err != nil) != tc.invalid || !strings.Contains(string(output), tc.want) {
				t.Fatalf("error=%v want invalid=%v output=%s", err, tc.invalid, output)
			}
		})
	}
}
