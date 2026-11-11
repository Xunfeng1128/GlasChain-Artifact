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

func writeCLPAProfileTestConfig(t *testing.T, fields map[string]interface{}) string {
	t.Helper()
	cfg := map[string]interface{}{
		"ConsensusMethod": 1, "UnifiedExecution": true, "UnifiedMetrics": true,
		"UnifiedTransactionsFile": "canonical.csv", "UnifiedCallsFile": "calls.csv", "UnifiedCrossCallGas": 3000,
		"BlockCapacityMode": "gas", "BlockGasLimit": 30000000, "Block_Interval": 8000,
		"InjectSpeed": 1000, "TxBatchSize": 2000, "TotalDataSize": 200000, "ReconfigTimeGap": 48,
		"GLASExecutionMode":     "map_only",
		"GARETGroupCatalogFile": "catalog.json",
	}
	for key, value := range fields {
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
	return path
}

func runCLPAProfileConfigLoad(t *testing.T, path, reload string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCLPAProfileConfigLoad$")
	cmd.Env = append(os.Environ(), "CLPA_PROFILE_CONFIG_TEST_CHILD=1",
		"BLOCKEMULATOR_CONFIG="+path, "CLPA_PROFILE_CONFIG_TEST_RELOAD="+reload)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestCLPAProfileConfigLoad(t *testing.T) {
	if os.Getenv("CLPA_PROFILE_CONFIG_TEST_CHILD") == "1" {
		ReadConfigFile()
		if reload := os.Getenv("CLPA_PROFILE_CONFIG_TEST_RELOAD"); reload != "" {
			if err := os.Setenv("BLOCKEMULATOR_CONFIG", reload); err != nil {
				t.Fatal(err)
			}
			ReadConfigFile()
		}
		fmt.Printf("loadedScheduler=%s loadedProfile=%s\n", CLPAReconfigurationScheduler, CLPAReconfigurationProfile)
		return
	}
	for _, method := range []int{1, 4, 7, 8, 9, 10} {
		for _, scheduler := range []string{"event", "poll"} {
			if method >= 7 && method <= 10 && scheduler == "event" {
				continue // This scheduler preserves the original constructor.
			}
			for _, profile := range []string{"fixed", "block_resume", "install_clock"} {
				t.Run(fmt.Sprintf("method_%d/%s/%s", method, scheduler, profile), func(t *testing.T) {
					path := writeCLPAProfileTestConfig(t, map[string]interface{}{
						"ConsensusMethod": method, "CLPAReconfigurationScheduler": scheduler,
						"CLPAReconfigurationProfile": profile,
					})
					out, err := runCLPAProfileConfigLoad(t, path, "")
					want := "loadedScheduler=" + scheduler + " loadedProfile=" + profile
					if err != nil || !strings.Contains(out, want) {
						t.Fatalf("error=%v output=%s", err, out)
					}
				})
			}
		}
	}
	for _, method := range []int{1, 4, 7, 8, 9, 10} {
		for _, field := range []string{"omitted", "null", "explicit"} {
			t.Run(fmt.Sprintf("default_method_%d/%s", method, field), func(t *testing.T) {
				cfg := map[string]interface{}{"ConsensusMethod": method}
				if field == "null" {
					cfg["CLPAReconfigurationProfile"] = nil
				} else if field == "explicit" {
					cfg["CLPAReconfigurationProfile"] = "install_clock"
				}
				out, err := runCLPAProfileConfigLoad(t, writeCLPAProfileTestConfig(t, cfg), "")
				if err != nil || !strings.Contains(out, "loadedScheduler=event loadedProfile=install_clock") {
					t.Fatalf("error=%v output=%s", err, out)
				}
			})
		}
	}
}

func TestCLPAProfileConfigReloadRestoresDefaults(t *testing.T) {
	for _, previous := range []string{"fixed", "block_resume"} {
		for _, method := range []int{1, 7, 8, 9, 10} {
			for _, null := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/method_%d/null_%t", previous, method, null), func(t *testing.T) {
					first := writeCLPAProfileTestConfig(t, map[string]interface{}{
						"CLPAReconfigurationScheduler": "poll", "CLPAReconfigurationProfile": previous,
					})
					fields := map[string]interface{}{"ConsensusMethod": method}
					if null {
						fields["CLPAReconfigurationScheduler"] = nil
						fields["CLPAReconfigurationProfile"] = nil
					}
					second := writeCLPAProfileTestConfig(t, fields)
					out, err := runCLPAProfileConfigLoad(t, first, second)
					if err != nil || !strings.Contains(out, "loadedScheduler=event loadedProfile=install_clock") {
						t.Fatalf("reload error=%v output=%s", err, out)
					}
				})
			}
		}
	}
}

func TestCLPAProfileConfigRejectsInvalidValuesEarly(t *testing.T) {
	tests := []struct {
		name    string
		method  int
		profile interface{}
		want    string
	}{
		{"unknown", 1, "legacy", "must be fixed, block_resume or install_clock"},
		{"empty", 1, "", "must be fixed, block_resume or install_clock"},
		{"case", 1, "Fixed", "must be fixed, block_resume or install_clock"},
		{"whitespace", 1, "install_clock ", "must be fixed, block_resume or install_clock"},
		{"boolean", 1, false, "cannot unmarshal"},
		{"integer", 1, 1, "cannot unmarshal"},
		{"float", 1, 1.5, "cannot unmarshal"},
		{"array", 1, []string{"fixed"}, "cannot unmarshal"},
		{"object", 1, map[string]string{"profile": "fixed"}, "cannot unmarshal"},
	}
	for _, method := range []int{0, 2, 3, 7, 8, 9, 10} {
		for _, profile := range []string{"fixed", "block_resume"} {
			tests = append(tests, struct {
				name    string
				method  int
				profile interface{}
				want    string
			}{fmt.Sprintf("unsupported_method_%d/%s", method, profile), method, profile,
				"requires an active shared CLPA reconfiguration mode"})
		}
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeCLPAProfileTestConfig(t, map[string]interface{}{
				"ConsensusMethod": tc.method, "CLPAReconfigurationProfile": tc.profile,
				"BlockCapacityMode": "invalid_after_scheduler",
			})
			out, err := runCLPAProfileConfigLoad(t, path, "")
			if err == nil || !strings.Contains(out, tc.want) || !strings.Contains(out, "CLPAReconfigurationProfile") {
				t.Fatalf("error=%v output=%s", err, out)
			}
			if strings.Contains(out, "Invalid BlockCapacityMode") || strings.Contains(out, "loadedScheduler=") {
				t.Fatalf("invalid profile reached later configuration handling: %s", out)
			}
		})
	}
}
