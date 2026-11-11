package committee

import (
	"blockEmulator/partition/plouvain"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
)

func TestPLouvainGasMetricsWriter(t *testing.T) {
	root := t.TempDir()
	w, err := newPLouvainGasMetricsWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	result := plouvain.Result{
		GraphTransactions:  3,
		GraphRelationships: 2,
		GraphGas:           123456,
		Communities: []plouvain.CommunityStats{{
			CommunityID: 1, AccountCount: 2, InternalGas: 100, ExternalGas: 20, AssignedShard: 0,
		}},
		Before: plouvain.Metrics{ShardLoads: []plouvain.ShardLoad{{ShardID: 0, PhysicalGas: 120, RequiredBlocks: 2}}},
		After:  plouvain.Metrics{ShardLoads: []plouvain.ShardLoad{{ShardID: 0, PhysicalGas: 100, RequiredBlocks: 1}}},
	}
	if err := w.Write(4, 2, 100, result); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	checks := map[string]int{
		"PLouvainGas_Round.csv":     2,
		"PLouvainGas_Shard.csv":     3,
		"PLouvainGas_Community.csv": 2,
	}
	for name, wantRows := range checks {
		file, err := os.Open(filepath.Join(root, "supervisor_measureOutput", name))
		if err != nil {
			t.Fatal(err)
		}
		rows, readErr := csv.NewReader(file).ReadAll()
		file.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(rows) != wantRows {
			t.Fatalf("%s has %d rows, want %d", name, len(rows), wantRows)
		}
	}
}
