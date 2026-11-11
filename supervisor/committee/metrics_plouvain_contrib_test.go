package committee

import (
	plouvaincontrib "blockEmulator/partition/plouvain_contrib"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
)

func TestPLouvainContribMetricsKeepCountUnits(t *testing.T) {
	g := plouvaincontrib.NewGraph()
	for _, edge := range [][2]string{{"a", "b"}, {"a", "b"}, {"b", "c"}} {
		if err := g.AddTransaction(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	result, err := plouvaincontrib.Optimize(g, map[string]uint64{"a": 0, "b": 0, "c": 1}, plouvaincontrib.DefaultConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	w, err := newPLouvainContribMetricsWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(2, 2, result); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for name, wantRows := range map[string]int{"PLouvainContrib_Round.csv": 2, "PLouvainContrib_Shard.csv": 5} {
		file, err := os.Open(filepath.Join(root, "supervisor_measureOutput", name))
		if err != nil {
			t.Fatal(err)
		}
		rows, err := csv.NewReader(file).ReadAll()
		file.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != wantRows {
			t.Fatalf("%s: got %d rows", name, len(rows))
		}
		if name == "PLouvainContrib_Round.csv" && (rows[1][1] != "3" || rows[1][2] != "2" || rows[1][7] != "0.333333333") {
			t.Fatalf("incorrect count statistics: %v", rows[1])
		}
	}
	if _, err := os.Stat(filepath.Join(root, "supervisor_measureOutput", "PLouvainGas_Round.csv")); !os.IsNotExist(err) {
		t.Fatal("PLouvainContrib diagnostics must not overwrite P-Louvain diagnostics")
	}
}
