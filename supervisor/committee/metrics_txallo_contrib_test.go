package committee

import (
	"blockEmulator/partition/txallo"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
)

func TestTxAlloMetricsKeepCountUnits(t *testing.T) {
	g := txallo.NewGraph()
	for _, edge := range [][2]string{{"a", "b"}, {"a", "b"}, {"b", "c"}} {
		if err := g.AddTransaction(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	result, err := txallo.Optimize(g, map[string]uint64{"a": 0, "b": 0, "c": 1}, txallo.DefaultConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	w, err := newTxAlloContribMetricsWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(2, 2, result); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for name, wantRows := range map[string]int{"TxAlloContrib_Round.csv": 2, "TxAlloContrib_Shard.csv": 5} {
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
		if name == "TxAlloContrib_Round.csv" && (rows[1][1] != "3" || rows[1][2] != "2" || rows[1][7] != "0.333333333") {
			t.Fatalf("incorrect count statistics: %v", rows[1])
		}
	}
	if _, err := os.Stat(filepath.Join(root, "supervisor_measureOutput", "PLouvainGas_Round.csv")); !os.IsNotExist(err) {
		t.Fatal("TxAllo diagnostics must not overwrite P-Louvain diagnostics")
	}
}
