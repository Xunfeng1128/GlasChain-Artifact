package committee

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"blockEmulator/params"
	"blockEmulator/utils"
)

func TestFormalCheckpointCapturesInputPositionAndRouting(t *testing.T) {
	oldMethod, oldShards := params.ConsensusMethod, params.ShardNum
	params.ConsensusMethod, params.ShardNum = 1, 4
	t.Cleanup(func() { params.ConsensusMethod, params.ShardNum = oldMethod, oldShards })
	overridden := fmt.Sprintf("%040x", 1)
	fallback := fmt.Sprintf("%040x", 6)
	m := &CLPACommitteeModule{modifiedMap: map[string]uint64{overridden: 3}}
	var checkpoint *formalMapSnapshot
	for _, batchEnd := range []int{3, 6, 9} {
		m.nowDataNum = batchEnd
		got := m.formalMapForBatch(5, 3, 2)
		if (got != nil) != (batchEnd == 6) {
			t.Fatalf("batch ending at %d captured position 5: %v", batchEnd, got)
		}
		if got != nil {
			checkpoint = got
		}
	}
	if checkpoint.SourcePosition != 5 || checkpoint.Shards != 4 || checkpoint.Method != 1 || checkpoint.Epoch != 2 || checkpoint.GARET != nil {
		t.Fatalf("wrong checkpoint: %+v", checkpoint)
	}
	if checkpoint.Overrides[overridden] != m.fetchModifiedMap(overridden) || m.fetchModifiedMap(fallback) != uint64(utils.Addr2Shard(fallback)) {
		t.Fatal("checkpoint differs from the mapping used to dispatch the batch")
	}
	if _, exists := checkpoint.Overrides[fallback]; exists {
		t.Fatal("unexpected explicit override for a hash-routed address")
	}
	m.modifiedMap[overridden] = 0
	if checkpoint.Overrides[overridden] != 3 {
		t.Fatal("checkpoint changed with a subsequent mapping")
	}
	m.modifiedMap = nil
	m.nowDataNum = 6
	if m.formalMapForBatch(3, 3, 2) != nil || m.formalMapForBatch(6, 3, 2).Overrides == nil {
		t.Fatal("batch endpoint or empty explicit override map lost")
	}
}

func TestFormalCheckpointGARETRouting(t *testing.T) {
	oldMethod := params.ConsensusMethod
	params.ConsensusMethod = 10
	t.Cleanup(func() { params.ConsensusMethod = oldMethod })
	m := garetSharedFixture(t)
	m.algorithm.placement = []uint64{1, 0}
	address := fmt.Sprintf("%040x", 2)
	m.modifiedMap[address] = 0 // The GARET group overrides this address route.
	m.nowDataNum = 5
	s := m.formalMapForBatch(4, 3, 7)
	group, err := m.algorithm.catalog.GroupFor(address)
	if err != nil {
		t.Fatal(err)
	}
	if s.GARET == nil || s.Overrides != nil || s.GARET.Epoch != 7 || s.GARET.CatalogDigest != m.algorithm.catalog.Digest() ||
		s.GARET.Placement[group] != m.fetchModifiedMap(address) || s.GARET.Placement[group] != 1 {
		t.Fatalf("checkpoint differs from live GARET group route: %+v", s)
	}
	m.algorithm.placement[group] = 0
	if s.GARET.Placement[group] != 1 {
		t.Fatal("archived group routing changed")
	}
}

func TestFormalCheckpointArchive(t *testing.T) {
	old := params.DataWrite_path
	params.DataWrite_path = filepath.Join(t.TempDir(), "result")
	t.Cleanup(func() { params.DataWrite_path = old })
	var none *formalMapSnapshot
	if none.save() == nil {
		t.Fatal("accepted unreached input checkpoint")
	}
	s := formalMapSnapshot{SourcePosition: 4, Epoch: 1, Method: 1, Shards: 4, Overrides: map[string]uint64{}}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	s.SourcePosition = 6
	if err := s.save(); !os.IsExist(err) {
		t.Fatalf("overwriting existing evidence: %v", err)
	}
	paths, err := os.ReadDir(params.DataWrite_path)
	if err != nil || len(paths) != 1 || paths[0].Name() != "e1_checkpoint.json" {
		t.Fatalf("unexpected checkpoint files: %v %v", paths, err)
	}
	b, err := os.ReadFile(filepath.Join(params.DataWrite_path, paths[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var saved formalMapSnapshot
	if err := json.Unmarshal(b, &saved); err != nil || saved.SourcePosition != 4 || saved.Overrides == nil {
		t.Fatalf("archived JSON lost original data: %s (%v)", b, err)
	}
}
