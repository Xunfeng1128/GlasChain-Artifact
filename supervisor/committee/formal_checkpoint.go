package committee

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// An opt-in copy of the routing map that priced a prescribed input position.
// Persist it after the run's dispatch/drain loop, outside the measured epochs.
type formalMapSnapshot struct {
	SourcePosition int                     `json:"source_position"`
	Epoch          uint64                  `json:"epoch"`
	Shards         int                     `json:"shards"`
	Method         int                     `json:"method"`
	Overrides      map[string]uint64       `json:"overrides"`
	GARET          *message.GARETPlacement `json:"garet,omitempty"`
}

func (ccm *CLPACommitteeModule) formalMapForBatch(position, batchSize int, epoch uint64) *formalMapSnapshot {
	// nowDataNum is the one-based last position of this dispatch. No new
	// mapping is installed between the first and last send within the batch.
	if position <= ccm.nowDataNum-batchSize || position > ccm.nowDataNum {
		return nil
	}
	m := &formalMapSnapshot{SourcePosition: position, Epoch: epoch, Method: params.ConsensusMethod, Shards: params.ShardNum}
	if ccm.partitionGARET != nil {
		// groupPayload returns its own copy. GARET routes every address by group.
		m.GARET = ccm.partitionGARET(epoch)
	} else {
		m.Overrides = make(map[string]uint64, len(ccm.modifiedMap))
		for address, shard := range ccm.modifiedMap {
			m.Overrides[address] = shard
		}
	}
	return m
}

func (m *formalMapSnapshot) save() error {
	if m == nil {
		return fmt.Errorf("dispatch never reached the configured checkpoint")
	}
	if err := os.MkdirAll(params.DataWrite_path, 0755); err != nil {
		return err
	}
	// Publish only complete JSON and never replace a previous run's evidence.
	file, err := os.CreateTemp(params.DataWrite_path, ".e1-checkpoint-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := json.NewEncoder(file).Encode(m); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Link(file.Name(), filepath.Join(params.DataWrite_path, "e1_checkpoint.json"))
}
