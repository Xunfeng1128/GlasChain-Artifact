package committee

import (
	"log"
)

// runPartition keeps input, timer, map installation and migration lifecycle in
// CLPA. Optional planners supply only the new map and prepare/snapshot handshake.
// No graph lock is held while an optional planner waits for network observations.
func (ccm *CLPACommitteeModule) runPartition(round uint64) {
	if ccm.partitionPlanner == nil {
		// Preserve the legacy graph-lock boundary, including map send and graph reset.
		ccm.clpaLock.Lock()
		defer ccm.clpaLock.Unlock()
		ccm.partitionPrepareSend(round)
		mapping, _ := ccm.clpaGraph.CLPA_Partition()
		ccm.clpaMapSend(mapping)
		for address, shard := range mapping {
			ccm.modifiedMap[address] = shard
		}
		ccm.clpaReset()
		return
	}
	mapping, err := ccm.partitionPlanner(round)
	if err != nil {
		log.Panicf("partition planner round %d: %v", round, err)
	}

	ccm.clpaLock.Lock()
	defer ccm.clpaLock.Unlock()
	ccm.clpaMapSend(mapping)
	for address, shard := range mapping {
		ccm.modifiedMap[address] = shard
	}
	if !ccm.skipHistoryGraph {
		ccm.clpaReset()
	}
}
