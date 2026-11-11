package committee

import (
	"fmt"
	"math"

	"blockEmulator/message"
)

// Validate the scalar queue inventory carried beside each early local window.
func pendingSnapshotScopeTotals(snapshots map[uint64]message.PendingSnapshot) (records, calls int, err error) {
	for shard, snapshot := range snapshots {
		if !snapshot.LocalWindow || snapshot.TotalRecords < len(snapshot.Txs) || snapshot.TotalCallRelations < 0 ||
			snapshot.TotalRecords > math.MaxInt-records || snapshot.TotalCallRelations > math.MaxInt-calls {
			return 0, 0, fmt.Errorf("invalid or overflowing local-window inventory for shard %d", shard)
		}
		records += snapshot.TotalRecords
		calls += snapshot.TotalCallRelations
	}
	return records, calls, nil
}
