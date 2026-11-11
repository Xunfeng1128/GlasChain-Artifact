package pbft_all

import (
	"fmt"
	"sort"

	"blockEmulator/core"
	"blockEmulator/message"
)

// orderedMigrationContents preserves each source queue's order, independently
// of arrival time and Go map iteration. The caller appends this sequence after
// locally retained records; no global transaction-arrival sort is applied.
func orderedMigrationContents(batches map[uint64]*message.AccountStateAndTx) (map[string]*core.AccountState, []*core.Transaction, error) {
	sources := make([]uint64, 0, len(batches))
	for source := range batches {
		sources = append(sources, source)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
	states := make(map[string]*core.AccountState)
	transactions := make([]*core.Transaction, 0)
	for _, source := range sources {
		batch := batches[source]
		if batch == nil || batch.FromShard != source || len(batch.Addrs) != len(batch.AccountState) {
			return nil, nil, fmt.Errorf("invalid migration batch from shard %d", source)
		}
		for i, address := range batch.Addrs {
			if batch.AccountState[i] == nil {
				return nil, nil, fmt.Errorf("nil migrated account state %q", address)
			}
			if _, duplicate := states[address]; duplicate {
				return nil, nil, fmt.Errorf("multiple sources supplied migrated account %q", address)
			}
			states[address] = batch.AccountState[i]
		}
		for _, tx := range batch.Txs {
			if tx == nil {
				return nil, nil, fmt.Errorf("nil migrated transaction from shard %d", source)
			}
			transactions = append(transactions, tx)
		}
	}
	return states, transactions, nil
}
