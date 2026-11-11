package chain

import (
	"fmt"

	"blockEmulator/core"
	"blockEmulator/partition/garet"
	"blockEmulator/utils"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

// InitGARETRouting installs only the compact catalog and group assignment. It
// creates no account state and must run before admission on a fresh chain.
func (bc *BlockChain) InitGARETRouting(catalog *garet.Catalog) error {
	bc.pmlock.Lock()
	defer bc.pmlock.Unlock()
	if catalog == nil || catalog.GroupCount() == 0 || bc.ChainConfig == nil || bc.ChainConfig.ShardNums == 0 {
		return fmt.Errorf("invalid GARET routing initialization")
	}
	if bc.garetCatalog != nil || len(bc.PartitionMap) != 0 || (bc.CurrentBlock != nil && bc.CurrentBlock.Header.Number != 0) {
		return fmt.Errorf("GARET routing requires a fresh chain without account overrides")
	}
	bc.garetCatalog = catalog
	bc.garetGroups = catalog.InitialPlacement(int(bc.ChainConfig.ShardNums))
	bc.garetEpoch = 0
	return nil
}

func (bc *BlockChain) GARETEnabled() bool {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	return bc.garetCatalog != nil
}

func (bc *BlockChain) GARETEpoch() uint64 {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	return bc.garetEpoch
}

func (bc *BlockChain) validateGARETPlacementLocked(digest string, epoch uint64, groups []uint64, delta map[string]uint64) error {
	if bc.garetCatalog == nil {
		return fmt.Errorf("GARET metadata on a non-GARET chain")
	}
	if digest != bc.garetCatalog.Digest() {
		return fmt.Errorf("GARET catalog digest mismatch")
	}
	if epoch != bc.garetEpoch+1 {
		return fmt.Errorf("GARET epoch %d must follow committed epoch %d", epoch, bc.garetEpoch)
	}
	if len(groups) != bc.garetCatalog.GroupCount() {
		return fmt.Errorf("GARET group count mismatch")
	}
	for _, shard := range groups {
		if shard >= bc.ChainConfig.ShardNums {
			return fmt.Errorf("GARET shard %d outside shard count %d", shard, bc.ChainConfig.ShardNums)
		}
	}
	for address, shard := range delta {
		group, err := bc.garetCatalog.GroupFor(address)
		if err != nil {
			return err
		}
		if groups[group] != shard {
			return fmt.Errorf("GARET account delta disagrees with group assignment for %s", address)
		}
	}
	return nil
}

func (bc *BlockChain) ValidateGARETPlacement(digest string, epoch uint64, groups []uint64, delta map[string]uint64) error {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	return bc.validateGARETPlacementLocked(digest, epoch, groups, delta)
}

// InstallGARETPlacement changes the entire resolver under one map lock. Address
// deltas identify migration candidates only; retaining them as overrides would
// pin previously seen addresses to stale groups on later group-only updates.
// The worker also holds its execution lock across this call and pool repricing.
func (bc *BlockChain) InstallGARETPlacement(digest string, epoch uint64, groups []uint64, delta map[string]uint64) error {
	bc.pmlock.Lock()
	defer bc.pmlock.Unlock()
	if err := bc.validateGARETPlacementLocked(digest, epoch, groups, delta); err != nil {
		return err
	}
	bc.garetGroups = append([]uint64(nil), groups...)
	bc.garetEpoch = epoch
	return nil
}

// GARETTargetShard resolves a proposed group assignment without installing it.
func (bc *BlockChain) GARETTargetShard(address string, groups []uint64) (uint64, error) {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	if bc.garetCatalog == nil || len(groups) != bc.garetCatalog.GroupCount() {
		return 0, fmt.Errorf("invalid GARET target assignment")
	}
	group, err := bc.garetCatalog.GroupFor(address)
	if err != nil {
		return 0, err
	}
	if groups[group] >= bc.ChainConfig.ShardNums {
		return 0, fmt.Errorf("invalid GARET target shard")
	}
	return groups[group], nil
}

func resolveExecutionShard(catalog *garet.Catalog, groups []uint64, explicit map[string]uint64, address string) uint64 {
	if catalog != nil {
		group, err := catalog.GroupFor(address)
		if err != nil {
			panic(fmt.Errorf("invalid GARET account: %w", err))
		}
		return groups[group]
	}
	if shard, ok := explicit[address]; ok {
		return shard
	}
	return uint64(utils.Addr2Shard(address))
}

// ExecutionShardResolver snapshots routing before taking a pool lock. Catalogs
// are immutable after construction; group assignments and legacy maps are copied.
func (bc *BlockChain) ExecutionShardResolver() func(string) uint64 {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	catalog := bc.garetCatalog
	groups := append([]uint64(nil), bc.garetGroups...)
	mapping := make(map[string]uint64, len(bc.PartitionMap))
	for address, shard := range bc.PartitionMap {
		mapping[address] = shard
	}
	return func(address string) uint64 { return resolveExecutionShard(catalog, groups, mapping, address) }
}

// FetchExistingAccounts never materializes untouched catalog members. Pending
// stages are migrated independently of this list by the GARET worker helper.
func (bc *BlockChain) FetchExistingAccounts(addresses []string) ([]string, []*core.AccountState, error) {
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		return nil, nil, err
	}
	found := make([]string, 0, len(addresses))
	states := make([]*core.AccountState, 0, len(addresses))
	for _, address := range addresses {
		encoded, err := st.Get([]byte(address))
		if err != nil {
			return nil, nil, err
		}
		if len(encoded) == 0 {
			continue
		}
		found = append(found, address)
		states = append(states, core.DecodeAS(encoded))
	}
	return found, states, nil
}

// validateGARETAddressesLocked rejects malformed wire addresses before invoking the
// infallible resolver used by shared gas arithmetic. Legacy modes are unchanged.
func (bc *BlockChain) validateGARETAddressesLocked(txs []*core.Transaction) error {
	if bc.garetCatalog == nil {
		return nil
	}
	for _, tx := range txs {
		if tx == nil {
			return fmt.Errorf("nil GARET execution record")
		}
		addresses := []string{tx.Sender, tx.Recipient}
		for _, call := range tx.GLASCallRelations {
			addresses = append(addresses, call.Caller, call.Callee)
		}
		for _, address := range addresses {
			if _, err := bc.garetCatalog.GroupFor(address); err != nil {
				return err
			}
		}
	}
	return nil
}
