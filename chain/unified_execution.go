package chain

import (
	"fmt"

	"blockEmulator/core"
	"blockEmulator/params"
)

// shardForLocked is used only while pmlock is held, avoiding recursive read
// locks when a writer is waiting. Callers never hold pmlock while locking a
// pool. The PBFT execution guard serializes map installation with admission,
// proposal construction, and commit.
func (bc *BlockChain) shardForLocked(address string) uint64 {
	return resolveExecutionShard(bc.garetCatalog, bc.garetGroups, bc.PartitionMap, address)
}

// PrepareUnifiedTransactions returns separately owned, map-priced records.
// Nothing in the incoming batch or pool changes on failure. Correctly priced
// records whose surcharge exceeds capacity remain admissible: they wait for
// a later mapping instead of receiving a discounted charge or overflowing a
// block. A base Gas demand exceeding capacity is always rejected.
func (bc *BlockChain) PrepareUnifiedTransactions(txs []*core.Transaction) ([]*core.Transaction, error) {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	if err := bc.validateGARETAddressesLocked(txs); err != nil {
		return nil, err
	}
	return prepareUnifiedTransactions(txs, bc.shardForLocked, bc.ChainConfig.BlockGasLimit)
}

func prepareUnifiedTransactions(txs []*core.Transaction, shardFor func(string) uint64, limit uint64) ([]*core.Transaction, error) {
	priced := make([]*core.Transaction, len(txs))
	for index, tx := range txs {
		if err := core.ValidateExecutionRecord(tx, params.UnifiedCrossCallGas); err != nil {
			return nil, err
		}
		copyTx := *tx
		copyTx.GLASCallRelations = append([]core.GLASCallRelation(nil), tx.GLASCallRelations...)
		if err := core.PriceExecutionStage(&copyTx, shardFor); err != nil {
			return nil, err
		}
		if err := core.ValidateExecutionAdmissionGas(&copyTx, shardFor, limit); err != nil {
			return nil, err
		}
		priced[index] = &copyTx
	}
	return priced, nil
}

// PrepareUnifiedTransactionsWithResolver prepares a migration forwarding batch
// against the caller's routing snapshot. Callers hold their execution guard and
// must not hold the pool lock. Each owned copy is priced once; declared prices
// from the received records are never trusted.
func (bc *BlockChain) PrepareUnifiedTransactionsWithResolver(txs []*core.Transaction, shardFor func(string) uint64) ([]*core.Transaction, error) {
	bc.pmlock.RLock()
	err := bc.validateGARETAddressesLocked(txs)
	bc.pmlock.RUnlock()
	if err != nil {
		return nil, err
	}
	priced := make([]*core.Transaction, len(txs))
	for index, tx := range txs {
		priced[index], err = prepareUnifiedMigrationTransaction(tx, shardFor, bc.ChainConfig.BlockGasLimit)
		if err != nil {
			return nil, err
		}
	}
	return priced, nil
}

// RepriceUnifiedPool replaces queued stage objects atomically after the caller
// has installed the full mapping. Committed/proposed objects remain unchanged.
// Snapshot the map before acquiring the pool lock to avoid inverse lock order
// with legacy migration paths. Callers must hold their PBFT execution guard.
func (bc *BlockChain) RepriceUnifiedPool() error {
	shardFor := bc.ExecutionShardResolver()
	bc.Txpool.GetLocked()
	defer bc.Txpool.GetUnlocked()
	priced, err := prepareUnifiedTransactions(bc.Txpool.TxQueue, shardFor, bc.ChainConfig.BlockGasLimit)
	if err != nil {
		return err
	}
	bc.Txpool.TxQueue = priced
	return nil
}

// InstallUnifiedMigrationWithResolver prepares retained and arriving records
// under one pool lock, using the same routing snapshot as migration ownership.
// The caller holds its PBFT execution guard. repriceExisting may be false only
// when the routing map and pricing configuration are unchanged; those trusted
// pool objects retain their identity. Incoming records are always copied and
// priced, and a failed preparation or admission leaves the pool unchanged.
func (bc *BlockChain) InstallUnifiedMigrationWithResolver(incoming []*core.Transaction, shardFor func(string) uint64, repriceExisting bool) error {
	// Catalogs are immutable. Capture this before the pool lock so address
	// validation in its callbacks never reverses the chain/pool lock order.
	bc.pmlock.RLock()
	catalog := bc.garetCatalog
	bc.pmlock.RUnlock()
	limit := bc.ChainConfig.BlockGasLimit
	prepareExisting := func(tx *core.Transaction) (*core.Transaction, error) {
		if repriceExisting {
			return prepareUnifiedMigrationTransaction(tx, shardFor, limit)
		}
		if err := core.ValidateExecutionRecord(tx, params.UnifiedCrossCallGas); err != nil {
			return nil, err
		}
		if err := core.ValidateExecutionAdmissionBaseGas(tx, limit); err != nil {
			return nil, err
		}
		return tx, nil
	}
	validatedIncomingAddresses := false
	prepareIncoming := func(tx *core.Transaction) (*core.Transaction, error) {
		// Preserve PrepareUnifiedTransactions' batch address-validation order:
		// after retained pricing, but before any incoming record is priced.
		if !validatedIncomingAddresses {
			if catalog != nil {
				for _, candidate := range incoming {
					if candidate == nil {
						return nil, fmt.Errorf("nil GARET execution record")
					}
					if _, err := catalog.GroupFor(candidate.Sender); err != nil {
						return nil, err
					}
					if _, err := catalog.GroupFor(candidate.Recipient); err != nil {
						return nil, err
					}
					for _, call := range candidate.GLASCallRelations {
						if _, err := catalog.GroupFor(call.Caller); err != nil {
							return nil, err
						}
						if _, err := catalog.GroupFor(call.Callee); err != nil {
							return nil, err
						}
					}
				}
			}
			validatedIncomingAddresses = true
		}
		return prepareUnifiedMigrationTransaction(tx, shardFor, limit)
	}
	_, err := bc.Txpool.InstallRepricedExecutionRecords(incoming, prepareExisting, prepareIncoming)
	return err
}

// A fresh internal price already passed the shared overflow-safe arithmetic.
// Check base admission afterward, preserving overflow-before-cap precedence
// without walking every call relation again to verify our own computed price.
func prepareUnifiedMigrationTransaction(tx *core.Transaction, shardFor func(string) uint64, limit uint64) (*core.Transaction, error) {
	if err := core.ValidateExecutionRecord(tx, params.UnifiedCrossCallGas); err != nil {
		return nil, err
	}
	copyTx := *tx
	copyTx.GLASCallRelations = append([]core.GLASCallRelation(nil), tx.GLASCallRelations...)
	if err := core.PriceExecutionStage(&copyTx, shardFor); err != nil {
		return nil, err
	}
	if err := core.ValidateExecutionAdmissionBaseGas(&copyTx, limit); err != nil {
		return nil, err
	}
	return &copyTx, nil
}

// ValidateUnifiedBlock checks declared prices rather than repairing them.
// Followers can validate without owning a copy of the leader's pending pool.
// The pool check enforces total capacity, unique execution IDs and replay
// tombstones; TOP and RELAY2 are distinct stages of the same logical ID.
func (bc *BlockChain) ValidateUnifiedBlock(txs []*core.Transaction) error {
	if params.BlockCapacityMode != "gas" || bc.ChainConfig.BlockGasLimit == 0 {
		return fmt.Errorf("unified execution requires a positive Gas block capacity")
	}
	if err := bc.validateUnifiedRecords(txs); err != nil {
		return err
	}
	return bc.Txpool.ValidateUncommittedExecutionRecords(txs, bc.ChainConfig.BlockGasLimit)
}

func (bc *BlockChain) validateUnifiedRecords(txs []*core.Transaction) error {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	if err := bc.validateGARETAddressesLocked(txs); err != nil {
		return err
	}
	for _, tx := range txs {
		if err := core.ValidateExecutionRecord(tx, params.UnifiedCrossCallGas); err != nil {
			return err
		}
		if err := core.ValidateExecutionStageGas(tx, bc.shardForLocked, bc.ChainConfig.BlockGasLimit); err != nil {
			return err
		}
		owner := tx.Sender
		if tx.Relayed {
			owner = tx.Recipient
		}
		if bc.shardForLocked(owner) != bc.ChainConfig.ShardID {
			return fmt.Errorf("execution %s owner is not local to shard %d", core.TransactionExecutionID(tx), bc.ChainConfig.ShardID)
		}
	}
	return nil
}
