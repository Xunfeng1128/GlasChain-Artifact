package pbft_all

import (
	"fmt"

	"blockEmulator/core"
	"blockEmulator/params"
)

// The legacy input and execution paths remain available when the switch is off.
// This lock binds baseline pool pricing to a complete installed partition map;
// it does not change a committee's graph or reconfiguration trigger.
func (node *PbftConsensusNode) generateExecutionBlock() *core.Block {
	if params.UnifiedExecution {
		node.unifiedExecutionMu.Lock()
		defer node.unifiedExecutionMu.Unlock()
	}
	return node.CurChain.GenerateBlock(int32(node.NodeID))
}

func (node *PbftConsensusNode) installExecutionTransactions(txs []*core.Transaction, relayed bool) error {
	if !params.UnifiedExecution {
		node.CurChain.Txpool.AddTxs2Pool(txs)
		return nil
	}
	node.unifiedExecutionMu.Lock()
	defer node.unifiedExecutionMu.Unlock()
	for _, tx := range txs {
		if tx == nil || tx.Relayed != relayed {
			return fmt.Errorf("incoming execution stage does not match its message kind")
		}
	}
	priced, err := node.CurChain.PrepareUnifiedTransactions(txs)
	if err != nil {
		return err
	}
	_, err = node.CurChain.Txpool.AddTxs2PoolUniqueByExecutionID(priced)
	return err
}

func (node *PbftConsensusNode) executionRelay2(top *core.Transaction, epoch uint64) (*core.Transaction, error) {
	if !params.UnifiedExecution {
		top.Relayed = true
		return top, nil
	}
	// The committed TOP/RELAY1 receipt must retain its original stage and Gas.
	return core.NewExecutionRelay2(top, epoch, node.CurChain.Get_PartitionMap)
}

func (node *PbftConsensusNode) commitUnifiedExecutionBlock(block *core.Block) error {
	if !params.UnifiedExecution {
		return nil
	}
	node.unifiedExecutionMu.Lock()
	defer node.unifiedExecutionMu.Unlock()
	if block == nil || block.Header == nil {
		return fmt.Errorf("nil execution block")
	}
	if block.Header.Number != node.CurChain.CurrentBlock.Header.Number+1 {
		return fmt.Errorf("unexpected execution block height")
	}
	if err := node.CurChain.IsValidBlock(block); err != nil {
		return err
	}
	pool, limit := node.CurChain.Txpool, node.CurChain.ChainConfig.BlockGasLimit
	if node.NodeID != uint64(node.view.Load()) {
		return pool.CommitExecutionRecordsWithoutPool(block.Body, limit)
	}
	if params.UnifiedPackingOptimized {
		return pool.CommitUnifiedExecutionRecords(block.Body, limit)
	}
	if _, err := pool.AddTxs2PoolUniqueByExecutionID(block.Body); err != nil {
		return err
	}
	ids := make([]string, len(block.Body))
	for i, tx := range block.Body {
		ids[i] = core.TransactionExecutionID(tx)
	}
	if _, committed, err := pool.CommitExactExecutionIDs(ids, limit, nil); err != nil || !committed {
		return fmt.Errorf("unified pool commit: committed=%t error=%v", committed, err)
	}
	return nil
}

// Caller holds unifiedExecutionMu and has installed every changed map entry.
// Reprice retained records too: an internal callee may move even when the
// TOP sender or RELAY2 recipient (the pending record's owner) does not move.
func (node *PbftConsensusNode) installUnifiedMigration(incoming []*core.Transaction, shardFor func(string) uint64, repriceExisting bool) error {
	return node.CurChain.InstallUnifiedMigrationWithResolver(incoming, shardFor, repriceExisting)
}
