package pbft_all

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition/garet"
)

func loadGARETWorkerCatalog(mode string) (*garet.Catalog, error) {
	if mode != "GARET" {
		return nil, nil
	}
	if !params.UnifiedExecution {
		return nil, fmt.Errorf("GARET requires unified execution")
	}
	catalog, err := garet.LoadCatalog(params.GARETGroupCatalogFile)
	if err != nil {
		return nil, err
	}
	if catalog.GroupCount() != params.GARETGroupCount {
		return nil, fmt.Errorf("GARET configured group count differs from catalog")
	}
	return catalog, nil
}

func copyGARETPlacement(p *message.GARETPlacement) *message.GARETPlacement {
	if p == nil {
		return nil
	}
	copy := *p
	copy.Placement = append([]uint64(nil), p.Placement...)
	return &copy
}

func (node *PbftConsensusNode) validateGARETMetadata(p *message.GARETPlacement, delta map[string]uint64) error {
	enabled := node.CurChain.GARETEnabled()
	if !enabled {
		if p != nil {
			return fmt.Errorf("GARET payload is not permitted in this mode")
		}
		return nil
	}
	if p == nil {
		return fmt.Errorf("GARET group assignment is missing")
	}
	return node.CurChain.ValidateGARETPlacement(p.CatalogDigest, p.Epoch, p.Placement, delta)
}

// acceptGARETPartition validates before changing control state and makes exact
// retransmission idempotent. Followers receive the same metadata through PBFT.
func (crom *CLPARelayOutsideModule) acceptGARETPartition(pm *message.PartitionModifiedMap) (bool, error) {
	if crom.pbftNode.CurChain.GARETEnabled() {
		crom.pbftNode.unifiedExecutionMu.Lock()
		defer crom.pbftNode.unifiedExecutionMu.Unlock()
	}
	if err := crom.pbftNode.validateGARETMetadata(pm.GARET, pm.PartitionModified); err != nil {
		return false, err
	}
	if pm.GARET == nil {
		return true, nil
	}
	crom.cdm.GARETLock.Lock()
	defer crom.cdm.GARETLock.Unlock()
	if prior := crom.cdm.GARETPending; prior != nil {
		if reflect.DeepEqual(prior, pm.GARET) && len(crom.cdm.ModifiedMap) > 0 &&
			reflect.DeepEqual(crom.cdm.ModifiedMap[len(crom.cdm.ModifiedMap)-1], pm.PartitionModified) {
			return false, nil
		}
		return false, fmt.Errorf("conflicting GARET pending group assignment")
	}
	crom.cdm.GARETPending = copyGARETPlacement(pm.GARET)
	crom.cdm.ModifiedMap = append(crom.cdm.ModifiedMap, pm.PartitionModified)
	return true, nil
}

func (cphm *CLPAPbftInsideExtraHandleMod) pendingGARETPlacement() *message.GARETPlacement {
	cphm.cdm.GARETLock.Lock()
	defer cphm.cdm.GARETLock.Unlock()
	return copyGARETPlacement(cphm.cdm.GARETPending)
}

func (cphm *CLPAPbftInsideExtraHandleMod) validateGARETTransfer(atm *message.AccountTransferMsg) error {
	if atm == nil {
		return fmt.Errorf("nil account transfer")
	}
	if err := cphm.pbftNode.validateGARETMetadata(atm.GARET, atm.ModifiedMap); err != nil {
		return err
	}
	if atm.GARET == nil {
		return nil
	}
	if atm.GARET.Epoch != atm.ATid || atm.ATid != cphm.cdm.AccountTransferRound+1 {
		return fmt.Errorf("GARET transfer epoch mismatch")
	}
	if pending := cphm.pendingGARETPlacement(); pending != nil && !reflect.DeepEqual(pending, atm.GARET) {
		return fmt.Errorf("GARET committed groups differ from announced groups")
	}
	if len(atm.Addrs) != len(atm.AccountState) {
		return fmt.Errorf("GARET transferred account/state lengths differ")
	}
	seen := make(map[string]bool, len(atm.Addrs))
	for i, address := range atm.Addrs {
		shard, err := cphm.pbftNode.CurChain.GARETTargetShard(address, atm.GARET.Placement)
		if err != nil || shard != cphm.pbftNode.ShardID || atm.AccountState[i] == nil || seen[address] {
			return fmt.Errorf("invalid GARET incoming account %s", address)
		}
		seen[address] = true
	}
	return nil
}

// prepareGARETMigration makes a complete outbound batch under the execution
// guard. State existence never determines whether an unexecuted TOP/RELAY2
// moves: their respective sender/recipient owners follow the new group map.
func (cphm *CLPAPbftInsideExtraHandleMod) prepareGARETMigration() (map[uint64]*message.AccountStateAndTx, error) {
	node := cphm.pbftNode
	node.unifiedExecutionMu.Lock()
	defer node.unifiedExecutionMu.Unlock()
	placement := cphm.pendingGARETPlacement()
	if len(cphm.cdm.ModifiedMap) == 0 {
		return nil, fmt.Errorf("GARET migration map is missing")
	}
	delta := cphm.cdm.ModifiedMap[len(cphm.cdm.ModifiedMap)-1]
	if err := node.validateGARETMetadata(placement, delta); err != nil {
		return nil, err
	}
	out := make(map[uint64]*message.AccountStateAndTx)
	for shard := uint64(0); shard < node.pbftChainConfig.ShardNums; shard++ {
		if shard != node.ShardID {
			out[shard] = &message.AccountStateAndTx{FromShard: node.ShardID, GARET: copyGARETPlacement(placement)}
			if cphm.cdm.ControlEventsEnabled {
				out[shard].Epoch = placement.Epoch
			}
		}
	}
	candidates := make([]string, 0)
	for address, target := range delta {
		if target != node.ShardID && node.CurChain.Get_PartitionMap(address) == node.ShardID {
			candidates = append(candidates, address)
		}
	}
	sort.Strings(candidates)
	addresses, states, err := node.CurChain.FetchExistingAccounts(candidates)
	if err != nil {
		return nil, err
	}
	for i, address := range addresses {
		target := delta[address]
		out[target].Addrs = append(out[target].Addrs, address)
		out[target].AccountState = append(out[target].AccountState, states[i])
	}
	pool := node.CurChain.Txpool
	pool.GetLocked()
	defer pool.GetUnlocked()
	retained := make([]*core.Transaction, 0, len(pool.TxQueue))
	for _, tx := range pool.TxQueue {
		if tx == nil {
			return nil, fmt.Errorf("nil GARET pending transaction")
		}
		owner := tx.Sender
		if tx.Relayed {
			owner = tx.Recipient
		}
		target, err := node.CurChain.GARETTargetShard(owner, placement.Placement)
		if err != nil {
			return nil, err
		}
		if node.CurChain.Get_PartitionMap(owner) != node.ShardID {
			return nil, fmt.Errorf("GARET pending owner is not local")
		}
		if target == node.ShardID {
			retained = append(retained, tx)
		} else {
			out[target].Txs = append(out[target].Txs, tx)
		}
	}
	pool.TxQueue = retained
	return out, nil
}

func (cphm *CLPAPbftInsideExtraHandleMod) sendGARETAccountsAndTxs() error {
	out, err := cphm.prepareGARETMigration()
	if err != nil {
		return err
	}
	for shard := uint64(0); shard < cphm.pbftNode.pbftChainConfig.ShardNums; shard++ {
		batch, exists := out[shard]
		if !exists {
			continue
		}
		if cphm.cdm.ControlEventsEnabled {
			cphm.pbftNode.sendCLPAControl(message.AccountState_and_TX, batch, cphm.pbftNode.ip_nodeTable[shard][0])
			continue
		}
		payload, err := json.Marshal(batch)
		if err != nil {
			return err
		}
		networks.TcpDial(message.MergeMessage(message.AccountState_and_TX, payload), cphm.pbftNode.ip_nodeTable[shard][0])
	}
	return nil
}

func (crom *CLPARelayOutsideModule) validateGARETBatch(batch *message.AccountStateAndTx) error {
	if err := crom.pbftNode.validateGARETMetadata(batch.GARET, nil); err != nil {
		return err
	}
	if batch.GARET == nil {
		return nil
	}
	crom.cdm.GARETLock.Lock()
	pending := copyGARETPlacement(crom.cdm.GARETPending)
	crom.cdm.GARETLock.Unlock()
	if pending == nil || !reflect.DeepEqual(pending, batch.GARET) || batch.FromShard == crom.pbftNode.ShardID ||
		batch.FromShard >= crom.pbftNode.pbftChainConfig.ShardNums || len(batch.Addrs) != len(batch.AccountState) {
		return fmt.Errorf("invalid GARET account migration metadata")
	}
	validOwner := func(address string) bool {
		target, err := crom.pbftNode.CurChain.GARETTargetShard(address, batch.GARET.Placement)
		return err == nil && target == crom.pbftNode.ShardID && crom.pbftNode.CurChain.Get_PartitionMap(address) == batch.FromShard
	}
	seen := make(map[string]bool, len(batch.Addrs))
	for index, address := range batch.Addrs {
		if batch.AccountState[index] == nil || seen[address] || !validOwner(address) {
			return fmt.Errorf("invalid GARET migrated state owner")
		}
		seen[address] = true
	}
	for _, tx := range batch.Txs {
		if err := core.ValidateExecutionRecord(tx, params.UnifiedCrossCallGas); err != nil {
			return err
		}
		owner := tx.Sender
		if tx.Relayed {
			owner = tx.Recipient
		}
		if !validOwner(owner) {
			return fmt.Errorf("invalid GARET migrated transaction owner")
		}
	}
	return nil
}
