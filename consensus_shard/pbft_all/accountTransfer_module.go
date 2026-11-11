// account transfer happens when the leader received the re-partition message.
// leaders send the infos about the accounts to be transferred to other leaders, and
// handle them.

package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"log"
	"time"
)

// this message used in propose stage, so it will be invoked by InsidePBFT_Module
func (cphm *CLPAPbftInsideExtraHandleMod) sendPartitionReady() {
	cphm.cdm.P_ReadyLock.Lock()
	cphm.cdm.PartitionReady[cphm.pbftNode.ShardID] = true
	cphm.cdm.P_ReadyLock.Unlock()

	pr := message.PartitionReady{
		FromShard: cphm.pbftNode.ShardID,
		NowSeqID:  cphm.pbftNode.sequenceID,
	}
	if cphm.cdm.ControlEventsEnabled {
		cphm.cdm.PartitionStateLock.Lock()
		pr.Epoch = cphm.cdm.PendingEpoch
		pr.RelaySeq = cphm.cdm.LastNormalCommittedSeq
		cphm.cdm.PartitionStateLock.Unlock()
		for sid := uint64(0); sid < cphm.pbftNode.pbftChainConfig.ShardNums; sid++ {
			if sid != pr.FromShard {
				cphm.pbftNode.sendCLPAControl(message.CPartitionReady, pr, cphm.pbftNode.ip_nodeTable[sid][0])
			}
		}
		return
	}
	pByte, err := json.Marshal(pr)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionReady, pByte)
	for sid := 0; sid < int(cphm.pbftNode.pbftChainConfig.ShardNums); sid++ {
		if sid != int(pr.FromShard) {
			go networks.TcpDial(send_msg, cphm.pbftNode.ip_nodeTable[uint64(sid)][0])
		}
	}
	cphm.pbftNode.pl.Plog.Print("Ready for partition\n")
}

// get whether all shards is ready, it will be invoked by InsidePBFT_Module
func (cphm *CLPAPbftInsideExtraHandleMod) getPartitionReady() bool {
	cphm.cdm.P_ReadyLock.Lock()
	defer cphm.cdm.P_ReadyLock.Unlock()
	cphm.pbftNode.seqMapLock.Lock()
	defer cphm.pbftNode.seqMapLock.Unlock()
	cphm.cdm.ReadySeqLock.Lock()
	defer cphm.cdm.ReadySeqLock.Unlock()

	if cphm.cdm.ControlEventsEnabled {
		for sid := uint64(0); sid < cphm.pbftNode.pbftChainConfig.ShardNums; sid++ {
			if !cphm.cdm.PartitionReady[sid] {
				return false
			}
			if sid == cphm.pbftNode.ShardID {
				continue
			}
			ready := cphm.cdm.ReadySeq[sid]
			required, exists := cphm.cdm.ReadyRelaySeq[sid]
			if ready == 0 || !exists || required >= ready || cphm.pbftNode.seqIDMap[sid] < required {
				return false
			}
		}
		return true
	}
	flag := true
	for sid, val := range cphm.pbftNode.seqIDMap {
		if rval, ok := cphm.cdm.ReadySeq[sid]; !ok || (rval-1 != val) {
			flag = false
		}
	}
	return len(cphm.cdm.PartitionReady) == int(cphm.pbftNode.pbftChainConfig.ShardNums) && flag
}

// send the transactions and the accountState to other leaders
func (cphm *CLPAPbftInsideExtraHandleMod) sendAccounts_and_Txs() {
	if cphm.pbftNode.CurChain.GARETEnabled() {
		if err := cphm.sendGARETAccountsAndTxs(); err != nil {
			log.Panicf("GARET migration: %v", err)
		}
		return
	}
	// generate accout transfer and txs message
	accountToFetch := make([]string, 0)
	cphm.cdm.PartitionStateLock.Lock()
	modifiedMap := cphm.cdm.ModifiedMap[len(cphm.cdm.ModifiedMap)-1]
	epoch := cphm.cdm.PendingEpoch
	cphm.cdm.PartitionStateLock.Unlock()
	for key, val := range modifiedMap {
		if val != cphm.pbftNode.ShardID && cphm.pbftNode.CurChain.Get_PartitionMap(key) == cphm.pbftNode.ShardID {
			accountToFetch = append(accountToFetch, key)
		}
	}
	asFetched := cphm.pbftNode.CurChain.FetchAccounts(accountToFetch)
	// send the accounts to other shards
	cphm.pbftNode.CurChain.Txpool.GetLocked()
	cphm.pbftNode.pl.Plog.Println("The size of tx pool is: ", len(cphm.pbftNode.CurChain.Txpool.TxQueue))
	for i := uint64(0); i < cphm.pbftNode.pbftChainConfig.ShardNums; i++ {
		if i == cphm.pbftNode.ShardID {
			continue
		}
		addrSend := make([]string, 0)
		addrSet := make(map[string]bool)
		asSend := make([]*core.AccountState, 0)
		for idx, addr := range accountToFetch {
			if modifiedMap[addr] == i {
				addrSend = append(addrSend, addr)
				addrSet[addr] = true
				asSend = append(asSend, asFetched[idx])
			}
		}
		// fetch transactions to it, after the transactions is fetched, delete it in the pool
		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		for secondPtr := 0; secondPtr < len(cphm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := cphm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			// if this is a normal transaction or ctx1 before re-sharding && the addr is correspond
			_, ok1 := addrSet[ptx.Sender]
			condition1 := ok1 && !ptx.Relayed
			// if this tx is ctx2
			_, ok2 := addrSet[ptx.Recipient]
			condition2 := ok2 && ptx.Relayed
			if condition1 || condition2 {
				txSend = append(txSend, ptx)
			} else {
				cphm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
				firstPtr++
			}
		}
		cphm.pbftNode.CurChain.Txpool.TxQueue = cphm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr]

		cphm.pbftNode.pl.Plog.Printf("The txSend to shard %d is generated \n", i)
		ast := message.AccountStateAndTx{
			Addrs:        addrSend,
			AccountState: asSend,
			FromShard:    cphm.pbftNode.ShardID,
			Txs:          txSend,
		}
		if params.GLASPendingFullWindowEarlyEnabled() {
			ast.PendingMarks = cphm.pbftNode.CurChain.Txpool.ExportPendingMarksLocked(txSend)
		}
		if cphm.cdm.ControlEventsEnabled {
			ast.Epoch = epoch
			cphm.pbftNode.sendCLPAControl(message.AccountState_and_TX, ast, cphm.pbftNode.ip_nodeTable[i][0])
			continue
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.AccountState_and_TX, aByte)
		networks.TcpDial(send_msg, cphm.pbftNode.ip_nodeTable[i][0])
		cphm.pbftNode.pl.Plog.Printf("The message to shard %d is sent\n", i)
	}
	cphm.pbftNode.pl.Plog.Println("after sending, The size of tx pool is: ", len(cphm.pbftNode.CurChain.Txpool.TxQueue))
	cphm.pbftNode.CurChain.Txpool.GetUnlocked()
}

// fetch collect infos
func (cphm *CLPAPbftInsideExtraHandleMod) getCollectOver() bool {
	cphm.cdm.CollectLock.Lock()
	defer cphm.cdm.CollectLock.Unlock()
	return cphm.cdm.CollectOver
}

// propose a partition message
func (cphm *CLPAPbftInsideExtraHandleMod) proposePartition() (bool, *message.Request) {
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : begin partition proposing\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	// add all data in pool into the set
	// 加锁原因: 此时 CollectOver=true 理论上没新 handler 写入 AccountStateTx，
	// 但若有迟到的 AccountStateAndTx 还在 in-flight, handler 仍可能写。
	// 用 CollectLock 包住 map 遍历，与 handler 串行化，避免并发 map 读写。
	cphm.cdm.CollectLock.Lock()
	states, incoming, err := orderedMigrationContents(cphm.cdm.AccountStateTx)
	if err != nil {
		cphm.cdm.CollectLock.Unlock()
		log.Panicf("invalid collected migration: %v", err)
	}
	cphm.cdm.ReceivedNewAccountState, cphm.cdm.ReceivedNewTx = states, incoming
	cphm.cdm.CollectLock.Unlock()
	// propose, send all txs to other nodes in shard
	cphm.pbftNode.pl.Plog.Println("The number of ReceivedNewTx: ", len(cphm.cdm.ReceivedNewTx))
	garetPlacement := cphm.pendingGARETPlacement()
	cphm.cdm.PartitionStateLock.Lock()
	modifiedMap := cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound]
	epoch := uint64(len(cphm.cdm.ModifiedMap))
	targetSeq := cphm.cdm.PendingTargetSeq
	cphm.cdm.PartitionStateLock.Unlock()
	for _, tx := range cphm.cdm.ReceivedNewTx {
		if garetPlacement != nil {
			owner := tx.Sender
			if tx.Relayed {
				owner = tx.Recipient
			}
			shard, err := cphm.pbftNode.CurChain.GARETTargetShard(owner, garetPlacement.Placement)
			if err != nil || shard != cphm.pbftNode.ShardID {
				log.Panicf("invalid GARET incoming transaction owner: %v", err)
			}
			continue
		}
		if !tx.Relayed && modifiedMap[tx.Sender] != cphm.pbftNode.ShardID {
			log.Panic("error tx")
		}
		if tx.Relayed && modifiedMap[tx.Recipient] != cphm.pbftNode.ShardID {
			log.Panic("error tx")
		}
	}
	// Unified records are installed only after the new execution map commits.
	if !params.UnifiedExecution {
		cphm.pbftNode.CurChain.Txpool.AddTxs2Pool(cphm.cdm.ReceivedNewTx)
	}
	cphm.pbftNode.pl.Plog.Println("The size of txpool: ", len(cphm.pbftNode.CurChain.Txpool.TxQueue))

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range cphm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}
	atm := message.AccountTransferMsg{
		ModifiedMap:  modifiedMap,
		Addrs:        atmaddr,
		AccountState: atmAs,
		ATid:         epoch,
		GARET:        garetPlacement,
	}
	if cphm.cdm.ControlEventsEnabled {
		atm.TargetSeq = targetSeq
	}
	atmbyte := atm.Encode()
	r := &message.Request{
		RequestType: message.PartitionReq,
		Msg: message.RawMessage{
			Content: atmbyte,
		},
		ReqTime: time.Now(),
	}
	return true, r
}

// all nodes in a shard will do accout Transfer, to sync the state trie
func (cphm *CLPAPbftInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
	var forwards map[uint64]*message.ExecutionForward
	var shardFor func(string) uint64
	if params.UnifiedExecution {
		cphm.pbftNode.unifiedExecutionMu.Lock()
		defer func() {
			cphm.pbftNode.unifiedExecutionMu.Unlock()
			if err := cphm.pbftNode.sendExecutionForwards(forwards); err != nil {
				log.Panicf("cannot forward late migration records: %v", err)
			}
			cphm.cdm.PartitionStateLock.Lock()
			cphm.cdm.PendingSnapshotPaused = false
			cphm.cdm.PartitionStateLock.Unlock()
		}()
	}
	if err := cphm.validatePendingWindowTransfer(atm); err != nil {
		log.Panicf("invalid committed planning window: %v", err)
	}
	if err := cphm.validateGARETTransfer(atm); err != nil {
		log.Panicf("invalid committed partition: %v", err)
	}
	// change the partition Map
	timing := cphm.pbftNode.applyingCLPAStep4(atm.ATid)
	started := timing.phaseStart()
	cnt := 0
	if atm.GARET != nil {
		if err := cphm.pbftNode.CurChain.InstallGARETPlacement(atm.GARET.CatalogDigest, atm.GARET.Epoch, atm.GARET.Placement, atm.ModifiedMap); err != nil {
			log.Panic(err)
		}
		cnt = len(atm.ModifiedMap)
	} else {
		for key, val := range atm.ModifiedMap {
			cnt++
			cphm.pbftNode.CurChain.Update_PartitionMap(key, val)
		}
	}
	if timing != nil {
		timing.MapUpdateNS = time.Since(started).Nanoseconds()
		timing.MappingEntries = cnt
		timing.IncomingRecords = len(cphm.cdm.ReceivedNewTx)
	}
	if params.UnifiedExecution {
		started = timing.phaseStart()
		// One snapshot of the fully installed map serves this entire application.
		// Keep it local: admission and later applications take their own snapshot.
		shardFor = cphm.pbftNode.CurChain.ExecutionShardResolver()
		// Ordinary CLPA/D use an empty delta to retain their previous routing.
		// GARET may change group placement even with no account-map delta.
		repriceExisting := atm.GARET != nil || len(atm.ModifiedMap) != 0
		if err := cphm.pbftNode.installUnifiedMigration(cphm.cdm.ReceivedNewTx, shardFor, repriceExisting); err != nil {
			log.Panicf("cannot reprice migrated execution records: %v", err)
		}
		if params.GLASPendingFullWindowEarlyEnabled() && cphm.pbftNode.NodeID == uint64(cphm.pbftNode.view.Load()) {
			cphm.mergeMigrationPendingMarks()
		}
		if timing != nil {
			timing.RepriceNS = time.Since(started).Nanoseconds()
		}
	}
	cphm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)
	// add the account into the state trie
	cphm.pbftNode.pl.Plog.Printf("%d addrs to add\n", len(atm.Addrs))
	cphm.pbftNode.pl.Plog.Printf("%d accountstates to add\n", len(atm.AccountState))
	if params.UnifiedExecution {
		// Every replica commits the same map/epoch context, independent of
		// local pool contents and gob's process-local type registration order.
		context, err := json.Marshal(struct {
			Epoch uint64
			Map   map[string]uint64
			GARET *message.GARETPlacement `json:",omitempty"`
		}{atm.ATid, atm.ModifiedMap, atm.GARET})
		if err != nil {
			log.Panic(err)
		}
		started = timing.phaseStart()
		cphm.pbftNode.CurChain.AddAccountsWithDeterministicHash(atm.Addrs, atm.AccountState, cphm.pbftNode.view.Load(), context)
	} else {
		started = timing.phaseStart()
		cphm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, cphm.pbftNode.view.Load())
	}
	if timing != nil {
		timing.AccountsWriteNS = time.Since(started).Nanoseconds()
		timing.AccountCount = len(atm.AccountState)
	}
	cphm.observePendingSnapshotSequence(cphm.pbftNode.CurChain.CurrentBlock.Header.Number + 1)

	cphm.cdm.PartitionStateLock.Lock()
	if uint64(len(cphm.cdm.ModifiedMap)) != atm.ATid {
		cphm.cdm.ModifiedMap = append(cphm.cdm.ModifiedMap, atm.ModifiedMap)
	}
	cphm.cdm.AccountTransferRound = atm.ATid
	if cphm.cdm.ControlEventsEnabled {
		cphm.cdm.ControlRequest = nil
	}
	cphm.cdm.PartitionOn = false
	cphm.cdm.PendingSnapshotRequested = false
	if !params.UnifiedExecution {
		cphm.cdm.PendingSnapshotPaused = false
	}
	cphm.cdm.PartitionStateLock.Unlock()
	if params.UnifiedExecution {
		var err error
		forwards, err = cphm.flushExecutionForwardsWithResolverLocked(shardFor)
		if err != nil {
			log.Panicf("cannot reconcile late migration records: %v", err)
		}
	}
	cphm.cdm.GARETLock.Lock()
	cphm.cdm.GARETPending = nil
	cphm.cdm.GARETLock.Unlock()
	// 把 AccountStateTx 等本轮 partition 共享状态的重置整体放到 CollectLock 内,
	// 避免与 handler 的并发 map 写冲突 (handler 也持有 CollectLock).
	cphm.cdm.CollectLock.Lock()
	cphm.cdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	cphm.cdm.ReceivedNewAccountState = make(map[string]*core.AccountState)
	cphm.cdm.ReceivedNewTx = make([]*core.Transaction, 0)
	cphm.cdm.CollectOver = false
	cphm.cdm.CollectLock.Unlock()

	cphm.cdm.P_ReadyLock.Lock()
	cphm.cdm.PartitionReady = make(map[uint64]bool)
	cphm.cdm.P_ReadyLock.Unlock()
	if cphm.cdm.ControlEventsEnabled {
		cphm.cdm.ReadySeqLock.Lock()
		cphm.cdm.ReadySeq = make(map[uint64]uint64)
		cphm.cdm.ReadyRelaySeq = make(map[uint64]uint64)
		cphm.cdm.ReadySeqLock.Unlock()
	}

	if atm.GARET != nil && cphm.pbftNode.CurChain.GARETEnabled() && atm.ATid > 1 {
		// GARET installs epochs sequentially. Retain the latest installed map
		// and any pending map, but release the previous installed map's slot.
		// Keep the slice length/indexes as epoch counters; do not clear the map
		// itself, since a request or receipt may still reference its contents.
		cphm.cdm.PartitionStateLock.Lock()
		cphm.cdm.ModifiedMap[atm.ATid-2] = nil
		cphm.cdm.PartitionStateLock.Unlock()
	}

	cphm.pbftNode.CurChain.PrintBlockChain()
}

// The leader owns its migration batches and local queue. The installation
// caller holds unifiedExecutionMu until the next priority epoch is active.
func (cphm *CLPAPbftInsideExtraHandleMod) mergeMigrationPendingMarks() {
	cphm.cdm.CollectLock.Lock()
	defer cphm.cdm.CollectLock.Unlock()
	for _, batch := range cphm.cdm.AccountStateTx {
		if err := cphm.pbftNode.CurChain.Txpool.MergePendingMarks(batch.Txs, batch.PendingMarks); err != nil {
			log.Panicf("cannot merge migrated priority marks: %v", err)
		}
	}
}

func (cphm *CLPAPbftInsideExtraHandleMod) activateFullWindowMarks(epoch uint64) {
	p := cphm.pbftNode
	if p.NodeID != uint64(p.view.Load()) {
		return
	}
	pool := p.CurChain.Txpool
	timing := p.applyingCLPAStep4(epoch)
	started := time.Now()
	if err := pool.ActivatePendingMarks(epoch); err != nil {
		log.Panicf("cannot activate local planning marks: %v", err)
	}
	elapsed := time.Since(started)
	count := pool.ActivePendingMarkCount()
	if timing != nil {
		timing.WindowIndexNS = elapsed.Nanoseconds()
		timing.WindowIndexApplied = true
		timing.WindowSources = count
	}
	p.pl.Plog.Printf("GLAS_Pending local marks activated: epoch=%d shard=%d active=%d install_us=%d",
		epoch, p.ShardID, count, elapsed.Microseconds())
}
