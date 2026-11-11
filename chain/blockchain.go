// Here the blockchain structrue is defined
// each node in this system will maintain a blockchain object.

package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/partition/garet"
	"blockEmulator/storage"
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

type BlockChain struct {
	db           ethdb.Database      // the leveldb database to store in the disk, for status trie
	triedb       *trie.Database      // the trie database which helps to store the status trie
	ChainConfig  *params.ChainConfig // the chain configuration, which can help to identify the chain
	CurrentBlock *core.Block         // the top block in this blockchain
	Storage      *storage.Storage    // Storage is the bolt-db to store the blocks
	Txpool       *core.TxPool        // the transaction pool
	PartitionMap map[string]uint64   // the partition map which is defined by some algorithm can help account parition
	pmlock       sync.RWMutex
	garetCatalog *garet.Catalog
	garetGroups  []uint64
	garetEpoch   uint64
}

// Get the transaction root, this root can be used to check the transactions
func GetTxTreeRoot(txs []*core.Transaction) []byte {
	// use a memory trie database to do this, instead of disk database
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range txs {
		transactionTree.Update(tx.TxHash, []byte{0})
	}
	return transactionTree.Hash().Bytes()
}

// Get bloom filter
func GetBloomFilter(txs []*core.Transaction) *bitset.BitSet {
	bs := bitset.New(2048)
	for _, tx := range txs {
		bs.Set(utils.ModBytes(tx.TxHash, 2048))
	}
	return bs
}

// Write Partition Map
func (bc *BlockChain) Update_PartitionMap(key string, val uint64) {
	bc.pmlock.Lock()
	defer bc.pmlock.Unlock()
	if bc.garetCatalog != nil {
		if bc.shardForLocked(key) != val {
			panic("GARET account override disagrees with group routing")
		}
		return
	}
	bc.PartitionMap[key] = val
}

// Get parition (if not exist, return default)
func (bc *BlockChain) Get_PartitionMap(key string) uint64 {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	return bc.shardForLocked(key)
}

// Send a transaction to the pool (need to decide which pool should be sended)
func (bc *BlockChain) SendTx2Pool(txs []*core.Transaction) {
	bc.Txpool.AddTxs2Pool(txs)
}

// gasUsedForPacking returns the gas this shard must reserve in its block for
// the current execution stage. Legacy records reserve their full GasUsed in
// each relay stage. Unified records already carry base Gas on RELAY1 and base
// plus the attached internal-call surcharge on local TOP or RELAY2.
func (bc *BlockChain) gasUsedForPacking(tx *core.Transaction) uint64 {
	return core.EffectiveGasUsed(tx)
}

// handle transactions and modify the status trie
func (bc *BlockChain) GetUpdateStatusTrie(txs []*core.Transaction) common.Hash {
	fmt.Printf("The len of txs is %d\n", len(txs))
	// the empty block (length of txs is 0) condition
	if len(txs) == 0 {
		return common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	}
	// build trie from the triedb (in disk)
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	cnt := 0
	// handle transactions, the signature check is ignored here
	for i, tx := range txs {
		// GLAS-FREIGHT call tasks reserve and execute gas in this block, but
		// they are not top-level value transfers and therefore have no account
		// state transition of their own.
		if tx.IsGLASCallTask {
			continue
		}
		// fmt.Printf("tx %d: %s, %s\n", i, tx.Sender, tx.Recipient)
		// senderIn := false
		if !tx.Relayed && (bc.Get_PartitionMap(tx.Sender) == bc.ChainConfig.ShardID || tx.HasBroker) {
			// senderIn = true
			// fmt.Printf("the sender %s is in this shard %d, \n", tx.Sender, bc.ChainConfig.ShardID)
			// modify local accountstate
			s_state_enc, _ := st.Get([]byte(tx.Sender))
			var s_state *core.AccountState
			if s_state_enc == nil {
				// fmt.Println("missing account SENDER, now adding account")
				ib := new(big.Int)
				ib.Add(ib, params.Init_Balance)
				s_state = &core.AccountState{
					Nonce:   uint64(i),
					Balance: ib,
				}
			} else {
				s_state = core.DecodeAS(s_state_enc)
			}
			s_balance := s_state.Balance
			if s_balance.Cmp(tx.Value) == -1 {
				fmt.Printf("the balance is less than the transfer amount\n")
				continue
			}
			s_state.Deduct(tx.Value)
			st.Update([]byte(tx.Sender), s_state.Encode())
			cnt++
		}
		// recipientIn := false
		if bc.Get_PartitionMap(tx.Recipient) == bc.ChainConfig.ShardID || tx.HasBroker {
			// fmt.Printf("the recipient %s is in this shard %d, \n", tx.Recipient, bc.ChainConfig.ShardID)
			// recipientIn = true
			// modify local state
			r_state_enc, _ := st.Get([]byte(tx.Recipient))
			var r_state *core.AccountState
			if r_state_enc == nil {
				// fmt.Println("missing account RECIPIENT, now adding account")
				ib := new(big.Int)
				ib.Add(ib, params.Init_Balance)
				r_state = &core.AccountState{
					Nonce:   uint64(i),
					Balance: ib,
				}
			} else {
				r_state = core.DecodeAS(r_state_enc)
			}
			r_state.Deposit(tx.Value)
			st.Update([]byte(tx.Recipient), r_state.Encode())
			cnt++
		}
	}
	// commit the memory trie to the database in the disk
	if cnt == 0 {
		return common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	}
	rt, ns := st.Commit(false)
	// if `ns` is nil, the `err = bc.triedb.Update(trie.NewWithNodeSet(ns))` will report an error.
	if ns != nil {
		err = bc.triedb.Update(trie.NewWithNodeSet(ns))
		if err != nil {
			log.Panic()
		}
		err = bc.triedb.Commit(rt, false)
		if err != nil {
			log.Panic(err)
		}
	}
	fmt.Println("modified account number is ", cnt)
	return rt
}

// generate (mine) a block, this function return a block
func (bc *BlockChain) GenerateBlock(miner int32) *core.Block {
	if params.UnifiedExecution {
		if params.UnifiedPackingOptimized {
			options := core.TxSelectionOptions{MaxGas: bc.ChainConfig.BlockGasLimit, GasCost: bc.gasUsedForPacking,
				PriorityLevel: core.ExecutionPriority, LevelCount: 2, DependencyKey: core.ExecutionDependency, OmitRetained: true}
			var txs []*core.Transaction
			if params.ConsensusMethod == 12 && params.GLASPendingExecutionPolicy == params.GLASPendingExecutionWindowFirst {
				txs = bc.Txpool.PeekWindowTransactions(options)
			} else {
				txs = bc.Txpool.PeekTxsWithSelection(options)
			}
			return bc.generateBlockFromTransactions(miner, txs)
		}
		// All comparison methods preview the same packing policy. PBFT commit
		// removes the exact committed records, so failed proposals lose none.
		return bc.PreviewDependencyAwareTieredPriorityBlock(miner, core.ExecutionPriority, 2, core.ExecutionDependency)
	}
	var txs []*core.Transaction
	// pack the transactions from the txpool
	switch params.BlockCapacityMode {
	case "gas":
		txs = bc.Txpool.PackTxsWithGasLimitAndCost(bc.ChainConfig.BlockGasLimit, bc.gasUsedForPacking)
	case "bytes":
		txs = bc.Txpool.PackTxsWithBytes(params.BlocksizeInBytes)
	default:
		txs = bc.Txpool.PackTxs(bc.ChainConfig.BlockSize)
	}

	return bc.generateBlockFromTransactions(miner, txs)
}

// GeneratePriorityBlock is a GLAS-FREIGHT-only gas block hook. In-flight
// execution stages are packed before untouched top-level transactions, while
// the latter still fill any remaining gas in the same ordinary block.
func (bc *BlockChain) GeneratePriorityBlock(miner int32, priority func(*core.Transaction) bool) *core.Block {
	if params.BlockCapacityMode != "gas" {
		log.Panic("priority block generation requires gas block capacity mode")
	}
	txs := bc.Txpool.PackTxsWithGasLimitAndCostPriority(bc.ChainConfig.BlockGasLimit, bc.gasUsedForPacking, priority)
	return bc.generateBlockFromTransactions(miner, txs)
}

// GenerateTieredPriorityBlock is the multi-level form used by GLAS-FREIGHT.
// Lower numeric levels are packed first while every level retains FIFO order.
func (bc *BlockChain) GenerateTieredPriorityBlock(
	miner int32,
	priorityLevel func(*core.Transaction) int,
	levelCount int,
) *core.Block {
	if params.BlockCapacityMode != "gas" {
		log.Panic("tiered priority block generation requires gas block capacity mode")
	}
	txs := bc.Txpool.PackTxsWithGasLimitAndCostPriorityLevels(
		bc.ChainConfig.BlockGasLimit,
		bc.gasUsedForPacking,
		priorityLevel,
		levelCount,
	)
	return bc.generateBlockFromTransactions(miner, txs)
}

// GenerateDependencyAwareTieredPriorityBlock is GLAS-FREIGHT's
// work-conserving block builder. A record that cannot fit may be bypassed only
// by records with a different dependency key.
func (bc *BlockChain) GenerateDependencyAwareTieredPriorityBlock(
	miner int32,
	priorityLevel func(*core.Transaction) int,
	levelCount int,
	dependencyKey func(*core.Transaction) string,
) *core.Block {
	if params.BlockCapacityMode != "gas" {
		log.Panic("dependency-aware tiered block generation requires gas block capacity mode")
	}
	txs := bc.Txpool.PackTxsWithGasLimitAndCostPriorityLevelsDependencyAware(
		bc.ChainConfig.BlockGasLimit,
		bc.gasUsedForPacking,
		priorityLevel,
		levelCount,
		dependencyKey,
	)
	return bc.generateBlockFromTransactions(miner, txs)
}

// PreviewDependencyAwareTieredPriorityBlock builds the same deterministic
// dependency-aware candidate as GenerateDependencyAwareTieredPriorityBlock,
// but does not remove records from the pool or write leave-pool timestamps.
func (bc *BlockChain) PreviewDependencyAwareTieredPriorityBlock(
	miner int32,
	priorityLevel func(*core.Transaction) int,
	levelCount int,
	dependencyKey func(*core.Transaction) string,
) *core.Block {
	if params.BlockCapacityMode != "gas" {
		log.Panic("dependency-aware tiered block preview requires gas block capacity mode")
	}
	txs := bc.Txpool.PeekTxsWithSelection(core.TxSelectionOptions{
		MaxGas:        bc.ChainConfig.BlockGasLimit,
		GasCost:       bc.gasUsedForPacking,
		PriorityLevel: priorityLevel,
		LevelCount:    levelCount,
		DependencyKey: dependencyKey,
	})
	return bc.generateBlockFromTransactions(miner, txs)
}

// GenerateExactManifestBlock non-destructively materializes one GLAS-FREIGHT
// proposal from its ordered execution-record manifest. If any planned
// descendant has not reached the local pool yet, ready is false and the pool
// remains untouched; callers must wait instead of substituting unplanned
// filler. Pool removal is deferred until the proposal is committed.
func (bc *BlockChain) GenerateExactManifestBlock(
	miner int32,
	executionIDs []string,
) (block *core.Block, ready bool, err error) {
	if params.BlockCapacityMode != "gas" {
		return nil, false, fmt.Errorf("exact manifest block generation requires gas block capacity mode")
	}
	txs, ready, err := bc.Txpool.PeekExactExecutionIDs(
		executionIDs,
		bc.ChainConfig.BlockGasLimit,
		bc.gasUsedForPacking,
	)
	if err != nil || !ready {
		return nil, ready, err
	}
	return bc.generateBlockFromTransactions(miner, txs), true, nil
}

// GenerateFilteredBlock is an additive hook used by GLAS-FREIGHT's quiescing
// phase. GLAS-FREIGHT requires gas-capacity blocks, so filtered packing keeps
// exactly the same gas accounting as an ordinary block.
func (bc *BlockChain) GenerateFilteredBlock(miner int32, include func(*core.Transaction) bool) *core.Block {
	if params.BlockCapacityMode != "gas" {
		log.Panic("filtered block generation requires gas block capacity mode")
	}
	txs := bc.Txpool.PackTxsWithGasLimitAndCostFiltered(bc.ChainConfig.BlockGasLimit, bc.gasUsedForPacking, include)
	return bc.generateBlockFromTransactions(miner, txs)
}

// GenerateFilteredFillBlock is used only for independent GLAS-FREIGHT call
// tasks. It may skip an oversized-for-the-remainder task and use a later,
// smaller task so that a batched call-drain block wastes less gas.
func (bc *BlockChain) GenerateFilteredFillBlock(miner int32, include func(*core.Transaction) bool) *core.Block {
	if params.BlockCapacityMode != "gas" {
		log.Panic("filtered fill block generation requires gas block capacity mode")
	}
	txs := bc.Txpool.PackTxsWithGasLimitAndCostFilteredFill(bc.ChainConfig.BlockGasLimit, bc.gasUsedForPacking, include)
	return bc.generateBlockFromTransactions(miner, txs)
}

func (bc *BlockChain) generateBlockFromTransactions(miner int32, txs []*core.Transaction) *core.Block {
	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Now(),
	}
	// handle transactions to build root
	rt := bc.GetUpdateStatusTrie(txs)

	bh.StateRoot = rt.Bytes()
	bh.TxRoot = GetTxTreeRoot(txs)
	bh.Bloom = *GetBloomFilter(txs)
	bh.Miner = miner
	b := core.NewBlock(bh, txs)

	b.Hash = b.Header.Hash()
	return b
}

// new a genisis block, this func will be invoked only once for a blockchain object
func (bc *BlockChain) NewGenisisBlock() *core.Block {
	body := make([]*core.Transaction, 0)
	bh := &core.BlockHeader{
		Number: 0,
	}
	// build a new trie database by db
	triedb := trie.NewDatabaseWithConfig(bc.db, &trie.Config{
		Cache:     0,
		Preimages: true,
	})
	bc.triedb = triedb
	statusTrie := trie.NewEmpty(triedb)
	bh.StateRoot = statusTrie.Hash().Bytes()
	bh.TxRoot = GetTxTreeRoot(body)
	bh.Bloom = *GetBloomFilter(body)
	b := core.NewBlock(bh, body)
	b.Hash = b.Header.Hash()
	return b
}

// add the genisis block in a blockchain
func (bc *BlockChain) AddGenisisBlock(gb *core.Block) {
	bc.Storage.AddBlock(gb)
	newestHash, err := bc.Storage.GetNewestBlockHash()
	if err != nil {
		log.Panic()
	}
	curb, err := bc.Storage.GetBlock(newestHash)
	if err != nil {
		log.Panic()
	}
	bc.CurrentBlock = curb
}

// add a block
func (bc *BlockChain) AddBlock(b *core.Block) {
	if b.Header.Number != bc.CurrentBlock.Header.Number+1 {
		fmt.Println("the block height is not correct")
		return
	}

	if !bytes.Equal(b.Header.ParentBlockHash, bc.CurrentBlock.Hash) {
		fmt.Println("err parent block hash")
		return
	}

	// if the treeRoot is existed in the node, the transactions is no need to be handled again
	_, err := trie.New(trie.TrieID(common.BytesToHash(b.Header.StateRoot)), bc.triedb)
	if err != nil {
		rt := bc.GetUpdateStatusTrie(b.Body)
		fmt.Println(bc.CurrentBlock.Header.Number+1, "the root = ", rt.Bytes())
	}
	bc.CurrentBlock = b
	bc.Storage.AddBlock(b)
}

// new a blockchain.
// the ChainConfig is pre-defined to identify the blockchain; the db is the status trie database in disk
func NewBlockChain(cc *params.ChainConfig, db ethdb.Database) (*BlockChain, error) {
	fmt.Println("Generating a new blockchain", db)
	chainDBfp := params.DatabaseWrite_path + fmt.Sprintf("chainDB/S%d_N%d", cc.ShardID, cc.NodeID)
	bc := &BlockChain{
		db:           db,
		ChainConfig:  cc,
		Txpool:       core.NewTxPool(),
		Storage:      storage.NewStorage(chainDBfp, cc),
		PartitionMap: make(map[string]uint64),
	}
	curHash, err := bc.Storage.GetNewestBlockHash()
	if err != nil {
		fmt.Println("There is no existed blockchain in the database. ")
		// if the Storage bolt database cannot find the newest blockhash,
		// it means the blockchain should be built in height = 0
		if err.Error() == "cannot find the newest block hash" {
			genisisBlock := bc.NewGenisisBlock()
			bc.AddGenisisBlock(genisisBlock)
			fmt.Println("New genisis block")
			return bc, nil
		}
		log.Panic()
	}

	// there is a blockchain in the storage
	fmt.Println("Existing blockchain found")
	curb, err := bc.Storage.GetBlock(curHash)
	if err != nil {
		log.Panic()
	}

	bc.CurrentBlock = curb
	triedb := trie.NewDatabaseWithConfig(db, &trie.Config{
		Cache:     0,
		Preimages: true,
	})
	bc.triedb = triedb
	// check the existence of the trie database
	_, err = trie.New(trie.TrieID(common.BytesToHash(curb.Header.StateRoot)), triedb)
	if err != nil {
		log.Panic()
	}
	fmt.Println("The status trie can be built")
	fmt.Println("Generated a new blockchain successfully")
	return bc, nil
}

// check a block is valid or not in this blockchain config
func (bc *BlockChain) IsValidBlock(b *core.Block) error {
	if b == nil || b.Header == nil {
		return errors.New("nil block or header")
	}
	if params.UnifiedExecution {
		if err := bc.ValidateUnifiedBlock(b.Body); err != nil {
			return err
		}
	}
	if string(b.Header.ParentBlockHash) != string(bc.CurrentBlock.Hash) {
		fmt.Println("the parentblock hash is not equal to the current block hash")
		return errors.New("the parentblock hash is not equal to the current block hash")
	} else if string(GetTxTreeRoot(b.Body)) != string(b.Header.TxRoot) {
		fmt.Println("the transaction root is wrong")
		return errors.New("the transaction root is wrong")
	}
	return nil
}

// migrationBlockHash deterministically identifies a locally materialized
// account-transfer block. Account-transfer requests are committed by PBFT, but
// every replica materializes the corresponding empty block independently.
// Hashing the BlockHeader's gob representation here is unsafe because gob is a
// storage encoding rather than a canonical consensus encoding.
func migrationBlockHash(bh *core.BlockHeader, shardID uint64, context []byte) []byte {
	h := sha256.New()
	h.Write([]byte("block-emulator/account-transfer/v1"))

	writeBytes := func(value []byte) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		h.Write(length[:])
		h.Write(value)
	}
	writeUint64 := func(value uint64) {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], value)
		h.Write(encoded[:])
	}

	writeUint64(shardID)
	writeUint64(bh.Number)
	writeBytes(bh.ParentBlockHash)
	writeBytes(bh.StateRoot)
	writeBytes(bh.TxRoot)
	writeBytes(context)
	return h.Sum(nil)
}

func (bc *BlockChain) addAccounts(ac []string, as []*core.AccountState, miner int32, deterministicContext []byte) []byte {
	fmt.Printf("The len of accounts is %d, now adding the accounts\n", len(ac))

	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Time{},
	}
	// handle transactions to build root
	rt := bc.CurrentBlock.Header.StateRoot
	if len(ac) != 0 {
		st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
		if err != nil {
			log.Panic(err)
		}
		for i, addr := range ac {
			if bc.Get_PartitionMap(addr) == bc.ChainConfig.ShardID {
				ib := new(big.Int)
				ib.Add(ib, as[i].Balance)
				new_state := &core.AccountState{
					Balance: ib,
					Nonce:   as[i].Nonce,
				}
				st.Update([]byte(addr), new_state.Encode())
			}
		}

		rrt, ns := st.Commit(false)

		// if `ns` is nil, the `err = bc.triedb.Update(trie.NewWithNodeSet(ns))` will report an error.
		if ns != nil {
			err = bc.triedb.Update(trie.NewWithNodeSet(ns))
			if err != nil {
				log.Panic(err)
			}
			err = bc.triedb.Commit(rrt, false)
			if err != nil {
				log.Panic(err)
			}
			rt = rrt.Bytes()
		}
	}

	emptyTxs := make([]*core.Transaction, 0)
	bh.StateRoot = rt
	bh.TxRoot = GetTxTreeRoot(emptyTxs)
	bh.Bloom = *GetBloomFilter(emptyTxs)
	bh.Miner = 0
	b := core.NewBlock(bh, emptyTxs)
	if deterministicContext == nil {
		// Preserve the original behavior for CLPA, Broker and existing callers.
		b.Hash = b.Header.Hash()
	} else {
		b.Hash = migrationBlockHash(b.Header, bc.ChainConfig.ShardID, deterministicContext)
	}

	bc.CurrentBlock = b
	bc.Storage.AddBlock(b)
	return append([]byte(nil), b.Hash...)
}

// AddAccounts preserves the original account-transfer behavior used by CLPA
// and Broker.
func (bc *BlockChain) AddAccounts(ac []string, as []*core.AccountState, miner int32) {
	bc.addAccounts(ac, as, miner, nil)
}

// AddAccountsWithDeterministicHash applies the same account state transition
// while giving every replica in a shard the same migration block hash. Context
// must identify the already committed reconfiguration (for JiangZiya it is the
// epoch plus the canonical partition-map digest).
func (bc *BlockChain) AddAccountsWithDeterministicHash(ac []string, as []*core.AccountState, miner int32, context []byte) []byte {
	if len(context) == 0 {
		log.Panic("deterministic account-transfer context must not be empty")
	}
	return bc.addAccounts(ac, as, miner, context)
}

// fetch accounts
func (bc *BlockChain) FetchAccounts(addrs []string) []*core.AccountState {
	res := make([]*core.AccountState, 0)
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	for _, addr := range addrs {
		asenc, _ := st.Get([]byte(addr))
		var state_a *core.AccountState
		if asenc == nil {
			ib := new(big.Int)
			ib.Add(ib, params.Init_Balance)
			state_a = &core.AccountState{
				Nonce:   uint64(0),
				Balance: ib,
			}
		} else {
			state_a = core.DecodeAS(asenc)
		}
		res = append(res, state_a)
	}
	return res
}

// close a blockChain, close the database inferfaces
func (bc *BlockChain) CloseBlockChain() {
	bc.Storage.DataBase.Close()
	bc.triedb.CommitPreimages()
	bc.db.Close()
}

// print the details of a blockchain
func (bc *BlockChain) PrintBlockChain() string {
	vals := []interface{}{
		bc.CurrentBlock.Header.Number,
		bc.CurrentBlock.Hash,
		bc.CurrentBlock.Header.StateRoot,
		bc.CurrentBlock.Header.Time,
		bc.triedb,
		// len(bc.Txpool.RelayPool[1]),
	}
	res := fmt.Sprintf("%v\n", vals)
	fmt.Println(res)
	return res
}
