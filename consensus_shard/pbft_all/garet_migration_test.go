package pbft_all

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"reflect"
	"testing"
	"time"

	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/partition/garet"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func garetTestAddr(value int) string { return fmt.Sprintf("%040x", value) }

func garetTestNode(t *testing.T, shard, nodeID uint64) (*PbftConsensusNode, *garet.Catalog) {
	t.Helper()
	catalog, err := garet.NewCatalog([]string{garetTestAddr(10), garetTestAddr(20), garetTestAddr(30), garetTestAddr(40)}, 2)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &params.ChainConfig{ShardID: shard, NodeID: nodeID, ShardNums: 2, Nodes_perShard: 4, BlockGasLimit: 100}
	bc, err := chain.NewBlockChain(cfg, rawdb.NewMemoryDatabase())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bc.CloseBlockChain)
	if err := bc.InitGARETRouting(catalog); err != nil {
		t.Fatal(err)
	}
	return &PbftConsensusNode{ShardID: shard, NodeID: nodeID, node_nums: 4, CurChain: bc, pbftChainConfig: cfg,
		seqIDMap: make(map[uint64]uint64), pl: &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)}}, catalog
}

func garetTestTx(source string, sender, recipient int, relayed bool) *core.Transaction {
	tx := core.NewTransaction(garetTestAddr(sender), garetTestAddr(recipient), big.NewInt(1), 0, time.Unix(1, 0))
	tx.SourceTxHash, tx.GLASBaseGas, tx.GasUsed, tx.GLASCallGasPerCall = source, 40, 40, 20
	tx.Relayed = relayed
	return tx
}

func TestGARETMigrationMovesPendingOwnersWithoutCreatingState(t *testing.T) {
	unifiedPBFTFixture(t)
	node, catalog := garetTestNode(t, 0, 0)
	if len(node.CurChain.PartitionMap) != 0 {
		t.Fatal("bootstrap materialized all account routes")
	}
	// Only one owner has state. The untouched TOP must still migrate, and
	// Relay2 follows its recipient independently of sender ownership.
	node.CurChain.AddAccountsWithDeterministicHash([]string{garetTestAddr(20)}, []*core.AccountState{{Balance: big.NewInt(123), Nonce: 2}}, 0, []byte("seed"))
	top, relay := garetTestTx("pending-top", 10, 30, false), garetTestTx("pending-relay", 30, 20, true)
	if err := node.installExecutionTransactions([]*core.Transaction{top}, false); err != nil {
		t.Fatal(err)
	}
	if err := node.installExecutionTransactions([]*core.Transaction{relay}, true); err != nil {
		t.Fatal(err)
	}
	placement := &message.GARETPlacement{CatalogDigest: catalog.Digest(), Epoch: 1, Placement: []uint64{1, 0}}
	cdm := dataSupport.NewCLPADataSupport()
	cdm.ModifiedMap = append(cdm.ModifiedMap, map[string]uint64{garetTestAddr(10): 1, garetTestAddr(20): 1})
	cdm.GARETPending = placement
	inside := &CLPAPbftInsideExtraHandleMod{pbftNode: node, cdm: cdm}
	out, err := inside.prepareGARETMigration()
	if err != nil {
		t.Fatal(err)
	}
	batch := out[1]
	if len(batch.Addrs) != 1 || batch.Addrs[0] != garetTestAddr(20) || batch.AccountState[0].Balance.Int64() != 123 || len(batch.Txs) != 2 {
		t.Fatalf("migration omitted pending stages or invented account state: %+v", batch)
	}
	if node.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("moved stages remain in source pool")
	}
	found, _, err := node.CurChain.FetchExistingAccounts([]string{garetTestAddr(10)})
	if err != nil || len(found) != 0 {
		t.Fatal("untouched account was materialized")
	}
	receiver, _ := garetTestNode(t, 1, 0)
	rcdm := dataSupport.NewCLPADataSupport()
	rcdm.GARETPending = copyGARETPlacement(placement)
	outside := &CLPARelayOutsideModule{pbftNode: receiver, cdm: rcdm}
	if err := outside.validateGARETBatch(batch); err != nil {
		t.Fatal(err)
	}
	batch.GARET.Epoch++
	if err := outside.validateGARETBatch(batch); err == nil {
		t.Fatal("wrong epoch migration accepted")
	}
	batch.GARET.Epoch--
	rcdm.ModifiedMap = append(rcdm.ModifiedMap, cdm.ModifiedMap[0])
	payload, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	outside.handleAccountStateAndTxMsg(payload)
	receiverInside := &CLPAPbftInsideExtraHandleMod{pbftNode: receiver, cdm: rcdm}
	ok, request := receiverInside.proposePartition()
	if !ok {
		t.Fatal("GARET transfer was not proposed")
	}
	transfer := message.DecodeAccountTransferMsg(request.Msg.Content)
	follower, _ := garetTestNode(t, 1, 1)
	followerInside := &CLPAPbftInsideExtraHandleMod{pbftNode: follower, cdm: dataSupport.NewCLPADataSupport()}
	for _, inside := range []*CLPAPbftInsideExtraHandleMod{receiverInside, followerInside} {
		if err := inside.validateGARETTransfer(transfer); err != nil {
			t.Fatal(err)
		}
		inside.accountTransfer_do(transfer)
		addrs, states, err := inside.pbftNode.CurChain.FetchExistingAccounts([]string{garetTestAddr(10), garetTestAddr(20)})
		if err != nil || len(addrs) != 1 || addrs[0] != garetTestAddr(20) || states[0].Nonce != 2 || states[0].Balance.Int64() != 123 {
			t.Fatal("committed migration lost state or materialized an untouched owner", err)
		}
	}
	if receiver.CurChain.Txpool.GetTxQueueLen() != 2 || follower.CurChain.Txpool.GetTxQueueLen() != 0 || !bytes.Equal(receiver.CurChain.CurrentBlock.Hash, follower.CurChain.CurrentBlock.Hash) {
		t.Fatal("replicated state migration disagrees or pending stages were lost")
	}
}

func TestGARETCommittedGroupOnlyMapRepricesAndReplicates(t *testing.T) {
	unifiedPBFTFixture(t)
	leader, catalog := garetTestNode(t, 0, 0)
	follower, _ := garetTestNode(t, 0, 1)
	retained := garetTestTx("retained", 10, 20, false)
	retained.GLASCallRelations = []core.GLASCallRelation{{Caller: garetTestAddr(20), Callee: garetTestAddr(30)}}
	if err := leader.installExecutionTransactions([]*core.Transaction{retained}, false); err != nil {
		t.Fatal(err)
	}
	old := leader.CurChain.Txpool.TxQueue[0]
	incoming := garetTestTx("incoming", 30, 20, false)
	transfer := &message.AccountTransferMsg{ATid: 1, ModifiedMap: map[string]uint64{}, GARET: &message.GARETPlacement{CatalogDigest: catalog.Digest(), Epoch: 1, Placement: []uint64{0, 0}}}
	decoded := message.DecodeAccountTransferMsg(transfer.Encode())
	if !reflect.DeepEqual(decoded.GARET, transfer.GARET) {
		t.Fatal("PBFT transfer lost group metadata")
	}
	for _, node := range []*PbftConsensusNode{leader, follower} {
		cdm := dataSupport.NewCLPADataSupport()
		if node == leader {
			cdm.ReceivedNewTx = []*core.Transaction{incoming}
			cdm.GARETPending = copyGARETPlacement(transfer.GARET)
		}
		inside := &CLPAPbftInsideExtraHandleMod{pbftNode: node, cdm: cdm}
		inside.accountTransfer_do(decoded)
		if cdm.GARETPending != nil || node.CurChain.Get_PartitionMap(garetTestAddr(35)) != 0 {
			t.Fatal("group-only map was not committed for future accounts")
		}
	}
	if leader.CurChain.Txpool.GetTxQueueLen() != 2 || follower.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("incorrect leader/follower pending pools")
	}
	for _, tx := range leader.CurChain.Txpool.TxQueue {
		if tx.GasUsed != 40 {
			t.Fatal("retained/incoming stage not repriced")
		}
	}
	if old.GasUsed != 60 || !bytes.Equal(leader.CurChain.CurrentBlock.Hash, follower.CurChain.CurrentBlock.Hash) {
		t.Fatal("receipt changed or replicas committed different group context")
	}
	if err := follower.CurChain.IsValidBlock(leader.generateExecutionBlock()); err != nil {
		t.Fatal(err)
	}
}

func TestGARETPartitionMetadataValidationAndReplay(t *testing.T) {
	unifiedPBFTFixture(t)
	node, catalog := garetTestNode(t, 0, 0)
	cdm := dataSupport.NewCLPADataSupport()
	outside := &CLPARelayOutsideModule{pbftNode: node, cdm: cdm}
	pm := &message.PartitionModifiedMap{PartitionModified: map[string]uint64{}, GARET: &message.GARETPlacement{CatalogDigest: catalog.Digest(), Epoch: 1, Placement: []uint64{1, 0}}}
	if accepted, err := outside.acceptGARETPartition(pm); err != nil || !accepted {
		t.Fatal(accepted, err)
	}
	if accepted, err := outside.acceptGARETPartition(pm); err != nil || accepted || len(cdm.ModifiedMap) != 1 {
		t.Fatal("partition replay changed control history", accepted, err)
	}
	wrong := *pm
	wrong.GARET = copyGARETPlacement(pm.GARET)
	wrong.GARET.Placement[0] = 0
	if _, err := outside.acceptGARETPartition(&wrong); err == nil {
		t.Fatal("conflicting pending groups accepted")
	}
	if err := node.validateGARETMetadata(nil, nil); err == nil {
		t.Fatal("missing GARET metadata accepted")
	}
	legacy := unifiedPBFTNode(t, 2)
	if err := legacy.validateGARETMetadata(pm.GARET, nil); err == nil {
		t.Fatal("GARET payload crossed into legacy mode")
	}
}
