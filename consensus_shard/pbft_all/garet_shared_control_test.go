package pbft_all

import (
	"reflect"
	"testing"

	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/partition/garet"
)

func garetSharedWorker(t *testing.T, shard uint64) (*PbftConsensusNode, *CLPAPbftInsideExtraHandleMod, *CLPARelayOutsideModule) {
	t.Helper()
	node, _ := garetTestNode(t, shard, 1) // follower: receipt recording needs no network
	cdm := dataSupport.NewCLPADataSupport()
	cdm.ControlEventsEnabled, cdm.ControlPolling = true, true
	cdm.PendingEpoch, cdm.PendingTargetSeq = 1, 2
	inside := &CLPAPbftInsideExtraHandleMod{pbftNode: node, cdm: cdm}
	outside := &CLPARelayOutsideModule{pbftNode: node, cdm: cdm}
	node.ihm, node.ohm = inside, outside
	return node, inside, outside
}

func TestGARETSharedControlMigratesAndAcknowledgesGroupOnlyMap(t *testing.T) {
	unifiedPBFTFixture(t)
	n0, i0, o0 := garetSharedWorker(t, 0)
	n1, i1, o1 := garetSharedWorker(t, 1)
	catalog, err := garet.NewCatalog([]string{garetTestAddr(10), garetTestAddr(20), garetTestAddr(30), garetTestAddr(40)}, 2)
	if err != nil {
		t.Fatal(err)
	}
	groups := &message.GARETPlacement{CatalogDigest: catalog.Digest(), Epoch: 1, Placement: []uint64{1, 0}}
	pm := &message.PartitionModifiedMap{Epoch: 1, PartitionModified: map[string]uint64{}, GARET: groups}
	for _, outside := range []*CLPARelayOutsideModule{o0, o1} {
		outside.acceptCLPAEventMap(pm)
		outside.acceptCLPAEventMap(pm)
		if len(outside.cdm.ModifiedMap) != 1 || !outside.cdm.PartitionOn {
			t.Fatal("poll map missing or duplicated")
		}
	}
	top, relay := garetTestTx("top", 10, 30, false), garetTestTx("relay", 30, 20, true)
	if err := n0.installExecutionTransactions([]*core.Transaction{top}, false); err != nil {
		t.Fatal(err)
	}
	if err := n0.installExecutionTransactions([]*core.Transaction{relay}, true); err != nil {
		t.Fatal(err)
	}
	out0, err := i0.prepareGARETMigration()
	if err != nil {
		t.Fatal(err)
	}
	out1, err := i1.prepareGARETMigration()
	if err != nil {
		t.Fatal(err)
	}
	if out0[1].Epoch != 1 || len(out0[1].Txs) != 2 || out0[1].GARET == nil {
		t.Fatal("shared outbound lost epoch, TOP/RELAY2 or group map")
	}
	o0.acceptCLPAEventMigration(out1[0])
	o1.acceptCLPAEventMigration(out0[1])
	o1.acceptCLPAEventMigration(out0[1])
	if !i0.cdm.CollectOver || !i1.cdm.CollectOver || len(i1.cdm.AccountStateTx) != 1 {
		t.Fatal("shared collection did not complete exactly once")
	}
	for _, inside := range []*CLPAPbftInsideExtraHandleMod{i0, i1} {
		ok, request := inside.proposePartition()
		if !ok {
			t.Fatal("partition proposal unavailable")
		}
		transfer := message.DecodeAccountTransferMsg(request.Msg.Content)
		if !reflect.DeepEqual(transfer.GARET, groups) || transfer.TargetSeq != 2 {
			t.Fatal("PBFT proposal lost common/group identity")
		}
		inside.accountTransfer_do(transfer)
		inside.pbftNode.recordCLPAPartitionInstall(request, 2)
		receipt := inside.cdm.LatestPartitionInstall
		if receipt == nil || receipt.MappingDigest != message.PartitionPlanDigest(nil, groups) || receipt.TargetSeq != 2 {
			t.Fatalf("wrong group install receipt %+v", receipt)
		}
		// Committed replay must not re-open the round or demand next-epoch metadata.
		outside := inside.pbftNode.ohm.(*CLPARelayOutsideModule)
		outside.acceptCLPAEventMap(pm)
		if inside.cdm.PartitionOn {
			t.Fatal("committed replay reopened map")
		}
	}
	if len(n0.CurChain.Txpool.TxQueue) != 0 || len(n1.CurChain.Txpool.TxQueue) != 2 {
		t.Fatal("pending owners were lost or retained at old shard")
	}
	if n1.CurChain.Get_PartitionMap(garetTestAddr(15)) != 1 {
		t.Fatal("unseen account did not follow committed group")
	}
}

func TestGARETSharedControlRejectsConflictingAndForeignMetadata(t *testing.T) {
	unifiedPBFTFixture(t)
	for _, kind := range []string{"catalog", "group", "missing", "outer_epoch"} {
		t.Run(kind, func(t *testing.T) {
			n, _, outside := garetSharedWorker(t, 0)
			catalog, err := garet.NewCatalog([]string{garetTestAddr(10), garetTestAddr(20), garetTestAddr(30), garetTestAddr(40)}, 2)
			if err != nil {
				t.Fatal(err)
			}
			pm := &message.PartitionModifiedMap{Epoch: 1, GARET: &message.GARETPlacement{CatalogDigest: catalog.Digest(), Epoch: 1, Placement: []uint64{1, 0}}}
			outside.acceptCLPAEventMap(pm)
			other := *pm
			other.GARET = copyGARETPlacement(pm.GARET)
			switch kind {
			case "catalog":
				other.GARET.CatalogDigest = "wrong"
			case "group":
				other.GARET.Placement[0] = 0
			case "missing":
				other.GARET = nil
			case "outer_epoch":
				other.GARET.Epoch = 2
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("invalid group map accepted")
					}
				}()
				outside.acceptCLPAEventMap(&other)
			}()
			if n.CurChain.GARETEpoch() != 0 {
				t.Fatal("receipt validation installed a map")
			}
		})
	}
}

func TestGARETSharedWorkerScopeIsExplicitPollOnly(t *testing.T) {
	old := params.CLPAReconfigurationScheduler
	t.Cleanup(func() { params.CLPAReconfigurationScheduler = old })
	for _, mode := range []string{"GARET", "TxAllo_Contrib", "P_Louvain_Contrib", "P_Louvain_Gas"} {
		params.CLPAReconfigurationScheduler = "event"
		if clpaControlEventsEnabled(mode) {
			t.Fatal("legacy default changed", mode)
		}
		params.CLPAReconfigurationScheduler = "poll"
		if !clpaControlEventsEnabled(mode) {
			t.Fatal("explicit poll not enabled", mode)
		}
	}
	for _, mode := range []string{"Broker", "CLPA_Broker", "GLAS_FREIGHT", "Relay"} {
		if clpaControlEventsEnabled(mode) {
			t.Fatal("legacy independent runtime changed", mode)
		}
	}
}
