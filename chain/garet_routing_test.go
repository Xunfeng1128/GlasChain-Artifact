package chain

import (
	"fmt"
	"testing"

	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/partition/garet"
)

func garetAddress(value int) string { return fmt.Sprintf("%040x", value) }

func garetRoutingFixture(t *testing.T) (*BlockChain, *garet.Catalog) {
	t.Helper()
	catalog, err := garet.NewCatalog([]string{garetAddress(10), garetAddress(20), garetAddress(30), garetAddress(40)}, 2)
	if err != nil {
		t.Fatal(err)
	}
	bc := &BlockChain{ChainConfig: &params.ChainConfig{ShardNums: 2, BlockGasLimit: 100}, PartitionMap: make(map[string]uint64), Txpool: core.NewTxPool()}
	if err := bc.InitGARETRouting(catalog); err != nil {
		t.Fatal(err)
	}
	return bc, catalog
}

func TestGARETGroupOnlyUpdateRoutesFutureAccountsAndSnapshots(t *testing.T) {
	bc, catalog := garetRoutingFixture(t)
	future := garetAddress(15)
	old := bc.ExecutionShardResolver()
	if bc.Get_PartitionMap(future) != 0 || len(bc.PartitionMap) != 0 {
		t.Fatal("initial groups created account overrides")
	}
	groups := []uint64{1, 0}
	if err := bc.InstallGARETPlacement(catalog.Digest(), 1, groups, nil); err != nil {
		t.Fatal(err)
	}
	groups[0] = 0
	if old(future) != 0 || bc.Get_PartitionMap(future) != 1 || bc.Get_PartitionMap(garetAddress(50)) != 0 {
		t.Fatal("future account fell back to hash or routing snapshot mutated")
	}
	if len(bc.PartitionMap) != 0 {
		t.Fatal("group placement materialized address overrides")
	}
	if err := bc.InstallGARETPlacement(catalog.Digest(), 2, []uint64{0, 1}, map[string]uint64{future: 0}); err != nil {
		t.Fatal(err)
	}
	if bc.Get_PartitionMap(future) != 0 {
		t.Fatal("previous placement pinned a seen account")
	}
}

func TestGARETRejectsBadPlacementWithoutChangingRouting(t *testing.T) {
	bc, catalog := garetRoutingFixture(t)
	cases := []struct {
		name, digest string
		epoch        uint64
		groups       []uint64
		delta        map[string]uint64
	}{
		{"digest", "wrong", 1, []uint64{1, 0}, nil},
		{"epoch", catalog.Digest(), 2, []uint64{1, 0}, nil},
		{"count", catalog.Digest(), 1, []uint64{1}, nil},
		{"shard", catalog.Digest(), 1, []uint64{2, 0}, nil},
		{"delta", catalog.Digest(), 1, []uint64{1, 0}, map[string]uint64{garetAddress(10): 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := bc.InstallGARETPlacement(tc.digest, tc.epoch, tc.groups, tc.delta); err == nil {
				t.Fatal("invalid placement accepted")
			}
			if bc.GARETEpoch() != 0 || bc.Get_PartitionMap(garetAddress(10)) != 0 {
				t.Fatal("invalid placement changed routing")
			}
		})
	}
}

func TestGARETRepriceUsesGroupMapWithoutAccountOverrides(t *testing.T) {
	enableUnifiedTest(t)
	bc, catalog := garetRoutingFixture(t)
	tx := unifiedTestTx("group-pricing", garetAddress(10), 40)
	tx.Recipient = garetAddress(20)
	tx.GLASCallRelations = []core.GLASCallRelation{{Caller: garetAddress(20), Callee: garetAddress(35)}}
	priced, err := bc.PrepareUnifiedTransactions([]*core.Transaction{tx})
	if err != nil {
		t.Fatal(err)
	}
	bc.Txpool.AddTxs2Pool(priced)
	if priced[0].GasUsed != 60 {
		t.Fatal("initial group cross-call price incorrect")
	}
	if err := bc.InstallGARETPlacement(catalog.Digest(), 1, []uint64{0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	if err := bc.RepriceUnifiedPool(); err != nil {
		t.Fatal(err)
	}
	if bc.Txpool.TxQueue[0].GasUsed != 40 || priced[0].GasUsed != 60 {
		t.Fatal("group repricing failed or mutated old execution object")
	}
	if err := bc.validateUnifiedRecords(bc.Txpool.TxQueue); err != nil {
		t.Fatal(err)
	}
}

func TestGARETRoutingSnapshotRemainsCompleteDuringUpdates(t *testing.T) {
	bc, catalog := garetRoutingFixture(t)
	stop := make(chan struct{})
	observed := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				observed <- nil
				return
			default:
				resolve := bc.ExecutionShardResolver()
				if resolve(garetAddress(15))+resolve(garetAddress(35)) != 1 {
					observed <- fmt.Errorf("resolver mixed groups from different epochs")
					return
				}
			}
		}
	}()
	for epoch := uint64(1); epoch <= 100; epoch++ {
		if err := bc.InstallGARETPlacement(catalog.Digest(), epoch, []uint64{epoch % 2, 1 - epoch%2}, nil); err != nil {
			close(stop)
			<-observed
			t.Fatal(err)
		}
	}
	close(stop)
	if err := <-observed; err != nil {
		t.Fatal(err)
	}
}

func TestGARETAdmissionRejectsMalformedAddressesBeforePricing(t *testing.T) {
	enableUnifiedTest(t)
	bc, _ := garetRoutingFixture(t)
	tx := unifiedTestTx("invalid-address", garetAddress(10), 40)
	tx.Recipient = garetAddress(20)
	tx.GLASCallRelations = []core.GLASCallRelation{{Caller: garetAddress(20), Callee: "malformed"}}
	if _, err := bc.PrepareUnifiedTransactions([]*core.Transaction{tx}); err == nil {
		t.Fatal("malformed internal endpoint accepted")
	}
	if err := bc.validateUnifiedRecords([]*core.Transaction{tx}); err == nil {
		t.Fatal("malformed committed endpoint accepted")
	}
}
