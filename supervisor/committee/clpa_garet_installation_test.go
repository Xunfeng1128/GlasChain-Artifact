package committee

import (
	"blockEmulator/message"
	"testing"
)

func TestGARETSharedInstallationBindsCompleteGroupPlan(t *testing.T) {
	tracker := new(clpaInstallationTracker)
	if err := tracker.begin(1, 2, 1); err != nil {
		t.Fatal(err)
	}
	groups := &message.GARETPlacement{CatalogDigest: "catalog", Epoch: tracker.preparedEpoch(), Placement: []uint64{1, 0}}
	if _, err := tracker.bindPlan(nil, groups); err != nil {
		t.Fatal(err)
	}
	receipt := message.PartitionInstalled{Epoch: 1, TargetSeq: 2, CommittedSeq: 2, MappingDigest: message.PartitionPlanDigest(nil, groups)}
	if tracker.accept(receipt).IsZero() {
		t.Fatal("valid group-only installation was not accepted")
	}
	groups.Placement[0] = 0
	if _, err := tracker.bindPlan(nil, groups); err == nil {
		t.Fatal("changed group assignment was accepted with unchanged address map")
	}
	groups.Epoch++
	if _, err := tracker.bindPlan(nil, groups); err == nil {
		t.Fatal("wrong group epoch bound")
	}
}
