package message

import "testing"

func TestGARETPlanDigestBindsGroupOnlyRouting(t *testing.T) {
	if PartitionPlanDigest(nil, nil) != PartitionMappingDigest(nil) {
		t.Fatal("non-GARET installation identity changed")
	}
	g := &GARETPlacement{CatalogDigest: "catalog", Epoch: 1, Placement: []uint64{0, 1}}
	baseline := PartitionPlanDigest(nil, g)
	if baseline != PartitionPlanDigest(map[string]uint64{}, g) {
		t.Fatal("nil and empty address delta differ")
	}
	for _, other := range []*GARETPlacement{
		{CatalogDigest: "other", Epoch: 1, Placement: []uint64{0, 1}},
		{CatalogDigest: "catalog", Epoch: 2, Placement: []uint64{0, 1}},
		{CatalogDigest: "catalog", Epoch: 1, Placement: []uint64{1, 0}},
	} {
		if baseline == PartitionPlanDigest(nil, other) {
			t.Fatal("different group decision has same identity")
		}
	}
}
