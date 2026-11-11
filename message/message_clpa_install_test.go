package message

import "testing"

func TestPartitionMappingDigestCanonical(t *testing.T) {
	if PartitionMappingDigest(nil) != PartitionMappingDigest(map[string]uint64{}) {
		t.Fatal("empty delta is not canonical")
	}
	if PartitionMappingDigest(map[string]uint64{"a": 1, "b": 2}) != PartitionMappingDigest(map[string]uint64{"b": 2, "a": 1}) {
		t.Fatal("map iteration order changes digest")
	}
	if PartitionMappingDigest(map[string]uint64{"a": 1}) == PartitionMappingDigest(map[string]uint64{"a": 2}) {
		t.Fatal("different placement shared digest")
	}
}
