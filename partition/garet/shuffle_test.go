package garet

import (
	"math"
	"reflect"
	"testing"
)

func TestFullShuffleHandCalculationAndTies(t *testing.T) {
	for _, test := range []struct {
		gas      []uint64
		shards   int
		expected []uint64
	}{
		{[]uint64{8, 7, 6, 5, 4}, 2, []uint64{0, 1, 1, 0, 0}},
		{[]uint64{2, 4, 4, 2}, 2, []uint64{0, 0, 1, 1}},
		// Group-count tie breaking spreads zero-Gas groups deterministically.
		{[]uint64{0, 0, 0, 0, 0}, 2, []uint64{0, 1, 0, 1, 0}},
		// Equal Gas load can arise with different numbers of assigned groups.
		{[]uint64{4, 2, 2, 0, 0}, 2, []uint64{0, 1, 1, 0, 0}},
		// A capacity-constrained algorithm would differ; GARET assigns all groups.
		{[]uint64{3, 3, 2, 2, 2}, 2, []uint64{0, 1, 0, 1, 0}},
	} {
		result, err := FullShuffle(test.gas, test.shards)
		if err != nil || !reflect.DeepEqual(result, test.expected) {
			t.Fatalf("Gas %v: got %v want %v, %v", test.gas, result, test.expected, err)
		}
	}
}

func TestFullShuffleRejectsOverflowAndInvalidShards(t *testing.T) {
	if _, err := FullShuffle([]uint64{math.MaxUint64, 1}, 1); err == nil {
		t.Fatal("overflow accepted")
	}
	if _, err := FullShuffle([]uint64{1}, 0); err == nil {
		t.Fatal("zero shards accepted")
	}
	if _, err := FullShuffle([]uint64{math.MaxUint64, 1}, 2); err != nil {
		t.Fatal(err)
	}
}
