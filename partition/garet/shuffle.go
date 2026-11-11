package garet

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
)

type shardLoad struct {
	id     int
	gas    uint64
	groups int
}
type shardHeap []shardLoad

func (h shardHeap) Len() int { return len(h) }
func (h shardHeap) Less(i, j int) bool {
	if h[i].gas != h[j].gas {
		return h[i].gas < h[j].gas
	}
	if h[i].groups != h[j].groups {
		return h[i].groups < h[j].groups
	}
	return h[i].id < h[j].id
}
func (h shardHeap) Swap(i, j int)           { h[i], h[j] = h[j], h[i] }
func (h *shardHeap) Push(value interface{}) { *h = append(*h, value.(shardLoad)) }
func (h *shardHeap) Pop() interface{} {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

// FullShuffle is Algorithm 1: all shard loads start at zero, then each group
// in descending predicted-Gas order is assigned to the least-loaded shard.
// Equal-load shards prefer fewer assigned groups, then ascending shard ID;
// equal-Gas groups use ascending group ID. This deterministic tie convention
// spreads zero-load groups. There is no capacity rejection, graph objective,
// current-placement preference, or migration penalty.
func FullShuffle(weightedGas []uint64, shards int) ([]uint64, error) {
	if shards <= 0 {
		return nil, fmt.Errorf("shard count must be positive")
	}
	order := make([]int, len(weightedGas))
	for group := range order {
		order[group] = group
	}
	sort.Slice(order, func(i, j int) bool {
		a, b := order[i], order[j]
		if weightedGas[a] != weightedGas[b] {
			return weightedGas[a] > weightedGas[b]
		}
		return a < b
	})
	loads := make(shardHeap, shards)
	for shard := range loads {
		loads[shard].id = shard
	}
	heap.Init(&loads)
	placement := make([]uint64, len(weightedGas))
	for _, group := range order {
		least := heap.Pop(&loads).(shardLoad)
		if math.MaxUint64-least.gas < weightedGas[group] {
			return nil, fmt.Errorf("predicted shard Gas overflows uint64 for shard %d", least.id)
		}
		placement[group] = uint64(least.id)
		least.gas += weightedGas[group]
		least.groups++
		heap.Push(&loads, least)
	}
	return placement, nil
}
