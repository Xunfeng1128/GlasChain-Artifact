package committee

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"blockEmulator/core"
	"blockEmulator/glasfreight/model"
	gplanner "blockEmulator/glasfreight/planner"
	"blockEmulator/message"
	"blockEmulator/workload"
)

func pendingWorkloadTx(t *testing.T, source, from, to string, base, nonce uint64, placement model.Placement, relations ...core.GLASCallRelation) *core.Transaction {
	t.Helper()
	input := &model.Transaction{SourceHash: source, From: from, To: to, Value: "1", GasUsed: base, HasGasUsed: true,
		FromIsContract: false, ToIsContract: true}
	for _, relation := range relations {
		input.Calls = append(input.Calls, model.Call{Caller: relation.Caller, Callee: relation.Callee, IsCCGEdge: true, CallerIsSCA: true, CalleeIsSCA: true})
	}
	tx, err := workload.CoreTransaction(input, nonce, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.PriceExecutionStage(tx, func(address string) uint64 { return placement[address] }); err != nil {
		t.Fatal(err)
	}
	return tx
}

func pendingWorkloadFixture(t *testing.T) (map[uint64]message.PendingSnapshot, model.Placement) {
	t.Helper()
	placement := model.Placement{"a": 0, "b": 0, "c": 1, "d": 2, "e": 2}
	call := core.GLASCallRelation{Caller: "b", Callee: "c"}
	local := pendingWorkloadTx(t, "local", "a", "b", 100, 1, placement, call, call)
	remote := pendingWorkloadTx(t, "remote", "d", "c", 40, 2, placement, core.GLASCallRelation{Caller: "c", Callee: "e"})
	carriedTop := pendingWorkloadTx(t, "carry", "a", "e", 70, 3, placement, call, call)
	carry, err := core.NewExecutionRelay2(carriedTop, 3, func(address string) uint64 { return placement[address] })
	if err != nil {
		t.Fatal(err)
	}
	return map[uint64]message.PendingSnapshot{
		0: {FromShard: 0, Epoch: 4, TargetSeq: 15, Txs: []*core.Transaction{local}},
		1: {FromShard: 1, Epoch: 4, TargetSeq: 15},
		2: {FromShard: 2, Epoch: 4, TargetSeq: 15, Txs: []*core.Transaction{remote, carry}},
	}, placement
}

func pendingPools(snapshots map[uint64]message.PendingSnapshot, epoch, target, shards, callGas uint64, resolve func(string) uint64) (gplanner.FrozenPools, model.Placement, error) {
	windows := make(map[uint64]message.PendingSnapshot, len(snapshots))
	for shard, snapshot := range snapshots {
		snapshot.LocalWindow = true
		snapshot.TotalRecords = len(snapshot.Txs)
		snapshot.TotalCallRelations = 0
		for _, tx := range snapshot.Txs {
			snapshot.TotalCallRelations += len(tx.GLASCallRelations)
		}
		windows[shard] = snapshot
	}
	stats := gplanner.WindowStats{PerShardGas: make([]uint64, shards)}
	return convertPendingPools(windows, epoch, target, shards, callGas, resolve, &stats)
}

func TestPendingWorkloadConversionIsImmutableVisibleQueueOnly(t *testing.T) {
	snapshots, placement := pendingWorkloadFixture(t)
	before, err := json.Marshal(snapshots)
	if err != nil {
		t.Fatal(err)
	}
	pools, current, err := pendingPools(snapshots, 4, 15, 3, 7, func(address string) uint64 { return placement[address] })
	if err != nil {
		t.Fatal(err)
	}
	if len(pools.Records[0]) != 1 || len(pools.Records[1]) != 0 || len(pools.Records[2]) != 2 || pools.Digest == "" || !reflect.DeepEqual(current, placement) {
		t.Fatalf("visible queues or mapping changed: %+v %v", pools, current)
	}
	top, carry := pools.Records[0][0], pools.Records[2][1]
	if top.FromIsContract || !top.ToIsContract || top.Priority != 1 || top.DependencyKey != "sender:a" || top.ArrivalSeq != 1 || top.OriginShard != 0 || top.QueueIndex != 0 {
		t.Fatalf("lost immutable account/queue metadata: %+v", top)
	}
	if carry.Stage != gplanner.Relay2Stage || carry.Owner != "e" || carry.Priority != 0 || carry.DependencyKey != "logical:carry" || carry.StageEpoch != 3 || !carry.StageEpochSet || carry.QueueIndex != 1 {
		t.Fatalf("carried stage was replayed/relabelled: %+v", carry)
	}
	// Editing the planning copy cannot alter the live/wire source objects.
	pools.Records[0][0].Calls[0].Callee = "changed"
	current["a"] = 2
	after, _ := json.Marshal(snapshots)
	if string(before) != string(after) || placement["a"] != 0 {
		t.Fatal("pending conversion shares mutable source state")
	}
	// A shard with no visible queue is valid. There is deliberately no check
	// against a global injected-source universe or pending network delivery.
	for shard, snapshot := range snapshots {
		snapshot.Txs = nil
		snapshots[shard] = snapshot
	}
	if empty, _, err := pendingPools(snapshots, 4, 15, 3, 7, func(address string) uint64 { return placement[address] }); err != nil || len(empty.Records) != 0 {
		t.Fatalf("empty visible queues incorrectly required global bijection: %+v %v", empty, err)
	}
}

func TestPendingWorkloadPricingMatchesRuntimeForCandidateMaps(t *testing.T) {
	snapshots, current := pendingWorkloadFixture(t)
	pools, _, err := pendingPools(snapshots, 4, 15, 3, 7, func(address string) uint64 { return current[address] })
	if err != nil {
		t.Fatal(err)
	}
	for _, placement := range []model.Placement{
		current,
		{"a": 0, "b": 0, "c": 0, "d": 0, "e": 0},
		{"a": 2, "b": 1, "c": 0, "d": 0, "e": 1},
	} {
		resolve := func(address string) uint64 { return placement[address] }
		demand, err := gplanner.EvaluateFrozenDemand(pools, placement, gplanner.SimulationConfig{
			ShardCount: 3, BlocksPerEpoch: 6, BlockGasLimit: 1000, Prepare: pendingStagePrice, Expand: pendingStageExpansion(4),
		})
		if err != nil {
			t.Fatal(err)
		}
		want := make([]uint64, 3)
		for _, snapshot := range snapshots {
			for _, original := range snapshot.Txs {
				tx := *original
				if err := core.PriceExecutionStage(&tx, resolve); err != nil {
					t.Fatal(err)
				}
				owner := tx.Sender
				if tx.Relayed {
					owner = tx.Recipient
				}
				want[resolve(owner)] += tx.GasUsed
				if !tx.Relayed && resolve(tx.Sender) != resolve(tx.Recipient) {
					relay, err := core.NewExecutionRelay2(&tx, 4, resolve)
					if err != nil {
						t.Fatal(err)
					}
					want[resolve(relay.Recipient)] += relay.GasUsed
				}
			}
		}
		if !reflect.DeepEqual(demand.PerShardGas, want) {
			t.Fatalf("candidate map %v: demand=%v runtime=%v", placement, demand.PerShardGas, want)
		}
		if reflect.DeepEqual(placement, current) && (demand.TotalGas != 285 || demand.NewCrossGas != 75 || !reflect.DeepEqual(want, []uint64{114, 47, 124})) {
			t.Fatalf("wrong repeated calls or carried sunk cost: %+v", demand)
		}
	}
}

func TestPendingWorkloadRejectsInvalidSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(map[uint64]message.PendingSnapshot, model.Placement)
		want   string
	}{
		{"missing shard", func(s map[uint64]message.PendingSnapshot, _ model.Placement) { delete(s, 1) }, "received"},
		{"wrong round", func(s map[uint64]message.PendingSnapshot, _ model.Placement) { x := s[0]; x.Epoch++; s[0] = x }, "invalid pending snapshot"},
		{"wrong pause height", func(s map[uint64]message.PendingSnapshot, _ model.Placement) { x := s[0]; x.TargetSeq++; s[0] = x }, "invalid pending snapshot"},
		{"wrong reporter", func(s map[uint64]message.PendingSnapshot, _ model.Placement) { x := s[0]; x.FromShard = 1; s[0] = x }, "invalid pending snapshot"},
		{"snapshot error", func(s map[uint64]message.PendingSnapshot, _ model.Placement) {
			x := s[0]
			x.Error = "failure"
			s[0] = x
		}, "invalid pending snapshot"},
		{"duplicate stage", func(s map[uint64]message.PendingSnapshot, _ model.Placement) {
			x := s[0]
			x.Txs = append(x.Txs, x.Txs[0])
			s[0] = x
		}, "duplicate pending stage"},
		{"wrong physical owner", func(s map[uint64]message.PendingSnapshot, _ model.Placement) {
			x, y := s[0], s[1]
			y.Txs, x.Txs = x.Txs, nil
			s[0], s[1] = x, y
		}, "owner is not shard"},
		{"wrong call unit", func(s map[uint64]message.PendingSnapshot, _ model.Placement) { s[0].Txs[0].GLASCallGasPerCall++ }, "unit cost"},
		{"wrong billed gas", func(s map[uint64]message.PendingSnapshot, _ model.Placement) { s[0].Txs[0].GasUsed++ }, "inconsistent"},
		{"missing endpoint", func(s map[uint64]message.PendingSnapshot, _ model.Placement) {
			s[0].Txs[0].GLASCallRelations[0].Callee = ""
		}, "empty call endpoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshots, placement := pendingWorkloadFixture(t)
			tc.mutate(snapshots, placement)
			_, _, err := pendingPools(snapshots, 4, 15, 3, 7, func(address string) uint64 { return placement[address] })
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
}
