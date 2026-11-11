package committee

import (
	"fmt"
	"testing"
	"time"

	"blockEmulator/core"
	"blockEmulator/glasfreight/model"
	"blockEmulator/message"
	"blockEmulator/params"
)

func TestFullPendingEarlyUsesObservedRelayOverStaleTOPOrForecast(t *testing.T) {
	oldFee := params.UnifiedCrossCallGas
	params.UnifiedCrossCallGas = 7
	t.Cleanup(func() { params.UnifiedCrossCallGas = oldFee })
	placement := model.Placement{"a": 1, "b": 0}
	resolve := func(addr string) uint64 { return placement[addr] }
	top := pendingWorkloadTx(t, "cross", "a", "b", 60, 1, placement,
		core.GLASCallRelation{Caller: "a", Callee: "b"})
	relay, err := core.NewExecutionRelay2(top, 1, resolve)
	if err != nil {
		t.Fatal(err)
	}
	actual := message.PendingEarlyReport{FromShard: 0, FullRecords: 1, FullCallRelations: 1, FullWindow: []*core.Transaction{relay}}
	for _, source := range []message.PendingEarlyReport{
		{FromShard: 1, FullRecords: 1, FullCallRelations: 1, FullWindow: []*core.Transaction{top}},
		{FromShard: 1, ForecastCrossTOP: []*core.Transaction{top}},
	} {
		snapshots, err := fullPendingEarlySnapshots(map[uint64]message.PendingEarlyReport{0: actual, 1: source}, 2, 10, resolve)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots[0].Txs) != 1 || snapshots[0].Txs[0] != relay || len(snapshots[1].Txs) != 0 {
			t.Fatalf("plan must contain only the actually observed relay: %+v", snapshots)
		}
	}
	conflict := *relay
	conflict.GLASStageEpoch++
	actual.FullWindow = []*core.Transaction{&conflict}
	_, err = fullPendingEarlySnapshots(map[uint64]message.PendingEarlyReport{
		0: actual, 1: {FromShard: 1, ForecastCrossTOP: []*core.Transaction{top}},
	}, 2, 10, resolve)
	if err == nil {
		t.Fatal("inconsistent observed stage was silently substituted for the forecast")
	}
}

func TestFullPendingEarlyPlansWholeLocalWindowAndForecast(t *testing.T) {
	module := fullWindowCommitteeFixture(t)
	oldMethod, oldEarly, oldBlocks := params.ConsensusMethod, params.GLASPendingFullWindowEarly, params.ExperimentBlocksPerEpoch
	oldGas, oldFee, oldBlock, oldGap, oldRatio := params.BlockGasLimit, params.UnifiedCrossCallGas, params.Block_Interval, params.ReconfigTimeGap, params.GLASPendingWindowRatio
	t.Cleanup(func() {
		params.ConsensusMethod, params.GLASPendingFullWindowEarly, params.ExperimentBlocksPerEpoch = oldMethod, oldEarly, oldBlocks
		params.BlockGasLimit, params.UnifiedCrossCallGas, params.Block_Interval, params.ReconfigTimeGap, params.GLASPendingWindowRatio = oldGas, oldFee, oldBlock, oldGap, oldRatio
	})
	params.ConsensusMethod, params.GLASPendingFullWindowEarly, params.ExperimentBlocksPerEpoch = 12, true, 6
	params.BlockGasLimit, params.UnifiedCrossCallGas, params.Block_Interval, params.ReconfigTimeGap, params.GLASPendingWindowRatio = 100, 7, 1000, 2, 1.3
	placement := model.Placement{"a": 0, "b": 1}
	resolve := func(addr string) uint64 { return placement[addr] }
	local := make([]*core.Transaction, 0, 9)
	for i := uint64(1); i <= 9; i++ {
		local = append(local, pendingWorkloadTx(t, fmt.Sprintf("local-%d", i), "b", "b", 87, i, placement))
	}
	top := pendingWorkloadTx(t, "forecast", "a", "b", 90, 10, placement)
	snapshots, err := fullPendingEarlySnapshots(map[uint64]message.PendingEarlyReport{
		0: {FromShard: 0, ForecastCrossTOP: []*core.Transaction{top}},
		1: {FromShard: 1, FullRecords: len(local), FullWindow: local},
	}, 1, 7, resolve)
	if err != nil {
		t.Fatal(err)
	}
	timing := newPendingPlanTiming(time.Now())
	_, err = module.computePendingPlan(1, 7, snapshots, resolve, timing)
	if err != nil {
		t.Fatal(err)
	}
	if timing.ReceivedRecords != 10 || timing.SelectedRecords != 10 {
		t.Fatalf("second selector trimmed work or created a global priority list: %+v", timing)
	}
}
