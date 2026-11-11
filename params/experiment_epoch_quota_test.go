package params

import "testing"

func TestFullWindowFormalBlockBoundariesAreIndependentOfStopPolicy(t *testing.T) {
	oldE, oldB, oldM := ExperimentEpochCount, ExperimentBlocksPerEpoch, ConsensusMethod
	oldFull, oldReport := GLASPendingFullWindowEarly, GLASPendingEarlyReport
	oldX, oldU, oldP, oldF, oldS := UnifiedExecution, UnifiedMetrics, E2PrefixCount, E2PrefixFile, E2ObservationSeconds
	oldI, oldG := Block_Interval, ReconfigTimeGap
	t.Cleanup(func() {
		ExperimentEpochCount, ExperimentBlocksPerEpoch, ConsensusMethod = oldE, oldB, oldM
		GLASPendingFullWindowEarly, GLASPendingEarlyReport = oldFull, oldReport
		UnifiedExecution, UnifiedMetrics, E2PrefixCount, E2PrefixFile, E2ObservationSeconds = oldX, oldU, oldP, oldF, oldS
		Block_Interval, ReconfigTimeGap = oldI, oldG
	})
	ExperimentEpochCount, ExperimentBlocksPerEpoch, ConsensusMethod = 0, 6, 12
	GLASPendingFullWindowEarly, GLASPendingEarlyReport = true, false
	UnifiedExecution, UnifiedMetrics, E2PrefixCount, E2PrefixFile = true, true, 0, ""
	Block_Interval, ReconfigTimeGap = 8000, 32
	for _, seconds := range []int{0, 960} {
		E2ObservationSeconds = seconds
		if err := ValidateExperimentEpochQuota(); err != nil {
			t.Fatal(err)
		}
		if !GLASPendingFormalRunEnabled() || !FixedBlockEpochBoundariesEnabled() ||
			!GLASPendingEarlyRoundEnabled(100) || GLASPendingEarlyRoundEnabled(0) || GLASPendingEarlyRoundEnabled(^uint64(0)) ||
			GLASPendingBlocksPerEpoch() != 6 || ExperimentEpochTarget(100) != 700 {
			t.Fatalf("wrong formal boundary policy with E2ObservationSeconds=%d", seconds)
		}
	}
	E2PrefixCount = 1
	if ValidateExperimentEpochQuota() == nil {
		t.Fatal("formal E2 accepted prefix stopping")
	}
	E2PrefixCount, E2ObservationSeconds, ExperimentEpochCount = 0, 0, 10
	if GLASPendingFormalRunEnabled() ||
		GLASPendingEarlyRoundEnabled(9) || GLASPendingEarlyRoundEnabled(10) {
		t.Fatal("a positive run quota enabled the formal block boundaries")
	}
	ExperimentEpochCount = 0
	for _, method := range []int{1, 12} {
		ConsensusMethod = method
		GLASPendingFullWindowEarly, GLASPendingEarlyReport = false, false
		if FixedBlockEpochBoundariesEnabled() || ValidateExperimentEpochQuota() == nil {
			t.Fatalf("legacy method %d accepted a block count without a quota", method)
		}
	}
	GLASPendingEarlyReport = true
	if FixedBlockEpochBoundariesEnabled() || ValidateExperimentEpochQuota() == nil {
		t.Fatal("ID reports accepted a block count without a quota")
	}
}
