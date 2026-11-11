package params

import "fmt"

// GlasChain sets the ordinary block count and stops by workload completion
// or observation time. A positive run quota is rejected.
var ExperimentEpochCount, ExperimentBlocksPerEpoch int

func GLASPendingFormalRunEnabled() bool {
	return GLASPendingFullWindowEarlyEnabled() && ExperimentEpochCount == 0 && ExperimentBlocksPerEpoch == 6
}

func FixedBlockEpochBoundariesEnabled() bool {
	return GLASPendingFormalRunEnabled()
}

// Early reports target a reconfiguration after an ordinary epoch.
func GLASPendingEarlyRoundEnabled(epoch uint64) bool {
	if epoch == 0 || !GLASPendingFormalRunEnabled() {
		return false
	}
	stride := uint64(ExperimentBlocksPerEpoch) + 1
	return epoch <= ^uint64(0)/stride
}

func ValidateExperimentEpochQuota() error {
	if ExperimentEpochCount == 0 && ExperimentBlocksPerEpoch == 0 {
		return nil
	}
	if GLASPendingFormalRunEnabled() {
		if !UnifiedExecution || !UnifiedMetrics ||
			E2PrefixCount != 0 || E2PrefixFile != "" || E2ObservationSeconds < 0 {
			return fmt.Errorf("full-window GLAS formal runs require six ordinary blocks per epoch, unified metrics and execution, and no E2 prefix")
		}
		return nil
	}
	return fmt.Errorf("fixed epoch boundaries are available only for formal GlasChain")
}

// A partition block occupies one sequence after each completed ordinary epoch.
func ExperimentEpochTarget(epoch uint64) uint64       { return epoch * uint64(ExperimentBlocksPerEpoch+1) }
func ExperimentEpochLastOrdinary(epoch uint64) uint64 { return ExperimentEpochTarget(epoch+1) - 1 }
