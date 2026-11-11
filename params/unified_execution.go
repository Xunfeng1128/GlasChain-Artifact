package params

import (
	"fmt"
	"math"
	"strings"
	"time"
)

func GLASPendingFormalE2Enabled() bool {
	return GLASPendingFormalRunEnabled() && E2ObservationSeconds > 0
}

// ValidateUnifiedExecutionConfig is opt-in: configurations without
// UnifiedExecution retain their legacy dataset and execution behavior.
func ValidateUnifiedExecutionConfig() error {
	if UnifiedPackingOptimized && !UnifiedExecution {
		return fmt.Errorf("UnifiedPackingOptimized requires unified shared execution")
	}
	if UnifiedMetrics && !UnifiedExecution {
		return fmt.Errorf("UnifiedMetrics requires UnifiedExecution=true")
	}
	if FormalMapCheckpoint != 0 {
		switch ConsensusMethod {
		case 1, 8, 9, 10, 12: // CLPA, TxAllo, P-Louvain, GARET and GLAS_Pending.
		default:
			return fmt.Errorf("FormalMapCheckpoint supports CLPA, TxAllo, P-Louvain, GARET and GLAS_Pending")
		}
		if !UnifiedExecution || !UnifiedMetrics || FormalMapCheckpoint < 1 || FormalMapCheckpoint >= TotalDataSize ||
			E2PrefixCount != 0 || E2ObservationSeconds != 0 || E2PrefixFile != "" ||
			!UsesSharedCLPAReconfiguration(ConsensusMethod, CLPAReconfigurationScheduler) {
			return fmt.Errorf("FormalMapCheckpoint requires a valid prefix within a unified shared full run")
		}
	}
	if E2ObservationSeconds != 0 || E2PrefixCount != 0 || E2PrefixFile != "" {
		if !UnifiedMetrics || !UnifiedExecution {
			return fmt.Errorf("E2 observation requires UnifiedMetrics and UnifiedExecution")
		}
		validDuration := E2ObservationSeconds > 0 && int64(E2ObservationSeconds) <= math.MaxInt64/int64(time.Second)
		if GLASPendingFormalRunEnabled() {
			if !validDuration || E2PrefixCount != 0 || E2PrefixFile != "" {
				return fmt.Errorf("formal GLAS E2 requires a positive E2ObservationSeconds and no input prefix")
			}
		} else if !validDuration || E2PrefixCount <= 0 || E2PrefixCount > TotalDataSize || strings.TrimSpace(E2PrefixFile) == "" {
			return fmt.Errorf("E2 observation requires positive E2ObservationSeconds, E2PrefixCount within TotalDataSize, and E2PrefixFile")
		}
		if ConsensusMethod != 3 && ((ConsensusMethod != 1 && ConsensusMethod != 9 && ConsensusMethod != 12) || !UsesSharedCLPAReconfiguration(ConsensusMethod, CLPAReconfigurationScheduler)) {
			return fmt.Errorf("E2 observation supports Hash, CLPA, shared P_Louvain_Contrib or GLAS_Pending")
		}
	}
	if !UnifiedExecution {
		return nil
	}
	switch ConsensusMethod {
	case 1, 3, 4, 7, 8, 9, 10, 12:
	default:
		return fmt.Errorf("UnifiedExecution supports only CLPA, Relay, CLPA_Gas, P_Louvain_Gas, TxAllo_Contrib, P_Louvain_Contrib, GARET and GlasChain")
	}
	if BlockCapacityMode != "gas" || BlockGasLimit == 0 {
		return fmt.Errorf("UnifiedExecution requires BlockCapacityMode=gas and a positive BlockGasLimit")
	}
	if strings.TrimSpace(UnifiedTransactionsFile) == "" || strings.TrimSpace(UnifiedCallsFile) == "" {
		return fmt.Errorf("UnifiedTransactionsFile and UnifiedCallsFile are required")
	}
	if InjectSpeed <= 0 || TxBatchSize <= 0 || TotalDataSize <= 0 {
		return fmt.Errorf("UnifiedExecution requires positive InjectSpeed, TxBatchSize and TotalDataSize")
	}
	if ConsensusMethod != 3 && ReconfigTimeGap <= 0 {
		return fmt.Errorf("UnifiedExecution requires a positive ReconfigTimeGap for dynamic baselines")
	}
	return nil
}
