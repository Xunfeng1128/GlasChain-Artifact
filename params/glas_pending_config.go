package params

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

const (
	GLASPendingPlanningSnapshot              = "snapshot"
	GLASPendingWindowRemainingDemand         = "remaining_demand"
	GLASPendingWindowQueueGas                = "queue_gas"
	GLASPendingSnapshotFullPool              = "full_pool"
	GLASPendingSnapshotLocalWindow           = "local_window"
	GLASPendingSnapshotTimingPollBoundary    = "poll_boundary"
	GLASPendingSnapshotTimingAfterLastCommit = "after_last_commit"
	GLASPendingExecutionQueue                = "queue"
	GLASPendingExecutionWindowFirst          = "window_first"
)

// The pending-input mode owns its planning parameters. Loading it never
// repurposes the legacy GLAS runtime or changes a legacy GLAS setting.
var (
	GLASPendingPlanningSource         = GLASPendingPlanningSnapshot
	GLASPendingHeadroomRatio          = 0.0
	GLASPendingEarlyReport            = false
	GLASPendingFullWindowEarly        = false
	GLASPendingWindowPolicy           = GLASPendingWindowQueueGas
	GLASPendingSnapshotPolicy         = GLASPendingSnapshotLocalWindow
	GLASPendingSnapshotTiming         = GLASPendingSnapshotTimingAfterLastCommit
	GLASPendingExecutionPolicy        = GLASPendingExecutionWindowFirst
	GLASPendingWindowAuditRecords     = false
	GLASPendingLocalSearch            = false // Try local moves only when the original planner keeps the installed map.
	GLASPendingWindowRatio            = 1.3
	GLASPendingMaxRecords             = 0
	GLASPendingMaxCallRelations       = 0
	GLASPendingObjectiveWeight        = 0.5
	GLASPendingFennelGamma            = 1.5
	GLASPendingFennelRestarts         = 2
	GLASPendingMaxMoves               = 0
	GLASPendingSnapshotTimeoutSeconds = 120
)

// The zero value disables the optional search. Reject a present non-boolean
// value (including null) just as the experiment runner does.
type pendingLocalSearchFlag bool

func (flag *pendingLocalSearchFlag) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case "true":
		*flag = true
	case "false":
		*flag = false
	default:
		return fmt.Errorf("GLASPendingLocalSearch must be a boolean")
	}
	return nil
}

type pendingEarlyReportFlag bool

func (flag *pendingEarlyReportFlag) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case "true":
		*flag = true
	case "false":
		*flag = false
	default:
		return fmt.Errorf("GLASPendingEarlyReport must be a boolean")
	}
	return nil
}

type pendingFullWindowEarlyFlag bool

func (flag *pendingFullWindowEarlyFlag) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case "true":
		*flag = true
	case "false":
		*flag = false
	default:
		return fmt.Errorf("GLASPendingFullWindowEarly must be a boolean")
	}
	return nil
}

// A present null/bool/string must not silently become the zero headroom.
type pendingHeadroom struct {
	value   float64
	present bool
}

func (v *pendingHeadroom) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return fmt.Errorf("GLASPendingHeadroomRatio must be numeric")
	}
	if err := json.Unmarshal(data, &v.value); err != nil {
		return err
	}
	v.present = true
	return nil
}

func GLASPendingFullWindowEarlyEnabled() bool {
	return ConsensusMethod == 12 && GLASPendingFullWindowEarly
}

type GLASPendingConfigFields struct {
	GLASPendingPlanningSource         *string                    `json:"GLASPendingPlanningSource"`
	GLASPendingHeadroomRatio          pendingHeadroom            `json:"GLASPendingHeadroomRatio"`
	GLASPendingEarlyReport            pendingEarlyReportFlag     `json:"GLASPendingEarlyReport"`
	GLASPendingFullWindowEarly        pendingFullWindowEarlyFlag `json:"GLASPendingFullWindowEarly"`
	GLASPendingSnapshotPolicy         *string                    `json:"GLASPendingSnapshotPolicy"`
	GLASPendingSnapshotTiming         *string                    `json:"GLASPendingSnapshotTiming"`
	GLASPendingExecutionPolicy        *string                    `json:"GLASPendingExecutionPolicy"`
	GLASPendingWindowPolicy           *string                    `json:"GLASPendingWindowPolicy"`
	GLASPendingWindowAuditRecords     *bool                      `json:"GLASPendingWindowAuditRecords"`
	GLASPendingLocalSearch            pendingLocalSearchFlag     `json:"GLASPendingLocalSearch"`
	GLASPendingWindowRatio            *float64                   `json:"GLASPendingWindowRatio"`
	GLASPendingMaxRecords             *int                       `json:"GLASPendingMaxRecords"`
	GLASPendingMaxCallRelations       *int                       `json:"GLASPendingMaxCallRelations"`
	GLASPendingObjectiveWeight        *float64                   `json:"GLASPendingObjectiveWeight"`
	GLASPendingFennelGamma            *float64                   `json:"GLASPendingFennelGamma"`
	GLASPendingFennelRestarts         *int                       `json:"GLASPendingFennelRestarts"`
	GLASPendingMaxMoves               *int                       `json:"GLASPendingMaxMoves"`
	GLASPendingSnapshotTimeoutSeconds *int                       `json:"GLASPendingSnapshotTimeoutSeconds"`
}

func loadGLASPendingConfig(c GLASPendingConfigFields) {
	GLASPendingPlanningSource, GLASPendingHeadroomRatio = GLASPendingPlanningSnapshot, 0
	if c.GLASPendingPlanningSource != nil {
		GLASPendingPlanningSource = *c.GLASPendingPlanningSource
	}
	if c.GLASPendingHeadroomRatio.present {
		GLASPendingHeadroomRatio = c.GLASPendingHeadroomRatio.value
	}
	GLASPendingLocalSearch = bool(c.GLASPendingLocalSearch)
	GLASPendingEarlyReport = bool(c.GLASPendingEarlyReport)
	GLASPendingFullWindowEarly = bool(c.GLASPendingFullWindowEarly)
	GLASPendingSnapshotPolicy, GLASPendingExecutionPolicy = GLASPendingSnapshotLocalWindow, GLASPendingExecutionWindowFirst
	if c.GLASPendingSnapshotPolicy != nil {
		GLASPendingSnapshotPolicy = *c.GLASPendingSnapshotPolicy
	}
	GLASPendingSnapshotTiming = GLASPendingSnapshotTimingAfterLastCommit
	if c.GLASPendingSnapshotTiming != nil {
		GLASPendingSnapshotTiming = *c.GLASPendingSnapshotTiming
	}
	if c.GLASPendingExecutionPolicy != nil {
		GLASPendingExecutionPolicy = *c.GLASPendingExecutionPolicy
	}
	GLASPendingWindowPolicy, GLASPendingWindowAuditRecords = GLASPendingWindowQueueGas, false
	if c.GLASPendingWindowPolicy != nil {
		GLASPendingWindowPolicy = *c.GLASPendingWindowPolicy
	}
	if c.GLASPendingWindowAuditRecords != nil {
		GLASPendingWindowAuditRecords = *c.GLASPendingWindowAuditRecords
	}
	GLASPendingWindowRatio, GLASPendingMaxRecords, GLASPendingMaxCallRelations = 1.3, 0, 0
	GLASPendingObjectiveWeight, GLASPendingFennelGamma, GLASPendingFennelRestarts = 0.5, 1.5, 2
	GLASPendingMaxMoves, GLASPendingSnapshotTimeoutSeconds = 0, 120
	if c.GLASPendingWindowRatio != nil {
		GLASPendingWindowRatio = *c.GLASPendingWindowRatio
	}
	if c.GLASPendingMaxRecords != nil {
		GLASPendingMaxRecords = *c.GLASPendingMaxRecords
	}
	if c.GLASPendingMaxCallRelations != nil {
		GLASPendingMaxCallRelations = *c.GLASPendingMaxCallRelations
	}
	if c.GLASPendingObjectiveWeight != nil {
		GLASPendingObjectiveWeight = *c.GLASPendingObjectiveWeight
	}
	if c.GLASPendingFennelGamma != nil {
		GLASPendingFennelGamma = *c.GLASPendingFennelGamma
	}
	if c.GLASPendingFennelRestarts != nil {
		GLASPendingFennelRestarts = *c.GLASPendingFennelRestarts
	}
	if c.GLASPendingMaxMoves != nil {
		GLASPendingMaxMoves = *c.GLASPendingMaxMoves
	}
	if c.GLASPendingSnapshotTimeoutSeconds != nil {
		GLASPendingSnapshotTimeoutSeconds = *c.GLASPendingSnapshotTimeoutSeconds
	}
}

func GLASPendingBlocksPerEpoch() uint64 {
	return uint64(ExperimentBlocksPerEpoch)
}

// GLASPendingGasBudget is a soft per-shard selection budget, not a block
// capacity. Zero ratio explicitly selects the full visible queue instead.
func GLASPendingGasBudget() (uint64, error) {
	if math.IsNaN(GLASPendingWindowRatio) || math.IsInf(GLASPendingWindowRatio, 0) || GLASPendingWindowRatio < 0 {
		return 0, fmt.Errorf("GLASPendingWindowRatio must be finite and nonnegative")
	}
	if GLASPendingWindowRatio == 0 {
		return 0, nil
	}
	value := math.Ceil(float64(GLASPendingBlocksPerEpoch()) * float64(BlockGasLimit) * GLASPendingWindowRatio)
	if value < 1 || math.IsInf(value, 0) || value >= float64(math.MaxUint64) {
		return 0, fmt.Errorf("GLASPendingWindowRatio yields an invalid or overflowing planning Gas budget")
	}
	return uint64(value), nil
}

// ValidateGLASPendingConfig admits the single GlasChain experiment path.
func ValidateGLASPendingConfig() error {
	if !GLASPendingFullWindowEarly || GLASPendingEarlyReport || GLASPendingLocalSearch ||
		GLASPendingPlanningSource != GLASPendingPlanningSnapshot || GLASPendingHeadroomRatio != 0 ||
		GLASPendingSnapshotPolicy != GLASPendingSnapshotLocalWindow ||
		GLASPendingSnapshotTiming != GLASPendingSnapshotTimingAfterLastCommit ||
		GLASPendingExecutionPolicy != GLASPendingExecutionWindowFirst ||
		GLASPendingWindowPolicy != GLASPendingWindowQueueGas ||
		GLASPendingMaxRecords != 0 || GLASPendingMaxCallRelations != 0 ||
		GLASPendingWindowRatio != 1.3 || ExperimentEpochCount != 0 || ExperimentBlocksPerEpoch != 6 {
		return fmt.Errorf("GlasChain requires the early full window, ratio 1.3, six ordinary blocks per epoch and no run quota")
	}
	if !UnifiedExecution || !UnifiedMetrics || !UnifiedPackingOptimized ||
		CLPAReconfigurationScheduler != CLPAReconfigurationSchedulerPoll ||
		CLPAReconfigurationProfile != CLPAReconfigurationProfileInstallClock {
		return fmt.Errorf("GlasChain requires unified execution, metrics, optimized packing and poll/install_clock")
	}
	if err := ValidateUnifiedExecutionConfig(); err != nil {
		return err
	}
	if err := ValidateExperimentEpochQuota(); err != nil {
		return err
	}
	if Block_Interval <= 0 || int64(ReconfigTimeGap) > int64(time.Duration(1<<63-1)/time.Second) {
		return fmt.Errorf("GlasChain requires a positive block interval and a representable control period")
	}
	if _, err := GLASPendingGasBudget(); err != nil {
		return err
	}
	if GLASPendingObjectiveWeight != 0.5 || GLASPendingFennelGamma != 1.5 || GLASPendingFennelRestarts != 2 || GLASPendingMaxMoves != 0 {
		return fmt.Errorf("GlasChain requires the paper objective weight and Fennel settings")
	}
	if GLASPendingSnapshotTimeoutSeconds <= 0 || int64(GLASPendingSnapshotTimeoutSeconds) > int64(time.Duration(1<<63-1)/time.Second) {
		return fmt.Errorf("GLASPendingSnapshotTimeoutSeconds must be positive and fit a duration")
	}
	return nil
}
