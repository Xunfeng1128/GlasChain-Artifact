package params

import "fmt"

const (
	CLPAReconfigurationSchedulerEvent = "event"
	CLPAReconfigurationSchedulerPoll  = "poll"

	CLPAReconfigurationProfileFixed        = "fixed"
	CLPAReconfigurationProfileBlockResume  = "block_resume"
	CLPAReconfigurationProfileInstallClock = "install_clock"
)

// Scheduling selects how control work is driven, independently of the profile's
// injection-resume gate and interval-clock origin.
// Existing CLPA modes share both schedulers. Additional dynamic baselines use
// the shared adapter only when poll is explicitly selected.
var CLPAReconfigurationScheduler = CLPAReconfigurationSchedulerEvent

// The default preserves the existing post-install clock and ordinary-block gate.
var CLPAReconfigurationProfile = CLPAReconfigurationProfileInstallClock

type CLPASchedulerConfigFields struct {
	CLPAReconfigurationScheduler *string `json:"CLPAReconfigurationScheduler"`
	CLPAReconfigurationProfile   *string `json:"CLPAReconfigurationProfile"`
}

func loadCLPASchedulerConfig(c CLPASchedulerConfigFields) error {
	// ReadConfigFile may be called more than once by tooling/tests. An omitted
	// or null field must always restore its default, not retain a prior load.
	CLPAReconfigurationScheduler = CLPAReconfigurationSchedulerEvent
	CLPAReconfigurationProfile = CLPAReconfigurationProfileInstallClock
	if c.CLPAReconfigurationScheduler != nil {
		CLPAReconfigurationScheduler = *c.CLPAReconfigurationScheduler
	}
	if c.CLPAReconfigurationProfile != nil {
		CLPAReconfigurationProfile = *c.CLPAReconfigurationProfile
	}
	return ValidateCLPASchedulerConfig()
}

// UsesSharedCLPAReconfiguration is also the constructor/worker dispatch rule.
// Omitted configuration resolves to event and preserves the original paths of
// the opt-in baselines. Hash and legacy GLAS/Broker never opt in.
func UsesSharedCLPAReconfiguration(method int, scheduler string) bool {
	if scheduler != CLPAReconfigurationSchedulerEvent && scheduler != CLPAReconfigurationSchedulerPoll {
		return false
	}
	switch method {
	case 1, 4, 12:
		return true
	case 7, 8, 9, 10:
		return scheduler == CLPAReconfigurationSchedulerPoll
	default:
		return false
	}
}

func ValidateCLPASchedulerConfig() error {
	if CLPAReconfigurationScheduler != CLPAReconfigurationSchedulerEvent &&
		CLPAReconfigurationScheduler != CLPAReconfigurationSchedulerPoll {
		return fmt.Errorf("CLPAReconfigurationScheduler must be event or poll")
	}
	if CLPAReconfigurationScheduler == CLPAReconfigurationSchedulerPoll &&
		!UsesSharedCLPAReconfiguration(ConsensusMethod, CLPAReconfigurationScheduler) {
		return fmt.Errorf("CLPAReconfigurationScheduler=poll requires a supported shared CLPA reconfiguration mode")
	}
	if CLPAReconfigurationProfile != CLPAReconfigurationProfileFixed &&
		CLPAReconfigurationProfile != CLPAReconfigurationProfileBlockResume &&
		CLPAReconfigurationProfile != CLPAReconfigurationProfileInstallClock {
		return fmt.Errorf("CLPAReconfigurationProfile must be fixed, block_resume or install_clock")
	}
	if CLPAReconfigurationProfile != CLPAReconfigurationProfileInstallClock &&
		!UsesSharedCLPAReconfiguration(ConsensusMethod, CLPAReconfigurationScheduler) {
		return fmt.Errorf("CLPAReconfigurationProfile=%s requires an active shared CLPA reconfiguration mode (additional baselines must explicitly select poll)", CLPAReconfigurationProfile)
	}
	return nil
}
