package committee

import (
	"encoding/json"
	"log"
	"time"
)

// Diagnostics contain scalar observations only: never retain snapshots, call
// traces, or placements, and never participate in a control/selection decision.
type pendingPlanTiming struct {
	PhasesNS        map[string]int64
	ReceivedRecords int
	SelectedRecords int
	started         time.Time
	phaseStarted    time.Time
	phase           string
}

func (module *GLASPendingCommitteeModule) diagnosticLogger() *log.Logger {
	if module == nil || module.CLPACommitteeModule == nil || module.sl == nil {
		return nil
	}
	return module.sl.Slog
}

func newPendingPlanTiming(started time.Time) *pendingPlanTiming {
	return &pendingPlanTiming{PhasesNS: make(map[string]int64), started: started}
}

func (t *pendingPlanTiming) begin(phase string) {
	t.phase, t.phaseStarted = phase, time.Now()
}

func (t *pendingPlanTiming) end() {
	t.PhasesNS[t.phase] += time.Since(t.phaseStarted).Nanoseconds()
}

func writePendingDiagnostic(logger *log.Logger, prefix string, value interface{}) {
	if logger == nil {
		return
	}
	// Encoding/logging failures must never alter the experiment's control path.
	if payload, err := json.Marshal(value); err == nil {
		logger.Printf("%s %s", prefix, payload)
	}
}
