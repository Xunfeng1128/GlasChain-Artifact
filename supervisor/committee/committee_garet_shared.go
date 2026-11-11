package committee

import (
	"fmt"
	"time"

	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/partition/garet"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
)

// GARETSharedCommitteeModule changes only the control lifecycle. The existing
// GARET observer, fixed sampling clock, lagged prediction, catalog and FS remain
// authoritative. Its legacy constructor and event/default path are unchanged.
type GARETSharedCommitteeModule struct {
	*CLPACommitteeModule
	algorithm *GARETCommitteeModule
}

func NewGARETSharedCommitteeModule(ip map[uint64]map[uint64]string, ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, path string, count, batch, frequency int) *GARETSharedCommitteeModule {
	algorithm := NewGARETCommitteeModule(ip, ss, sl, path, count, batch, frequency)
	base := NewCLPACommitteeModule(ip, ss, sl, path, count, batch, frequency)
	module := &GARETSharedCommitteeModule{CLPACommitteeModule: base, algorithm: algorithm}
	base.skipHistoryGraph = true
	base.inputRouting = algorithm.fetchModifiedMap
	// The private observer's recorder stays nil. Unified injection accounting is
	// performed exactly once by the base at this same dispatch timestamp.
	base.dispatchObserved = algorithm.observeDispatch
	base.partitionPlanner = module.planGARET
	base.partitionGARET = module.groupPayload
	algorithm.writeDiagnostic(map[string]interface{}{"type": "shared_runtime", "scheduler": params.CLPAReconfigurationScheduler, "profile": params.CLPAReconfigurationProfile,
		"history_clock": "first_dispatch_fixed_slots", "history_lag_cycles": 1, "cold_start_control": "identity_placement_without_FS"})
	return module
}

func (module *GARETSharedCommitteeModule) MsgSendingControl() {
	defer func() {
		m := module.algorithm
		m.mu.Lock()
		terminal := map[string]interface{}{"type": "controller_end", "late_history_stages": m.lateStages,
			"late_history_gas": m.lateGas, "observed_blocks": len(m.blocks), "observed_sources": len(m.stages),
			"last_prediction_cycle": m.lastPredictionCycle}
		m.mu.Unlock()
		m.writeDiagnostic(terminal)
		if m.metrics != nil {
			if err := m.metrics.Close(); err != nil {
				panic(err)
			}
		}
	}()
	module.CLPACommitteeModule.MsgSendingControl()
}

func (module *GARETSharedCommitteeModule) HandleBlockInfo(block *message.BlockInfoMsg) {
	module.HandleBlockInfoAt(block, time.Now())
}

func (module *GARETSharedCommitteeModule) HandleBlockInfoAt(block *message.BlockInfoMsg, at time.Time) {
	// Preserve the shared epoch/height observations and the original physical
	// stage observer. Never turn completed logical TOPs into GARET workload.
	module.algorithm.HandleBlockInfoAt(block, at)
	module.CLPACommitteeModule.HandleBlockInfo(block)
}

func (module *GARETSharedCommitteeModule) groupPayload(epoch uint64) *message.GARETPlacement {
	m := module.algorithm
	return &message.GARETPlacement{CatalogDigest: m.catalog.Digest(), Epoch: epoch,
		Placement: append([]uint64(nil), m.placement...)}
}

func (module *GARETSharedCommitteeModule) planGARET(round uint64) (map[string]uint64, error) {
	return module.planGARETAt(round, time.Now())
}

// now is injected only for deterministic tests of the unchanged sample clock.
// It never becomes a second runtime scheduler.
func (module *GARETSharedCommitteeModule) planGARETAt(round uint64, now time.Time) (map[string]uint64, error) {
	m := module.algorithm
	m.mu.Lock()
	if m.start.IsZero() {
		m.mu.Unlock()
		return nil, fmt.Errorf("GARET planning precedes first dispatch")
	}
	prediction, ready, err := m.history.Predict(now.Sub(m.start).Nanoseconds())
	lateStages, lateGas := m.lateStages, m.lateGas
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	before := append([]uint64(nil), m.placement...)
	changed := make(map[string]uint64)
	row := map[string]interface{}{"type": "plan", "round": round, "trigger_elapsed_ns": now.Sub(m.start).Nanoseconds(),
		"history_clock": "first_dispatch_fixed_slots", "history_lag_cycles": 1,
		"late_history_stages": lateStages, "late_history_gas": lateGas, "before_placement": before}
	started := time.Now()
	if !ready || prediction.Cycle <= m.lastPredictionCycle {
		// Shared control still has a real install identity and ordinary-block
		// resume gate. Insufficient/newly unavailable history never runs FS.
		row["type"], row["fs_ran"] = "cold_start", false
		if ready {
			row["type"], row["prediction_cycle"] = "unchanged_prediction", prediction.Cycle
		}
	} else {
		placement, err := garet.FullShuffle(prediction.WeightedGas, m.shards)
		if err != nil {
			return nil, err
		}
		for address := range m.known {
			group, err := m.catalog.GroupFor(address)
			if err != nil {
				return nil, err
			}
			if placement[group] != before[group] {
				changed[address] = placement[group]
			}
		}
		m.placement, m.lastPredictionCycle = placement, prediction.Cycle
		row["fs_ran"], row["prediction_cycle"] = true, prediction.Cycle
		row["history_start_ns"], row["history_end_ns"] = prediction.SourceStartNS, prediction.SourceEndNS
		row["weighted_gas_numerators"], row["weight_denominator"] = prediction.WeightedGas, prediction.WeightDenominator
	}
	moved := 0
	for group, shard := range m.placement {
		if before[group] != shard {
			moved++
		}
	}
	row["planning_ns"], row["placement"] = time.Since(started).Nanoseconds(), m.placement
	row["moved_groups"], row["changed_observed_accounts"] = moved, len(changed)
	m.writeDiagnostic(row)
	module.partitionPrepareSend(round)
	return changed, nil
}
