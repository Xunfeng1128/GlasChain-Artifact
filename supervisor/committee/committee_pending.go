package committee

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"

	"blockEmulator/glasfreight/optimizer"
	gplanner "blockEmulator/glasfreight/planner"
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
)

// GLASPendingCommitteeModule provides GlasChain's early full-window planner
// to the shared CLPA reconfiguration lifecycle.
type GLASPendingCommitteeModule struct {
	*CLPACommitteeModule
	earlyState *earlyPendingState
}

func NewGLASPendingCommitteeModule(table map[uint64]map[uint64]string, ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, path string, count, batch, frequency int) *GLASPendingCommitteeModule {
	if err := params.ValidateGLASPendingConfig(); err != nil {
		log.Panic(err)
	}
	base := NewCLPACommitteeModule(table, ss, sl, path, count, batch, frequency)
	base.skipHistoryGraph = true
	module := &GLASPendingCommitteeModule{CLPACommitteeModule: base}
	module.initPendingEarly()
	return module
}

func (module *GLASPendingCommitteeModule) HandleOtherMessage(raw []byte) {
	kind, content := message.SplitMessage(raw)
	if !module.observeEarlyPendingMessage(kind, content) {
		module.CLPACommitteeModule.HandleOtherMessage(raw)
	}
}

func (module *GLASPendingCommitteeModule) MsgSendingControl() {
	defer module.stopPendingEarly()
	module.CLPACommitteeModule.MsgSendingControl()
}

func (module *GLASPendingCommitteeModule) HandleBlockInfo(block *message.BlockInfoMsg) {
	module.observeEarlyPendingBlock(block)
	module.CLPACommitteeModule.HandleBlockInfo(block)
}

type pendingComputedPlan struct {
	mapping map[string]uint64
	audit   func() error
}

func (module *GLASPendingCommitteeModule) computePendingPlan(
	round, target uint64, snapshots map[uint64]message.PendingSnapshot,
	resolveShard func(string) uint64, timing *pendingPlanTiming,
) (pendingComputedPlan, error) {
	fullRecords, fullCalls, err := pendingSnapshotScopeTotals(snapshots)
	if err != nil {
		return pendingComputedPlan{}, err
	}
	budget, err := params.GLASPendingGasBudget()
	if err != nil {
		return pendingComputedPlan{}, err
	}
	stats := gplanner.WindowStats{GasBudgetPerShard: budget, PerShardGas: make([]uint64, params.ShardNum)}
	timing.begin("snapshot_convert")
	pools, current, err := convertPendingPools(snapshots, round, target, uint64(params.ShardNum), params.UnifiedCrossCallGas, resolveShard, &stats)
	timing.end()
	if err != nil {
		return pendingComputedPlan{}, err
	}
	stats.FullRecords, stats.FullCallRelations = fullRecords, fullCalls
	stats.Truncated = stats.SelectedRecords != stats.FullRecords
	timing.ReceivedRecords, timing.SelectedRecords = stats.SelectedRecords, stats.SelectedRecords
	blocks := params.GLASPendingBlocksPerEpoch()
	resolve := func(address string) (uint64, error) { return resolveShard(address), nil }
	weight := params.GLASPendingObjectiveWeight
	timing.begin("candidate_plan")
	result, err := gplanner.PlanPendingDemand(pools, current, gplanner.Config{
		Epoch: round, ShardCount: uint64(params.ShardNum), BlocksPerEpoch: blocks, BlockGasLimit: params.BlockGasLimit,
		PlannerMode: "cost_fennel", SelectionMode: optimizer.SelectionWeightedCost, AttachedCallObjective: true,
		Iterations: 1, EvaluationBudget: 3, ObjectiveWeight: &weight, Prepare: pendingStagePrice, Expand: pendingStageExpansion(round), ResolveShard: resolve,
		OptimizerConfig: optimizer.Config{ShardCount: uint64(params.ShardNum), BlockGasLimit: params.BlockGasLimit, BlocksPerEpoch: blocks,
			FennelGamma: params.GLASPendingFennelGamma, Restarts: params.GLASPendingFennelRestarts,
			RefinementCandidates: 1, MaxMoves: params.GLASPendingMaxMoves, ObjectiveWeight: weight, SelectionMode: optimizer.SelectionWeightedCost},
	})
	timing.end()
	if err != nil {
		return pendingComputedPlan{}, err
	}
	timing.begin("mapping_diff")
	dirty := make(map[string]uint64)
	for address, shard := range result.Placement {
		if resolveShard(address) != shard {
			dirty[address] = shard
		}
	}
	timing.end()
	digest := pools.Digest
	return pendingComputedPlan{mapping: dirty, audit: func() error {
		if err := writePendingPlanAudit(round, target, digest, stats, result, time.Since(timing.started)); err != nil {
			return err
		}
		if module.sl != nil {
			module.sl.Slog.Printf("GLAS_Pending round=%d window_policy=queue_gas window_ms=0 visible=%d selected=%d calls=%d truncated=%t record_cap=%t call_cap=%t candidate_maps=%d moved_accounts=%d objective=%.8f planner_ms=%d simulator=disabled\n", round, stats.FullRecords, stats.SelectedRecords, stats.SelectedCallRelations, stats.Truncated, stats.RecordLimitHit, stats.CallLimitHit, len(result.Evaluations), len(dirty), result.Score.FinalObjectiveValue(result.SelectionMode, weight), time.Since(timing.started).Milliseconds())
		}
		return nil
	}}, nil
}

func pendingScoreAudit(score optimizer.ExecutionScore, weight float64) map[string]interface{} {
	return map[string]interface{}{"objective": score.FinalObjectiveValue(optimizer.SelectionWeightedCost, weight), "capacity_pressure": score.CapacityPressure,
		"extra_cross_gas": score.NewCrossGas, "base_demand_gas": score.BaseDemandGas, "total_remaining_gas": score.FullDemandTotalGas,
		"max_shard_remaining_gas": score.FullDemandMaxGas, "remaining_gas_per_shard": score.FullDemandPerShardGas, "moved_accounts": score.Moves}
}
func writePendingPlanAudit(round, target uint64, digest string, stats gplanner.WindowStats, result gplanner.PendingResult, elapsed time.Duration) error {
	evaluations := make([]map[string]interface{}, 0, len(result.Evaluations))
	for _, evaluation := range result.Evaluations {
		row := pendingScoreAudit(evaluation.Score, result.ObjectiveWeight)
		row["index"] = evaluation.Index
		row["placement_digest"] = evaluation.PlacementDigest
		row["improved"] = evaluation.Improved
		evaluations = append(evaluations, row)
	}
	audit := map[string]interface{}{"schema_version": 1, "mode": "GLAS_Pending", "round": round, "target_sequence": target, "snapshot_scope": "per_shard_visible_queue_at_pause", "planning_window_digest": digest, "window": stats, "objective_version": result.ObjectiveVersion, "objective_weight": result.ObjectiveWeight, "simulator_enabled": false, "predicted_completion_available": false, "baseline": pendingScoreAudit(result.BaselineScore, result.ObjectiveWeight), "selected": pendingScoreAudit(result.Score, result.ObjectiveWeight), "candidates": evaluations, "planning_ms": elapsed.Milliseconds()}
	audit["snapshot_scope"] = "per_shard_early_local_window_with_forecast_relays"
	audit["window_policy"] = "queue_gas"
	audit["window_gas_basis"] = "current_map_local_pending_stage"
	audit["window_full_visible"] = false
	audit["window_selection_ms"] = int64(0)
	audit["window_records_available"] = false
	audit["snapshot_policy"] = "local_window"
	audit["execution_policy"] = "window_first"
	audit["received_candidate_records"] = stats.SelectedRecords
	audit["received_candidate_call_relations"] = stats.SelectedCallRelations
	audit["window_visible_records_complete"] = false
	if err := os.MkdirAll(params.DataWrite_path, 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(params.DataWrite_path, "glas_pending_plans.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	encodeErr := json.NewEncoder(f).Encode(audit)
	closeErr := f.Close()
	if encodeErr != nil {
		return encodeErr
	}
	return closeErr
}
