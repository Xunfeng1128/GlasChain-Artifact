package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const pendingFullWindowMaxRecords = 262144

// Called after an ordinary block has been applied, with pbftLock held. For a
// six-block epoch, target is the partition sequence and target-2 is block five.
// Select and mark records here so the next block's Relay2 can carry the marks.
func (cphm *CLPAPbftInsideExtraHandleMod) observePendingEarlyCommit(block *core.Block) {
	p := cphm.pbftNode
	if !params.GLASPendingFullWindowEarlyEnabled() || !params.FixedBlockEpochBoundariesEnabled() ||
		params.ExperimentBlocksPerEpoch < 2 || block == nil || block.Header == nil ||
		p.stopSignal.Load() || uint64(p.view.Load()) != p.NodeID {
		return
	}
	cphm.cdm.PartitionStateLock.Lock()
	epoch := cphm.cdm.AccountTransferRound + 1
	cphm.cdm.PartitionStateLock.Unlock()
	if !params.GLASPendingEarlyRoundEnabled(epoch) {
		return
	}
	target := params.ExperimentEpochTarget(epoch)
	if target < 2 || block.Header.Number != target-2 ||
		cphm.pendingEarlyReportedEpoch == epoch {
		return
	}
	cphm.pendingEarlyReportedEpoch = epoch // at most one capture per replica/epoch
	budget, err := params.GLASPendingGasBudget()
	if err != nil {
		cphm.logPendingEarlyFailure(epoch, err)
		return
	}
	started := time.Now()
	report := message.PendingEarlyReport{Epoch: epoch, TargetSeq: target, FromShard: p.ShardID,
		Leader: p.NodeID, View: uint64(p.view.Load()), ObservedBlockHash: fmt.Sprintf("%x", block.Hash)}
	limit := p.pbftChainConfig.BlockGasLimit
	p.unifiedExecutionMu.Lock()
	cross, window, forecast, err := p.CurChain.Txpool.CaptureFullPendingWindow(
		epoch, budget, limit, pendingFullWindowMaxRecords, p.CurChain.ExecutionShardResolver())
	p.unifiedExecutionMu.Unlock()
	if err != nil {
		cphm.logPendingEarlyFailure(epoch, err)
		return
	}
	captured := time.Now()
	report.FullWindow, report.ForecastCrossTOP = window.Records, cross
	report.LastBlockDigest = message.PendingEarlyBlockDigest(forecast)
	report.FullRecords, report.FullCallRelations = window.FullRecords, window.FullCallRelations
	go func() {
		p.pbftLock.Lock()
		p.pbftLock.Unlock()
		cphm.sendPendingEarlyReport(report, len(forecast), len(window.Records), started, captured)
	}()
}

func (cphm *CLPAPbftInsideExtraHandleMod) sendPendingEarlyReport(report message.PendingEarlyReport, forecastCount, windowCount int, started, captured time.Time) {
	p := cphm.pbftNode
	cphm.cdm.PartitionStateLock.Lock()
	stillCurrent := cphm.cdm.AccountTransferRound+1 == report.Epoch
	cphm.cdm.PartitionStateLock.Unlock()
	if p.stopSignal.Load() || !stillCurrent || uint64(p.view.Load()) != report.View {
		return
	}
	payload, err := json.Marshal(report)
	if err != nil {
		cphm.logPendingEarlyFailure(report.Epoch, err)
		return
	}
	address := p.ip_nodeTable[params.SupervisorShard][0]
	if address == "" {
		cphm.logPendingEarlyFailure(report.Epoch, fmt.Errorf("missing supervisor address"))
		return
	}
	timeout := time.Duration(params.GLASPendingSnapshotTimeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(p.clpaControlContext(), timeout)
	defer cancel()
	if err := networks.TcpDialContext(ctx, message.MergeMessage(message.CPendingEarlyReport, payload), address); err != nil {
		if !p.stopSignal.Load() {
			cphm.logPendingEarlyFailure(report.Epoch, err)
		}
		return
	}
	if p.pl != nil && p.pl.Plog != nil {
		p.pl.Plog.Printf("GLAS_Pending early report: epoch=%d target=%d observed=%d forecast=%d candidates=%d residual=%d capture_us=%d total_ms=%d bytes=%d",
			report.Epoch, report.TargetSeq, report.TargetSeq-2, forecastCount, windowCount, report.FullRecords,
			captured.Sub(started).Microseconds(), time.Since(started).Milliseconds(), len(payload))
	}
}

func (cphm *CLPAPbftInsideExtraHandleMod) logPendingEarlyFailure(epoch uint64, err error) {
	panic(fmt.Sprintf("GLAS full-window early report failed: epoch=%d: %v", epoch, err))
}
