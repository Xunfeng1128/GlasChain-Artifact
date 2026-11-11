package supervisor

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/unifiedmetrics"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func (d *Supervisor) initUnifiedMetrics(method string) {
	if !params.UnifiedMetrics {
		return
	}
	if err := params.ValidateUnifiedExecutionConfig(); err != nil {
		log.Panic(err)
	}
	configPath := strings.TrimSpace(os.Getenv("BLOCKEMULATOR_CONFIG"))
	if configPath == "" {
		configPath = "paramsConfig.json"
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Panicf("read metrics configuration identity: %v", err)
	}
	configPath, err = filepath.Abs(configPath)
	if err != nil {
		log.Panic(err)
	}
	r, err := unifiedmetrics.New(filepath.Join(params.DataWrite_path, "unified_metrics"), unifiedmetrics.RunConfig{
		Method: method, ConsensusMethod: params.ConsensusMethod, ShardCount: int(d.ChainConfig.ShardNums),
		ExpectedTransactions: uint64(params.TotalDataSize), InjectSpeed: params.InjectSpeed,
		BlockGasLimit: params.BlockGasLimit, CrossCallGas: params.UnifiedCrossCallGas,
		TransactionsFile: params.UnifiedTransactionsFile, CallsFile: params.UnifiedCallsFile,
		ConfigPath: configPath, ConfigSHA256: fmt.Sprintf("%x", sha256.Sum256(data)),
		E2ObservationSeconds: params.E2ObservationSeconds, E2PrefixCount: params.E2PrefixCount, E2PrefixFile: params.E2PrefixFile,
		E2FixedWindow: params.GLASPendingFormalE2Enabled(),
	})
	if err != nil {
		log.Panicf("initialize unified metrics: %v", err)
	}
	setter, ok := d.comMod.(interface {
		SetUnifiedRecorder(*unifiedmetrics.Recorder)
	})
	if !ok {
		log.Panicf("committee %s has no unified injection observer", method)
	}
	d.unifiedRecorder = r
	if params.E2PrefixCount > 0 || params.GLASPendingFormalRunEnabled() {
		d.e2ReceiptFence = new(sync.RWMutex)
	}
	d.unifiedMetricsClosed = false
	d.unifiedBlockDigests = make(map[[2]uint64][32]byte)
	setter.SetUnifiedRecorder(r)
	if params.GLASPendingFormalRunEnabled() {
		control, ok := d.comMod.(interface{ SetFormalRunContext(context.Context) })
		if !ok {
			log.Panicf("committee %s does not support the GLAS formal stop policy", method)
		}
		ctx, cancel := context.WithCancel(context.Background())
		d.formalRunCancel = cancel
		control.SetFormalRunContext(ctx)
	}
}

// Deduplicate all algorithms before graph, stop-signal or metric side effects.
// The payload digest excludes timestamps but binds committed execution details.
// Different receipts claiming the same shard/height must fail visibly.
func (d *Supervisor) deduplicateUnifiedBlockInfo(b *message.BlockInfoMsg) (bool, error) {
	if b == nil || b.ExecutionBlockHash == "" {
		return false, fmt.Errorf("missing block identity")
	}
	groups := [][]*core.Transaction{b.InnerShardTxs, b.Relay1Txs, b.Relay2Txs, b.Broker1Txs, b.Broker2Txs}
	digests := make([][]string, len(groups))
	for i, group := range groups {
		digests[i] = make([]string, len(group))
		for j, tx := range group {
			digests[i][j] = core.TransactionExecutionSemanticDigest(tx)
		}
	}
	payload, err := json.Marshal(struct {
		Hash       string
		BodyLength int
		Stages     [][]string
		Epoch      int
	}{b.ExecutionBlockHash, b.BlockBodyLength, digests, b.Epoch})
	if err != nil {
		return false, err
	}
	digest := sha256.Sum256(payload)
	key := [2]uint64{b.SenderShardID, b.BlockHeight}
	d.blockInfoReceiptLock.Lock()
	defer d.blockInfoReceiptLock.Unlock()
	if d.unifiedBlockDigests == nil {
		d.unifiedBlockDigests = make(map[[2]uint64][32]byte)
	}
	if old, ok := d.unifiedBlockDigests[key]; ok {
		if old != digest {
			return false, fmt.Errorf("conflicting block receipt shard=%d height=%d", key[0], key[1])
		}
		return true, nil
	}
	d.unifiedBlockDigests[key] = digest
	return false, nil
}

// Call outside tcpLock. Serializing with receipt processing prevents a partial
// last block and excludes worker shutdown acknowledgement time from coverage.
func (d *Supervisor) closeUnifiedMetrics() {
	if err := d.finishUnifiedMetrics(nil, 0); err != nil {
		log.Panicf("close unified metrics: %v", err)
	}
}

// Serialize the final completion check and sealing with receipt processing.
// No caller may hold tcpLock while waiting for workers or committee callbacks.
func (d *Supervisor) finishUnifiedMetrics(runErr error, grace time.Duration) error {
	// A TCP reader timestamps a report before taking tcpLock. At an E2
	// cutoff the system can still be busy: process every already-timestamped
	// report before sealing, so the 600/960-second windows lose no rows.
	if d.e2ReceiptFence != nil {
		d.e2ReceiptFence.Lock()
		defer d.e2ReceiptFence.Unlock()
	}
	d.tcpLock.Lock()
	defer d.tcpLock.Unlock()
	if d.unifiedRecorder == nil || d.unifiedMetricsClosed {
		return runErr
	}
	progress, e2Reached := d.unifiedRecorder.StopProgress(time.Now())
	if runErr == nil && progress.IsE2() && (!e2Reached || progress.Injected > progress.Expected || progress.FinalSources != progress.Completed) {
		runErr = fmt.Errorf("E2 observation failed final check: window_and_prefix_complete=%t counts=%+v", e2Reached, progress)
	}
	if runErr == nil && !progress.IsE2() && !progress.Complete() {
		runErr = fmt.Errorf("incomplete unified execution: expected=%d unique_injected=%d unique_completed=%d unique_final_sources=%d", progress.Expected, progress.Injected, progress.Completed, progress.FinalSources)
	}
	var closeErr error
	if runErr != nil {
		if err := d.unifiedRecorder.WriteCompletionDiagnostic(runErr.Error(), grace); err != nil {
			runErr = fmt.Errorf("%v; write completion diagnostic: %w", runErr, err)
		}
		closeErr = d.unifiedRecorder.CloseIncomplete(time.Now(), runErr.Error())
	} else {
		if d.sl != nil && e2Reached {
			d.sl.Slog.Printf("Supervisor: E2 goals verified, prefix=%d all_injected=%d all_completed=%d outstanding=%d\n", progress.E2PrefixCount, progress.Injected, progress.Completed, progress.Injected-progress.Completed)
		} else if d.sl != nil {
			d.sl.Slog.Printf("Supervisor: unified completion verified expected=%d unique_injected=%d unique_completed=%d\n", progress.Expected, progress.Injected, progress.Completed)
		}
		closedAt := time.Now()
		if progress.E2FixedWindow {
			closedAt = d.unifiedRecorder.E2Deadline()
		}
		closeErr = d.unifiedRecorder.Close(closedAt)
	}
	d.unifiedMetricsClosed = true
	if closeErr != nil {
		return fmt.Errorf("%v; seal unified metrics: %w", runErr, closeErr)
	}
	return runErr
}

// An idle-block threshold is a candidate for stopping, never proof that all
// logical work completed. A bounded quiet period permits delayed final receipts
// and (for GLAS) further reconfiguration attempts without hanging indefinitely.
type completionStopGuard struct {
	grace             time.Duration
	idleSince         time.Time
	lastCompleted     uint64
	e2StopRequestedAt time.Time
}

func (g *completionStopGuard) observe(now time.Time, p unifiedmetrics.CompletionProgress, senderDone, idle bool) (bool, error) {
	if p.Injected > p.Expected || (senderDone && p.Injected != p.Expected) {
		return false, fmt.Errorf("unified injection count mismatch: expected=%d unique_injected=%d unique_completed=%d", p.Expected, p.Injected, p.Completed)
	}
	if p.Complete() {
		g.idleSince = time.Time{}
		return senderDone && idle, nil
	}
	if (!senderDone && p.Injected < p.Expected) || !idle {
		g.idleSince = time.Time{}
		return false, nil
	}
	if g.idleSince.IsZero() || p.Completed != g.lastCompleted {
		g.idleSince, g.lastCompleted = now, p.Completed
		return false, nil
	}
	if now.Sub(g.idleSince) >= g.grace {
		return false, fmt.Errorf("unified execution stalled after idle-block stop candidate: expected=%d unique_injected=%d unique_completed=%d unique_final_sources=%d; no final completion progress for %s", p.Expected, p.Injected, p.Completed, p.FinalSources, g.grace)
	}
	return false, nil
}

// Ask the committee to leave cooperatively while continuing its normal routing
// and reconfiguration until BOTH measurements are complete. The controller may
// also return naturally after EOF, before the observation clock expires.
func (g *completionStopGuard) observeE2(now time.Time, p unifiedmetrics.CompletionProgress, reached, senderDone, idle bool, stop *signal.StopSignal) (bool, error) {
	if p.Injected > p.Expected || (p.Injected == p.Expected && p.E2PrefixInjected != p.E2PrefixCount) {
		return false, fmt.Errorf("E2 input differs from declared workload: expected=%d injected=%d prefix_injected=%d/%d", p.Expected, p.Injected, p.E2PrefixInjected, p.E2PrefixCount)
	}
	if !reached {
		// Reuse EOF, missing receipt and bounded no-progress diagnostics.
		// A finished workload alone does not satisfy the observation horizon.
		_, err := g.observe(now, p, senderDone, idle)
		return false, err
	}
	if g.e2StopRequestedAt.IsZero() {
		if senderDone && p.Injected != p.Expected {
			return false, fmt.Errorf("committee exited without reaching configured input cap or receiving an E2 stop request")
		}
		stop.RequestE2Stop()
		g.e2StopRequestedAt = now
	}
	if senderDone {
		if p.Completed != p.FinalSources {
			return false, fmt.Errorf("E2 has final receipts for non-injected transactions")
		}
		return true, nil
	}
	if now.Sub(g.e2StopRequestedAt) >= g.grace {
		return false, fmt.Errorf("committee did not leave E2 control within %s after observation completed", g.grace)
	}
	return false, nil
}

func unifiedCompletionGrace() time.Duration {
	gap := params.ReconfigTimeGap
	if params.GLASReconfigTimeGap > gap {
		gap = params.GLASReconfigTimeGap
	}
	if gap < 0 {
		gap = 0
	}
	return time.Minute + 2*time.Duration(gap)*time.Second
}

// MsgSendingControl can keep running in GLAS's EOF drain loop. Monitoring must
// therefore run alongside it; success still requires that loop to finish.
func (d *Supervisor) waitUnifiedCompletion(sender <-chan error, grace time.Duration) error {
	guard := completionStopGuard{grace: grace}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	senderDone := false
	for {
		select {
		case err := <-sender:
			if err != nil {
				return err
			}
			senderDone, sender = true, nil
		case <-ticker.C:
		}
		// This also waits for committee side effects on the last receipt.
		d.tcpLock.Lock()
		now := time.Now()
		progress, e2Reached := d.unifiedRecorder.StopProgress(now)
		var ready bool
		var err error
		if progress.E2PrefixCount != 0 {
			ready, err = guard.observeE2(now, progress, e2Reached, senderDone, d.Ss.GapEnough(), d.Ss)
		} else {
			ready, err = guard.observe(now, progress, senderDone, d.Ss.GapEnough())
		}
		d.tcpLock.Unlock()
		if err != nil || ready {
			return err
		}
	}
}
