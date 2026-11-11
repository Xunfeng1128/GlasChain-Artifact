package committee

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"blockEmulator/core"
	"blockEmulator/glasfreight/model"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/utils"
)

type earlyPendingState struct {
	mu            sync.Mutex
	epoch         uint64
	installed     uint64
	reportErr     error
	reports       map[uint64]message.PendingEarlyReport
	reportDigests map[uint64][sha256.Size]byte
	fifth         map[uint64]string
	last          map[uint64]string
	job           *earlyPendingJob
	active        *earlyPendingJob
	stopped       bool
	busy          bool
	workers       sync.WaitGroup
	wake          chan struct{}
}

type earlyPendingJob struct {
	epoch        uint64
	reports      map[uint64]message.PendingEarlyReport
	cancel       context.CancelFunc
	plan         pendingComputedPlan
	wire         message.PendingEarlyPlan
	staged       map[uint64]bool
	ready        time.Time
	allStaged    time.Time
	restageAfter time.Time
	err          error
}

func (module *GLASPendingCommitteeModule) initPendingEarly() {
	state := &earlyPendingState{
		wake:  make(chan struct{}, 1),
		epoch: 1, reports: make(map[uint64]message.PendingEarlyReport), fifth: make(map[uint64]string), last: make(map[uint64]string),
	}
	module.earlyState = state
	module.partitionPlanner = module.planPendingEarly
	module.partitionPublishStaged = module.publishPendingEarly
	module.partitionRecoverStaged = module.recoverPendingEarly
	module.partitionInstalled = func(epoch uint64) {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.installed = epoch
		if state.active != nil && state.active.epoch == epoch {
			state.active = nil
		}
		module.startPendingEarlyLocked()
	}
}

func (module *GLASPendingCommitteeModule) stopPendingEarly() {
	state := module.earlyState
	state.mu.Lock()
	state.stopped = true
	state.notify()
	if state.job != nil {
		state.job.cancel()
	}
	state.mu.Unlock()
	if module.formalRunContext == nil {
		state.workers.Wait()
	}
}

func (state *earlyPendingState) notify() {
	select {
	case state.wake <- struct{}{}:
	default:
	}
}

func (module *GLASPendingCommitteeModule) observeEarlyPendingMessage(kind message.MessageType, content []byte) bool {
	state := module.earlyState
	if state == nil {
		return false
	}
	switch kind {
	case message.CPendingEarlyReport:
		var report message.PendingEarlyReport
		if json.Unmarshal(content, &report) == nil {
			module.acceptEarlyPendingReport(report, sha256.Sum256(content))
		}
	case message.CPendingEarlyPlanAck:
		var ack message.PendingEarlyPlanAck
		if json.Unmarshal(content, &ack) == nil {
			module.acceptEarlyPendingAck(ack)
		}
	default:
		return false
	}
	return true
}

func (module *GLASPendingCommitteeModule) acceptEarlyPendingAck(ack message.PendingEarlyPlanAck) {
	state := module.earlyState
	state.mu.Lock()
	if state.stopped {
		state.mu.Unlock()
		return
	}
	job := state.job
	if job != nil && earlyPendingAckMatches(job, ack) {
		report, found := job.reports[ack.FromShard]
		if found && ack.View == report.View && ack.Leader == report.Leader && !job.staged[ack.FromShard] {
			job.staged[ack.FromShard] = true
			if job.allStaged.IsZero() && len(job.staged) == params.ShardNum {
				job.allStaged = time.Now()
			}
			state.notify()
		}
	}
	active := state.active
	recovering := active != nil && !active.restageAfter.IsZero() && earlyPendingAckMatches(active, ack) && ack.Leader == ack.View
	state.mu.Unlock()
	if recovering {
		if address := module.IpNodeTable[ack.FromShard][ack.Leader]; address != "" {
			module.sendPartitionControl(earlyPendingActivation(active), address)
		}
	}
}

func earlyPendingAckMatches(job *earlyPendingJob, ack message.PendingEarlyPlanAck) bool {
	return ack.Epoch == job.epoch && ack.TargetSeq == job.wire.TargetSeq && ack.Digest != "" && ack.Digest == job.wire.Digest
}

func (module *GLASPendingCommitteeModule) acceptEarlyPendingReport(report message.PendingEarlyReport, digest [sha256.Size]byte) {
	state := module.earlyState
	if report.FromShard >= uint64(params.ShardNum) || report.ObservedBlockHash == "" || report.LastBlockDigest == "" ||
		module.IpNodeTable[report.FromShard][report.Leader] == "" || report.Leader != report.View {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.stopped || report.Epoch != state.epoch || !params.GLASPendingEarlyRoundEnabled(report.Epoch) ||
		report.TargetSeq != params.ExperimentEpochTarget(report.Epoch) {
		return
	}
	if report.FullRecords < len(report.FullWindow) || report.FullCallRelations < 0 {
		state.reportErr = fmt.Errorf("invalid full-window report from shard %d", report.FromShard)
		state.notify()
		if state.job != nil {
			state.job.cancel()
		}
		return
	}
	if previous, exists := state.reports[report.FromShard]; exists {
		if report.View != previous.View || report.ObservedBlockHash != previous.ObservedBlockHash ||
			report.LastBlockDigest != previous.LastBlockDigest || digest != state.reportDigests[report.FromShard] {
			// View numbers rotate through replica IDs. Any conflicting report
			// invalidates this speculative generation.
			state.reportErr = fmt.Errorf("shard %d changed its early report", report.FromShard)
			state.notify()
			if state.job != nil {
				state.job.cancel()
			}
		}
		return
	}
	if state.reportDigests == nil {
		state.reportDigests = make(map[uint64][sha256.Size]byte, params.ShardNum)
	}
	state.reportDigests[report.FromShard] = digest
	state.reports[report.FromShard] = report
	module.startPendingEarlyLocked()
	state.notify()
}

func (module *GLASPendingCommitteeModule) startPendingEarlyLocked() {
	state := module.earlyState
	if state.stopped || state.reportErr != nil || state.busy || state.job != nil ||
		(module.formalRunContext != nil && module.formalRunStopped(time.Now())) ||
		state.installed+1 != state.epoch || len(state.reports) != params.ShardNum {
		return
	}
	reports := make(map[uint64]message.PendingEarlyReport, len(state.reports))
	for shard, candidate := range state.reports {
		reports[shard] = candidate
	}
	ctx, cancel := context.WithCancel(module.controlContext())
	job := &earlyPendingJob{epoch: state.epoch, reports: reports, cancel: cancel, staged: make(map[uint64]bool)}
	state.job, state.busy = job, true
	state.workers.Add(1)
	go func() {
		defer state.workers.Done()
		defer func() {
			cancel()
			state.mu.Lock()
			state.busy = false
			module.startPendingEarlyLocked()
			state.mu.Unlock()
		}()
		module.computeAndStagePendingEarly(ctx, job)
	}()
}

func (module *GLASPendingCommitteeModule) observeEarlyPendingBlock(block *message.BlockInfoMsg) {
	state := module.earlyState
	if state == nil || block.Epoch < 0 || block.SenderShardID >= uint64(params.ShardNum) {
		return
	}
	epoch := uint64(block.Epoch) + 1
	target := params.ExperimentEpochTarget(epoch)
	if block.BlockHeight == target-2 {
		state.mu.Lock()
		if epoch == state.epoch && !state.stopped && block.ExecutionBlockHash != "" {
			state.fifth[block.SenderShardID] = block.ExecutionBlockHash
			state.notify()
		}
		state.mu.Unlock()
		return
	}
	if block.BlockHeight != target-1 {
		return
	}
	txs := make([]*core.Transaction, 0, len(block.InnerShardTxs)+len(block.Relay1Txs)+len(block.Relay2Txs))
	txs = append(txs, block.InnerShardTxs...)
	txs = append(txs, block.Relay1Txs...)
	txs = append(txs, block.Relay2Txs...)
	if len(txs) != block.BlockBodyLength {
		return
	}
	actual := message.PendingEarlyBlockDigest(txs)
	state.mu.Lock()
	if epoch == state.epoch && !state.stopped {
		state.last[block.SenderShardID] = actual
		state.notify()
	}
	state.mu.Unlock()
}

func (module *GLASPendingCommitteeModule) computeAndStagePendingEarly(ctx context.Context, job *earlyPendingJob) {
	state := module.earlyState
	started := time.Now()
	writePendingDiagnostic(module.diagnosticLogger(), "GLAS_PENDING_EARLY_START", map[string]interface{}{
		"epoch": job.epoch, "reports": len(job.reports),
	})
	module.clpaLock.Lock()
	current := make(model.Placement, len(module.modifiedMap))
	for address, shard := range module.modifiedMap {
		current[address] = shard
	}
	module.clpaLock.Unlock()
	resolve := installedMapResolver(current)
	target := params.ExperimentEpochTarget(job.epoch)
	timing := newPendingPlanTiming(started)
	timing.begin("assemble_full_window")
	snapshots, err := fullPendingEarlySnapshots(job.reports, job.epoch, target, resolve)
	timing.end()
	var plan pendingComputedPlan
	if err == nil {
		plan, err = module.computePendingPlan(job.epoch, target, snapshots, resolve, timing)
	}
	if err == nil {
		err = ctx.Err()
	}
	var payload []byte
	var wire message.PendingEarlyPlan
	if err == nil {
		wire = message.PendingEarlyPlan{
			TargetSeq: target,
			Plan:      message.PartitionModifiedMap{Epoch: job.epoch, PartitionModified: plan.mapping},
			Digest:    message.PartitionPlanDigest(plan.mapping, nil),
		}
		payload, err = json.Marshal(wire)
	}
	state.mu.Lock()
	if state.stopped || state.job != job || state.reportErr != nil {
		state.mu.Unlock()
		return
	}
	if err != nil {
		job.err = err
		state.notify()
		state.mu.Unlock()
		return
	}
	// Staging and boundary checks need only the report envelope now.
	// Duplicates are checked against the digest of the received frame.
	for shard, report := range job.reports {
		report.FullWindow, report.ForecastCrossTOP = nil, nil
		job.reports[shard], state.reports[shard] = report, report
	}
	job.plan, job.wire, job.ready = plan, wire, time.Now()
	state.notify()
	state.mu.Unlock()
	readyDiagnostic := map[string]interface{}{
		"epoch": job.epoch, "target": target, "bytes": len(payload), "elapsed_ns": time.Since(started).Nanoseconds(),
		"phases_ns": timing.PhasesNS, "input": "full_window", "selected_records": timing.SelectedRecords,
	}
	writePendingDiagnostic(module.diagnosticLogger(), "GLAS_PENDING_EARLY_READY", readyDiagnostic)
	module.stagePendingEarly(ctx, job, message.MergeMessage(message.CPendingEarlyPlan, payload))
}

func (module *GLASPendingCommitteeModule) stagePendingEarly(ctx context.Context, job *earlyPendingJob, raw []byte) {
	state := module.earlyState
	for ctx.Err() == nil {
		state.mu.Lock()
		if state.stopped || state.job != job || state.reportErr != nil || !job.allStaged.IsZero() {
			state.mu.Unlock()
			return
		}
		addresses := make([]string, 0, len(job.reports)-len(job.staged))
		for shard, report := range job.reports {
			if !job.staged[shard] {
				addresses = append(addresses, module.IpNodeTable[shard][report.Leader])
			}
		}
		state.mu.Unlock()

		var sends sync.WaitGroup
		for _, address := range addresses {
			sends.Add(1)
			go func(address string) {
				defer sends.Done()
				_ = networks.TcpDialContext(ctx, raw, address)
			}(address)
		}
		sends.Wait()
		timer := time.NewTimer(earlyPendingRestageInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (module *GLASPendingCommitteeModule) planPendingEarly(epoch uint64) (map[string]uint64, error) {
	boundary := time.Now()
	ctx, cancel := context.WithTimeout(module.controlContext(), time.Duration(params.GLASPendingSnapshotTimeoutSeconds)*time.Second)
	defer cancel()

	// The fixed ordinary-block boundary holds workers at the boundary. Prepare advertises the
	// pause to every replica, without requesting a fresh snapshot or activating.
	module.partitionPrepareSend(epoch)
	return module.waitPendingEarly(ctx, epoch, boundary)
}

func (module *GLASPendingCommitteeModule) waitPendingEarly(ctx context.Context, epoch uint64, boundary time.Time) (map[string]uint64, error) {
	state := module.earlyState
	for {
		if module.formalRunContext != nil && module.formalRunStopped(time.Now()) {
			return nil, errFormalRunStopped
		}
		state.mu.Lock()
		stage, err := state.boundaryStatus(epoch)
		if err == nil && ctx.Err() != nil {
			err = ctx.Err()
		}
		job := state.job
		if err != nil {
			if job != nil && job.epoch == epoch {
				job.cancel()
			}
			state.mu.Unlock()
			writePendingDiagnostic(module.diagnosticLogger(), "GLAS_PENDING_EARLY_BOUNDARY", map[string]interface{}{
				"epoch": epoch, "decision": "failed", "stage": stage,
				"boundary_wait_ns": time.Since(boundary).Nanoseconds(), "error": err.Error(),
			})
			return nil, fmt.Errorf("early GLAS plan epoch=%d stage=%s: %w", epoch, stage, err)
		}
		if stage == "" {
			job.cancel()
			state.active, state.job = job, nil
			state.epoch = epoch + 1
			last := state.last
			state.reports = make(map[uint64]message.PendingEarlyReport)
			state.reportDigests = nil
			state.fifth = make(map[uint64]string)
			state.last = make(map[uint64]string)
			state.mu.Unlock()

			exact := 0
			for shard, report := range job.reports {
				if last[shard] == report.LastBlockDigest {
					exact++
				}
			}
			boundaryDiagnostic := map[string]interface{}{
				"epoch": epoch, "decision": "staged", "ready_lead_ns": boundary.Sub(job.ready).Nanoseconds(),
				"staged_lead_ns": boundary.Sub(job.allStaged).Nanoseconds(), "boundary_wait_ns": time.Since(boundary).Nanoseconds(),
				"exact_last_block_shards": exact, "input": "full_window",
			}
			writePendingDiagnostic(module.diagnosticLogger(), "GLAS_PENDING_EARLY_BOUNDARY", boundaryDiagnostic)
			return job.plan.mapping, nil
		}
		state.mu.Unlock()

		select {
		case <-ctx.Done():
		case <-state.wake:
		}
	}
}

// A missing input or acknowledgement can still arrive after Prepare. A
// contradictory fifth-block hash cannot describe the current boundary.
func (state *earlyPendingState) boundaryStatus(epoch uint64) (string, error) {
	switch {
	case state.stopped:
		return "stopped", context.Canceled
	case epoch != state.epoch:
		return "epoch", fmt.Errorf("expected epoch %d", state.epoch)
	case state.reportErr != nil:
		return "reports", state.reportErr
	case state.job == nil:
		return fmt.Sprintf("reports %d/%d", len(state.reports), params.ShardNum), nil
	case state.job.err != nil:
		return "planning", state.job.err
	}
	job := state.job
	for shard, report := range job.reports {
		if hash := state.fifth[shard]; hash != "" && hash != report.ObservedBlockHash {
			return "boundary", fmt.Errorf("shard %d reported a stale fifth block", shard)
		}
	}
	switch {
	case len(state.fifth) != params.ShardNum || len(state.last) != params.ShardNum:
		return fmt.Sprintf("boundary blocks fifth=%d sixth=%d/%d", len(state.fifth), len(state.last), params.ShardNum), nil
	case job.ready.IsZero():
		return "planning", nil
	case job.allStaged.IsZero():
		return fmt.Sprintf("staging %d/%d", len(job.staged), params.ShardNum), nil
	default:
		return "", nil
	}
}

func (module *GLASPendingCommitteeModule) publishPendingEarly(epoch uint64) bool {
	if module.formalRunContext != nil && module.formalRunStopped(time.Now()) {
		return false
	}
	state := module.earlyState
	state.mu.Lock()
	job := state.active
	if job != nil && job.epoch == epoch {
		job.restageAfter = time.Now().Add(2 * earlyPendingRestageInterval())
	}
	state.mu.Unlock()
	if job == nil || job.epoch != epoch {
		return false
	}
	raw := earlyPendingActivation(job)
	for shard, report := range job.reports {
		module.sendPartitionControl(raw, module.IpNodeTable[shard][report.Leader])
	}
	audit := func() {
		if err := job.plan.audit(); err != nil {
			log.Panicf("write early pending plan audit: %v", err)
		}
	}
	if module.formalRunContext != nil {
		audit()
	} else {
		state.workers.Add(1)
		go func() {
			defer state.workers.Done()
			audit()
		}()
	}
	return true
}

func earlyPendingActivation(job *earlyPendingJob) []byte {
	payload, _ := json.Marshal(message.PendingEarlyActivate{Epoch: job.epoch, TargetSeq: job.wire.TargetSeq, Digest: job.wire.Digest})
	return message.MergeMessage(message.CPendingEarlyActivate, payload)
}

func earlyPendingRestageInterval() time.Duration {
	return time.Duration(params.Block_Interval)*time.Millisecond + 2*time.Second
}

func (module *GLASPendingCommitteeModule) recoverPendingEarly(ctx context.Context, epoch uint64, missing []uint64) {
	state := module.earlyState
	state.mu.Lock()
	job := state.active
	restage := job != nil && job.epoch == epoch && !job.restageAfter.IsZero() && !time.Now().Before(job.restageAfter)
	if restage {
		job.restageAfter = time.Now().Add(earlyPendingRestageInterval())
	}
	state.mu.Unlock()
	if job == nil || job.epoch != epoch {
		return
	}
	// Once activation is chosen, retry only that immutable plan. Ordinary
	// polling can take several seconds, so common retries carry just Prepare
	// and Activate. A delayed restage supports a replacement leader.
	prepare, _ := json.Marshal(message.PartitionPrepare{Epoch: epoch, TargetSeq: job.wire.TargetSeq})
	rawPrepare := message.MergeMessage(message.CPartitionPrepare, prepare)
	rawActivate := earlyPendingActivation(job)
	var rawPlan []byte
	if restage {
		if payload, err := json.Marshal(job.wire); err == nil {
			rawPlan = message.MergeMessage(message.CPendingEarlyPlan, payload)
		}
	}
	var sends sync.WaitGroup
	for _, shard := range missing {
		for _, address := range module.IpNodeTable[shard] {
			if address == "" {
				continue
			}
			sends.Add(1)
			go func(address string) {
				defer sends.Done()
				_ = networks.TcpDialContext(ctx, rawPrepare, address)
				_ = networks.TcpDialContext(ctx, rawActivate, address)
				if rawPlan != nil {
					_ = networks.TcpDialContext(ctx, rawPlan, address)
				}
			}(address)
		}
	}
	sends.Wait()
}

func installedMapResolver(current model.Placement) func(string) uint64 {
	return func(address string) uint64 {
		if shard, ok := current[address]; ok {
			return shard
		}
		return uint64(utils.Addr2Shard(address))
	}
}
