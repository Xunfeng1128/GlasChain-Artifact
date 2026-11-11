package committee

import (
	"blockEmulator/networks"
	"blockEmulator/params"
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

var errFormalRunStopped = errors.New("formal GLAS reached its stop condition")

// SetFormalRunContext is called before dispatch and before the supervisor listens.
func (module *GLASPendingCommitteeModule) SetFormalRunContext(ctx context.Context) {
	module.formalRunContext = ctx
}

func (ccm *CLPACommitteeModule) controlContext() context.Context {
	if ccm.formalRunContext != nil {
		return ccm.formalRunContext
	}
	return context.Background()
}

func (ccm *CLPACommitteeModule) formalRunStopped(at time.Time) bool {
	if ccm.formalRunContext == nil {
		return false
	}
	deadline := ccm.recorder.E2Deadline()
	return ccm.formalRunContext.Err() != nil || (!deadline.IsZero() && !at.Before(deadline))
}

func (ccm *CLPACommitteeModule) waitControlTick() bool {
	if ccm.formalRunContext == nil {
		time.Sleep(time.Second)
		return true
	}
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ccm.formalRunContext.Done():
		return false
	case <-timer.C:
		return !ccm.formalRunStopped(time.Now())
	}
}

func (ccm *CLPACommitteeModule) sendPartitionControl(raw []byte, address string) {
	if ccm.formalRunContext == nil {
		go networks.TcpDial(raw, address)
		return
	}
	go func() {
		if !ccm.formalRunStopped(time.Now()) {
			_ = networks.TcpDialContext(ccm.formalRunContext, raw, address)
		}
	}()
}

func (ccm *CLPACommitteeModule) controlReconfiguration(round uint64, injecting bool) bool {
	at := time.Now()
	if ccm.formalRunStopped(at) {
		return false
	}
	observation := ccm.beginReconfigurationAt(round, injecting, clpaReconfigurationCompletionBasis(), at)
	if ccm.formalRunContext == nil {
		ccm.runPartition(round)
	} else if err := ccm.runFormalPartition(round); err != nil {
		return ccm.finishControlReconfiguration(observation, err)
	}
	if err := ccm.resumeAfterReconfiguration(round); err != nil {
		return ccm.finishControlReconfiguration(observation, err)
	}
	ccm.sl.Slog.Println("Next CLPA epoch begins. ")
	return ccm.finishControlReconfiguration(observation, nil)
}

func (ccm *CLPACommitteeModule) finishControlReconfiguration(observation reconfigurationObservation, err error) bool {
	at := time.Now()
	if ccm.formalRunContext != nil {
		if deadline := ccm.recorder.E2Deadline(); !deadline.IsZero() && !at.Before(deadline) {
			at = deadline
		}
	}
	switch {
	case errors.Is(err, errFormalRunStopped):
		observation.finishAt("skipped", at)
		return false
	case err != nil:
		observation.finishAt("failed", at)
		log.Panic(err)
	default:
		observation.finishAt("completed", at)
	}
	return true
}

func (ccm *CLPACommitteeModule) runFormalPartition(round uint64) error {
	mapping, err := ccm.partitionPlanner(round)
	if err != nil {
		return err
	}
	if ccm.formalRunStopped(time.Now()) {
		return errFormalRunStopped
	}
	if _, err := ccm.installation.bindPlan(mapping, nil); err != nil {
		return err
	}
	if !ccm.partitionPublishStaged(round) {
		if ccm.formalRunStopped(time.Now()) {
			return errFormalRunStopped
		}
		return fmt.Errorf("missing staged GLAS plan for round %d", round)
	}
	if err := ccm.waitFormalInstallation(round); err != nil {
		return err
	}
	ccm.clpaLock.Lock()
	for address, shard := range mapping {
		ccm.modifiedMap[address] = shard
	}
	ccm.clpaLock.Unlock()
	if ccm.partitionInstalled != nil {
		ccm.partitionInstalled(round)
	}
	return nil
}

// Reuse existing matching-receipt queries without closing the installation
// tracker: install_clock's ordinary-block resume still closes that tracker.
func (ccm *CLPACommitteeModule) waitFormalInstallation(epoch uint64) error {
	deadline := time.Now().Add(5 * time.Minute)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	retry := time.Now().Add(2 * time.Second)
	for {
		ccm.installation.mu.Lock()
		t := &ccm.installation
		err := t.err
		if !t.active || t.epoch != epoch {
			err = fmt.Errorf("formal installation epoch mismatch")
		}
		done := err == nil && !t.allInstalledAt.IsZero()
		missing := make([]uint64, 0)
		if time.Now().After(retry) {
			for s := uint64(0); s < uint64(t.shards); s++ {
				if _, ok := t.receipts[s]; !ok {
					missing = append(missing, s)
				}
			}
		}
		t.mu.Unlock()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if ccm.formalRunContext != nil && ccm.formalRunStopped(time.Now()) {
			return errFormalRunStopped
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("formal installation epoch %d timed out", epoch)
		}
		if len(missing) > 0 {
			timeout := 2 * time.Second
			if ccm.partitionRecoverStaged != nil {
				timeout = earlyPendingRestageInterval()
			}
			ctx, cancel := context.WithTimeout(ccm.controlContext(), timeout)
			ccm.queryPartitionInstallations(ctx, epoch, missing)
			cancel()
			retry = time.Now().Add(2 * time.Second)
		}
		select {
		case <-ccm.controlContext().Done():
			return errFormalRunStopped
		case <-ticker.C:
		}
	}
}

// Wait for the last ordinary block from every shard before choosing a staged plan.
func (ccm *CLPACommitteeModule) formalReconfigurationDue() bool {
	epoch := uint64(atomic.LoadInt32(&ccm.curEpoch))
	if !params.GLASPendingEarlyRoundEnabled(epoch + 1) {
		return false
	}
	last := params.ExperimentEpochLastOrdinary(epoch)
	ccm.shardMaxSeqLock.Lock()
	defer ccm.shardMaxSeqLock.Unlock()
	for s := 0; s < params.ShardNum; s++ {
		if ccm.shardMaxSeq[uint64(s)] != last {
			return false
		}
	}
	return true
}
