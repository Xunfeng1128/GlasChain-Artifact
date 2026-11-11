package pbft_all

import (
	"blockEmulator/message"
	"time"
)

const (
	clpaMapPollInterval       = 50 * time.Millisecond
	clpaMessagePollInterval   = time.Second
	clpaConsensusPollInterval = 100 * time.Millisecond
)

// waitCLPAPoll deliberately ignores control-message notifications. Cancellation
// is still immediate; no data, PBFT or sequence lock is held across this wait.
func (p *PbftConsensusNode) waitCLPAPoll(delay time.Duration) bool {
	if p.stopSignal.Load() || p.clpaControlContext().Err() != nil {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-p.pStop:
		return false
	case <-p.clpaControlContext().Done():
		return false
	case <-timer.C:
		return !p.stopSignal.Load() && p.clpaControlContext().Err() == nil
	}
}

// The shared phase machine sets ControlEpoch at the safe target boundary.
// Full-window GLAS can enter on activation; other poll modes enter on an
// ordinary proposal opportunity.
func (p *PbftConsensusNode) clpaPollRoundState() (active bool, interval time.Duration) {
	inside := p.ihm.(*CLPAPbftInsideExtraHandleMod)
	cdm := inside.cdm
	cdm.PartitionStateLock.Lock()
	defer cdm.PartitionStateLock.Unlock()
	active = cdm.PendingEpoch == cdm.AccountTransferRound+1 && cdm.PendingTargetSeq != 0 && cdm.ControlEpoch == cdm.PendingEpoch
	if !cdm.PartitionOn {
		return active, clpaMapPollInterval
	}
	return active, clpaMessagePollInterval
}

// Once an ordinary tick enters reconfiguration, retry the same finite phases
// by timer until the migration can be proposed. Ready/Relay and collection use
// the old one-second checking cadence; waiting for the map uses 50ms. Reusing
// tryCLPAProposal preserves single-proposer ownership, duplicate fences and
// installation receipts without holding a mutex while waiting for messages.
func (p *PbftConsensusNode) pollCLPAReconfiguration(send func(*message.PrePrepare)) bool {
	for {
		active, interval := p.clpaPollRoundState()
		if !active || uint64(p.view.Load()) != p.NodeID {
			return true
		}
		if !p.waitCLPAPoll(interval) {
			return false
		}
		proposal, _, _ := p.tryCLPAProposal(true)
		if proposal != nil {
			send(proposal)
			return true
		}
	}
}

// Polling changes only the opportunity to enter/check control phases. Ordinary
// blocks keep their configured cadence. A committed migration resets the next
// ordinary deadline to this shard's local installation time plus BlockInterval,
// exactly as in the event scheduler. The supervisor always waits for a new-epoch
// ordinary block report, independently of this scheduling policy.
func (p *PbftConsensusNode) runCLPAPollProposalScheduler(send func(*message.PrePrepare)) {
	interval := p.glasProposalInterval()
	nextOrdinary := time.Now().Add(interval)
	earlyMigration := p.fullWindowMigrationWakeEnabled()
	var observedBoundary int64
	for {
		if p.stopSignal.Load() || p.clpaControlContext().Err() != nil {
			return
		}
		if boundary := p.lastPartitionCommitTime.Load(); boundary > observedBoundary {
			observedBoundary = boundary
			nextOrdinary = time.UnixMilli(boundary).Add(interval)
		}
		if wait := time.Until(nextOrdinary); wait > 0 {
			if earlyMigration {
				if !p.waitFullWindowOrdinary(send, nextOrdinary) {
					return
				}
			} else if !p.waitCLPAPoll(wait) {
				return
			}
			// A partition may have committed while the timer was sleeping.
			continue
		}
		started := time.Now()
		proposal, consumed, retryAt := p.tryCLPAProposal(false)
		if !retryAt.IsZero() {
			// Recheck the actual installation deadline, including a newer
			// installation observed under the PBFT lock. This is not a generic
			// consensus wait and does not consume an ordinary opportunity.
			nextOrdinary = retryAt
			continue
		}
		if !consumed {
			// The ordinary opportunity remains due while PBFT/catch-up is busy.
			if !p.waitCLPAPoll(clpaConsensusPollInterval) {
				return
			}
			continue
		}
		nextOrdinary = started.Add(interval)
		if proposal != nil {
			send(proposal)
			continue
		}
		if !p.pollCLPAReconfiguration(send) {
			return
		}
	}
}
