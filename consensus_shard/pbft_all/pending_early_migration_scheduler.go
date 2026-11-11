package pbft_all

import (
	"time"

	"blockEmulator/message"
	"blockEmulator/params"
)

func (p *PbftConsensusNode) fullWindowMigrationWakeEnabled() bool {
	if !params.GLASPendingFullWindowEarlyEnabled() {
		return false
	}
	inside, ok := p.ihm.(*CLPAPbftInsideExtraHandleMod)
	return ok && inside.cdm.PendingSnapshotMode && p.clpaPollingEnabled()
}

// Called only by the ordinary proposer. A wake can enter the partition
// protocol, but it cannot consume or move the ordinary block deadline.
func (p *PbftConsensusNode) waitFullWindowOrdinary(send func(*message.PrePrepare), deadline time.Time) bool {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-p.pStop:
		return false
	case <-p.clpaControlContext().Done():
		return false
	case <-timer.C:
		return true
	case <-p.clpaControlWakeChannel():
		if p.stopSignal.Load() || p.clpaControlContext().Err() != nil {
			return false
		}
		// Ordinary Relay wakes do not need the PBFT lock before activation.
		// An already started partition is driven by phase polling or PBFT.
		cdm := p.ihm.(*CLPAPbftInsideExtraHandleMod).cdm
		cdm.PartitionStateLock.Lock()
		canStart := cdm.PartitionOn && cdm.ControlEpoch != cdm.PendingEpoch
		cdm.PartitionStateLock.Unlock()
		if !canStart {
			return true
		}
		if proposal, _, _ := p.tryCLPAProposal(true); proposal != nil {
			send(proposal)
			return true
		}
		return p.pollCLPAReconfiguration(send)
	}
}

func (p *PbftConsensusNode) logFullWindowMigrationPhase(epoch uint64, phase string) {
	if p.fullWindowMigrationWakeEnabled() && p.pl != nil && p.pl.Plog != nil {
		p.pl.Plog.Printf("GLAS_FULL_WINDOW_MIGRATION epoch=%d phase=%s unix_ns=%d", epoch, phase, time.Now().UnixNano())
	}
}
