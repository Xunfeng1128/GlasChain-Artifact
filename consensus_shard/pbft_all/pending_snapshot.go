package pbft_all

import (
	"fmt"

	"blockEmulator/message"
	"blockEmulator/params"
)

func (cphm *CLPAPbftInsideExtraHandleMod) partitionControlState() (epoch, target, round uint64, on bool) {
	cphm.cdm.PartitionStateLock.Lock()
	defer cphm.cdm.PartitionStateLock.Unlock()
	return cphm.cdm.PendingEpoch, cphm.cdm.PendingTargetSeq, cphm.cdm.AccountTransferRound, cphm.cdm.PartitionOn
}

// Caller holds PartitionStateLock. Existing CLPA modes ignore Snapshot.
func (crom *CLPARelayOutsideModule) acceptPendingSnapshotPrepare(pp *message.PartitionPrepare) (bool, error) {
	if !crom.cdm.PendingSnapshotMode {
		return true, nil
	}
	if pp.Epoch <= crom.cdm.AccountTransferRound {
		return false, nil
	}
	if pp.Epoch != crom.cdm.AccountTransferRound+1 || pp.TargetSeq == 0 {
		return false, fmt.Errorf("invalid pending snapshot prepare: epoch=%d target=%d snapshot=%t", pp.Epoch, pp.TargetSeq, pp.Snapshot)
	}
	if pp.Epoch == crom.cdm.PendingEpoch && pp.TargetSeq != crom.cdm.PendingTargetSeq {
		return false, fmt.Errorf("conflicting pending snapshot target for epoch %d", pp.Epoch)
	}
	if !validPendingEarlyRound(pp.Epoch, pp.TargetSeq) {
		return false, fmt.Errorf("early prepare does not match the epoch boundary: epoch=%d target=%d", pp.Epoch, pp.TargetSeq)
	}
	if pp.Snapshot {
		return false, fmt.Errorf("full-window early reporting requires a staged plan for epoch %d", pp.Epoch)
	}
	return true, nil
}

// Waiting for the supervisor's planner is an intentional pause, not a failed
// consensus leader. Legacy CLPA modes retain their previous timeout behavior.
func (cphm *CLPAPbftInsideExtraHandleMod) SuppressViewChange() bool {
	cphm.cdm.PartitionStateLock.Lock()
	defer cphm.cdm.PartitionStateLock.Unlock()
	return (params.FixedBlockEpochBoundariesEnabled() && cphm.cdm.PendingSnapshotObservedSeq >= params.ExperimentEpochTarget(cphm.cdm.AccountTransferRound+1)) || (cphm.cdm.ControlEventsEnabled && cphm.cdm.PendingEpoch == cphm.cdm.AccountTransferRound+1 && cphm.cdm.PendingSnapshotObservedSeq >= cphm.cdm.PendingTargetSeq) || cphm.cdm.PendingSnapshotMode && (cphm.cdm.PendingSnapshotPaused ||
		(cphm.cdm.PendingSnapshotRequested &&
			cphm.cdm.PendingEpoch == cphm.cdm.AccountTransferRound+1 &&
			cphm.cdm.PendingSnapshotObservedSeq >= cphm.cdm.PendingTargetSeq))
}

// Every replica observes committed blocks, including followers that never
// enter HandleinPropose. Do not acquire sequenceLock in SuppressViewChange:
// the leader intentionally holds it while waiting for the planner's map.
func (cphm *CLPAPbftInsideExtraHandleMod) observePendingSnapshotSequence(next uint64) {
	if !cphm.cdm.PendingSnapshotMode && !cphm.cdm.ControlEventsEnabled && !params.FixedBlockEpochBoundariesEnabled() {
		return
	}
	cphm.cdm.PartitionStateLock.Lock()
	defer cphm.cdm.PartitionStateLock.Unlock()
	if next > cphm.cdm.PendingSnapshotObservedSeq {
		cphm.cdm.PendingSnapshotObservedSeq = next
	}
}
