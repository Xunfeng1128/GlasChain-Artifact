package pbft_all

import "blockEmulator/params"

// The caller holds the consensus sequence lock. Only ordinary proposal
// generation is gated: control, relay receipt and installation remain active.
func (cphm *CLPAPbftInsideExtraHandleMod) epochQuotaOrdinaryPaused() bool {
	if !params.FixedBlockEpochBoundariesEnabled() {
		return false
	}
	cphm.cdm.PartitionStateLock.Lock()
	defer cphm.cdm.PartitionStateLock.Unlock()
	return cphm.pbftNode.sequenceID >= params.ExperimentEpochTarget(cphm.cdm.AccountTransferRound+1)
}
