package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/params"
	"testing"
)

func TestFullWindowFormalRunsPauseAndStageAtLateBoundary(t *testing.T) {
	enableFullWindowEarlyTest(t)
	params.ExperimentEpochCount = 0
	p := &PbftConsensusNode{sequenceID: 139}
	c := &CLPAPbftInsideExtraHandleMod{pbftNode: p, cdm: dataSupport.NewCLPADataSupport()}
	c.cdm.AccountTransferRound = 19
	c.observePendingSnapshotSequence(139)
	if c.epochQuotaOrdinaryPaused() || c.SuppressViewChange() {
		t.Fatal("paused before sixth ordinary block of formal epoch")
	}
	p.sequenceID = 140
	c.observePendingSnapshotSequence(140)
	if !c.epochQuotaOrdinaryPaused() || !c.SuppressViewChange() ||
		!validPendingEarlyRound(20, 140) || validPendingEarlyRound(20, 141) {
		t.Fatal("formal boundary did not pause ordinary work and accept its early plan")
	}
	c.cdm.AccountTransferRound = 20
	p.sequenceID = 141
	c.observePendingSnapshotSequence(141)
	if c.epochQuotaOrdinaryPaused() || c.SuppressViewChange() {
		t.Fatal("formal epoch stayed paused after installation")
	}
}
