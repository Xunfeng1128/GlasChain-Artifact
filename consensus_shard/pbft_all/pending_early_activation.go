package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"encoding/json"
)

func validPendingEarlyRound(epoch, target uint64) bool {
	return params.GLASPendingEarlyRoundEnabled(epoch) &&
		target == params.ExperimentEpochTarget(epoch)
}

func (crom *CLPARelayOutsideModule) pendingEarlyControlEnabled() bool {
	p, cdm := crom.pbftNode, crom.cdm
	return params.GLASPendingFullWindowEarlyEnabled() && !p.stopSignal.Load() &&
		cdm.ControlEventsEnabled && cdm.ControlPolling && cdm.PendingSnapshotMode
}

func (crom *CLPARelayOutsideModule) handlePendingEarlyPlan(content []byte) {
	if !crom.pendingEarlyControlEnabled() || uint64(crom.pbftNode.view.Load()) != crom.pbftNode.NodeID {
		return
	}
	var staged message.PendingEarlyPlan
	if err := json.Unmarshal(content, &staged); err != nil {
		return // The supervisor can still take the snapshot fallback.
	}
	if ack := crom.stagePendingEarlyPlan(&staged); ack != nil {
		p := crom.pbftNode
		p.sendCLPAControl(message.CPendingEarlyPlanAck, *ack, p.ip_nodeTable[params.SupervisorShard][0])
	}
}

func (crom *CLPARelayOutsideModule) stagePendingEarlyPlan(staged *message.PendingEarlyPlan) *message.PendingEarlyPlanAck {
	p, cdm := crom.pbftNode, crom.cdm
	plan := &staged.Plan
	view := uint64(p.view.Load())
	if view != p.NodeID || !validPendingEarlyRound(plan.Epoch, staged.TargetSeq) ||
		plan.GARET != nil || validatePendingWindowMetadata(plan.PlanningWindow, plan.Epoch) != nil {
		return nil
	}
	for address, target := range plan.PartitionModified {
		if address == "" || target >= p.pbftChainConfig.ShardNums {
			return nil
		}
	}
	if staged.Digest != message.PartitionPlanDigest(plan.PartitionModified, plan.GARET) {
		return nil
	}
	cdm.PartitionStateLock.Lock()
	defer cdm.PartitionStateLock.Unlock()
	if p.stopSignal.Load() || uint64(p.view.Load()) != view || plan.Epoch != cdm.AccountTransferRound+1 ||
		cdm.PartitionOn || cdm.PendingSnapshotRequested ||
		(cdm.PendingEpoch == plan.Epoch && cdm.PendingTargetSeq != staged.TargetSeq) {
		return nil
	}
	if old := cdm.PendingEarlyPlan; old != nil && old.Plan.Epoch <= cdm.AccountTransferRound {
		crom.clearPendingEarlyPlanLocked()
	}
	if old := cdm.PendingEarlyPlan; old != nil {
		if old.Digest != staged.Digest || old.Plan.Epoch != plan.Epoch ||
			old.TargetSeq != staged.TargetSeq || cdm.PendingEarlyPlanView != view {
			return nil
		}
	} else {
		cdm.PendingEarlyPlan, cdm.PendingEarlyPlanView = staged, view
	}
	return &message.PendingEarlyPlanAck{Epoch: plan.Epoch, TargetSeq: staged.TargetSeq,
		FromShard: p.ShardID, Leader: p.NodeID, View: view, Digest: staged.Digest}
}

func (crom *CLPARelayOutsideModule) handlePendingEarlyActivate(content []byte) {
	if !crom.pendingEarlyControlEnabled() {
		return
	}
	var activate message.PendingEarlyActivate
	if err := json.Unmarshal(content, &activate); err != nil {
		return
	}
	defer crom.pbftNode.wakeCLPAControl()
	cdm := crom.cdm
	cdm.PartitionStateLock.Lock()
	defer cdm.PartitionStateLock.Unlock()
	staged := cdm.PendingEarlyPlan
	if staged == nil || activate.Epoch != staged.Plan.Epoch || activate.TargetSeq != staged.TargetSeq ||
		activate.Digest != staged.Digest || cdm.PendingSnapshotRequested || cdm.PartitionOn ||
		cdm.PendingEarlyPlanView != uint64(crom.pbftNode.view.Load()) {
		return
	}
	cdm.PendingEarlyActivationRequested = true
	crom.pbftNode.logFullWindowMigrationPhase(activate.Epoch, "activation_received")
	crom.activatePendingEarlyPlanLocked()
}

// Activation can overtake Prepare on separate connections. Publication waits
// for the matching non-snapshot Prepare and retains the existing target
// boundary and Ready/migration gates.
func (crom *CLPARelayOutsideModule) activatePendingEarlyPlanLocked() {
	p, cdm := crom.pbftNode, crom.cdm
	staged := cdm.PendingEarlyPlan
	if staged == nil || !cdm.PendingEarlyActivationRequested || p.stopSignal.Load() ||
		cdm.PendingEarlyPlanView != uint64(p.view.Load()) || uint64(p.view.Load()) != p.NodeID ||
		cdm.PartitionOn || cdm.PendingSnapshotRequested || staged.Plan.Epoch != cdm.AccountTransferRound+1 ||
		cdm.PendingEpoch != staged.Plan.Epoch || cdm.PendingTargetSeq != staged.TargetSeq {
		return
	}
	crom.acceptCLPAEventMapLocked(&staged.Plan)
	p.logFullWindowMigrationPhase(staged.Plan.Epoch, "activation_applied")
	crom.clearPendingEarlyPlanLocked()
}

func (crom *CLPARelayOutsideModule) clearPendingEarlyPlanLocked() {
	crom.cdm.PendingEarlyPlan = nil
	crom.cdm.PendingEarlyPlanView = 0
	crom.cdm.PendingEarlyActivationRequested = false
}
