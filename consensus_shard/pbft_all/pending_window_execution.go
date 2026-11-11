package pbft_all

import (
	"fmt"

	"blockEmulator/message"
	"blockEmulator/params"
)

func pendingWindowExecutionEnabled() bool {
	return params.ConsensusMethod == 12 && params.GLASPendingExecutionPolicy == params.GLASPendingExecutionWindowFirst
}

func validatePendingWindowMetadata(window *message.PendingExecutionWindow, _ uint64) error {
	if window != nil {
		return fmt.Errorf("partition proposals use leader-local priority marks")
	}
	return nil
}

func (cphm *CLPAPbftInsideExtraHandleMod) validatePendingWindowTransfer(atm *message.AccountTransferMsg) error {
	return validatePendingWindowMetadata(atm.PlanningWindow, atm.ATid)
}
