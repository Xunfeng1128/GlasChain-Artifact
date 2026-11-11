package supervisor

import (
	"blockEmulator/params"
	"fmt"
	"time"
)

// E2 seals a fixed interval measured from the first dispatch. E1/E3 stop
// only when the unified ledger accounts for every configured input.
func (d *Supervisor) waitGLASFormalCompletion(grace time.Duration) error {
	defer d.formalRunCancel()
	sender := startCommitteeControl(d.comMod)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	guard := completionStopGuard{grace: grace}
	var cutoff <-chan time.Time
	fixedWindow := params.GLASPendingFormalE2Enabled()

	for {
		select {
		case err := <-sender:
			if err != nil {
				return err
			}
			progress, reached := d.unifiedRecorder.StopProgress(time.Now())
			if (fixedWindow && reached) || (!fixedWindow && progress.Complete()) {
				return nil
			}
			return fmt.Errorf("GLAS control stopped before the formal goal: %+v", progress)
		case <-cutoff:
			return d.stopGLASFormalControl(sender, nil)
		case <-ticker.C:
		}
		if fixedWindow {
			if cutoff == nil {
				if deadline := d.unifiedRecorder.E2Deadline(); !deadline.IsZero() {
					timer := time.NewTimer(time.Until(deadline))
					defer timer.Stop()
					cutoff = timer.C
					ticker.Stop()
				}
			}
			continue
		}
		d.tcpLock.Lock()
		now := time.Now()
		progress := d.unifiedRecorder.CompletionProgress()
		_, err := guard.observe(now, progress, false, d.Ss.GapEnough())
		d.tcpLock.Unlock()
		if progress.Complete() || err != nil {
			return d.stopGLASFormalControl(sender, err)
		}
	}
}

func (d *Supervisor) stopGLASFormalControl(sender <-chan error, runErr error) error {
	d.formalRunCancel()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case err := <-sender:
		if runErr != nil {
			return runErr
		}
		return err
	case <-timer.C:
		return fmt.Errorf("GLAS control did not stop within %s (run error: %v)", 10*time.Second, runErr)
	}
}
