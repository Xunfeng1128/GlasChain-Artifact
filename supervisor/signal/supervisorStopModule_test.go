package signal

import (
	"testing"

	"blockEmulator/params"
)

func TestMarkCompleteSatisfiesBothStopModes(t *testing.T) {
	originalMode := params.StopSignalMode
	defer func() { params.StopSignalMode = originalMode }()

	for _, mode := range []string{"global", "per_shard"} {
		t.Run(mode, func(t *testing.T) {
			params.StopSignalMode = mode
			stop := NewStopSignal(3)
			if stop.GapEnough() {
				t.Fatal("fresh stop signal is already complete")
			}
			stop.MarkComplete()
			if !stop.GapEnough() {
				t.Fatal("MarkComplete did not satisfy stop condition")
			}
		})
	}
}
