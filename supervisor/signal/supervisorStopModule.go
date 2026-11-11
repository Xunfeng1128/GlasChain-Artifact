package signal

import (
	"blockEmulator/params"
	"sync"
)

// StopSignal controls when the supervisor sends the termination message.
//
// Two modes are supported (configured via params.StopSignalMode):
//
//	"global"    – original behaviour. A single shared counter is incremented
//	              for every empty-block report from any shard and reset to 0
//	              whenever any shard reports a non-empty block.  The system
//	              stops when the counter reaches 3 × ShardNums.
//
//	"per_shard" – each shard maintains its own independent counter.  An empty
//	              block increments only that shard's counter; a non-empty block
//	              resets only that shard's counter.  The system stops only when
//	              every shard has individually reached 3 consecutive empty
//	              blocks. This detects observed idleness, not empty pools or
//	              completed delivery; unified runs also verify logical completion.
type StopSignal struct {
	stoplock        sync.Mutex
	e2StopRequested bool // experimental goal observed; leave the control loop at a safe boundary
	perShard        bool // true → per-shard mode; false → global mode

	// global mode
	stopGap       int
	stopThreshold int

	// per-shard mode
	shardGaps      map[uint64]int
	shardThreshold int
}

func NewStopSignal(shardNum int) *StopSignal {
	ss := &StopSignal{}
	if params.StopSignalMode == "per_shard" {
		ss.perShard = true
		ss.shardThreshold = 3
		ss.shardGaps = make(map[uint64]int, shardNum)
		for i := 0; i < shardNum; i++ {
			ss.shardGaps[uint64(i)] = 0
		}
	} else {
		// default: global mode (original behaviour)
		ss.perShard = false
		ss.stopGap = 0
		ss.stopThreshold = 3 * shardNum
	}
	return ss
}

// StopGap_Inc is called when shardID reports an empty block.
func (ss *StopSignal) StopGap_Inc(shardID uint64) {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	if ss.perShard {
		ss.shardGaps[shardID]++
	} else {
		ss.stopGap++
	}
}

// StopGap_Reset is called when shardID reports a non-empty block.
func (ss *StopSignal) StopGap_Reset(shardID uint64) {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	if ss.perShard {
		ss.shardGaps[shardID] = 0
	} else {
		ss.stopGap = 0
	}
}

// StopGap_ResetAll resets all counters; called each time a new tx batch is
// injected so that in-flight transactions are not misread as idle.
func (ss *StopSignal) StopGap_ResetAll() {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	if ss.perShard {
		for k := range ss.shardGaps {
			ss.shardGaps[k] = 0
		}
	} else {
		ss.stopGap = 0
	}
}

// MarkComplete satisfies the stop condition explicitly. Protocols with an
// exact execution horizon (for example GLAS-FREIGHT's strict B-block
// manifest) use this after reconciling every planned block, instead of
// manufacturing three additional empty blocks merely to trip the legacy
// idle heuristic.
func (ss *StopSignal) MarkComplete() {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	if ss.perShard {
		for shard := range ss.shardGaps {
			ss.shardGaps[shard] = ss.shardThreshold
		}
		return
	}
	ss.stopGap = ss.stopThreshold
}

// GapEnough returns true when the stop condition is satisfied.
func (ss *StopSignal) GapEnough() bool {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	if ss.perShard {
		for _, gap := range ss.shardGaps {
			if gap < ss.shardThreshold {
				return false
			}
		}
		return true
	}
	return ss.stopGap >= ss.stopThreshold
}

// RequestE2Stop asks injection/control to return at a safe boundary. It never
// changes the empty-block counters used by ordinary complete-workload runs.
func (ss *StopSignal) RequestE2Stop() {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	ss.e2StopRequested = true
}

func (ss *StopSignal) E2StopRequested() bool {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	return ss.e2StopRequested
}
