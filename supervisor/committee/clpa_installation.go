package committee

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"fmt"
	"sync"
	"time"
)

// Retain the prepared mapping identity and validate installation receipts.
// install_clock retains the last ACK receipt time independently of block resume.
// fixed reuses the original ACK notification wait; block_resume ignores ACKs
// received after its ordinary-block gate closes the round.
type clpaInstallationTracker struct {
	mu             sync.Mutex
	wake           chan struct{} // Used only by the original fixed ACK wait.
	active         bool
	epoch, target  uint64
	shards         int
	digest         string
	receipts       map[uint64]message.PartitionInstalled
	allInstalledAt time.Time
	err            error
}

func (t *clpaInstallationTracker) begin(epoch, target uint64, shards int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if epoch == 0 || target == 0 || shards < 1 {
		return fmt.Errorf("invalid partition installation identity")
	}
	if t.active {
		return fmt.Errorf("partition installation epoch %d still active", t.epoch)
	}
	if epoch <= t.epoch {
		return fmt.Errorf("partition installation epoch %d does not advance %d", epoch, t.epoch)
	}
	t.active, t.epoch, t.target, t.shards = true, epoch, target, shards
	t.digest, t.err = "", nil
	t.allInstalledAt = time.Time{}
	t.receipts = make(map[uint64]message.PartitionInstalled)
	if params.CLPAReconfigurationProfile == params.CLPAReconfigurationProfileFixed {
		if t.wake == nil {
			t.wake = make(chan struct{}, 1)
		}
	} else {
		t.wake = nil
	}
	return nil
}

// Same buffered notification as the original fixed installation wait. No
// notification channel is allocated for block_resume or install_clock.
func (t *clpaInstallationTracker) notifyLocked() {
	if t.wake == nil {
		return
	}
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

func (t *clpaInstallationTracker) fail(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active && t.err == nil {
		t.err = err
		t.notifyLocked()
	}
}
func (t *clpaInstallationTracker) bindMapping(mapping map[string]uint64) (uint64, error) {
	return t.bindPlan(mapping, nil)
}

func (t *clpaInstallationTracker) preparedEpoch() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.epoch
}

func (t *clpaInstallationTracker) bindPlan(mapping map[string]uint64, groups *message.GARETPlacement) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.active {
		return 0, fmt.Errorf("partition map has no active prepare")
	}
	if groups != nil && groups.Epoch != t.epoch {
		return 0, fmt.Errorf("GARET group assignment epoch %d differs from prepare %d", groups.Epoch, t.epoch)
	}
	digest := message.PartitionPlanDigest(mapping, groups)
	if t.digest != "" && t.digest != digest {
		return 0, fmt.Errorf("partition map changed within epoch %d", t.epoch)
	}
	t.digest = digest
	return t.epoch, nil
}
func (t *clpaInstallationTracker) accept(receipt message.PartitionInstalled) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Replays cannot open a round or complete a different round. A receipt before
	// map dispatch cannot be valid, and is deliberately not retained.
	if receipt.Epoch != t.epoch || t.digest == "" ||
		(!t.active && params.CLPAReconfigurationProfile != params.CLPAReconfigurationProfileInstallClock) {
		return time.Time{}
	}
	if receipt.FromShard >= uint64(t.shards) || receipt.TargetSeq != t.target || receipt.CommittedSeq < t.target || receipt.MappingDigest != t.digest {
		if t.err == nil {
			t.err = fmt.Errorf("partition installation mismatch epoch=%d shard=%d target=%d committed=%d", receipt.Epoch, receipt.FromShard, receipt.TargetSeq, receipt.CommittedSeq)
		}
		t.notifyLocked()
		return time.Time{}
	}
	if previous, exists := t.receipts[receipt.FromShard]; exists {
		if previous != receipt && t.err == nil {
			t.err = fmt.Errorf("conflicting partition installation epoch=%d shard=%d", receipt.Epoch, receipt.FromShard)
			t.notifyLocked()
		}
		return time.Time{}
	}
	t.receipts[receipt.FromShard] = receipt
	if len(t.receipts) == t.shards && t.err == nil && t.allInstalledAt.IsZero() {
		t.allInstalledAt = time.Now()
		t.notifyLocked()
		return t.allInstalledAt
	}
	t.notifyLocked()
	return time.Time{}
}

// Before the first prepare, use the original first-batch origin. Afterwards,
// an incomplete ACK set has no next-interval origin yet; it does not hold up
// transaction injection after the ordinary-block gate has been released.
func (t *clpaInstallationTracker) intervalStart(initial time.Time) (time.Time, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return time.Time{}, t.err
	}
	if t.epoch == 0 {
		return initial, nil
	}
	return t.allInstalledAt, nil
}

// This replaces the cleanup previously performed by the all-receipt wait.
// It does not wait for acknowledgements or impose another completion condition.
func (t *clpaInstallationTracker) finishObservedEpoch(epoch uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.active || t.epoch != epoch {
		return fmt.Errorf("no active installation epoch %d", epoch)
	}
	if t.err != nil {
		return t.err
	}
	if t.digest == "" {
		return fmt.Errorf("partition epoch %d has no bound mapping", epoch)
	}
	t.active = false
	return nil
}
