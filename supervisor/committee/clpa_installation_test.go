package committee

import (
	"blockEmulator/message"
	"encoding/json"
	"sync"
	"testing"
)

func installationFixture(t *testing.T) (*clpaInstallationTracker, message.PartitionInstalled) {
	t.Helper()
	tracker := new(clpaInstallationTracker)
	if err := tracker.begin(3, 9, 2); err != nil {
		t.Fatal(err)
	}
	mapping := map[string]uint64{"a": 1}
	if _, err := tracker.bindMapping(mapping); err != nil {
		t.Fatal(err)
	}
	return tracker, message.PartitionInstalled{Epoch: 3, FromShard: 0, TargetSeq: 9, CommittedSeq: 9, MappingDigest: message.PartitionMappingDigest(mapping)}
}
func TestCLPAInstallationClosesObservedEpochWithoutEveryReceipt(t *testing.T) {
	tracker, receipt := installationFixture(t)
	tracker.accept(receipt)
	tracker.accept(receipt)
	if err := tracker.begin(4, 11, 2); err == nil {
		t.Fatal("allowed a new round before the ordinary-block gate closed this one")
	}
	// Called only after the supervisor observes the new-epoch ordinary block.
	// One receipt is deliberately missing: receipts are not the resume gate.
	if err := tracker.finishObservedEpoch(3); err != nil {
		t.Fatal(err)
	}
	// A late receipt cannot reopen this epoch.
	tracker.accept(receipt)
	if err := tracker.begin(4, 11, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := tracker.bindMapping(nil); err != nil {
		t.Fatal(err)
	}
	tracker.accept(receipt)
	tracker.mu.Lock()
	received := len(tracker.receipts)
	tracker.mu.Unlock()
	if received != 0 {
		t.Fatal("old epoch receipt counted in new epoch")
	}
}
func TestCLPAInstallationIgnoresPreMapReceipt(t *testing.T) {
	tracker := new(clpaInstallationTracker)
	if err := tracker.begin(1, 5, 1); err != nil {
		t.Fatal(err)
	}
	receipt := message.PartitionInstalled{Epoch: 1, TargetSeq: 5, CommittedSeq: 5, MappingDigest: message.PartitionMappingDigest(nil)}
	tracker.accept(receipt)
	if len(tracker.receipts) != 0 {
		t.Fatal("accepted receipt before map binding")
	}
	if _, err := tracker.bindMapping(map[string]uint64{}); err != nil {
		t.Fatal(err)
	}
	tracker.accept(receipt)
	if err := tracker.finishObservedEpoch(1); err != nil {
		t.Fatal(err)
	}
}
func TestCLPAInstallationRejectsInvalidAndConflictingReceipts(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*message.PartitionInstalled)
		duplicate bool
	}{
		{"target", func(r *message.PartitionInstalled) { r.TargetSeq++ }, false},
		{"height", func(r *message.PartitionInstalled) { r.CommittedSeq = 8 }, false},
		{"digest", func(r *message.PartitionInstalled) { r.MappingDigest = "wrong" }, false},
		{"shard", func(r *message.PartitionInstalled) { r.FromShard = 2 }, false},
		{"conflicting replay", func(r *message.PartitionInstalled) { r.CommittedSeq++ }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker, receipt := installationFixture(t)
			if tc.duplicate {
				tracker.accept(receipt)
			}
			tc.mutate(&receipt)
			tracker.accept(receipt)
			if err := tracker.finishObservedEpoch(3); err == nil {
				t.Fatal("invalid receipt allowed completion")
			}
		})
	}
}
func TestCLPAInstallationConcurrentDuplicatesAndWrongEpochIgnored(t *testing.T) {
	tracker, receipt := installationFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); r := receipt; r.FromShard = uint64(i % 2); tracker.accept(r) }(i)
	}
	wrong := receipt
	wrong.Epoch++
	tracker.accept(wrong)
	wg.Wait()
	if err := tracker.finishObservedEpoch(3); err != nil {
		t.Fatal(err)
	}
}
func TestPendingDelegatesInstallationReceiptsToSharedTracker(t *testing.T) {
	_, receipt := installationFixture(t)
	// Initialize the real embedded tracker independently; never copy its mutex.
	base := &CLPACommitteeModule{}
	if err := base.installation.begin(3, 9, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := base.installation.bindMapping(map[string]uint64{"a": 1}); err != nil {
		t.Fatal(err)
	}
	pending := &GLASPendingCommitteeModule{CLPACommitteeModule: base}
	for shard := uint64(0); shard < 2; shard++ {
		receipt.FromShard = shard
		body, err := json.Marshal(receipt)
		if err != nil {
			t.Fatal(err)
		}
		pending.HandleOtherMessage(message.MergeMessage(message.CPartitionInstalled, body))
	}
	if len(base.installation.receipts) != 2 {
		t.Fatal("pending did not delegate installation receipts")
	}
	if !base.installation.active {
		t.Fatal("receipts prematurely closed the ordinary-block gate")
	}
	if err := base.installation.finishObservedEpoch(3); err != nil {
		t.Fatal(err)
	}
}
