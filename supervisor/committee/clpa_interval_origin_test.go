package committee

import (
	"testing"
	"time"
)

func TestCLPAIntervalStartsOnlyOnLastValidUniqueReceipt(t *testing.T) {
	tracker, receipt := installationFixture(t)
	oldOrigin := time.Now().Add(-time.Hour)
	assertUnset := func() {
		t.Helper()
		at, err := tracker.intervalStart(oldOrigin)
		if err != nil || !at.IsZero() {
			t.Fatalf("incomplete ACKs started interval: at=%v err=%v", at, err)
		}
	}
	assertUnset()
	if at := tracker.accept(receipt); !at.IsZero() {
		t.Fatal("first of two ACKs started interval")
	}
	tracker.accept(receipt)
	wrong := receipt
	wrong.Epoch++
	tracker.accept(wrong)
	assertUnset()

	receipt.FromShard = 1
	before := time.Now()
	started := tracker.accept(receipt)
	after := time.Now()
	if started.IsZero() || started.Before(before) || started.After(after) {
		t.Fatalf("origin is not last valid ACK receipt time: %v", started)
	}
	if duplicate := tracker.accept(receipt); !duplicate.IsZero() {
		t.Fatal("duplicate last ACK reset the interval")
	}
	if err := tracker.finishObservedEpoch(3); err != nil {
		t.Fatal(err)
	}
	tracker.accept(receipt)
	got, err := tracker.intervalStart(oldOrigin)
	if err != nil || !got.Equal(started) {
		t.Fatalf("ordinary block or replay changed origin: got=%v want=%v err=%v", got, started, err)
	}
	if err := tracker.begin(4, 11, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := tracker.bindMapping(nil); err != nil {
		t.Fatal(err)
	}
	tracker.accept(receipt)
	assertUnset()
}

func TestCLPAInvalidReceiptCannotStartInterval(t *testing.T) {
	tracker, receipt := installationFixture(t)
	tracker.accept(receipt)
	bad := receipt
	bad.FromShard = 1
	bad.MappingDigest = "wrong"
	if at := tracker.accept(bad); !at.IsZero() {
		t.Fatal("invalid ACK started interval")
	}
	receipt.FromShard = 1
	if at := tracker.accept(receipt); !at.IsZero() {
		t.Fatal("existing invalid ACK error was hidden")
	}
	if _, err := tracker.intervalStart(time.Time{}); err == nil {
		t.Fatal("interval check ignored invalid ACK")
	}
}

func TestCLPANextDeadlineUsesInstallationNotBlockResume(t *testing.T) {
	for _, tc := range []struct {
		name string
		age  time.Duration
		due  bool
	}{
		{"before_48_seconds", 47 * time.Second, false},
		{"after_48_seconds", 49 * time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &CLPACommitteeModule{clpaFreq: 48, clpaLastRunningTime: time.Now()}
			if err := base.installation.begin(1, 5, 1); err != nil {
				t.Fatal(err)
			}
			// Model a block-resume time later than all-shard installation.
			origin := time.Now().Add(-tc.age)
			base.installation.allInstalledAt = origin
			if due := base.reconfigurationDue(); due != tc.due {
				t.Fatalf("due=%v want=%v; installation age=%v", due, tc.due, tc.age)
			}
			if !base.clpaLastRunningTime.Equal(origin) {
				t.Fatal("sending/draining check replaced installation origin with current time")
			}
		})
	}
}

func TestCLPAFirstIntervalStillStartsFromFirstBatch(t *testing.T) {
	base := &CLPACommitteeModule{clpaFreq: 48}
	if base.reconfigurationDue() {
		t.Fatal("unstarted injection has a deadline")
	}
	base.clpaLastRunningTime = time.Now().Add(-49 * time.Second)
	if !base.reconfigurationDue() {
		t.Fatal("first interval no longer uses first-batch origin")
	}
}
