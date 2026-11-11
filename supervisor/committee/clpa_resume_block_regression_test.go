package committee

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/supervisor/supervisor_log"
)

// This exercises the real supervisor message handlers and resume wait, with no
// TCP listener, worker node, transaction injection, or experiment process.
// Both node schedulers must share this supervisor completion condition.
func TestCLPAResumeRequiresObservedNewEpochBlock(t *testing.T) {
	for _, scheduler := range []string{params.CLPAReconfigurationSchedulerEvent, params.CLPAReconfigurationSchedulerPoll} {
		for _, pending := range []bool{false, true} {
			name := "CLPA"
			if pending {
				name = "GLAS_Pending"
			}
			t.Run(name+"/"+scheduler, func(t *testing.T) {
				oldScheduler, oldMethod, oldShards := params.CLPAReconfigurationScheduler, params.ConsensusMethod, params.ShardNum
				t.Cleanup(func() {
					params.CLPAReconfigurationScheduler, params.ConsensusMethod, params.ShardNum = oldScheduler, oldMethod, oldShards
				})
				params.CLPAReconfigurationScheduler, params.ShardNum = scheduler, 2
				params.ConsensusMethod = 1
				if pending {
					params.ConsensusMethod = 12
				}
				if err := params.ValidateCLPASchedulerConfig(); err != nil {
					t.Fatal(err)
				}

				base := &CLPACommitteeModule{
					sl:                  &supervisor_log.SupervisorLog{Slog: log.New(io.Discard, "", 0)},
					shardMaxSeq:         make(map[uint64]uint64),
					skipHistoryGraph:    pending,
					clpaFreq:            48,
					clpaLastRunningTime: time.Now().Add(-time.Hour),
				}
				var handler interface {
					HandleOtherMessage([]byte)
					HandleBlockInfo(*message.BlockInfoMsg)
					waitNewEpochBlock(uint64) error
				} = base
				if pending {
					// Use actual embedding and delegation, not a copied implementation.
					handler = &GLASPendingCommitteeModule{CLPACommitteeModule: base}
				}

				mapping := map[string]uint64{"account": 1}
				for epoch := uint64(1); epoch <= 2; epoch++ {
					target := uint64(5) + epoch
					if err := base.installation.begin(epoch, target, 2); err != nil {
						t.Fatalf("begin round %d: %v", epoch, err)
					}
					if _, err := base.installation.bindMapping(mapping); err != nil {
						t.Fatal(err)
					}
					deliverReceipts := func() {
						// All installation receipts are already present. They must not
						// resume injection before a new-epoch ordinary block is observed.
						for shard := uint64(0); shard < 2; shard++ {
							receipt := message.PartitionInstalled{
								Epoch: epoch, FromShard: shard, TargetSeq: target, CommittedSeq: target,
								MappingDigest: message.PartitionMappingDigest(mapping),
							}
							body, err := json.Marshal(receipt)
							if err != nil {
								t.Fatal(err)
							}
							handler.HandleOtherMessage(message.MergeMessage(message.CPartitionInstalled, body))
						}
					}
					if epoch == 1 {
						deliverReceipts()
					}
					beforeBlock, err := base.installation.intervalStart(base.clpaLastRunningTime)
					if err != nil || (epoch == 1) == beforeBlock.IsZero() {
						t.Fatalf("round %d unexpected pre-block interval origin %v, err=%v", epoch, beforeBlock, err)
					}
					if base.reconfigurationDue() {
						t.Fatal("next round used the expired previous origin instead of its own ACKs")
					}
					// Round 2 deliberately has no receipts at all: the block is the
					// completion condition, rather than a second barrier after ACKs.
					assertCLPAResumeOnEmptyBlock(t, base, handler, epoch, target)
					afterBlock, err := base.installation.intervalStart(base.clpaLastRunningTime)
					if err != nil || !afterBlock.Equal(beforeBlock) {
						t.Fatalf("ordinary block reset interval origin: before=%v after=%v err=%v", beforeBlock, afterBlock, err)
					}
					if epoch == 2 {
						// Injection has already resumed, but its clock must still wait
						// for the current round's late ACKs without reopening the gate.
						if base.reconfigurationDue() {
							t.Fatal("missing ACKs allowed another reconfiguration")
						}
						deliverReceipts()
						afterBlock, err = base.installation.intervalStart(base.clpaLastRunningTime)
						if err != nil || afterBlock.IsZero() {
							t.Fatalf("late ACKs did not start the interval: %v, %v", afterBlock, err)
						}
						base.installation.mu.Lock()
						active := base.installation.active
						base.installation.mu.Unlock()
						if active {
							t.Fatal("late ACKs reopened the ordinary-block gate")
						}
					}
				}

				// Finishing an observed epoch must retire the tracker even when
				// receipts were missing. Otherwise the next prepare is rejected.
				if err := base.installation.begin(3, 8, 2); err != nil {
					t.Fatalf("previous observed epoch left an active installation: %v", err)
				}
			})
		}
	}
}

func assertCLPAResumeOnEmptyBlock(t *testing.T, base *CLPACommitteeModule, handler interface {
	HandleBlockInfo(*message.BlockInfoMsg)
	waitNewEpochBlock(uint64) error
}, epoch, target uint64) {
	t.Helper()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- handler.waitNewEpochBlock(epoch)
	}()
	<-started
	joined := false
	defer func() {
		if !joined {
			// Release a failed test's real one-second wait instead of leaking a
			// goroutine into another subtest that changes the global parameters.
			atomic.StoreInt32(&base.curEpoch, int32(epoch))
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Errorf("round %d waiter did not exit during test cleanup", epoch)
			}
		}
	}()
	assertWaiting := func(reason string) {
		t.Helper()
		select {
		case err := <-done:
			joined = true
			t.Fatalf("round %d resumed %s: %v", epoch, reason, err)
		case <-time.After(80 * time.Millisecond):
		}
	}
	assertWaiting("without a new-epoch block")
	handler.HandleBlockInfo(&message.BlockInfoMsg{
		Epoch: int(epoch - 1), SenderShardID: 0, BlockHeight: target,
	})
	assertWaiting("after only an old-epoch block")
	if got := atomic.LoadInt32(&base.curEpoch); got != int32(epoch-1) {
		t.Fatalf("old block changed epoch to %d; want %d", got, epoch-1)
	}

	// Alternate reporting shards across rounds. A single empty ordinary block
	// from either shard is sufficient; no all-shard block observation is needed.
	handler.HandleBlockInfo(&message.BlockInfoMsg{
		Epoch: int(epoch), SenderShardID: epoch % 2, BlockHeight: target + 1,
		BlockBodyLength: 0,
	})
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatalf("round %d could not finish after new-epoch empty block: %v", epoch, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("round %d did not resume after observing one new-epoch empty block", epoch)
	}
	base.installation.mu.Lock()
	active := base.installation.active
	base.installation.mu.Unlock()
	if active {
		t.Fatalf("round %d completed but tracker is still active", epoch)
	}
}

func TestCLPAObservedEpochFinishKeepsInstallationIdentityChecks(t *testing.T) {
	for _, name := range []string{"inactive", "wrong_epoch", "unbound_mapping", "existing_receipt_error"} {
		t.Run(name, func(t *testing.T) {
			tracker := new(clpaInstallationTracker)
			if name != "inactive" {
				if err := tracker.begin(1, 5, 2); err != nil {
					t.Fatal(err)
				}
				if name != "unbound_mapping" {
					if _, err := tracker.bindMapping(nil); err != nil {
						t.Fatal(err)
					}
				}
			}
			epoch := uint64(1)
			if name == "wrong_epoch" {
				epoch = 2
			}
			if name == "existing_receipt_error" {
				tracker.fail(fmt.Errorf("invalid installation receipt"))
			}
			if err := tracker.finishObservedEpoch(epoch); err == nil {
				t.Fatal("invalid installation identity/error was hidden by a block observation")
			}
		})
	}
}
