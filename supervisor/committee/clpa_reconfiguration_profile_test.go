package committee

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/supervisor/supervisor_log"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"time"
)

type profileCommitteeHandler interface {
	HandleOtherMessage([]byte)
	HandleBlockInfo(*message.BlockInfoMsg)
	resumeAfterReconfiguration(uint64) error
}

func profileCommitteeFixture(t *testing.T, profile, scheduler string, pending bool) (*CLPACommitteeModule, profileCommitteeHandler, message.PartitionInstalled) {
	t.Helper()
	oldProfile, oldScheduler, oldMethod, oldShards := params.CLPAReconfigurationProfile, params.CLPAReconfigurationScheduler, params.ConsensusMethod, params.ShardNum
	t.Cleanup(func() {
		params.CLPAReconfigurationProfile, params.CLPAReconfigurationScheduler, params.ConsensusMethod, params.ShardNum = oldProfile, oldScheduler, oldMethod, oldShards
	})
	params.CLPAReconfigurationProfile, params.CLPAReconfigurationScheduler, params.ShardNum = profile, scheduler, 2
	params.ConsensusMethod = 1
	if pending {
		params.ConsensusMethod = 12
	}
	if err := params.ValidateCLPASchedulerConfig(); err != nil {
		t.Fatal(err)
	}
	base := &CLPACommitteeModule{
		sl:          &supervisor_log.SupervisorLog{Slog: log.New(io.Discard, "", 0)},
		shardMaxSeq: make(map[uint64]uint64), skipHistoryGraph: pending,
		clpaFreq: 48, clpaLastRunningTime: time.Now().Add(-time.Hour),
	}
	var handler profileCommitteeHandler = base
	if pending {
		handler = &GLASPendingCommitteeModule{CLPACommitteeModule: base}
	}
	if err := base.installation.begin(1, 5, 2); err != nil {
		t.Fatal(err)
	}
	mapping := map[string]uint64{"account": 1}
	if _, err := base.installation.bindMapping(mapping); err != nil {
		t.Fatal(err)
	}
	receipt := message.PartitionInstalled{Epoch: 1, TargetSeq: 5, CommittedSeq: 5, MappingDigest: message.PartitionMappingDigest(mapping)}
	return base, handler, receipt
}

func deliverProfileReceipt(t *testing.T, handler profileCommitteeHandler, receipt message.PartitionInstalled, shard uint64) {
	t.Helper()
	receipt.FromShard = shard
	b, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	handler.HandleOtherMessage(message.MergeMessage(message.CPartitionInstalled, b))
}

func TestCLPAProfilesResumeAndClockOrder(t *testing.T) {
	for _, profile := range []string{params.CLPAReconfigurationProfileFixed, params.CLPAReconfigurationProfileBlockResume, params.CLPAReconfigurationProfileInstallClock} {
		for _, scheduler := range []string{params.CLPAReconfigurationSchedulerEvent, params.CLPAReconfigurationSchedulerPoll} {
			for _, pending := range []bool{false, true} {
				method := "CLPA"
				if pending {
					method = "GLAS_Pending"
				}
				t.Run(profile+"/"+scheduler+"/"+method, func(t *testing.T) {
					base, handler, receipt := profileCommitteeFixture(t, profile, scheduler, pending)
					// Exercise two consecutive rounds, as used by injection and drain.
					for epoch := uint64(1); epoch <= 2; epoch++ {
						if epoch == 2 {
							receipt.Epoch, receipt.TargetSeq, receipt.CommittedSeq = epoch, 7, 7
							if err := base.installation.begin(epoch, 7, 2); err != nil {
								t.Fatal(err)
							}
							if _, err := base.installation.bindMapping(map[string]uint64{"account": 1}); err != nil {
								t.Fatal(err)
							}
						}
						deliverProfileReceipt(t, handler, receipt, 0)
						deliverProfileReceipt(t, handler, receipt, 1)
						base.installation.mu.Lock()
						ackAt := base.installation.allInstalledAt
						base.installation.mu.Unlock()
						if ackAt.IsZero() {
							t.Fatal("missing ACK receipt timestamp")
						}
						if profile != params.CLPAReconfigurationProfileFixed {
							// A block has been observed, allowing the existing gate to return.
							handler.HandleBlockInfo(&message.BlockInfoMsg{Epoch: int(epoch), SenderShardID: 0, BlockHeight: receipt.TargetSeq + 1})
						}
						before := time.Now()
						if err := handler.resumeAfterReconfiguration(epoch); err != nil {
							t.Fatal(err)
						}
						after := time.Now()
						if profile == params.CLPAReconfigurationProfileInstallClock {
							if base.reconfigurationDue() {
								t.Fatal("new ACK origin should not be due")
							}
							if !base.clpaLastRunningTime.Equal(ackAt) {
								t.Fatal("ordinary-block resume moved the ACK origin")
							}
						} else {
							if base.clpaLastRunningTime.Before(before) || base.clpaLastRunningTime.After(after) {
								t.Fatal("48-second origin was not recorded before returning to the sending loop")
							}
							if base.clpaLastRunningTime.Before(ackAt) {
								t.Fatal("origin precedes the resume condition")
							}
							if base.reconfigurationDue() {
								t.Fatal("new resume origin should not be due")
							}
						}
						base.installation.mu.Lock()
						active := base.installation.active
						base.installation.mu.Unlock()
						if active {
							t.Fatal("resume left an active round")
						}
						want := "observed_new_epoch_block"
						if profile == params.CLPAReconfigurationProfileFixed {
							want = "partition_installed_all_shards"
							if atomic.LoadInt32(&base.curEpoch) != 0 {
								t.Fatal("fixed required an ordinary block")
							}
						}
						if got := clpaReconfigurationCompletionBasis(); got != want {
							t.Fatalf("basis=%s want=%s", got, want)
						}
					}
				})
			}
		}
	}
}

func TestCLPAFixedWaitRequiresEveryValidACKAndUsesNotification(t *testing.T) {
	base, handler, receipt := profileCommitteeFixture(t, params.CLPAReconfigurationProfileFixed, params.CLPAReconfigurationSchedulerPoll, true)
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// This is the same original wait as fixed uses, with no query needed.
	go func() { done <- base.installation.wait(ctx, 1, time.Hour, nil) }()
	joined := false
	defer func() {
		if !joined {
			cancel()
			<-done
		}
	}()
	assertBlocked := func() {
		t.Helper()
		select {
		case err := <-done:
			joined = true
			t.Fatalf("released incomplete round: %v", err)
		case <-time.After(30 * time.Millisecond):
		}
	}
	handler.HandleBlockInfo(&message.BlockInfoMsg{Epoch: 1, SenderShardID: 0, BlockHeight: 6})
	assertBlocked()
	deliverProfileReceipt(t, handler, receipt, 0)
	deliverProfileReceipt(t, handler, receipt, 0) // duplicate is not another shard
	old := receipt
	old.Epoch = 0
	deliverProfileReceipt(t, handler, old, 1)
	assertBlocked()
	deliverProfileReceipt(t, handler, receipt, 1)
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("last ACK did not wake fixed wait; retry is one hour")
	}
}

func TestCLPAFixedWaitPropagatesInvalidReceipt(t *testing.T) {
	base, handler, receipt := profileCommitteeFixture(t, params.CLPAReconfigurationProfileFixed, params.CLPAReconfigurationSchedulerEvent, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- base.installation.wait(ctx, 1, time.Hour, nil) }()
	receipt.MappingDigest = "incorrect"
	deliverProfileReceipt(t, handler, receipt, 0)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("invalid ACK allowed fixed resume")
		}
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("invalid ACK did not wake fixed wait")
	}
}

func TestCLPAFixedOriginalRetryQueriesOnlyMissingShards(t *testing.T) {
	base, _, receipt := profileCommitteeFixture(t, params.CLPAReconfigurationProfileFixed, params.CLPAReconfigurationSchedulerPoll, false)
	base.installation.accept(receipt)
	queried := make(chan []uint64, 1)
	query := func(ctx context.Context, epoch uint64, missing []uint64) {
		queried <- missing
		ack := receipt
		ack.FromShard = 1
		base.installation.accept(ack)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := base.installation.wait(ctx, 1, time.Millisecond, query); err != nil {
		t.Fatal(err)
	}
	missing := <-queried
	if len(missing) != 1 || missing[0] != 1 {
		t.Fatalf("queried %v instead of missing shard 1", missing)
	}
}

func TestCLPABlockProfilesKeepBlockGateAndLateACKRules(t *testing.T) {
	for _, profile := range []string{params.CLPAReconfigurationProfileBlockResume, params.CLPAReconfigurationProfileInstallClock} {
		t.Run(profile, func(t *testing.T) {
			base, handler, receipt := profileCommitteeFixture(t, profile, params.CLPAReconfigurationSchedulerPoll, false)
			if base.installation.wake != nil {
				t.Fatal("ordinary-block profile allocated a fixed wait notifier")
			}
			// No ACKs: an ordinary block alone releases these profiles.
			handler.HandleBlockInfo(&message.BlockInfoMsg{Epoch: 1, SenderShardID: 0, BlockHeight: 6})
			if err := handler.resumeAfterReconfiguration(1); err != nil {
				t.Fatal(err)
			}
			resumedAt := base.clpaLastRunningTime
			deliverProfileReceipt(t, handler, receipt, 0)
			deliverProfileReceipt(t, handler, receipt, 1)
			base.installation.mu.Lock()
			ackAt := base.installation.allInstalledAt
			base.installation.mu.Unlock()
			if profile == params.CLPAReconfigurationProfileInstallClock {
				if ackAt.IsZero() {
					t.Fatal("late ACKs failed to start install_clock")
				}
				if base.reconfigurationDue() {
					t.Fatal("late fresh origin already due")
				}
				if !base.clpaLastRunningTime.Equal(ackAt) {
					t.Fatal("install_clock did not retain late ACK time")
				}
			} else {
				if !ackAt.IsZero() {
					t.Fatal("block_resume accepted ACKs after its closed round")
				}
				if base.reconfigurationDue() {
					t.Fatal("new block-resume origin already due")
				}
				if !base.clpaLastRunningTime.Equal(resumedAt) {
					t.Fatal("late ACK changed block-resume origin")
				}
			}
		})
	}
}

func TestCLPAProfileDeadlinesDoNotShareWrongOrigin(t *testing.T) {
	for _, profile := range []string{params.CLPAReconfigurationProfileFixed, params.CLPAReconfigurationProfileBlockResume, params.CLPAReconfigurationProfileInstallClock} {
		t.Run(profile, func(t *testing.T) {
			base, _, _ := profileCommitteeFixture(t, profile, params.CLPAReconfigurationSchedulerPoll, false)
			base.clpaLastRunningTime = time.Now().Add(-47 * time.Second)
			base.installation.allInstalledAt = time.Now().Add(-49 * time.Second)
			if got := base.reconfigurationDue(); got != (profile == params.CLPAReconfigurationProfileInstallClock) {
				t.Fatal("deadline used wrong origin")
			}
			base.clpaLastRunningTime = time.Now().Add(-49 * time.Second)
			base.installation.allInstalledAt = time.Now().Add(-47 * time.Second)
			if got := base.reconfigurationDue(); got != (profile != params.CLPAReconfigurationProfileInstallClock) {
				t.Fatal("deadline used wrong origin after reversal")
			}
		})
	}
}

func TestCLPAFixedOriginalWaitCancellation(t *testing.T) {
	base, _, _ := profileCommitteeFixture(t, params.CLPAReconfigurationProfileFixed, params.CLPAReconfigurationSchedulerPoll, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := base.installation.wait(ctx, 1, time.Hour, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation not preserved: %v", err)
	}
}
