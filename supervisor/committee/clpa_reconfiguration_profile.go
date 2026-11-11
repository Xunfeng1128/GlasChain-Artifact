package committee

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Select existing supervisor behaviour; node scheduling and the single local
// installation timestamp are independent of this option.
func clpaReconfigurationCompletionBasis() string {
	if params.CLPAReconfigurationProfile == params.CLPAReconfigurationProfileFixed {
		return "partition_installed_all_shards"
	}
	return "observed_new_epoch_block"
}

// This runs on the existing sending/draining goroutine. For fixed and
// block_resume, record the new clock origin before returning to the next send.
// install_clock must retain the receive time of the last valid ACK instead.
func (ccm *CLPACommitteeModule) resumeAfterReconfiguration(epoch uint64) error {
	var err error
	if params.CLPAReconfigurationProfile == params.CLPAReconfigurationProfileFixed {
		err = ccm.waitPartitionInstallation(epoch)
	} else {
		err = ccm.waitNewEpochBlock(epoch)
	}
	if err != nil {
		return err
	}
	if params.CLPAReconfigurationProfile != params.CLPAReconfigurationProfileInstallClock {
		ccm.clpaLastRunningTime = time.Now()
		ccm.sl.Slog.Printf("DIAG CLPA reconfiguration interval started: epoch=%d, profile=%s, originUnixNS=%d, intervalSeconds=%d\n", epoch, params.CLPAReconfigurationProfile, ccm.clpaLastRunningTime.UnixNano(), ccm.clpaFreq)
	}
	return nil
}

// Completion is independent of the next ordinary block. Receipts are emitted
// only after PBFT commit has installed the map, state and migration queues.
func (ccm *CLPACommitteeModule) waitPartitionInstallation(epoch uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := ccm.installation.wait(ctx, epoch, 2*time.Second, ccm.queryPartitionInstallations); err != nil {
		return err
	}
	ccm.sl.Slog.Printf("DIAG CLPA all shards installed: epoch=%d shards=%d; injection may resume\n", epoch, params.ShardNum)
	return nil
}

// query retries only missing receipts. Its timer is transport recovery, never
// a minimum wait: an event received before/during wait is checked immediately.
func (t *clpaInstallationTracker) wait(ctx context.Context, epoch uint64, retry time.Duration, query func(context.Context, uint64, []uint64)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if retry <= 0 {
		return fmt.Errorf("invalid installation retry interval")
	}
	timer := time.NewTimer(retry)
	defer timer.Stop()
	var queryDone <-chan struct{}
	for {
		t.mu.Lock()
		if !t.active || t.epoch != epoch {
			t.mu.Unlock()
			return fmt.Errorf("no active installation epoch %d", epoch)
		}
		err := t.err
		missing := make([]uint64, 0, t.shards)
		for shard := uint64(0); shard < uint64(t.shards); shard++ {
			if _, ok := t.receipts[shard]; !ok {
				missing = append(missing, shard)
			}
		}
		complete := t.digest != "" && len(missing) == 0 && err == nil
		if complete {
			t.active = false
		}
		wake := t.wake
		t.mu.Unlock()
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("partition installation epoch=%d missing_shards=%v: %w", epoch, missing, ctx.Err())
		case <-wake:
		case <-queryDone:
			queryDone = nil
			timer.Reset(retry)
		case <-timer.C:
			if query == nil {
				timer.Reset(retry)
				continue
			}
			done := make(chan struct{})
			queryDone = done
			go func(missing []uint64) { defer close(done); query(ctx, epoch, missing) }(missing)
		}
	}
}

func (ccm *CLPACommitteeModule) queryPartitionInstallations(ctx context.Context, epoch uint64, missing []uint64) {
	if ccm.partitionRecoverStaged != nil {
		ccm.partitionRecoverStaged(ctx, epoch, missing)
	}
	payload, _ := json.Marshal(message.PartitionInstallQuery{Epoch: epoch})
	raw := message.MergeMessage(message.CPartitionInstallQuery, payload)
	// Notify all replicas of each missing shard; after a view change only its
	// current leader responds using the receipt saved by every committing replica.
	var sends sync.WaitGroup
	for _, shard := range missing {
		for _, address := range ccm.IpNodeTable[shard] {
			if address == "" {
				continue
			}
			sends.Add(1)
			go func(address string) {
				defer sends.Done()
				bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				if err := networks.TcpDialContext(bounded, raw, address); err != nil && ctx.Err() == nil && ccm.sl != nil {
					ccm.sl.Slog.Printf("DIAG CLPA install receipt query failed: epoch=%d address=%s err=%v\n", epoch, address, err)
				}
			}(address)
		}
	}
	sends.Wait()
}
