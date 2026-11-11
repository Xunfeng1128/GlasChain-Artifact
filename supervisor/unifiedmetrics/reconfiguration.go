package unifiedmetrics

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ReconfigurationHeader describes supervisor control observations, never a
// claim that every worker enters or leaves a barrier at that instant.
var ReconfigurationHeader = []string{
	"event", "reconfiguration_id", "target_epoch", "phase", "scope",
	"elapsed_ns", "observed_unix_ns", "clock_phase", "status", "completion_basis",
}

type ReconfigurationEvent struct {
	Event           string
	ID              uint64
	TargetEpoch     uint64
	Phase           string
	Status          string
	CompletionBasis string
}

// CompletionBasisAllShards records GARET's stronger completion condition:
// the supervisor has observed a new-epoch block from every shard.
const CompletionBasisAllShards = "observed_new_epoch_block_all_shards"

// CLPA and GLAS_Pending resume dispatch once every shard reports actual
// partition installation, without waiting for its next ordinary block.
const CompletionBasisInstalledAllShards = "partition_installed_all_shards"

func (r *Recorder) openReconfigurationLog() error {
	file, err := os.OpenFile(filepath.Join(r.directory, "reconfiguration.csv"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	r.reconfigurationFile, r.reconfigurationCSV = file, csv.NewWriter(file)
	r.meta.ReconfigurationSchemaVersion = 1
	if err := r.reconfigurationCSV.Write(append([]string(nil), ReconfigurationHeader...)); err != nil {
		file.Close()
		return err
	}
	if err := r.flushReconfiguration(); err != nil {
		file.Close()
		return err
	}
	return nil
}

// RecordReconfiguration uses exactly the transaction recorder's monotonic
// first-dispatch origin. Events before it is available retain their wall-clock
// observation and an explicitly empty elapsed field; they never establish or
// move the origin. This also leaves a readable diagnostic on pre-dispatch abort.
func (r *Recorder) RecordReconfiguration(event ReconfigurationEvent, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.writable(); err != nil {
		return err
	}
	if at.IsZero() || event.ID == 0 || event.TargetEpoch == 0 {
		return fmt.Errorf("invalid reconfiguration identity or timestamp")
	}
	if event.Phase != "injecting" && event.Phase != "draining" {
		return fmt.Errorf("invalid reconfiguration phase %q", event.Phase)
	}
	switch event.Event {
	case "trigger":
		if event.Status != "" {
			return fmt.Errorf("trigger cannot have completion status")
		}
	case "injection_pause_begin":
		if event.Phase != "injecting" || event.Status != "" {
			return fmt.Errorf("invalid injection pause begin")
		}
	case "injection_pause_end":
		if event.Phase != "injecting" || (event.Status != "completed" && event.Status != "skipped") {
			return fmt.Errorf("invalid injection pause end")
		}
	case "finish":
		if event.Status != "completed" && event.Status != "skipped" && event.Status != "failed" {
			return fmt.Errorf("invalid reconfiguration finish status")
		}
	default:
		return fmt.Errorf("invalid reconfiguration event %q", event.Event)
	}
	if event.CompletionBasis != "observed_new_epoch_block" && event.CompletionBasis != "glas_start_barrier" &&
		event.CompletionBasis != CompletionBasisAllShards && event.CompletionBasis != CompletionBasisInstalledAllShards {
		return fmt.Errorf("invalid reconfiguration completion basis %q", event.CompletionBasis)
	}
	elapsed, clockPhase := "", "before_first_injection"
	if !r.start.IsZero() {
		elapsed, clockPhase = i64(at.Sub(r.start).Nanoseconds()), "relative_to_first_injection"
	}
	row := []string{event.Event, u64(event.ID), u64(event.TargetEpoch), event.Phase,
		"supervisor_control", elapsed, i64(at.UnixNano()), clockPhase, event.Status, event.CompletionBasis}
	if err := r.reconfigurationCSV.Write(row); err != nil {
		r.failure = err
		return err
	}
	r.meta.ReconfigurationRows++
	if at.After(r.lastReconfigurationTime) {
		r.lastReconfigurationTime = at
	}
	if !r.start.IsZero() {
		r.advance(at)
	}
	return r.flushReconfiguration()
}

func (r *Recorder) flushReconfiguration() error {
	r.reconfigurationCSV.Flush()
	if err := r.reconfigurationCSV.Error(); err != nil {
		r.failure = err
		return err
	}
	return nil
}
