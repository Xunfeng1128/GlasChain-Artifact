package unifiedmetrics

import (
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

func (r *Recorder) prepareE2(c RunConfig) error {
	if c.E2FixedWindow {
		if c.ConsensusMethod != 12 ||
			!validE2Duration(c.E2ObservationSeconds) || c.E2PrefixCount != 0 || c.E2PrefixFile != "" {
			return fmt.Errorf("invalid formal GLAS fixed E2 observation")
		}
		r.progress.E2FixedWindow = true
		r.meta.CompletionScope = "e2"
		r.meta.E2StopPolicy = "fixed_window"
		r.meta.E2ObservationSeconds = c.E2ObservationSeconds
		return nil
	}
	if c.E2ObservationSeconds == 0 && c.E2PrefixCount == 0 && c.E2PrefixFile == "" {
		return nil
	}
	if !validE2Duration(c.E2ObservationSeconds) || c.E2PrefixCount <= 0 || uint64(c.E2PrefixCount) > c.ExpectedTransactions || c.E2PrefixFile == "" {
		return fmt.Errorf("invalid E2 observation or input prefix")
	}
	// Tag the input-ordered prefix in the logical-source ledger. Dispatch order
	// within a batch depends on the shard and cannot define the prefix.
	f, err := os.Open(c.E2PrefixFile)
	if err != nil {
		return fmt.Errorf("open E2 input prefix: %w", err)
	}
	defer f.Close()
	digest := sha256.New()
	input := csv.NewReader(io.TeeReader(f, digest))
	input.ReuseRecord = true
	input.FieldsPerRecord = 1
	header, err := input.Read()
	if err != nil || header[0] != "source_tx_hash" {
		return fmt.Errorf("E2 prefix must have one source_tx_hash column")
	}
	for {
		row, err := input.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read E2 prefix: %w", err)
		}
		key, err := sourceKey(row[0])
		if err != nil {
			return fmt.Errorf("E2 prefix: %w", err)
		}
		if _, ok := r.sources[key]; ok {
			return fmt.Errorf("duplicate E2 prefix source %s", row[0])
		}
		r.sources[key] = sourceE2Prefix
		if len(r.sources) > c.E2PrefixCount {
			return fmt.Errorf("E2 prefix exceeds configured %d hashes", c.E2PrefixCount)
		}
	}
	if len(r.sources) != c.E2PrefixCount {
		return fmt.Errorf("E2 prefix contains %d hashes; expected %d", len(r.sources), c.E2PrefixCount)
	}
	r.progress.E2PrefixCount = uint64(c.E2PrefixCount)
	r.meta.CompletionScope = "e2"
	r.meta.E2ObservationSeconds = c.E2ObservationSeconds
	r.meta.E2PrefixCount = c.E2PrefixCount
	r.meta.E2PrefixSHA256 = fmt.Sprintf("%x", digest.Sum(nil))
	return nil
}

func validE2Duration(seconds int) bool {
	return seconds > 0 && int64(seconds) <= math.MaxInt64/int64(time.Second)
}

func (p CompletionProgress) IsE2() bool {
	return p.E2FixedWindow || p.E2PrefixCount != 0
}

// FirstInjectionTime retains the supervisor's monotonic clock.
func (r *Recorder) FirstInjectionTime() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.start
}

// E2Deadline is zero for prefix observations and before the first fixed-window dispatch.
func (r *Recorder) E2Deadline() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.e2Deadline()
}

func (r *Recorder) e2Deadline() time.Time {
	if !r.progress.E2FixedWindow || r.start.IsZero() {
		return time.Time{}
	}
	return r.start.Add(time.Duration(r.meta.E2ObservationSeconds) * time.Second)
}

// StopProgress takes one snapshot of the source counts and the E2 stop condition.
// Counters advance only after flush; coverage begins at the CSV first dispatch.
func (r *Recorder) StopProgress(at time.Time) (p CompletionProgress, e2GoalReached bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.progress, r.e2GoalReached(at)
}

func (r *Recorder) e2GoalReached(at time.Time) bool {
	p := r.progress
	if !p.IsE2() || r.start.IsZero() || at.Sub(r.start) < time.Duration(r.meta.E2ObservationSeconds)*time.Second {
		return false
	}
	return p.E2FixedWindow || (p.E2PrefixInjected == p.E2PrefixCount && p.E2PrefixCompleted == p.E2PrefixCount)
}
