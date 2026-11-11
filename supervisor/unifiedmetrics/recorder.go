// Package unifiedmetrics records raw logical-transaction events on a single
// supervisor clock. Deduplication and window aggregation happen offline, so a
// long run retains only a compact source-state map for the online stop guard.
package unifiedmetrics

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RunConfig identifies the execution setup. It describes the shared execution
// rules, not a replacement for any algorithm's graph or partition settings.
type RunConfig struct {
	Method               string `json:"method"`
	ConsensusMethod      int    `json:"consensus_method"`
	ShardCount           int    `json:"shard_count"`
	ExpectedTransactions uint64 `json:"expected_transactions"`
	InjectSpeed          int    `json:"inject_speed"`
	BlockGasLimit        uint64 `json:"block_gas_limit"`
	CrossCallGas         uint64 `json:"cross_call_gas"`
	TransactionsFile     string `json:"transactions_file"`
	CallsFile            string `json:"calls_file"`
	ConfigPath           string `json:"config_path"`
	ConfigSHA256         string `json:"config_sha256"`
	E2FixedWindow        bool   `json:"-"`
	E2ObservationSeconds int    `json:"-"`
	E2PrefixCount        int    `json:"-"`
	E2PrefixFile         string `json:"-"`
}

// Header is the version-1 event schema. It is copied before use by the writer.
var Header = []string{
	"event", "source_tx_hash", "stage", "elapsed_ns", "observed_unix_ns",
	"worker_commit_unix_ns", "shard_id", "block_height", "block_hash", "block_body_length",
	"base_gas", "stage_gas", "cross_call_count", "attached_gas", "top_cross", "internal_cross", "union_cross",
}

type metadata struct {
	SchemaVersion                int                 `json:"schema_version"`
	Status                       string              `json:"status"`
	Clock                        string              `json:"clock"`
	Config                       RunConfig           `json:"config"`
	FirstInjectionUnixNS         int64               `json:"first_injection_unix_ns"`
	ObservedDurationNS           int64               `json:"observed_duration_ns"`
	InjectionRows                uint64              `json:"injection_rows"`
	BlockRows                    uint64              `json:"block_rows"`
	StageRows                    uint64              `json:"stage_rows"`
	ReconfigurationSchemaVersion int                 `json:"reconfiguration_schema_version"`
	ReconfigurationRows          uint64              `json:"reconfiguration_rows"`
	CompletionScope              string              `json:"completion_scope,omitempty"`
	E2StopPolicy                 string              `json:"e2_stop_policy,omitempty"`
	E2ObservationSeconds         int                 `json:"e2_observation_seconds,omitempty"`
	E2PrefixCount                int                 `json:"e2_prefix_count,omitempty"`
	E2PrefixSHA256               string              `json:"e2_prefix_sha256,omitempty"`
	CompletionFailure            string              `json:"completion_failure,omitempty"`
	Completion                   *CompletionProgress `json:"completion_check,omitempty"`
}

// CompletionProgress is an online termination guard, not a replacement for
// the offline ledger validation. Completed counts injected sources with a
// LOCAL or RELAY2 receipt, so an unknown source cannot replace missing work.
type CompletionProgress struct {
	Expected          uint64 `json:"expected_transactions"`
	Injected          uint64 `json:"unique_injected"`
	Completed         uint64 `json:"unique_completed"`
	FinalSources      uint64 `json:"unique_final_sources"`
	E2FixedWindow     bool   `json:"-"`
	E2PrefixCount     uint64 `json:"e2_prefix_count,omitempty"`
	E2PrefixInjected  uint64 `json:"e2_prefix_injected,omitempty"`
	E2PrefixCompleted uint64 `json:"e2_prefix_completed,omitempty"`
}

func (p CompletionProgress) Complete() bool {
	return p.Expected > 0 && p.Injected == p.Expected && p.Completed == p.Expected && p.FinalSources == p.Completed
}

const sourceInjected uint8 = 1
const sourceFinal uint8 = 2
const sourceE2Prefix uint8 = 4

func sourceKey(source string) ([32]byte, error) {
	var key [32]byte
	if len(source) != 66 || !strings.HasPrefix(source, "0x") || source != strings.ToLower(source) {
		return key, fmt.Errorf("source transaction hash must be canonical lowercase 0x + 64 hex digits: %q", source)
	}
	_, err := hex.Decode(key[:], []byte(source[2:]))
	return key, err
}

// Recorder serializes writes, but accepts out-of-order receipt timestamps from
// concurrent network handlers. Each timestamp must still follow first injection.
type Recorder struct {
	mu                      sync.Mutex
	directory               string
	file                    *os.File
	csv                     *csv.Writer
	meta                    metadata
	start                   time.Time
	maxElapsed              time.Duration
	closed                  bool
	failure                 error
	sources                 map[[32]byte]uint8
	progress                CompletionProgress
	reconfigurationFile     *os.File
	reconfigurationCSV      *csv.Writer
	lastReconfigurationTime time.Time
}

// New refuses an existing output directory, including an empty one. Reusing a
// directory could otherwise silently combine runs or truncate previous results.
func New(directory string, config RunConfig) (*Recorder, error) {
	if strings.TrimSpace(directory) == "" {
		return nil, fmt.Errorf("unified metrics directory is empty")
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	r := &Recorder{
		directory: directory, sources: make(map[[32]byte]uint8), progress: CompletionProgress{Expected: config.ExpectedTransactions},
		meta: metadata{SchemaVersion: 1, Status: "recording", Clock: "supervisor_receipt_monotonic", Config: config},
	}
	if err := r.prepareE2(config); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(directory), 0755); err != nil {
		return nil, err
	}
	if err := os.Mkdir(directory, 0755); err != nil {
		return nil, fmt.Errorf("create fresh unified metrics directory: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(directory, "events.csv"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}
	r.file, r.csv = file, csv.NewWriter(file)
	if err := r.openReconfigurationLog(); err != nil {
		file.Close()
		return nil, err
	}
	if err := r.csv.Write(append([]string(nil), Header...)); err != nil {
		file.Close()
		r.reconfigurationFile.Close()
		return nil, err
	}
	if err := r.flush(); err != nil {
		file.Close()
		r.reconfigurationFile.Close()
		return nil, err
	}
	if err := r.writeMetadata(); err != nil {
		file.Close()
		r.reconfigurationFile.Close()
		return nil, err
	}
	return r, nil
}

func validateConfig(c RunConfig) error {
	switch c.ConsensusMethod {
	case 1, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13:
	default:
		return fmt.Errorf("unsupported unified metrics consensus method %d", c.ConsensusMethod)
	}
	if strings.TrimSpace(c.Method) == "" || c.ShardCount < 1 || c.ExpectedTransactions == 0 || c.InjectSpeed < 1 || c.BlockGasLimit == 0 {
		return fmt.Errorf("invalid unified metrics run configuration")
	}
	return nil
}

// RecordInjection must run before transport sends the nonempty batch. Retries
// remain visible as raw rows; the offline reader detects conflicting identities.
func (r *Recorder) RecordInjection(txs []*core.Transaction, shard uint64, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.writable(); err != nil {
		return err
	}
	if len(txs) == 0 {
		return nil
	}
	if err := r.checkTimeAndShard(at, shard); err != nil {
		return err
	}
	for _, tx := range txs {
		if err := r.validateTransaction(tx); err != nil {
			return fmt.Errorf("injection: %w", err)
		}
		if tx.Relayed {
			return fmt.Errorf("injection contains RELAY2 transaction %s", tx.SourceTxHash)
		}
	}
	if r.start.IsZero() {
		r.start = at
		r.meta.FirstInjectionUnixNS = at.UnixNano()
		// Persist the origin immediately: a crashed run must be recognizable as
		// incomplete, while still permitting explicitly requested diagnostics.
		if err := r.writeMetadata(); err != nil {
			r.failure = err
			return err
		}
	}
	for _, tx := range txs {
		row := make([]string, len(Header))
		row[0], row[1], row[2] = "inject", tx.SourceTxHash, "TOP"
		row[3], row[4], row[6] = i64(at.Sub(r.start).Nanoseconds()), i64(at.UnixNano()), u64(shard)
		row[10] = u64(tx.GLASBaseGas)
		if err := r.csv.Write(row); err != nil {
			r.failure = err
			return err
		}
	}
	r.meta.InjectionRows += uint64(len(txs))
	r.advance(at)
	if err := r.flush(); err != nil {
		return err
	}
	r.recordProgress(txs, sourceInjected)
	return nil
}

// RecordBlock records one receipt plus every physical execution stage. LOCAL
// and RELAY2 finish an original transaction; RELAY1 never increments logical TPS.
// The complete receipt is checked before writing any row from it.
func (r *Recorder) RecordBlock(b *message.BlockInfoMsg, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.writable(); err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("nil unified metrics block receipt")
	}
	if r.start.IsZero() || at.Before(r.start) {
		if b.BlockBodyLength == 0 && len(b.InnerShardTxs)+len(b.Relay1Txs)+len(b.Relay2Txs)+len(b.Broker1Txs)+len(b.Broker2Txs) == 0 {
			// A TCP handler can capture an empty pre-injection receipt before
			// another goroutine starts injection, then acquire its lock later.
			return nil
		}
		return fmt.Errorf("nonempty block received before first injection")
	}
	if err := r.checkTimeAndShard(at, b.SenderShardID); err != nil {
		return err
	}
	if b.BlockBodyLength < 0 || b.BlockBodyLength != len(b.InnerShardTxs)+len(b.Relay1Txs)+len(b.Relay2Txs) ||
		len(b.Broker1Txs)+len(b.Broker2Txs) != 0 {
		return fmt.Errorf("block receipt has inconsistent or unsupported transaction classes")
	}
	if strings.TrimSpace(b.ExecutionBlockHash) == "" || b.CommitTime.IsZero() {
		return fmt.Errorf("block receipt lacks block hash or worker commit timestamp")
	}
	type group struct {
		stage string
		txs   []*core.Transaction
	}
	groups := []group{{"LOCAL", b.InnerShardTxs}, {"RELAY1", b.Relay1Txs}, {"RELAY2", b.Relay2Txs}}
	seen := make(map[string]bool, b.BlockBodyLength)
	var totalGas uint64
	for _, g := range groups {
		for _, tx := range g.txs {
			if err := r.validateTransaction(tx); err != nil {
				return fmt.Errorf("%s: %w", g.stage, err)
			}
			if tx.Relayed != (g.stage == "RELAY2") {
				return fmt.Errorf("%s transaction %s has inconsistent relay flag", g.stage, tx.SourceTxHash)
			}
			key := tx.SourceTxHash + ":" + g.stage
			if seen[key] {
				return fmt.Errorf("duplicate physical stage within block: %s", key)
			}
			seen[key] = true
			gas, attached, err := core.PriceGLASStageGas(tx.GLASBaseGas, r.meta.Config.CrossCallGas, tx.GLASCrossCallCount, g.stage != "RELAY1")
			if err != nil || gas != tx.GasUsed || attached != tx.GLASAttachedGas {
				return fmt.Errorf("%s transaction %s has inconsistent committed Gas", g.stage, tx.SourceTxHash)
			}
			if tx.GasUsed > r.meta.Config.BlockGasLimit-totalGas {
				return fmt.Errorf("block receipt exceeds hard Gas limit")
			}
			totalGas += tx.GasUsed
		}
	}
	rowForBlock := func() []string {
		row := make([]string, len(Header))
		row[3], row[4], row[5] = i64(at.Sub(r.start).Nanoseconds()), i64(at.UnixNano()), i64(b.CommitTime.UnixNano())
		row[6], row[7], row[8], row[9] = u64(b.SenderShardID), u64(b.BlockHeight), b.ExecutionBlockHash, strconv.Itoa(b.BlockBodyLength)
		return row
	}
	row := rowForBlock()
	row[0] = "block"
	if err := r.csv.Write(row); err != nil {
		r.failure = err
		return err
	}
	for _, g := range groups {
		for _, tx := range g.txs {
			row := rowForBlock()
			row[0], row[1], row[2] = "stage", tx.SourceTxHash, g.stage
			row[10], row[11], row[12], row[13] = u64(tx.GLASBaseGas), u64(tx.GasUsed), u64(tx.GLASCrossCallCount), u64(tx.GLASAttachedGas)
			topCross := g.stage != "LOCAL"
			internalCross := g.stage != "RELAY1" && tx.GLASCrossCallCount > 0
			row[14], row[15], row[16] = bit(topCross), bit(internalCross), bit(topCross || internalCross)
			if err := r.csv.Write(row); err != nil {
				r.failure = err
				return err
			}
		}
	}
	r.meta.BlockRows++
	r.meta.StageRows += uint64(b.BlockBodyLength)
	r.advance(at)
	if err := r.flush(); err != nil {
		return err
	}
	r.recordProgress(b.InnerShardTxs, sourceFinal)
	r.recordProgress(b.Relay2Txs, sourceFinal)
	return nil
}

// Called only after validation and a successful raw-event flush, under mu.
func (r *Recorder) recordProgress(txs []*core.Transaction, flag uint8) {
	for _, tx := range txs {
		key, _ := sourceKey(tx.SourceTxHash)
		previous := r.sources[key]
		current := previous | flag
		if current == previous {
			continue
		}
		r.sources[key] = current
		if flag == sourceInjected {
			r.progress.Injected++
			if previous&sourceE2Prefix != 0 {
				r.progress.E2PrefixInjected++
			}
		} else {
			r.progress.FinalSources++
		}
		if current&(sourceInjected|sourceFinal) == sourceInjected|sourceFinal {
			r.progress.Completed++
			if previous&sourceE2Prefix != 0 {
				r.progress.E2PrefixCompleted++
			}
		}
	}
}

func (r *Recorder) CompletionProgress() CompletionProgress {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.progress
}

// Save a bounded diagnostic without copying the full source-state map.
func (r *Recorder) WriteCompletionDiagnostic(reason string, grace time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	samples := make([]string, 0, 20)
	for key, flags := range r.sources {
		if flags&(sourceInjected|sourceFinal) == sourceInjected && len(samples) < cap(samples) {
			samples = append(samples, "0x"+hex.EncodeToString(key[:]))
		}
		if len(samples) == cap(samples) {
			break
		}
	}
	sort.Strings(samples)
	data, err := json.MarshalIndent(struct {
		Reason            string             `json:"reason"`
		GraceSeconds      float64            `json:"no_progress_grace_seconds"`
		Progress          CompletionProgress `json:"progress"`
		UnfinishedSamples []string           `json:"unfinished_source_samples"`
	}{reason, grace.Seconds(), r.progress, samples}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(r.directory, "completion_check.json"), append(data, '\n'), 0644)
}

func (r *Recorder) validateTransaction(tx *core.Transaction) error {
	if err := core.ValidateExecutionRecord(tx, r.meta.Config.CrossCallGas); err != nil {
		return err
	}
	if _, err := sourceKey(tx.SourceTxHash); err != nil {
		return err
	}
	if strings.TrimSpace(tx.SourceTxHash) != tx.SourceTxHash || tx.GLASBaseGas > r.meta.Config.BlockGasLimit ||
		tx.GLASCrossCallCount > uint64(len(tx.GLASCallRelations)) || tx.SenderIsBroker || tx.OriginalSender != "" || tx.FinalRecipient != "" || len(tx.RawTxHash) > 0 {
		return fmt.Errorf("invalid unified metrics transaction metadata for %s", tx.SourceTxHash)
	}
	return nil
}

func (r *Recorder) checkTimeAndShard(at time.Time, shard uint64) error {
	if at.IsZero() || (!r.start.IsZero() && at.Before(r.start)) {
		return fmt.Errorf("invalid event timestamp before first injection")
	}
	if deadline := r.e2Deadline(); !deadline.IsZero() && !at.Before(deadline) {
		return fmt.Errorf("event timestamp at or after fixed E2 cutoff")
	}
	if shard >= uint64(r.meta.Config.ShardCount) {
		return fmt.Errorf("event shard %d outside run topology", shard)
	}
	return nil
}

func (r *Recorder) advance(at time.Time) {
	if elapsed := at.Sub(r.start); elapsed > r.maxElapsed {
		r.maxElapsed = elapsed
	}
}

func (r *Recorder) writable() error {
	if r.closed {
		return fmt.Errorf("unified metrics recorder is closed")
	}
	return r.failure
}

func (r *Recorder) flush() error {
	r.csv.Flush()
	if err := r.csv.Error(); err != nil {
		r.failure = err
		return err
	}
	return nil
}

// Close seals observation coverage, including intervals without completions.
// It does not assert full completion; the offline validator checks that using
// unique source identities. An unclean run keeps status="recording".
func (r *Recorder) Close(at time.Time) error {
	return r.close(at, "")
}

// CloseIncomplete flushes and closes files without marking the run successful.
// The existing offline reader accepts recording and reports not_closed.
func (r *Recorder) CloseIncomplete(at time.Time, reason string) error {
	if reason == "" {
		reason = "logical transaction completion was not verified"
	}
	return r.close(at, reason)
}

func (r *Recorder) close(at time.Time, incompleteReason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.failure
	}
	if deadline := r.e2Deadline(); !deadline.IsZero() && at.After(deadline) {
		at = deadline
	}
	if at.IsZero() || at.Before(r.lastReconfigurationTime) || (!r.start.IsZero() && (at.Before(r.start) || at.Sub(r.start) < r.maxElapsed)) {
		return fmt.Errorf("close timestamp precedes recorded observation coverage")
	}
	var goalErr error
	if incompleteReason == "" && r.meta.CompletionScope == "e2" && !r.e2GoalReached(at) {
		goalErr = fmt.Errorf("incomplete E2 window or cohort at close")
		incompleteReason = goalErr.Error()
	}
	r.closed = true
	defer r.reconfigurationFile.Close()
	if r.failure != nil {
		r.file.Close()
		return r.failure
	}
	if err := r.flush(); err != nil {
		r.file.Close()
		return err
	}
	if err := r.flushReconfiguration(); err != nil {
		r.file.Close()
		return err
	}
	if err := r.reconfigurationFile.Sync(); err != nil {
		r.file.Close()
		r.failure = err
		return err
	}
	if err := r.reconfigurationFile.Close(); err != nil {
		r.file.Close()
		r.failure = err
		return err
	}
	if err := r.file.Sync(); err != nil {
		r.file.Close()
		r.failure = err
		return err
	}
	if err := r.file.Close(); err != nil {
		r.failure = err
		return err
	}
	if !r.start.IsZero() {
		r.meta.ObservedDurationNS = at.Sub(r.start).Nanoseconds()
	}
	if incompleteReason == "" {
		r.meta.Status = "closed"
	}
	r.meta.CompletionFailure = incompleteReason
	progress := r.progress
	r.meta.Completion = &progress
	if err := r.writeMetadata(); err != nil {
		r.failure = err
		return err
	}
	if goalErr != nil {
		r.failure = goalErr
	}
	return goalErr
}

func (r *Recorder) writeMetadata() error {
	data, err := json.MarshalIndent(r.meta, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(r.directory, ".run-*.json")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if _, err := temp.Write(append(data, '\n')); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(name, filepath.Join(r.directory, "run.json"))
}

func i64(v int64) string  { return strconv.FormatInt(v, 10) }
func u64(v uint64) string { return strconv.FormatUint(v, 10) }
func bit(v bool) string {
	if v {
		return "1"
	}
	return "0"
}
