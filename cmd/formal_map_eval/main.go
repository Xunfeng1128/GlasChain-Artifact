// formal_map_eval evaluates a frozen, archived placement on an identical source cohort.
// It does not run a planner, execute a block, or modify the run.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"blockEmulator/core"
	"blockEmulator/glasfreight/model"
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/partition/garet"
	"blockEmulator/utils"
	"blockEmulator/workload"
)

const evalCount = 50_000

type checkpoint struct {
	Position  uint64                  `json:"source_position"`
	Epoch     uint64                  `json:"epoch"`
	Shards    int                     `json:"shards"`
	Method    int                     `json:"method"`
	Overrides map[string]uint64       `json:"overrides"`
	GARET     *message.GARETPlacement `json:"garet"`
}
type config struct {
	ConsensusMethod                                                        int
	TotalDataSize, BlockGasLimit, UnifiedCrossCallGas, FormalMapCheckpoint uint64
	UnifiedTransactionsFile, UnifiedCallsFile                              string
	UnifiedMetrics, UnifiedExecution                                       bool
}
type runState struct {
	RunID       string            `json:"run_id"`
	Status      string            `json:"status"`
	Files       map[string]string `json:"artifacts_sha256"`
	Fingerprint struct {
		Shards int               `json:"shards"`
		Data   map[string]string `json:"data_sha256"`
	} `json:"fingerprint"`
}

func checkFile(path, want string) error {
	if len(want) != 64 {
		return fmt.Errorf("%s: missing SHA-256 fingerprint", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != want {
		return fmt.Errorf("%s: SHA-256 differs from the validated run", path)
	}
	return nil
}

func readJSON(path string, dest interface{}) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, dest); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

type resolver func(string) (uint64, error)

func (p checkpoint) resolver(catalog *garet.Catalog) (resolver, error) {
	if p.Shards <= 0 {
		return nil, fmt.Errorf("invalid shard count")
	}
	if p.Method == 10 {
		g := p.GARET
		if g == nil || catalog == nil || g.CatalogDigest != catalog.Digest() || g.Epoch != p.Epoch || len(g.Placement) != catalog.GroupCount() || len(p.Overrides) != 0 {
			return nil, fmt.Errorf("GARET group placement does not match the checkpoint epoch/catalog")
		}
		for _, s := range g.Placement {
			if s >= uint64(p.Shards) {
				return nil, fmt.Errorf("invalid group shard %d", s)
			}
		}
		return func(address string) (uint64, error) {
			i, e := catalog.GroupFor(address)
			if e != nil {
				return 0, e
			}
			return g.Placement[i], nil
		}, nil
	}
	if p.GARET != nil || p.Overrides == nil {
		return nil, fmt.Errorf("missing explicit override map or unexpected GARET placement for method %d", p.Method)
	}
	for address, s := range p.Overrides {
		normalized, err := garet.NormalizeAddress(address)
		if err != nil || normalized != address || s >= uint64(p.Shards) {
			return nil, fmt.Errorf("invalid placement override %q -> %d", address, s)
		}
	}
	// The live supervisor routes non-overridden addresses with the project's Addr2Shard.
	params.ShardNum = p.Shards
	return func(a string) (uint64, error) {
		if s, ok := p.Overrides[a]; ok {
			return s, nil
		}
		return uint64(utils.Addr2Shard(a)), nil // the canonical workload reader validates address hex
	}, nil
}

type metrics struct {
	count, top, union, gas uint64
	shardGas               []uint64
	cohort                 string
}

func (m *metrics) add(s, gas, limit uint64) error {
	if s >= uint64(len(m.shardGas)) || gas > limit || gas > math.MaxUint64-m.gas {
		return fmt.Errorf("unexecutable/overflowing stage: shard=%d gas=%d limit=%d", s, gas, limit)
	}
	m.shardGas[s] += gas
	m.gas += gas
	return nil
}

func (m *metrics) price(record *model.Transaction, pos, epoch, limit, unit uint64, resolve resolver) error {
	// The supervisor uses a zero-based nonce; the cohort range uses one-based positions.
	tx, err := workload.CoreTransaction(record, pos-1, unit)
	if err != nil {
		return err
	}
	bindings := make(map[string]uint64, 2+len(tx.GLASCallRelations))
	bind := func(a string) error {
		if _, ok := bindings[a]; ok {
			return nil
		}
		s, err := resolve(a)
		if err == nil {
			bindings[a] = s
		}
		return err
	}
	if err = bind(tx.Sender); err != nil {
		return err
	}
	if err = bind(tx.Recipient); err != nil {
		return err
	}
	for _, call := range tx.GLASCallRelations {
		if err = bind(call.Caller); err != nil {
			return err
		}
		if err = bind(call.Callee); err != nil {
			return err
		}
	}
	shardFor := func(a string) uint64 { return bindings[a] }
	if err = core.PriceGLASStage(tx, shardFor); err != nil {
		return err
	}
	source, target := bindings[tx.Sender], bindings[tx.Recipient]
	if err = m.add(source, tx.GasUsed, limit); err != nil {
		return err
	}
	if source != target {
		relay, err := core.NewGLASRelay2(tx, epoch, shardFor)
		if err != nil {
			return err
		}
		if err = m.add(target, relay.GasUsed, limit); err != nil {
			return err
		}
		m.top++
	}
	if source != target || tx.GLASCrossCallCount > 0 {
		m.union++
	}
	m.count++
	return nil
}

func evaluate(reader *workload.Reader, p checkpoint, c config, take uint64, resolve resolver) (metrics, error) {
	m := metrics{shardGas: make([]uint64, p.Shards)}
	h := sha256.New()
	for pos := uint64(1); pos <= p.Position+take; pos++ {
		record, err := reader.Next()
		if err != nil {
			return m, fmt.Errorf("source transaction %d: %w", pos, err)
		}
		if pos <= p.Position {
			continue
		}
		if err = m.price(record, pos, p.Epoch, c.BlockGasLimit, c.UnifiedCrossCallGas, resolve); err != nil {
			return m, fmt.Errorf("source position %d: %w", pos, err)
		}
		b, err := hex.DecodeString(record.SourceHash[2:])
		if err != nil {
			return m, err
		}
		h.Write(b)
	}
	m.cohort = hex.EncodeToString(h.Sum(nil))
	return m, nil
}

var csvFields = []string{"run_id", "method_id", "shards", "source_transactions", "source_position", "epoch", "evaluation_count", "cohort_sha256", "input_transactions_sha256", "input_calls_sha256", "top_cross_count", "top_cross_ratio", "union_cross_count", "union_cross_ratio", "per_shard_gas", "max_shard_gas", "mean_shard_gas", "peak_to_mean", "evaluation_scope"}

func (m metrics) csv(run runState, c config, p checkpoint) ([]byte, error) {
	highest := uint64(0)
	for _, g := range m.shardGas {
		if g > highest {
			highest = g
		}
	}
	mean := float64(m.gas) / float64(p.Shards)
	if mean == 0 || m.count == 0 {
		return nil, fmt.Errorf("empty Gas/cohort")
	}
	j, err := json.Marshal(m.shardGas)
	if err != nil {
		return nil, err
	}
	i := func(v uint64) string { return strconv.FormatUint(v, 10) }
	f := func(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err = w.Write(csvFields); err != nil {
		return nil, err
	}
	err = w.Write([]string{run.RunID, strconv.Itoa(p.Method), strconv.Itoa(p.Shards), i(c.TotalDataSize), i(p.Position), i(p.Epoch), i(m.count), m.cohort, run.Fingerprint.Data["UnifiedTransactionsFile"], run.Fingerprint.Data["UnifiedCallsFile"], i(m.top), f(float64(m.top) / float64(m.count)), i(m.union), f(float64(m.union) / float64(m.count)), string(j), i(highest), f(mean), f(float64(highest) / mean), "frozen_map_next_50000_admitted"})
	if err != nil {
		return nil, err
	}
	w.Flush()
	return buf.Bytes(), w.Error()
}

// Publish a complete immutable file; concurrent invocations accept identical results.
func publish(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".map-eval-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Link(tmp.Name(), path); err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(b, data) {
		return fmt.Errorf("conflicting existing output %s", path)
	}
	return nil
}

func run(dir, out string) error {
	if dir == "" || out == "" {
		return fmt.Errorf("--run and --out are required")
	}
	var state runState
	if err := readJSON(filepath.Join(dir, "run_state.json"), &state); err != nil {
		return err
	}
	if state.Status != "completed" {
		return fmt.Errorf("strict completed run required")
	}
	load := func(name string, dest interface{}) error {
		path := filepath.Join(dir, name)
		if err := checkFile(path, state.Files[name]); err != nil {
			return err
		}
		return readJSON(path, dest)
	}
	var valid struct {
		Ok bool `json:"valid_for_publication"`
	}
	var c config
	var p checkpoint
	if err := load("analysis/validation.json", &valid); err != nil {
		return err
	}
	if !valid.Ok {
		return fmt.Errorf("run validation failed")
	}
	var summary struct {
		Ok   bool `json:"valid_for_publication"`
		Full struct {
			Complete    bool   `json:"complete"`
			Completed   uint64 `json:"completed_unique"`
			Dispatched  uint64 `json:"dispatched_unique"`
			Outstanding uint64 `json:"outstanding"`
		} `json:"full_run"`
	}
	if err := load("analysis/summary.json", &summary); err != nil {
		return err
	}
	if err := load("runtime_config.json", &c); err != nil {
		return err
	}
	if !summary.Ok || !summary.Full.Complete || summary.Full.Completed != c.TotalDataSize ||
		summary.Full.Dispatched != c.TotalDataSize || summary.Full.Outstanding != 0 {
		return fmt.Errorf("all source transactions must be dispatched and finalized")
	}
	if err := load("result/e1_checkpoint.json", &p); err != nil {
		return err
	}
	if !c.UnifiedExecution || !c.UnifiedMetrics || p.Shards != state.Fingerprint.Shards ||
		c.ConsensusMethod != p.Method || p.Position == 0 || c.FormalMapCheckpoint != p.Position ||
		p.Position != c.TotalDataSize/2 || c.TotalDataSize-p.Position < evalCount {
		return fmt.Errorf("checkpoint inconsistent with experiment configuration")
	}
	if err := checkFile(c.UnifiedTransactionsFile, state.Fingerprint.Data["UnifiedTransactionsFile"]); err != nil {
		return err
	}
	if err := checkFile(c.UnifiedCallsFile, state.Fingerprint.Data["UnifiedCallsFile"]); err != nil {
		return err
	}
	var catalog *garet.Catalog
	if p.Method == 10 {
		const name = "garet_group_catalog.json"
		path := filepath.Join(dir, name)
		if err := checkFile(path, state.Files[name]); err != nil {
			return err
		}
		var err error
		catalog, err = garet.LoadCatalog(path)
		if err != nil {
			return err
		}
	}
	resolve, err := p.resolver(catalog)
	if err != nil {
		return err
	}
	reader, err := workload.Open(c.UnifiedTransactionsFile, c.UnifiedCallsFile, c.BlockGasLimit)
	if err != nil {
		return err
	}
	defer reader.Close()
	m, err := evaluate(reader, p, c, evalCount, resolve)
	if err != nil {
		return err
	}
	b, err := m.csv(state, c, p)
	if err != nil {
		return err
	}
	return publish(out, b)
}

func main() {
	dir := flag.String("run", "", "completed E1 r01 directory")
	out := flag.String("out", "", "new plotting-data CSV path")
	flag.Parse()
	if err := run(*dir, *out); err != nil {
		fmt.Fprintln(os.Stderr, "frozen-map evaluation:", err)
		os.Exit(2)
	}
}
