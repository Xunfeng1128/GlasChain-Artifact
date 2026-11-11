package workload

import (
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/big"

	gdataset "blockEmulator/glasfreight/dataset"
)

const defaultPreflightExamples = 20

// PreflightOptions selects the same first Count usable canonical transactions
// as unified injection. MaxExamples bounds diagnostics, not the scan length;
// zero selects the default of 20 examples.
type PreflightOptions struct {
	Count         uint64
	BlockGasLimit uint64
	CrossCallGas  uint64
	MaxExamples   int
	// Required only by planners such as JiangZiya that use measured call Gas.
	// A zero measured value is valid; a missing observation is not.
	RequireCallGas bool
	// Optional observer used to materialize prefixes in canonical input order
	// during this scan. A caller may publish its output only if Valid is true.
	ObserveSource func(number uint64, sourceHash string) error
}

type PreflightExample struct {
	TransactionNumber uint64   `json:"transaction_number"`
	TxHash            string   `json:"tx_hash"`
	BaseGas           uint64   `json:"base_gas"`
	ValidCCGCalls     uint64   `json:"valid_ccg_calls"`
	StageGasBound     uint64   `json:"stage_gas_bound"`
	BoundOverflow     bool     `json:"bound_overflow"`
	Reasons           []string `json:"reasons"`
}

// PreflightReport describes a sufficient, mapping-independent single-stage
// feasibility check: base Gas plus every valid CCG call charged as cross-shard.
// An overflowing bound is represented by MaxUint64 and an explicit overflow
// flag; it is never considered valid. No transactions are filtered or modified.
type PreflightReport struct {
	SchemaVersion            int                `json:"schema_version"`
	Valid                    bool               `json:"valid"`
	RequestedTransactions    uint64             `json:"requested_transactions"`
	CheckedTransactions      uint64             `json:"checked_transactions"`
	BlockGasLimit            uint64             `json:"block_gas_limit"`
	CrossCallGas             uint64             `json:"cross_call_gas"`
	MaxStageGasBound         uint64             `json:"max_stage_gas_bound"`
	MaxStageGasBoundOverflow bool               `json:"max_stage_gas_bound_overflow"`
	MaxTxHash                string             `json:"max_tx_hash"`
	InvalidCount             uint64             `json:"invalid_count"`
	Examples                 []PreflightExample `json:"examples"`
	ExamplesTruncated        bool               `json:"examples_truncated"`
	Error                    string             `json:"error,omitempty"`
	RequireCallGas           bool               `json:"require_call_gas"`
	ValidCCGCalls            uint64             `json:"valid_ccg_calls"`
	CCGCallsWithGas          uint64             `json:"ccg_calls_with_gas"`
}

func newPreflightReport(options PreflightOptions) PreflightReport {
	return PreflightReport{
		SchemaVersion: 1, RequestedTransactions: options.Count,
		BlockGasLimit: options.BlockGasLimit, CrossCallGas: options.CrossCallGas,
		Examples:       make([]PreflightExample, 0),
		RequireCallGas: options.RequireCallGas,
	}
}

func validatePreflightOptions(options PreflightOptions) error {
	if options.Count == 0 || options.BlockGasLimit == 0 {
		return fmt.Errorf("preflight requires a positive count and block Gas limit")
	}
	if options.MaxExamples < 0 {
		return fmt.Errorf("preflight MaxExamples must not be negative")
	}
	return nil
}

func preflightFailure(report PreflightReport, err error) (PreflightReport, error) {
	report.Valid = false
	report.Error = err.Error()
	return report, err
}

// Preflight reads caller-owned CSV streams with the canonical two-table merge
// reader. Transaction-level violations are accumulated over the entire prefix.
// A structural/read error or a short prefix returns a partial report and error;
// a fully scanned but infeasible prefix returns Valid=false and a nil error.
// Memory holds the current transaction's calls plus source hashes needed to
// enforce the same duplicate-source rule as unified input.
func Preflight(transactions, calls io.Reader, options PreflightOptions) (PreflightReport, error) {
	report := newPreflightReport(options)
	if err := validatePreflightOptions(options); err != nil {
		return preflightFailure(report, err)
	}
	reader, err := gdataset.NewReader(transactions, calls)
	if err != nil {
		return preflightFailure(report, err)
	}
	defer reader.Close()
	return preflightReader(reader, options, report)
}

// PreflightFiles is the file-backed equivalent of Preflight. It opens both
// inputs read-only and never loads a complete CSV into memory.
func PreflightFiles(transactionsPath, callsPath string, options PreflightOptions) (PreflightReport, error) {
	report := newPreflightReport(options)
	if err := validatePreflightOptions(options); err != nil {
		return preflightFailure(report, err)
	}
	reader, err := gdataset.Open(transactionsPath, callsPath)
	if err != nil {
		return preflightFailure(report, err)
	}
	defer reader.Close()
	return preflightReader(reader, options, report)
}

func preflightReader(reader *gdataset.Reader, options PreflightOptions, report PreflightReport) (PreflightReport, error) {
	maxExamples := options.MaxExamples
	if maxExamples == 0 {
		maxExamples = defaultPreflightExamples
	}
	seen := make(map[[32]byte]struct{})
	for report.CheckedTransactions < options.Count {
		record, err := reader.Next()
		if err == io.EOF {
			return preflightFailure(report, fmt.Errorf("canonical input ended after %d usable transactions; requested %d", report.CheckedTransactions, options.Count))
		}
		if err != nil {
			return preflightFailure(report, err)
		}
		report.CheckedTransactions++
		reasons := make([]string, 0)
		if !record.HasGasUsed || record.GasUsed == 0 {
			reasons = append(reasons, "missing_or_zero_base_gas")
		}
		// The canonical CSV parser accepts signed integers, but execution's
		// CoreTransaction rejects negative values. Keep admission equivalent.
		if value, ok := new(big.Int).SetString(record.Value, 10); !ok || value.Sign() < 0 {
			reasons = append(reasons, "invalid_transaction_value")
		}
		var hash [32]byte
		// Canonical parsing already validates and normalizes source hashes.
		if _, err := hex.Decode(hash[:], []byte(record.SourceHash[2:])); err != nil {
			return preflightFailure(report, err)
		}
		if _, duplicate := seen[hash]; duplicate {
			reasons = append(reasons, "duplicate_source_hash")
		}
		seen[hash] = struct{}{}
		if options.ObserveSource != nil {
			if err := options.ObserveSource(report.CheckedTransactions, record.SourceHash); err != nil {
				return preflightFailure(report, err)
			}
		}

		var callCount uint64
		missingCallGas := false
		for _, call := range record.Calls {
			// Match CoreTransaction exactly. Calls are actual canonical trace
			// occurrences; CSV aggregate counts are deliberately never consulted.
			if call.IsCCGEdge && call.CallerIsSCA && call.CalleeIsSCA && call.Caller != "" && call.Callee != "" {
				callCount++
				report.ValidCCGCalls++
				if call.HasGasUsed {
					report.CCGCallsWithGas++
				} else {
					missingCallGas = true
				}
			}
		}
		if options.RequireCallGas && missingCallGas {
			reasons = append(reasons, "missing_ccg_call_gas")
		}
		overflow := options.CrossCallGas != 0 && callCount > (math.MaxUint64-record.GasUsed)/options.CrossCallGas
		bound := uint64(math.MaxUint64)
		if overflow {
			reasons = append(reasons, "stage_gas_bound_overflow")
		} else {
			bound = record.GasUsed + callCount*options.CrossCallGas
			if bound > options.BlockGasLimit {
				reasons = append(reasons, "stage_gas_bound_exceeds_block_limit")
			}
		}
		if report.MaxTxHash == "" || (!report.MaxStageGasBoundOverflow && (overflow || bound > report.MaxStageGasBound)) {
			report.MaxStageGasBound, report.MaxTxHash = bound, record.SourceHash
			report.MaxStageGasBoundOverflow = overflow
		}
		if len(reasons) != 0 {
			report.InvalidCount++
			if len(report.Examples) < maxExamples {
				report.Examples = append(report.Examples, PreflightExample{
					TransactionNumber: report.CheckedTransactions, TxHash: record.SourceHash,
					BaseGas: record.GasUsed, ValidCCGCalls: callCount, StageGasBound: bound,
					BoundOverflow: overflow, Reasons: reasons,
				})
			} else {
				report.ExamplesTruncated = true
			}
		}
	}
	report.Valid = report.InvalidCount == 0
	return report, nil
}
