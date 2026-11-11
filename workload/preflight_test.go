package workload

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func preflightCall(index int, hash, trace string, ccg bool) string {
	return fmt.Sprintf("1,%d,0x%s,%s,1,0x%s,0x%s,call,999,,1,true,true,%t\n",
		index, strings.Repeat(hash, 64), trace, strings.Repeat("b", 40), strings.Repeat("c", 40), ccg)
}

func TestPreflightUsesCanonicalPrefixAndRealCallOccurrences(t *testing.T) {
	to := "0x" + strings.Repeat("b", 40)
	// Filtered failed/create/self rows do not consume the requested prefix.
	rows := txRow(0, "1", "40", "0", to, "", "true") +
		txRow(1, "2", "40", "1", "", to, "true") +
		txRow(2, "3", "40", "1", "0x"+strings.Repeat("a", 40), "", "false") +
		txRow(3, "4", "40", "1", to, "", "true") +
		txRow(4, "5", "1000", "1", to, "", "true")
	// A misleading summary count must not affect the trace-based bound.
	txs := strings.TrimSuffix(txHeader, "\n") + ",contractCallCount\n"
	for _, row := range strings.Split(strings.TrimSuffix(rows, "\n"), "\n") {
		txs += row + ",99999999\n"
	}
	calls := callHeader + preflightCall(3, "4", "0", true) +
		preflightCall(3, "4", "1", true) + preflightCall(3, "4", "2", false)
	report, err := Preflight(strings.NewReader(txs), strings.NewReader(calls), PreflightOptions{
		Count: 1, BlockGasLimit: 50, CrossCallGas: 5,
	})
	if err != nil || !report.Valid || report.CheckedTransactions != 1 || report.InvalidCount != 0 ||
		report.MaxStageGasBound != 50 || report.MaxTxHash != "0x"+strings.Repeat("4", 64) {
		t.Fatalf("unexpected canonical bound: %+v, err=%v", report, err)
	}
}

func TestPreflightContinuesAfterViolationsAndBoundsExamples(t *testing.T) {
	to := "0x" + strings.Repeat("b", 40)
	txs := txHeader + txRow(0, "1", "40", "1", to, "", "true") +
		txRow(1, "2", "60", "1", to, "", "true") +
		txRow(2, "3", "100", "1", to, "", "true") +
		txRow(3, "4", "10", "1", to, "", "true")
	calls := callHeader + preflightCall(0, "1", "0", true) + preflightCall(0, "1", "1", true)
	report, err := Preflight(strings.NewReader(txs), strings.NewReader(calls), PreflightOptions{
		Count: 4, BlockGasLimit: 50, CrossCallGas: 20, MaxExamples: 1,
	})
	if err != nil || report.Valid || report.CheckedTransactions != 4 || report.InvalidCount != 3 ||
		report.MaxStageGasBound != 100 || report.MaxTxHash != "0x"+strings.Repeat("3", 64) ||
		len(report.Examples) != 1 || !report.ExamplesTruncated {
		t.Fatalf("prefix not fully checked: %+v, err=%v", report, err)
	}
	example := report.Examples[0]
	if example.ValidCCGCalls != 2 || example.StageGasBound != 80 || example.TransactionNumber != 1 {
		t.Fatalf("wrong trace accounting: %+v", example)
	}
}

func TestPreflightGasArithmeticCannotWrap(t *testing.T) {
	to := "0x" + strings.Repeat("b", 40)
	for _, tc := range []struct {
		name     string
		base     uint64
		unit     uint64
		calls    int
		overflow bool
	}{
		{"addition_overflow", math.MaxUint64 - 3, 4, 1, true},
		{"multiplication_overflow", 1, math.MaxUint64, 2, true},
		{"exact_uint64_boundary", math.MaxUint64 - 4, 4, 1, false},
		{"zero_unit", math.MaxUint64, 0, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txs := txHeader + txRow(0, "1", fmt.Sprint(tc.base), "1", to, "", "true")
			calls := callHeader
			for i := 0; i < tc.calls; i++ {
				calls += preflightCall(0, "1", fmt.Sprint(i), true)
			}
			report, err := Preflight(strings.NewReader(txs), strings.NewReader(calls), PreflightOptions{
				Count: 1, BlockGasLimit: math.MaxUint64, CrossCallGas: tc.unit,
			})
			if err != nil || report.Valid == tc.overflow || report.MaxStageGasBoundOverflow != tc.overflow || report.MaxStageGasBound != math.MaxUint64 {
				t.Fatalf("overflow handling: %+v, err=%v", report, err)
			}
			if tc.overflow && (report.InvalidCount != 1 || !report.Examples[0].BoundOverflow) {
				t.Fatalf("overflow not diagnosed: %+v", report)
			}
		})
	}
}

func TestPreflightDiagnosesInvalidGasAndDuplicateHashAcrossPrefix(t *testing.T) {
	to := "0x" + strings.Repeat("b", 40)
	txs := txHeader + txRow(0, "1", "0", "1", to, "", "true") +
		txRow(1, "2", "", "1", to, "", "true") +
		txRow(2, "2", "10", "1", to, "", "true")
	report, err := Preflight(strings.NewReader(txs), strings.NewReader(callHeader), PreflightOptions{
		Count: 3, BlockGasLimit: 50,
	})
	if err != nil || report.Valid || report.CheckedTransactions != 3 || report.InvalidCount != 3 || len(report.Examples) != 3 {
		t.Fatalf("invalid input not diagnosed: %+v, err=%v", report, err)
	}
	if report.Examples[2].Reasons[0] != "duplicate_source_hash" {
		t.Fatalf("missing duplicate-source diagnosis: %+v", report.Examples[2])
	}
}

func TestPreflightFailsOnShortPrefixAndCanonicalParseError(t *testing.T) {
	to := "0x" + strings.Repeat("b", 40)
	validTxs := txHeader + txRow(0, "1", "40", "1", to, "", "true")
	for _, tc := range []struct {
		name    string
		txs     string
		calls   string
		count   uint64
		checked uint64
	}{
		{"short_prefix", validTxs, callHeader, 2, 1},
		{"missing_header", "wrong\n", callHeader, 1, 0},
		{"invalid_ccg_trace", validTxs, callHeader + strings.Replace(preflightCall(0, "1", "0", true), ",999,,1,", ",999,reverted,1,", 1), 1, 0},
		{"bad_order_after_valid_row", validTxs + txRow(0, "2", "40", "1", to, "", "true"), callHeader, 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report, err := Preflight(strings.NewReader(tc.txs), strings.NewReader(tc.calls), PreflightOptions{Count: tc.count, BlockGasLimit: 100})
			if err == nil || report.Valid || report.Error == "" || report.CheckedTransactions != tc.checked {
				t.Fatalf("expected partial failure: %+v, err=%v", report, err)
			}
		})
	}
}

func TestPreflightRejectsInvalidOptions(t *testing.T) {
	for _, options := range []PreflightOptions{
		{BlockGasLimit: 100}, {Count: 1}, {Count: 1, BlockGasLimit: 100, MaxExamples: -1},
	} {
		report, err := Preflight(nil, nil, options)
		if err == nil || report.Valid || report.Error == "" || report.SchemaVersion != 1 {
			t.Fatalf("invalid options accepted: %+v, err=%v", report, err)
		}
	}
}

func TestPreflightMatchesExecutionValueAdmission(t *testing.T) {
	to := "0x" + strings.Repeat("b", 40)
	txs := txHeader
	for index, value := range []string{"-1", "-0", "1234567890123456789012345678901234567890"} {
		row := txRow(index, fmt.Sprint(index+1), "40", "1", to, "", "true")
		txs += strings.Replace(row, ",,0,40,", ",,"+value+",40,", 1)
	}
	report, err := Preflight(strings.NewReader(txs), strings.NewReader(callHeader), PreflightOptions{Count: 3, BlockGasLimit: 100})
	if err != nil || report.Valid || report.CheckedTransactions != 3 || report.InvalidCount != 1 || len(report.Examples) != 1 ||
		report.Examples[0].Reasons[0] != "invalid_transaction_value" {
		t.Fatalf("execution value admission differs: %+v, err=%v", report, err)
	}
}

func TestJiangZiyaPreflightRequiresMeasuredCallGasOnlyWhenOptedIn(t *testing.T) {
	to := "0x" + strings.Repeat("b", 40)
	txs := txHeader + txRow(0, "1", "40", "1", to, "", "true")
	for _, tc := range []struct {
		name, gas      string
		require, valid bool
		observed       uint64
	}{
		{"legacy_missing_allowed", "", false, true, 0},
		{"core_missing_rejected", "", true, false, 0},
		{"measured_zero", "0", true, true, 1},
		{"measured_gas", "999", true, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call := strings.Replace(preflightCall(0, "1", "0", true), ",999,,", ","+tc.gas+",,", 1)
			// Missing Gas on a non-CCG trace does not affect the placement model.
			ignored := strings.Replace(preflightCall(0, "1", "1", false), ",999,,", ",,,", 1)
			r, err := Preflight(strings.NewReader(txs), strings.NewReader(callHeader+call+ignored),
				PreflightOptions{Count: 1, BlockGasLimit: 100, CrossCallGas: 5, RequireCallGas: tc.require})
			if err != nil || r.Valid != tc.valid || r.RequireCallGas != tc.require || r.ValidCCGCalls != 1 || r.CCGCallsWithGas != tc.observed || r.MaxStageGasBound != 45 {
				t.Fatalf("report=%+v err=%v", r, err)
			}
			if !tc.valid && (len(r.Examples) != 1 || r.Examples[0].Reasons[0] != "missing_ccg_call_gas") {
				t.Fatalf("missing diagnostic: %+v", r)
			}
		})
	}
}
