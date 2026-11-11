package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"blockEmulator/workload"
)

func TestCLIReportsJSONForSuccessAndFailure(t *testing.T) {
	dir := t.TempDir()
	txs, calls := filepath.Join(dir, "transactions.csv"), filepath.Join(dir, "calls.csv")
	txData := "blockNumber,transactionIndex,transactionHash,from,to,toCreate,value,gasUsed,receiptStatus,fromIsContract,toIsContract,txCategory\n" +
		"1,0,0x" + strings.Repeat("1", 64) + ",0x" + strings.Repeat("a", 40) + ",0x" + strings.Repeat("b", 40) + ",,0,40,1,false,true,inter_contract_call\n"
	callData := "blockNumber,transactionIndex,transactionHash,traceAddress,callDepth,caller,callee,callType,callGasUsed,error,txStatus,callerIsContract,calleeIsContract,isCCGEdge\n" +
		"1,0,0x" + strings.Repeat("1", 64) + ",0,1,0x" + strings.Repeat("b", 40) + ",0x" + strings.Repeat("c", 40) + ",call,10,,1,true,true,true\n"
	if err := os.WriteFile(txs, []byte(txData), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(calls, []byte(callData), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		count string
		limit string
		calls string
		valid bool
	}{
		{"valid", "1", "50", calls, true},
		{"over_limit", "1", "44", calls, false},
		{"short_input", "2", "50", calls, false},
		{"missing_file", "1", "50", filepath.Join(dir, "missing.csv"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run([]string{"--transactions", txs, "--calls", tc.calls, "--count", tc.count,
				"--block-gas-limit", tc.limit, "--cross-call-gas", "5"}, &stdout, &stderr)
			var report workload.PreflightReport
			if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
				t.Fatalf("stdout is not one JSON report: %q (%v)", stdout.String(), err)
			}
			if (code == 0) != tc.valid || report.Valid != tc.valid || report.SchemaVersion != 1 || report.Examples == nil {
				t.Fatalf("code=%d report=%+v stderr=%q", code, report, stderr.String())
			}
			if tc.valid && report.MaxStageGasBound != 45 {
				t.Fatalf("CLI lost call surcharge: %+v", report)
			}
		})
	}
}

func TestCLIFlagErrorsStillProduceJSON(t *testing.T) {
	for _, args := range [][]string{
		{}, {"--unknown"}, {"--count", "-1"},
		{"--transactions", "x", "--calls", "y", "--count", "1", "--block-gas-limit", "100"},
	} {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		var report workload.PreflightReport
		if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || code == 0 || report.Valid || report.Error == "" {
			t.Fatalf("args=%v code=%d stdout=%q err=%v", args, code, stdout.String(), err)
		}
	}
}

func TestE2PrefixContainsOnlyAdmittedRowsInInputOrder(t *testing.T) {
	dir := t.TempDir()
	txs := filepath.Join(dir, "transactions.csv")
	calls := filepath.Join(dir, "calls.csv")
	output := filepath.Join(dir, "prefix.csv")
	header := "blockNumber,transactionIndex,transactionHash,from,to,toCreate,value,gasUsed,receiptStatus,fromIsContract,toIsContract,txCategory\n"
	txRow := func(index, hash, gas, receiptStatus string) string {
		return "1," + index + ",0x" + strings.Repeat(hash, 64) + ",0x" + strings.Repeat("a", 40) +
			",0x" + strings.Repeat("b", 40) + ",,0," + gas + "," + receiptStatus + ",false,false,transfer\n"
	}
	// Canonical order differs from sorted hash order; failed rows do not count.
	first := txRow("0", "9", "30", "0") + txRow("1", "7", "30", "1") + txRow("2", "2", "30", "1")
	validInput := header + first + txRow("3", "8", "30", "1")
	if err := os.WriteFile(txs, []byte(validInput), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(calls, []byte("blockNumber,transactionIndex,transactionHash,traceAddress,callDepth,caller,callee,callType,callGasUsed,error,txStatus,callerIsContract,calleeIsContract,isCCGEdge\n"), 0600); err != nil {
		t.Fatal(err)
	}
	runAt := func(path, prefixCount string) int {
		return run([]string{"--transactions", txs, "--calls", calls, "--count", "3", "--block-gas-limit", "50", "--cross-call-gas", "0", "--e2-prefix-count", prefixCount, "--e2-prefix-file", path}, &bytes.Buffer{}, &bytes.Buffer{})
	}
	if code := runAt(output, "2"); code != 0 {
		t.Fatalf("valid prefix code=%d", code)
	}
	want := "source_tx_hash\n0x" + strings.Repeat("7", 64) + "\n0x" + strings.Repeat("2", 64) + "\n"
	for _, test := range []struct{ path, count string }{{txs, "2"}, {calls, "2"}, {output, "4"}, {output, "0"}} {
		if code := runAt(test.path, test.count); code == 0 {
			t.Fatalf("unexpected success for invalid prefix %v", test)
		}
	}
	if actual, err := os.ReadFile(txs); err != nil || string(actual) != validInput {
		t.Fatal("preflight replaced an input file")
	}
	// An invalid transaction after the cohort boundary must abort publication.
	if err := os.WriteFile(txs, []byte(header+first+txRow("3", "8", "60", "1")), 0600); err != nil {
		t.Fatal(err)
	}
	if code := runAt(output, "1"); code == 0 {
		t.Fatal("invalid full workload published prefix")
	}
	if got, err := os.ReadFile(output); err != nil || string(got) != want {
		t.Fatalf("last valid prefix lost: %q, err=%v", got, err)
	}
}
