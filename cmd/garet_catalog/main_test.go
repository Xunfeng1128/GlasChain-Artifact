package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"blockEmulator/partition/garet"
)

const transactionHeader = "blockNumber,transactionIndex,transactionHash,from,to,toCreate,value,gasUsed,receiptStatus,fromIsContract,toIsContract,txCategory\n"
const callsHeader = "blockNumber,transactionIndex,transactionHash,traceAddress,callDepth,caller,callee,callType,callGasUsed,error,txStatus,callerIsContract,calleeIsContract,isCCGEdge\n"

func endpoint(character string) string { return strings.Repeat(character, 40) }
func transaction(index int, hash, to string, gas, status int) string {
	return fmt.Sprintf("1,%d,0x%s,0x%s,0x%s,,0,%d,%d,false,true,inter_contract_call\n", index, strings.Repeat(hash, 64), endpoint("a"), endpoint(to), gas, status)
}

func fixture(t *testing.T, transactions, calls string, count, groups string) ([]string, string) {
	t.Helper()
	dir := t.TempDir()
	txPath, callPath, output := filepath.Join(dir, "transactions.csv"), filepath.Join(dir, "calls.csv"), filepath.Join(dir, "catalog.json")
	if err := os.WriteFile(txPath, []byte(transactionHeader+transactions), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(callPath, []byte(callsHeader+calls), 0600); err != nil {
		t.Fatal(err)
	}
	return []string{"--transactions", txPath, "--calls", callPath, "--count", count, "--block-gas-limit", "100", "--groups", groups, "--output", output}, output
}

func TestCatalogUsesAdmittedPrefixIdentitiesAndCallEndpoints(t *testing.T) {
	transactions := transaction(0, "1", "e", 10, 0) + transaction(1, "2", "b", 30, 1) + transaction(2, "3", "f", 101, 1)
	call := fmt.Sprintf("1,1,0x%s,0,1,0x%s,0x%s,call,1,,1,true,true,true\n", strings.Repeat("2", 64), endpoint("c"), endpoint("d"))
	args, path := fixture(t, transactions, call, "1", "2")
	var stdout, stderr bytes.Buffer
	if code := run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	catalog, err := garet.LoadCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	want, err := garet.NewCatalog([]string{endpoint("a"), endpoint("b"), endpoint("c"), endpoint("d")}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Digest() != want.Digest() || !strings.Contains(stdout.String(), `"prefix_transactions":1`) {
		t.Fatalf("wrong prefix catalog or summary: %s", stdout.String())
	}
}

func TestCatalogFailsEarlyEOFAndInvalidGasWithoutOutput(t *testing.T) {
	for _, test := range []struct{ transactions, count string }{
		{transaction(0, "1", "b", 10, 1), "2"},
		{transaction(0, "1", "b", 101, 1), "1"},
	} {
		args, path := fixture(t, test.transactions, "", test.count, "2")
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code == 0 {
			t.Fatal("invalid prefix accepted")
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("failed build wrote output: %v", err)
		}
	}
}

func TestCatalogRejectsInvalidArgumentsAndInputOverwrite(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr); code != 2 {
		t.Fatalf("missing args code=%d", code)
	}
	args, _ := fixture(t, transaction(0, "1", "b", 10, 1), "", "1", "2")
	args[len(args)-1] = args[1]
	if code := run(args, &stdout, &stderr); code != 2 {
		t.Fatalf("input overwrite code=%d", code)
	}
}
