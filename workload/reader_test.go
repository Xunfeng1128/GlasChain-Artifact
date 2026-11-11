package workload

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

const txHeader = "blockNumber,transactionIndex,transactionHash,from,to,toCreate,value,gasUsed,receiptStatus,fromIsContract,toIsContract,txCategory\n"
const callHeader = "blockNumber,transactionIndex,transactionHash,traceAddress,callDepth,caller,callee,callType,callGasUsed,error,txStatus,callerIsContract,calleeIsContract,isCCGEdge\n"

func txRow(index int, hash, gas, status, recipient, created, contract string) string {
	return fmt.Sprintf("1,%d,0x%s,0x%s,%s,%s,0,%s,%s,false,%s,inter_contract_call\n", index, strings.Repeat(hash, 64), strings.Repeat("a", 40), recipient, created, gas, status, contract)
}

func TestCanonicalInputIncludesContractsAndFiltersIdentically(t *testing.T) {
	to := "0x" + strings.Repeat("b", 40)
	txs := txHeader + txRow(0, "1", "40", "0", to, "", "true") +
		txRow(1, "2", "40", "1", "", to, "true") +
		txRow(2, "3", "40", "1", "0x"+strings.Repeat("a", 40), "", "false") +
		txRow(3, "4", "40", "1", strings.ToUpper(to), "", "true") +
		txRow(4, "5", "20", "1", to, "", "false")
	call := "1,3,0x" + strings.Repeat("4", 64) + ",%d,1," + to + ",0x" + strings.Repeat("c", 40) + ",call,10,,1,true,true,true\n"
	reader, err := NewReader(strings.NewReader(txs), strings.NewReader(callHeader+fmt.Sprintf(call, 0)+fmt.Sprintf(call, 1)), 100)
	if err != nil {
		t.Fatal(err)
	}
	first, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceHash != "0x"+strings.Repeat("4", 64) || first.To != strings.Repeat("b", 40) || len(first.Calls) != 2 || !first.ToIsContract {
		t.Fatalf("canonical contract record lost: %+v", first)
	}
	tx, err := CoreTransaction(first, 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	if tx.SourceTxHash != first.SourceHash || tx.GLASBaseGas != 40 || tx.GasUsed != 40 || tx.GLASCallGasPerCall != 5 || len(tx.GLASCallRelations) != 2 {
		t.Fatalf("execution metadata lost: %+v", tx)
	}
	second, err := reader.Next()
	if err != nil || second.SourceHash != "0x"+strings.Repeat("5", 64) {
		t.Fatalf("accepted order changed: %+v %v", second, err)
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if stats := reader.Stats(); stats.UsableTxs != 2 || stats.SkippedTxs != 3 {
		t.Fatalf("filter counters: %+v", stats)
	}
}

func TestInputRejectsInvalidGasAndDuplicateSource(t *testing.T) {
	to := "0x" + strings.Repeat("b", 40)
	for _, gas := range []string{"", "0", "101"} {
		t.Run("gas_"+gas, func(t *testing.T) {
			r, err := NewReader(strings.NewReader(txHeader+txRow(0, "1", gas, "1", to, "", "true")), strings.NewReader(callHeader), 100)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.Next(); err == nil {
				t.Fatal("invalid gas admitted")
			}
		})
	}
	r, err := NewReader(strings.NewReader(txHeader+txRow(0, "1", "40", "1", to, "", "true")+txRow(1, "1", "40", "1", to, "", "true")), strings.NewReader(callHeader), 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate source accepted: %v", err)
	}
}
