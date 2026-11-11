package dataset

import (
	"io"
	"strings"
	"testing"
)

const (
	testHash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	addrA    = "1111111111111111111111111111111111111111"
	addrB    = "2222222222222222222222222222222222222222"
	addrC    = "3333333333333333333333333333333333333333"
)

func TestReaderJoinsAndRetainsOnlyCCGRelevantCalls(t *testing.T) {
	txCSV := strings.Join([]string{
		"blockNumber,transactionIndex,transactionHash,from,to,toCreate,value,gasUsed,receiptStatus,fromIsContract,toIsContract,txCategory",
		"14000000,0," + testHash + ",0x" + addrA + ",0x" + addrB + ",,17,960551,1,false,true,inter_contract",
	}, "\n")
	callCSV := strings.Join([]string{
		"blockNumber,transactionIndex,transactionHash,traceAddress,callDepth,caller,callee,callType,callGasUsed,error,txStatus,callerIsContract,calleeIsContract,isCCGEdge",
		"14000000,0," + testHash + ",[],0,0x" + addrA + ",0x" + addrB + ",call,950000,,1,false,true,false",
		"14000000,0," + testHash + ",[0],1,0x" + addrB + ",0x" + addrC + ",call,48113,,1,true,true,true",
		"14000000,0," + testHash + ",[1],1,0x" + addrB + ",0x" + addrA + ",call,100,,1,true,false,false",
	}, "\n")

	r, err := NewReader(strings.NewReader(txCSV), strings.NewReader(callCSV))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !tx.Usable() || tx.From != addrA || tx.To != addrB || tx.SourceHash != testHash {
		t.Fatalf("unexpected transaction: %+v", tx)
	}
	if got := len(tx.Calls); got != 1 {
		t.Fatalf("retained calls = %d, want only one CCG edge", got)
	}
	if !tx.Calls[0].IsCCGEdge || tx.Calls[0].CallGasUsed != 48113 {
		t.Fatalf("unexpected CCG call: %+v", tx.Calls[0])
	}
	if _, err = r.Next(); err != io.EOF {
		t.Fatalf("second Next error = %v, want EOF", err)
	}
	stats := r.Stats()
	if stats.TransactionRows != 1 || stats.CallRows != 3 || stats.MatchedCalls != 3 || stats.UsableTxs != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestReaderRejectsUnsortedTransactions(t *testing.T) {
	header := "blockNumber,transactionIndex,transactionHash,from,to,toCreate,value,gasUsed,receiptStatus,fromIsContract,toIsContract,txCategory\n"
	txCSV := header +
		"14000001,0," + testHash + ",0x" + addrA + ",0x" + addrB + ",,0,21000,1,false,false,transfer\n" +
		"14000000,0,0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,0x" + addrA + ",0x" + addrB + ",,0,21000,1,false,false,transfer\n"
	callCSV := "blockNumber,transactionIndex,transactionHash,traceAddress,callDepth,caller,callee,callType,callGasUsed,error,txStatus,callerIsContract,calleeIsContract,isCCGEdge\n"
	r, err := NewReader(strings.NewReader(txCSV), strings.NewReader(callCSV))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Next(); err == nil || !strings.Contains(err.Error(), "not strictly ordered") {
		t.Fatalf("got error %v, want ordering error", err)
	}
}
