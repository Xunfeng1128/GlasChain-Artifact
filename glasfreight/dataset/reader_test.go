package dataset

import (
	"strings"
	"testing"
)

func TestReaderIncludesEOAsAndInternalCalls(t *testing.T) {
	txs := `blockNumber,transactionIndex,transactionHash,from,to,toCreate,value,gasLimit,gasUsed,receiptStatus,fromIsContract,toIsContract,txCategory
1,0,0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0x1111111111111111111111111111111111111111,0x2222222222222222222222222222222222222222,,0,150000,100000,1,false,true,inter_contract_call
`
	calls := `blockNumber,transactionIndex,transactionHash,traceAddress,callDepth,caller,callee,callType,callGasUsed,error,txStatus,callerIsContract,calleeIsContract,isCCGEdge
1,0,0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0,1,0x2222222222222222222222222222222222222222,0x3333333333333333333333333333333333333333,call,25000,,1,true,true,true
`
	reader, err := NewReader(strings.NewReader(txs), strings.NewReader(calls))
	if err != nil {
		t.Fatal(err)
	}
	record, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if record.FromIsContract || !record.ToIsContract || len(record.Calls) != 1 {
		t.Fatalf("unexpected converted record: %+v", record)
	}
	if !record.HasGasLimit || record.GasLimit != 150000 {
		t.Fatalf("gas limit was not retained: %+v", record)
	}
	if !record.HasGasUsed || record.GasUsed != 100000 {
		t.Fatalf("pre-executed gasUsed was not retained: %+v", record)
	}
	if record.Calls[0].Caller != "2222222222222222222222222222222222222222" || record.Calls[0].GasUsed != 25000 {
		t.Fatalf("unexpected call: %+v", record.Calls[0])
	}
}
