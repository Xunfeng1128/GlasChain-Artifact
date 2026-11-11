package core

import "testing"

func TestExecutionAdmissionBaseCheckKeepsIndependentPriceValidation(t *testing.T) {
	resolve := func(address string) uint64 {
		if address == "c" {
			return 1
		}
		return 0
	}
	tx := executionTestTransaction("source", "a", "b", 80)
	if err := PriceExecutionStage(tx, resolve); err != nil {
		t.Fatal(err)
	}
	if err := ValidateExecutionAdmissionBaseGas(tx, 100); err != nil {
		t.Fatal("base-valid surcharge must remain queued", err)
	}
	if err := ValidateExecutionAdmissionGas(tx, resolve, 100); err != nil {
		t.Fatal(err)
	}
	if err := ValidateExecutionStageGas(tx, resolve, 100); err == nil {
		t.Fatal("block validation accepted a surcharge above capacity")
	}
	for _, change := range []func(*Transaction){
		func(tx *Transaction) { tx.GasUsed-- },
		func(tx *Transaction) { tx.GLASAttachedGas-- },
		func(tx *Transaction) { tx.GLASCrossCallCount-- },
		func(tx *Transaction) { tx.ExecDelayMs++ },
	} {
		bad := *tx
		change(&bad)
		if ValidateExecutionAdmissionGas(&bad, resolve, 100) == nil || ValidateExecutionStageGas(&bad, resolve, 0) == nil {
			t.Fatal("full external validation stopped checking declared prices")
		}
	}
	wantErr := ValidateExecutionAdmissionGas(tx, resolve, 79)
	if err := ValidateExecutionAdmissionBaseGas(tx, 79); err == nil || wantErr == nil || err.Error() != wantErr.Error() {
		t.Fatalf("base rejection changed: got=%v want=%v", err, wantErr)
	}
	if ValidateExecutionAdmissionBaseGas(tx, 0) != nil || ValidateExecutionAdmissionBaseGas(nil, 100) != nil || ValidateExecutionAdmissionBaseGas(&Transaction{}, 100) != nil {
		t.Fatal("base-only helper changed the existing zero-limit or legacy rules")
	}
}
