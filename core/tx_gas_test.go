package core

import (
	"math/big"
	"testing"
	"time"
)

func TestRelayStageGasUsedChargesFullGasPerStage(t *testing.T) {
	for _, total := range []uint64{0, 1, 21_000, 10_000_000, 10_000_001} {
		relay1 := RelayStageGasUsed(total)
		relay2 := RelayStageGasUsed(total)
		if relay1 != total || relay2 != total {
			t.Fatalf("total %d charged as relay1=%d relay2=%d", total, relay1, relay2)
		}
	}
}

func TestEffectiveGasUsedFallsBackToIntrinsicGas(t *testing.T) {
	tx := NewTransaction("sender", "recipient", big.NewInt(1), 1, time.Now())
	if got := EffectiveGasUsed(tx); got != IntrinsicGas {
		t.Fatalf("expected intrinsic gas %d, got %d", IntrinsicGas, got)
	}
	tx.GasUsed = 10_000_000
	if got := EffectiveGasUsed(tx); got != 10_000_000 {
		t.Fatalf("expected explicit gas, got %d", got)
	}
}
