package core

import (
	"math/big"
	"testing"
	"time"
)

func TestGLASCallTaskMetadataSurvivesEncoding(t *testing.T) {
	tx := NewTransaction("from", "to", big.NewInt(0), 1, time.Unix(1, 0))
	tx.IsGLASCallTask = true
	tx.GLASCallTaskID = "logical:epoch:target"
	tx.GLASCallOriginShard = 2
	tx.GLASCallTargetShard = 7
	tx.GLASCallCallee = "to"
	tx.GLASCallCount = 3
	tx.GLASStageEpoch = 4
	tx.GLASStageEpochSet = true
	tx.GasUsed = 12345
	decoded := DecodeTx(tx.Encode())
	if !decoded.IsGLASCallTask || decoded.GLASCallTaskID != tx.GLASCallTaskID || decoded.GLASCallOriginShard != 2 || decoded.GLASCallTargetShard != 7 || decoded.GLASCallCallee != "to" || decoded.GLASCallCount != 3 || decoded.GLASStageEpoch != 4 || !decoded.GLASStageEpochSet || decoded.GasUsed != 12345 {
		t.Fatalf("GLAS call-task metadata changed: %+v", decoded)
	}
}
