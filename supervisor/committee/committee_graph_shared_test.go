package committee

import (
	"io"
	"log"
	"reflect"
	"sync"
	"testing"

	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/supervisor/supervisor_log"
)

func TestSharedGraphObservesOnlyCompletedLogicalWork(t *testing.T) {
	base := NewCLPACommitteeModule(nil, nil, &supervisor_log.SupervisorLog{Slog: log.New(io.Discard, "", 0)}, "", 0, 0, 48)
	base.skipHistoryGraph = true
	var observed []*core.Transaction
	module := &GraphSharedCommitteeModule{CLPACommitteeModule: base, historyLock: &sync.Mutex{}, addHistory: func(tx *core.Transaction) { observed = append(observed, tx) }}
	local := &core.Transaction{Sender: "a", Recipient: "b", GasUsed: 123}
	relay1 := &core.Transaction{Sender: "c", Recipient: "d", GasUsed: 456}
	relay2 := &core.Transaction{Sender: "e", Recipient: "f", GasUsed: 789}
	module.HandleBlockInfo(&message.BlockInfoMsg{SenderShardID: 2, BlockHeight: 9, Epoch: 1, BlockBodyLength: 3,
		InnerShardTxs: []*core.Transaction{local}, Relay1Txs: []*core.Transaction{relay1}, Relay2Txs: []*core.Transaction{relay2}})
	if !reflect.DeepEqual(observed, []*core.Transaction{local, relay2}) {
		t.Fatalf("graph population must remain LOCAL + RELAY2: %+v", observed)
	}
	if base.curEpoch != 1 || base.shardMaxSeq[2] != 9 {
		t.Fatal("algorithm history bypassed the shared block/epoch observer")
	}
	module.HandleBlockInfo(&message.BlockInfoMsg{SenderShardID: 2, BlockHeight: 10, Epoch: 1})
	module.HandleBlockInfo(nil)
	if len(observed) != 2 || base.shardMaxSeq[2] != 10 {
		t.Fatal("empty blocks must update progress without adding algorithm work")
	}
	if local.GasUsed != 123 || relay1.GasUsed != 456 || relay2.GasUsed != 789 {
		t.Fatal("graph observation changed execution charging")
	}
}

func TestSharedGraphConstructorsKeepCommonLifecycle(t *testing.T) {
	oldPath, oldShards := params.DataWrite_path, params.ShardNum
	t.Cleanup(func() { params.DataWrite_path, params.ShardNum = oldPath, oldShards })
	params.ShardNum = 2
	logger := &supervisor_log.SupervisorLog{Slog: log.New(io.Discard, "", 0)}
	for _, method := range []string{"TxAllo_Contrib", "P_Louvain_Contrib", "P_Louvain_Gas"} {
		t.Run(method, func(t *testing.T) {
			params.DataWrite_path = t.TempDir()
			module := NewGraphSharedCommitteeModule(method, nil, nil, logger, "original-input.csv", 100000, 2000, 48)
			if module.partitionPlanner == nil || !module.skipHistoryGraph || module.historyLock == nil || module.addHistory == nil {
				t.Fatal("shared algorithm adapter is not fully wired")
			}
			if module.csvPath != "original-input.csv" || module.dataTotalNum != 100000 || module.batchDataNum != 2000 || module.clpaFreq != 48 {
				t.Fatal("adapter changed common input/reconfiguration parameters")
			}
			module.modifiedMap["known"] = 1
			if module.fetchModifiedMap("known") != 1 {
				t.Fatal("adapter bypassed common installed map routing")
			}
		})
	}
}
