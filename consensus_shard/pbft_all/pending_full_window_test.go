package pbft_all

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
)

func enableFullWindowEarlyTest(t *testing.T) {
	t.Helper()
	mode, opt, policy, limit, ratio := params.ConsensusMethod, params.UnifiedPackingOptimized,
		params.GLASPendingExecutionPolicy, params.BlockGasLimit, params.GLASPendingWindowRatio
	t.Cleanup(func() {
		params.ConsensusMethod, params.UnifiedPackingOptimized, params.GLASPendingExecutionPolicy,
			params.BlockGasLimit, params.GLASPendingWindowRatio = mode, opt, policy, limit, ratio
	})
	params.ConsensusMethod, params.UnifiedPackingOptimized, params.GLASPendingExecutionPolicy = 12, true, params.GLASPendingExecutionWindowFirst
	params.BlockGasLimit, params.GLASPendingWindowRatio = 100, 1.3
	full, epochs, blocks, timeout := params.GLASPendingFullWindowEarly, params.ExperimentEpochCount,
		params.ExperimentBlocksPerEpoch, params.GLASPendingSnapshotTimeoutSeconds
	t.Cleanup(func() {
		params.GLASPendingFullWindowEarly, params.ExperimentEpochCount,
			params.ExperimentBlocksPerEpoch, params.GLASPendingSnapshotTimeoutSeconds = full, epochs, blocks, timeout
	})
	params.GLASPendingFullWindowEarly, params.ExperimentEpochCount, params.ExperimentBlocksPerEpoch = true, 0, 6
	params.GLASPendingSnapshotTimeoutSeconds = 2
}

func TestFullWindowReportRegistersMarksWithoutChangingSixthBlock(t *testing.T) {
	p, _, wires := clpaInstallFixture(t, 0)
	enableFullWindowEarlyTest(t)
	inside := p.ihm.(*CLPAPbftInsideExtraHandleMod)
	cross, planned := unifiedPBFTTx("predicted-cross"), unifiedPBFTTx("planned-local")
	cross.Nonce, cross.Recipient = 1, "c"
	planned.Nonce, planned.Sender, planned.GLASBaseGas = 2, "b", 80
	if err := p.installExecutionTransactions([]*core.Transaction{cross, planned}, false); err != nil {
		t.Fatal(err)
	}

	p.pbftLock.Lock()
	inside.observePendingEarlyCommit(&core.Block{Header: &core.BlockHeader{Number: 5}})
	marks := p.CurChain.Txpool.ExportPendingMarks([]*core.Transaction{cross, planned})
	p.pbftLock.Unlock()
	if marks == nil || marks.Epoch != 0 || !bytes.Equal(marks.Flags, []byte{2, 2}) {
		t.Fatalf("capture did not synchronously register only pending marks: %+v", marks)
	}
	if got := p.generateExecutionBlock().Body; len(got) != 1 || got[0].SourceTxHash != cross.SourceTxHash {
		t.Fatalf("next-round window changed the sixth block: %v", got)
	}
	relay, err := p.executionRelay2(cross, 0)
	if err != nil {
		t.Fatal(err)
	}
	if relayMarks := p.CurChain.Txpool.ExportPendingMarks([]*core.Transaction{relay}); relayMarks == nil || !bytes.Equal(relayMarks.Flags, []byte{2}) {
		t.Fatalf("predicted cross-shard TOP's mark did not carry to its real Relay2: %+v", relayMarks)
	}
	select {
	case wire := <-wires:
		kind, body := message.SplitMessage(wire)
		var report message.PendingEarlyReport
		if err := json.Unmarshal(body, &report); err != nil {
			t.Fatal(err)
		}
		if kind != message.CPendingEarlyReport || len(report.FullWindow) != 1 || len(report.ForecastCrossTOP) != 1 ||
			report.FullWindow[0].SourceTxHash != planned.SourceTxHash || len(report.FullWindow[0].GLASCallRelations) != 1 ||
			report.ForecastCrossTOP[0].SourceTxHash != cross.SourceTxHash {
			t.Fatalf("expected complete selected/forecast bodies instead of IDs: %+v", report)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("full-window report did not arrive")
	}
}

func TestFullWindowFutureRelayPreservesBothPriorityEpochs(t *testing.T) {
	unifiedPBFTFixture(t)
	enableFullWindowEarlyTest(t)
	node := unifiedPBFTNode(t, 0)
	data := dataSupport.NewCLPADataSupport()
	inside := &CLPAPbftInsideExtraHandleMod{pbftNode: node, cdm: data}
	outside := &CLPARelayOutsideModule{pbftNode: node, cdm: data}
	relay := unifiedPBFTTx("future-relay")
	relay.Relayed, relay.Sender, relay.Recipient = true, "c", "a"
	if err := core.PriceExecutionStage(relay, node.CurChain.Get_PartitionMap); err != nil {
		t.Fatal(err)
	}
	// The sender has installed plan 1 and is already forecasting plan 2.
	wire := clpaEventWire(t, message.Relay{Txs: []*core.Transaction{relay}, SenderShardID: 1, SenderSeq: 8,
		PendingMarks: &core.PendingMarks{Epoch: 1, Flags: []byte{3}}})
	outside.handleRelay(wire)
	if len(data.FutureExecutionForwards) != 1 || node.CurChain.Txpool.GetTxQueueLen() != 0 || node.seqIDMap[1] != 8 {
		t.Fatal("future Relay2 was not buffered while recording its Relay sequence")
	}
	data.AccountTransferRound = 1
	out, err := inside.flushExecutionForwardsLocked()
	if err != nil || len(out) != 0 || len(data.FutureExecutionForwards) != 0 || node.CurChain.Txpool.GetTxQueueLen() != 1 {
		t.Fatalf("future Relay2 was not admitted after installation: %v %v", out, err)
	}
	outside.handleRelay(wire)
	if node.CurChain.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("duplicate Relay2 was admitted again")
	}
	marks := node.CurChain.Txpool.ExportPendingMarks([]*core.Transaction{relay})
	if marks == nil || marks.Epoch != 1 || !bytes.Equal(marks.Flags, []byte{3}) {
		t.Fatalf("buffering or replay lost a plan mark: %+v", marks)
	}
}
