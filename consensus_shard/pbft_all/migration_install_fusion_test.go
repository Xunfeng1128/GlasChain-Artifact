package pbft_all

import (
	"testing"

	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
)

// Exercise the complete installation boundary: empty routing delta must still
// admit migration records, release eligible forwards, deduplicate them and
// install the window only after those records are present.
func TestUnifiedEmptyDeltaInstallKeepsForwardAndWindowSemantics(t *testing.T) {
	for _, withWindow := range []bool{false} {
		name := "clpa"
		if withWindow {
			name = "d_window"
		}
		t.Run(name, func(t *testing.T) {
			unifiedPBFTFixture(t)
			node := unifiedPBFTNode(t, 0)
			if err := node.installExecutionTransactions([]*core.Transaction{unifiedPBFTTx("ordinary")}, false); err != nil {
				t.Fatal(err)
			}
			retained := node.CurChain.Txpool.TxQueue[0]
			incoming, chosen := unifiedPBFTTx("incoming"), unifiedPBFTTx("chosen")
			chosen.Sender = "b"
			data := dataSupport.NewCLPADataSupport()
			data.ReceivedNewTx = []*core.Transaction{incoming}
			data.FutureExecutionForwards = []*message.ExecutionForward{{
				Epoch: 1, FromShard: 1, Txs: []*core.Transaction{chosen, incoming},
			}}
			transfer := &message.AccountTransferMsg{ATid: 1, ModifiedMap: map[string]uint64{}}
			if withWindow {
				transfer.PlanningWindow = &message.PendingExecutionWindow{Epoch: 1, Sources: []string{"chosen"}}
			}
			(&CLPAPbftInsideExtraHandleMod{pbftNode: node, cdm: data}).accountTransfer_do(transfer)

			queue := node.CurChain.Txpool.Snapshot().TxQueue
			if len(queue) != 3 || queue[0].SourceTxHash != "ordinary" || queue[1].SourceTxHash != "incoming" || queue[2].SourceTxHash != "chosen" {
				t.Fatalf("installation changed stable queue order or duplicated a forward: %+v", queue)
			}
			if node.CurChain.Txpool.TxQueue[0] != retained || retained.GasUsed != 60 || incoming.GasUsed != 40 || chosen.GasUsed != 40 {
				t.Fatal("empty delta replaced retained work or mutated incoming/forward input objects")
			}
			for _, tx := range queue {
				if tx.GasUsed != 60 {
					t.Fatal("migration or future forward was not priced with the installed map")
				}
			}
			if data.AccountTransferRound != 1 || len(data.FutureExecutionForwards) != 0 || len(data.ReceivedNewTx) != 0 {
				t.Fatal("installation did not advance/clear the original migration state")
			}
			first := "ordinary"
			if withWindow {
				// The eligible forward must be present when the final window
				// index is installed, even though it arrived after migration.
				first = "chosen"
			}
			block := node.generateExecutionBlock()
			if len(block.Body) != 1 || block.Body[0].SourceTxHash != first {
				t.Fatalf("wrong next proposal after installation: %+v", block.Body)
			}
		})
	}
}
