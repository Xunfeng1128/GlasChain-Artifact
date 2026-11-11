package pbft_all

import (
	"bytes"
	"testing"

	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/shard"
)

func TestCLPAPartitionPrePrepareKeepsBothMetadataChecks(t *testing.T) {
	node, _, _ := clpaInstallFixture(t, 0)
	for _, tc := range []struct {
		name     string
		window   *message.PendingExecutionWindow
		groups   *message.GARETPlacement
		accepted bool
	}{
		{"valid", nil, nil, true},
		{"different_window", &message.PendingExecutionWindow{Epoch: 1, Sources: []string{"other"}}, nil, false},
		{"unexpected_groups", nil, &message.GARETPlacement{Epoch: 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transfer := message.AccountTransferMsg{ATid: 1, PlanningWindow: tc.window, GARET: tc.groups}
			request := &message.Request{RequestType: message.PartitionReq, Msg: message.RawMessage{Content: transfer.Encode()}}
			pp := &message.PrePrepare{SeqID: 1, RequestMsg: request, Digest: getDigest(request)}
			if got := node.ihm.HandleinPrePrepare(pp); got != tc.accepted {
				t.Fatalf("accepted=%t, want %t", got, tc.accepted)
			}
		})
	}
}

// Replay still decodes the wire request separately for the installation receipt.
// Use the same wire request to compare it with the normal quorum path.
func TestCLPAPartitionDecodeNormalMatchesReplay(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window *message.PendingExecutionWindow
		first  string
	}{
		{"clpa", nil, "ordinary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wire *message.Request
			var normalRoot, normalHash []byte
			var normalReceipt message.PartitionInstalled
			for _, path := range []string{"normal", "replay"} {
				t.Run(path, func(t *testing.T) {
					node, proposal, _, receipts := step4ProposalFixture(t, tc.window)
					if err := node.installExecutionTransactions([]*core.Transaction{unifiedPBFTTx("ordinary")}, false); err != nil {
						t.Fatal(err)
					}
					if path == "normal" {
						wire = proposal.RequestMsg
						if !node.ihm.HandleinPrePrepare(proposal) {
							t.Fatal("valid partition proposal rejected")
						}
						for sender := uint64(0); sender < 3; sender++ {
							node.handleCommit(clpaEventWire(t, message.Commit{SeqID: proposal.SeqID, Digest: proposal.Digest, SenderNode: &shard.Node{NodeID: sender}}))
						}
					} else {
						defer node.sequenceLock.Unlock()
						node.catchUpPending, node.catchUpStart, node.catchUpEnd = true, 1, 1
						node.handleSendOldSeq(clpaEventWire(t, message.SendOldMessage{SeqStartHeight: 1, SeqEndHeight: 1, OldRequest: []*message.Request{wire}}))
					}

					receipt := clpaInstallReceipt(t, receipts)
					block := node.CurChain.CurrentBlock
					if path == "normal" {
						normalReceipt = receipt
						normalRoot, normalHash = block.Header.StateRoot, block.Hash
					} else if receipt != normalReceipt || !bytes.Equal(block.Header.StateRoot, normalRoot) || !bytes.Equal(block.Hash, normalHash) {
						t.Fatal("replay differs from normal commit in receipt, state root or migration block hash")
					}
					pool := node.CurChain.Txpool
					queue := pool.Snapshot().TxQueue
					if len(queue) != 2 || queue[0].SourceTxHash != "ordinary" || queue[1].SourceTxHash != "chosen" {
						t.Fatalf("changed retained/incoming order: %+v", queue)
					}
					next := pool.PeekWindowTransactions(core.TxSelectionOptions{MaxGas: 40})
					if len(next) != 1 || next[0].SourceTxHash != tc.first {
						t.Fatalf("wrong first executable record: %+v", next)
					}
				})
			}
		})
	}
}
