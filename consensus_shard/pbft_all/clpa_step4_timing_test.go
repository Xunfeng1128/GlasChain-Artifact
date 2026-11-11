package pbft_all

import (
	"bytes"
	"encoding/json"
	"log"
	"math/big"
	"strings"
	"testing"
	"time"

	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/shard"
)

// Drive the real partition proposal path with one prepared incoming state and
// one transaction. The supervisor/peer captures are loopback test endpoints.
func step4ProposalFixture(t *testing.T, window *message.PendingExecutionWindow) (*PbftConsensusNode, *message.PrePrepare, *bytes.Buffer, <-chan []byte) {
	t.Helper()
	node, outside, receipt := clpaInstallFixture(t, 0)
	oldMethod := params.ConsensusMethod
	t.Cleanup(func() { params.ConsensusMethod = oldMethod })
	params.ConsensusMethod = 1
	node.pbftStage.Store(1)
	node.malicious_nums = 1 // install only when a third distinct vote arrives
	outside.cdm.ControlPolling = true
	peer, _, _ := clpaInstallCapture(t, nil)
	node.ip_nodeTable[1] = map[uint64]string{0: peer}
	buf := new(bytes.Buffer)
	node.pl.Plog = log.New(buf, "", 0)
	outside.handlePartitionPrepare(clpaEventWire(t, message.PartitionPrepare{Epoch: 1, TargetSeq: 1}))
	outside.handlePartitionMsg(clpaEventWire(t, message.PartitionModifiedMap{Epoch: 1,
		PartitionModified: map[string]uint64{"incoming": 0, "c": 0}, PlanningWindow: window}))
	outside.handlePartitionReady(clpaEventWire(t, message.PartitionReady{Epoch: 1, FromShard: 1, NowSeqID: 1}))
	tx := unifiedPBFTTx("chosen")
	tx.Sender = "incoming"
	outside.handleAccountStateAndTxMsg(clpaEventWire(t, message.AccountStateAndTx{Epoch: 1, FromShard: 1,
		Addrs: []string{"incoming"}, AccountState: []*core.AccountState{{Balance: big.NewInt(7)}}, Txs: []*core.Transaction{tx}}))
	proposal, _, _ := node.tryCLPAProposal(false)
	if proposal == nil || proposal.RequestMsg.RequestType != message.PartitionReq {
		t.Fatalf("no migration proposal: %s", buf)
	}
	node.prePrepareAccepted = true
	node.pbftStage.Store(3)
	return node, proposal, buf, receipt
}

func step4Records(t *testing.T, buf *bytes.Buffer) []map[string]json.RawMessage {
	t.Helper()
	var records []map[string]json.RawMessage
	for _, line := range strings.Split(buf.String(), "\n") {
		_, raw, ok := strings.Cut(line, "CLPA_STEP4_TIMING ")
		if ok {
			var record map[string]json.RawMessage
			if err := json.Unmarshal([]byte(raw), &record); err != nil {
				t.Fatal(err)
			}
			records = append(records, record)
		}
	}
	return records
}

func step4Int(t *testing.T, record map[string]json.RawMessage, key string) int64 {
	t.Helper()
	var value int64
	if err := json.Unmarshal(record[key], &value); err != nil {
		t.Fatalf("missing or invalid %s: %v", key, err)
	}
	return value
}

func TestStep4TimingNormalMigration(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window *message.PendingExecutionWindow
	}{
		{"clpa", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, proposal, buf, receipt := step4ProposalFixture(t, tc.window)
			vote := func(from uint64) {
				node.handleCommit(clpaEventWire(t, message.Commit{SeqID: proposal.SeqID, Digest: proposal.Digest, SenderNode: &shard.Node{NodeID: from}}))
			}
			vote(0)
			vote(1)
			if len(step4Records(t, buf)) != 0 || node.sequenceID != 1 {
				t.Fatal("timed or applied a migration below quorum")
			}
			lastVote := time.Now().UnixNano()
			vote(2)
			if r := clpaInstallReceipt(t, receipt); r.Epoch != 1 || node.CurChain.CurrentBlock.Header.Number != 1 || node.CurChain.Txpool.GetTxQueueLen() != 1 {
				t.Fatalf("migration/receipt did not complete: %+v", r)
			}
			vote(2) // delayed duplicate must not emit another duration
			node.recordCLPAPartitionInstall(proposal.RequestMsg, 1)
			records := step4Records(t, buf)
			if len(records) != 1 {
				t.Fatalf("got %d step4 records: %s", len(records), buf)
			}
			r := records[0]
			for k, want := range map[string]int64{"schema_version": 1, "epoch": 1, "seq": 1, "shard": 0, "leader": 0, "view": 0, "mapping_entries": 2, "incoming_records": 1, "account_count": 1} {
				if got := step4Int(t, r, k); got != want {
					t.Errorf("%s=%d, want %d", k, got, want)
				}
			}
			if step4Int(t, r, "quorum_unix_ns") < lastVote || step4Int(t, r, "proposal_unix_ns") > step4Int(t, r, "quorum_unix_ns") || step4Int(t, r, "install_unix_ns") < step4Int(t, r, "quorum_unix_ns") {
				t.Fatal("timing boundaries do not span the final qualifying vote")
			}
			wantMethod := "CLPA"
			if tc.window != nil {
				wantMethod = "GLAS_Pending"
			}
			var method string
			if err := json.Unmarshal(r["method"], &method); err != nil || method != wantMethod {
				t.Fatalf("method=%s, want %s", r["method"], wantMethod)
			}
			// This migration really moved an account and repriced an incoming record.
			// A missing phase hook must not disappear into the residual unnoticed.
			for _, key := range []string{"pbft_wait_ns", "map_update_ns", "reprice_ns", "accounts_write_ns", "receipt_prepare_ns"} {
				if step4Int(t, r, key) <= 0 {
					t.Fatalf("missing elapsed time for %s", key)
				}
			}
			var accounted int64
			for _, k := range []string{"pbft_wait_ns", "map_update_ns", "reprice_ns", "accounts_write_ns", "window_index_ns", "receipt_prepare_ns", "other_ns"} {
				n := step4Int(t, r, k)
				if n < 0 {
					t.Errorf("negative %s=%d", k, n)
				}
				accounted += n
			}
			if total := step4Int(t, r, "total_ns"); total != accounted || total <= 0 {
				t.Errorf("stage durations sum to %d, total=%d", accounted, total)
			}
			var applied bool
			wantIndex := tc.window != nil
			if err := json.Unmarshal(r["window_index_applied"], &applied); err != nil || applied != wantIndex {
				t.Errorf("window_index_applied=%s", r["window_index_applied"])
			}
			var sources int64
			if tc.window != nil {
				sources = int64(len(tc.window.Sources))
			}
			if step4Int(t, r, "window_sources") != sources || (!wantIndex && step4Int(t, r, "window_index_ns") != 0) {
				t.Error("absent and empty windows are not distinguished")
			}

		})
	}
}

// Replacement requests for an already timed sequence must not inherit the
// original request's start instant, even when the epoch and sequence match.
func TestStep4TimingRejectsReplacedProposal(t *testing.T) {
	node, proposal, buf, receipt := step4ProposalFixture(t, nil)
	replacement := clpaInstallRequest(1, proposal.SeqID, map[string]uint64{"incoming": 0})
	digest := getDigest(replacement)
	node.cacheRequest(proposal.SeqID, string(digest), replacement)
	for sender := uint64(0); sender < 3; sender++ {
		node.handleCommit(clpaEventWire(t, message.Commit{SeqID: proposal.SeqID, Digest: digest, SenderNode: &shard.Node{NodeID: sender}}))
	}
	clpaInstallReceipt(t, receipt)
	if len(step4Records(t, buf)) != 0 {
		t.Fatal("a replaced request acquired the wrong proposal timer")
	}
}

func TestStep4TimingOmitsCatchUp(t *testing.T) {
	node, proposal, buf, receipt := step4ProposalFixture(t, nil)
	// The timed proposal has not reached a local Commit quorum. Recovering
	// its payload must install it without manufacturing a normal quorum sample.
	defer node.sequenceLock.Unlock()
	request := proposal.RequestMsg
	node.catchUpPending, node.catchUpStart, node.catchUpEnd = true, 1, 1
	node.handleSendOldSeq(clpaEventWire(t, message.SendOldMessage{SeqStartHeight: 1, SeqEndHeight: 1, OldRequest: []*message.Request{request}}))
	if got := clpaInstallReceipt(t, receipt); got.Epoch != 1 || len(step4Records(t, buf)) != 0 || node.clpaStep4 != nil {
		t.Fatalf("catch-up should install and acknowledge without a proposal timing sample: %+v", got)
	}
}
