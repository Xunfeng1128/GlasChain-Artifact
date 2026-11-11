package pbft_all

import (
	"encoding/json"
	"time"

	"blockEmulator/message"
	"blockEmulator/params"
)

// One in-flight normal migration per leader. All access is under pbftLock.
// Local durations use Go's monotonic clock. Unix timestamps only locate events
// across processes; independent shards' duration averages are not fleet time.
type clpaStep4Timing struct {
	proposed, quorum time.Time
	digest           string

	SchemaVersion      int    `json:"schema_version"`
	Method             string `json:"method"`
	Epoch              uint64 `json:"epoch"`
	Seq                uint64 `json:"seq"`
	Shard              uint64 `json:"shard"`
	Leader             uint64 `json:"leader"`
	View               int32  `json:"view"`
	ProposalUnixNS     int64  `json:"proposal_unix_ns"`
	QuorumUnixNS       int64  `json:"quorum_unix_ns"`
	InstallUnixNS      int64  `json:"install_unix_ns"`
	PBFTWaitNS         int64  `json:"pbft_wait_ns"`
	MapUpdateNS        int64  `json:"map_update_ns"`
	RepriceNS          int64  `json:"reprice_ns"`
	AccountsWriteNS    int64  `json:"accounts_write_ns"`
	WindowIndexNS      int64  `json:"window_index_ns"`
	ReceiptPrepareNS   int64  `json:"receipt_prepare_ns"`
	OtherNS            int64  `json:"other_ns"`
	TotalNS            int64  `json:"total_ns"`
	WindowIndexApplied bool   `json:"window_index_applied"`
	MappingEntries     int    `json:"mapping_entries"`
	IncomingRecords    int    `json:"incoming_records"`
	AccountCount       int    `json:"account_count"`
	WindowSources      int    `json:"window_sources"`
}

func (p *PbftConsensusNode) startCLPAStep4(epoch uint64) {
	method := "CLPA"
	if params.ConsensusMethod != 1 {
		if !pendingWindowExecutionEnabled() {
			return
		}
		method = "GLAS_Pending"
	}
	view := p.view.Load()
	if uint64(view) != p.NodeID {
		return
	}
	now := time.Now()
	p.clpaStep4 = &clpaStep4Timing{SchemaVersion: 1, Method: method, Epoch: epoch, Seq: p.sequenceID,
		Shard: p.ShardID, Leader: p.NodeID, View: view, proposed: now, ProposalUnixNS: now.UnixNano()}
}

func (p *PbftConsensusNode) markCLPAStep4Quorum(commit *message.Commit) {
	t := p.clpaStep4
	if t == nil || !t.quorum.IsZero() {
		return
	}
	if t.Seq != commit.SeqID || t.View != p.view.Load() || t.digest == "" || t.digest != string(commit.Digest) {
		p.clpaStep4 = nil
		return
	}
	t.quorum = time.Now()
	t.QuorumUnixNS = t.quorum.UnixNano()
	t.PBFTWaitNS = t.quorum.Sub(t.proposed).Nanoseconds()
}

// A pending proposal alone never times follower application, replay or catch-up.
func (p *PbftConsensusNode) applyingCLPAStep4(epoch uint64) *clpaStep4Timing {
	t := p.clpaStep4
	if t == nil || t.quorum.IsZero() || t.Epoch != epoch || t.Seq != p.sequenceID || t.View != p.view.Load() {
		return nil
	}
	return t
}

func (t *clpaStep4Timing) phaseStart() time.Time {
	if t != nil {
		return time.Now()
	}
	return time.Time{}
}

func (p *PbftConsensusNode) finishCLPAStep4(t *clpaStep4Timing, receiptStart, installed time.Time) {
	p.clpaStep4 = nil
	t.InstallUnixNS = installed.UnixNano()
	t.ReceiptPrepareNS = installed.Sub(receiptStart).Nanoseconds()
	t.TotalNS = installed.Sub(t.proposed).Nanoseconds()
	t.OtherNS = t.TotalNS - t.PBFTWaitNS - t.MapUpdateNS - t.RepriceNS - t.AccountsWriteNS - t.WindowIndexNS - t.ReceiptPrepareNS
	if data, err := json.Marshal(t); err == nil && p.pl != nil && p.pl.Plog != nil {
		p.pl.Plog.Printf("CLPA_STEP4_TIMING %s", data)
	}
}
