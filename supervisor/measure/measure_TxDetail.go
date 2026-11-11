package measure

import (
	"blockEmulator/message"
	"math/big"
	"strconv"
	"time"
)

type txMetricDetailTime struct {
	// normal tx time
	TxProposeTimestamp, BlockProposeTimestamp, TxCommitTimestamp time.Time

	// relay tx time
	Relay1CommitTimestamp, Relay2CommitTimestamp time.Time

	// broker_300k_BlockTransaction tx time
	Broker1CommitTimestamp, Broker2CommitTimestamp time.Time

	// pool enter/leave time (broker_300k_BlockTransaction mode)
	Broker1EnterPoolTime, Broker1LeavePoolTime time.Time
	Broker2EnterPoolTime, Broker2LeavePoolTime time.Time
}

// to test Tx detail
type TestTxDetail struct {
	txHash2DetailTime map[string]*txMetricDetailTime
	written           map[string]struct{}
	output            metricCSVStream
}

func NewTestTxDetail() *TestTxDetail {
	return &TestTxDetail{
		txHash2DetailTime: make(map[string]*txMetricDetailTime),
		written:           make(map[string]struct{}),
	}
}

func (ttd *TestTxDetail) OutputMetricName() string {
	return "Tx_Details"
}

func (ttd *TestTxDetail) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	if b.BlockBodyLength == 0 { // empty block
		return
	}

	for _, innertx := range b.InnerShardTxs {
		if _, done := ttd.written[string(innertx.TxHash)]; done {
			continue
		}
		if _, ok := ttd.txHash2DetailTime[string(innertx.TxHash)]; !ok {
			ttd.txHash2DetailTime[string(innertx.TxHash)] = &txMetricDetailTime{}
		}
		ttd.txHash2DetailTime[string(innertx.TxHash)].TxProposeTimestamp = innertx.Time
		ttd.txHash2DetailTime[string(innertx.TxHash)].BlockProposeTimestamp = b.ProposeTime
		ttd.txHash2DetailTime[string(innertx.TxHash)].TxCommitTimestamp = b.CommitTime
		ttd.txHash2DetailTime[string(innertx.TxHash)].Broker1EnterPoolTime = innertx.Broker1EnterPoolTime
		ttd.txHash2DetailTime[string(innertx.TxHash)].Broker1LeavePoolTime = innertx.Broker1LeavePoolTime
	}
	for _, r1tx := range b.Relay1Txs {
		if _, done := ttd.written[string(r1tx.TxHash)]; done {
			continue
		}
		if _, ok := ttd.txHash2DetailTime[string(r1tx.TxHash)]; !ok {
			ttd.txHash2DetailTime[string(r1tx.TxHash)] = &txMetricDetailTime{}
		}
		ttd.txHash2DetailTime[string(r1tx.TxHash)].TxProposeTimestamp = r1tx.Time
		ttd.txHash2DetailTime[string(r1tx.TxHash)].BlockProposeTimestamp = b.ProposeTime
		ttd.txHash2DetailTime[string(r1tx.TxHash)].Relay1CommitTimestamp = b.CommitTime
	}
	for _, r2tx := range b.Relay2Txs {
		if _, done := ttd.written[string(r2tx.TxHash)]; done {
			continue
		}
		if _, ok := ttd.txHash2DetailTime[string(r2tx.TxHash)]; !ok {
			ttd.txHash2DetailTime[string(r2tx.TxHash)] = &txMetricDetailTime{}
		}
		ttd.txHash2DetailTime[string(r2tx.TxHash)].Relay2CommitTimestamp = b.CommitTime
		ttd.txHash2DetailTime[string(r2tx.TxHash)].TxCommitTimestamp = b.CommitTime
	}
	for _, b1tx := range b.Broker1Txs {
		if _, done := ttd.written[string(b1tx.RawTxHash)]; done {
			continue
		}
		if _, ok := ttd.txHash2DetailTime[string(b1tx.RawTxHash)]; !ok {
			ttd.txHash2DetailTime[string(b1tx.RawTxHash)] = &txMetricDetailTime{}
		}
		ttd.txHash2DetailTime[string(b1tx.RawTxHash)].TxProposeTimestamp = b1tx.Time
		ttd.txHash2DetailTime[string(b1tx.RawTxHash)].BlockProposeTimestamp = b.ProposeTime
		ttd.txHash2DetailTime[string(b1tx.RawTxHash)].Broker1CommitTimestamp = b.CommitTime
		ttd.txHash2DetailTime[string(b1tx.RawTxHash)].Broker1EnterPoolTime = b1tx.Broker1EnterPoolTime
		ttd.txHash2DetailTime[string(b1tx.RawTxHash)].Broker1LeavePoolTime = b1tx.Broker1LeavePoolTime
	}
	for _, b2tx := range b.Broker2Txs {
		if _, done := ttd.written[string(b2tx.RawTxHash)]; done {
			continue
		}
		if _, ok := ttd.txHash2DetailTime[string(b2tx.RawTxHash)]; !ok {
			ttd.txHash2DetailTime[string(b2tx.RawTxHash)] = &txMetricDetailTime{}
		}
		ttd.txHash2DetailTime[string(b2tx.RawTxHash)].Broker2CommitTimestamp = b.CommitTime
		ttd.txHash2DetailTime[string(b2tx.RawTxHash)].TxCommitTimestamp = b.CommitTime
		ttd.txHash2DetailTime[string(b2tx.RawTxHash)].Broker2EnterPoolTime = b2tx.Broker2EnterPoolTime
		ttd.txHash2DetailTime[string(b2tx.RawTxHash)].Broker2LeavePoolTime = b2tx.Broker2LeavePoolTime
	}
	// Visit only this receipt's hashes. Keep partial records until both halves
	// arrive, including out-of-order relay/broker receipts.
	for _, tx := range b.InnerShardTxs {
		ttd.flushCompleted(string(tx.TxHash), false, false)
	}
	for _, tx := range b.Relay1Txs {
		ttd.flushCompleted(string(tx.TxHash), true, false)
	}
	for _, tx := range b.Relay2Txs {
		ttd.flushCompleted(string(tx.TxHash), true, false)
	}
	for _, tx := range b.Broker1Txs {
		ttd.flushCompleted(string(tx.RawTxHash), false, true)
	}
	for _, tx := range b.Broker2Txs {
		ttd.flushCompleted(string(tx.RawTxHash), false, true)
	}
	ttd.output.flush()
}

func (ttd *TestTxDetail) flushCompleted(key string, relay, broker bool) {
	v := ttd.txHash2DetailTime[key]
	if v == nil || v.TxCommitTimestamp.IsZero() {
		return
	}
	if relay && (v.Relay1CommitTimestamp.IsZero() || v.Relay2CommitTimestamp.IsZero()) {
		return
	}
	if broker && (v.Broker1CommitTimestamp.IsZero() || v.Broker2CommitTimestamp.IsZero()) {
		return
	}
	ttd.writeDetail(key, v)
	delete(ttd.txHash2DetailTime, key)
	if ttd.written == nil {
		ttd.written = make(map[string]struct{})
	}
	ttd.written[key] = struct{}{}
}

func (ttd *TestTxDetail) HandleExtraMessage([]byte) {}

func (ttd *TestTxDetail) OutputRecord() (perEpochCTXs []float64, totTxNum float64) {
	ttd.writeToCSV()
	return []float64{}, 0
}

func (ttd *TestTxDetail) writeToCSV() {
	ttd.output.open(ttd.OutputMetricName(), txDetailColumns())
	for key, val := range ttd.txHash2DetailTime {
		ttd.writeDetail(key, val)
		delete(ttd.txHash2DetailTime, key)
	}
	ttd.output.close()
	ttd.written = nil
}

func txDetailColumns() []string {
	return []string{
		"TxHash (Byte -> Big Int)",
		"Tx propose timestamp",
		"Block propose timestamp",
		"Tx finally commit timestamp",
		"Relay1 Tx commit timestamp (not a relay tx -> nil)",
		"Relay2 Tx commit timestamp (not a relay tx -> nil)",
		"Broker1 Tx commit timestamp (not a broker_300k_BlockTransaction tx -> nil)",
		"Broker2 Tx commit timestamp (not a broker_300k_BlockTransaction tx -> nil)",
		"Confirmed latency of this tx (ms)",
		"Broker1 enter pool timestamp",
		"Broker1 leave pool timestamp",
		"Broker2 enter pool timestamp",
		"Broker2 leave pool timestamp",
	}
}

func (ttd *TestTxDetail) writeDetail(key string, val *txMetricDetailTime) {
	csvLine := []string{
		new(big.Int).SetBytes([]byte(key)).String(),

		timestampToString(val.TxProposeTimestamp),
		timestampToString(val.BlockProposeTimestamp),
		timestampToString(val.TxCommitTimestamp),

		timestampToString(val.Relay1CommitTimestamp),
		timestampToString(val.Relay2CommitTimestamp),

		timestampToString(val.Broker1CommitTimestamp),
		timestampToString(val.Broker2CommitTimestamp),

		strconv.FormatInt(int64(val.TxCommitTimestamp.Sub(val.TxProposeTimestamp).Milliseconds()), 10),

		timestampToString(val.Broker1EnterPoolTime),
		timestampToString(val.Broker1LeavePoolTime),
		timestampToString(val.Broker2EnterPoolTime),
		timestampToString(val.Broker2LeavePoolTime),
	}
	ttd.output.write(ttd.OutputMetricName(), txDetailColumns(), csvLine)
}

// zero time to empty string
func timestampToString(thisTime time.Time) string {
	if thisTime.IsZero() {
		return ""
	}
	return strconv.FormatInt(thisTime.UnixMilli(), 10)
}
