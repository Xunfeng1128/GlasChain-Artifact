package measure

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"encoding/csv"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestPackedGasUsedChargesFullGasToBothRelayStages(t *testing.T) {
	tx := core.NewTransaction("sender", "recipient", big.NewInt(1), 1, time.Now())
	tx.GasUsed = 10_000_001

	if got := packedGasUsed(tx, "relay1"); got != 10_000_001 {
		t.Fatalf("relay1 expected full packed gas 10000001, got %d", got)
	}
	if got := packedGasUsed(tx, "relay2"); got != 10_000_001 {
		t.Fatalf("relay2 expected full packed gas 10000001, got %d", got)
	}
	if got := packedGasUsed(tx, "inner"); got != 10_000_001 {
		t.Fatalf("inner expected full packed gas, got %d", got)
	}
}

func TestPackedGasUsedUsesIntrinsicFallback(t *testing.T) {
	tx := core.NewTransaction("sender", "recipient", big.NewInt(1), 1, time.Now())
	first := packedGasUsed(tx, "relay1")
	second := packedGasUsed(tx, "relay2")
	if first != core.IntrinsicGas || second != core.IntrinsicGas {
		t.Fatalf("intrinsic gas charged as relay1=%d relay2=%d", first, second)
	}
}

func TestExecutionTxHashPrefersCanonicalSourceHash(t *testing.T) {
	tx := &core.Transaction{TxHash: []byte{1, 2, 3}, SourceTxHash: "0xcanonical"}
	if got := executionTxHash(tx); got != "0xcanonical" {
		t.Fatalf("execution hash = %q, want canonical source hash", got)
	}
}

func metricTestOutputDir(t *testing.T) string {
	t.Helper()
	previous := params.DataWrite_path
	dir := t.TempDir()
	params.DataWrite_path = dir + string(os.PathSeparator)
	t.Cleanup(func() { params.DataWrite_path = previous })
	return filepath.Join(dir, "supervisor_measureOutput")
}

func readMetricTestCSV(t *testing.T, dir, name string) [][]string {
	t.Helper()
	file, err := os.Open(filepath.Join(dir, name+".csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func legacyTxDetailHeader() []string {
	return []string{
		"TxHash (Byte -> Big Int)", "Tx propose timestamp", "Block propose timestamp",
		"Tx finally commit timestamp", "Relay1 Tx commit timestamp (not a relay tx -> nil)",
		"Relay2 Tx commit timestamp (not a relay tx -> nil)",
		"Broker1 Tx commit timestamp (not a broker_300k_BlockTransaction tx -> nil)",
		"Broker2 Tx commit timestamp (not a broker_300k_BlockTransaction tx -> nil)",
		"Confirmed latency of this tx (ms)", "Broker1 enter pool timestamp",
		"Broker1 leave pool timestamp", "Broker2 enter pool timestamp", "Broker2 leave pool timestamp",
	}
}

func legacyExecutionDetailHeader() []string {
	return []string{
		"tx_hash", "tx_nonce", "sender", "recipient", "epoch", "executed_shard",
		"stage", "gasUsed", "gasUsed_full", "gasUsed_split", "gasUsed_packed", "commit_time",
	}
}

func TestExecutionDetailsStreamsLegacyCSVAfterEachBlock(t *testing.T) {
	dir := metricTestOutputDir(t)
	previousBeta := params.Beta
	params.Beta = 0.5
	t.Cleanup(func() { params.Beta = previousBeta })
	metric := NewTestTxExecutionDetails()
	t.Cleanup(metric.output.close)
	tx := &core.Transaction{
		TxHash: []byte{1, 2}, Nonce: 9, Sender: "sender,quoted", Recipient: "recipient",
		GasUsed: 100001,
	}
	broker := *tx
	broker.HasBroker, broker.RawTxHash = true, []byte{3, 4}
	canonical := *tx
	canonical.SourceTxHash = "0xcanonical"
	metric.UpdateMeasureRecord(&message.BlockInfoMsg{
		BlockBodyLength: 6, Epoch: 7, SenderShardID: 3, CommitTime: time.UnixMilli(10000),
		InnerShardTxs: []*core.Transaction{tx, &broker}, Relay1Txs: []*core.Transaction{tx},
		Relay2Txs: []*core.Transaction{&canonical}, Broker1Txs: []*core.Transaction{&broker},
		Broker2Txs: []*core.Transaction{&broker},
	})
	want := [][]string{
		legacyExecutionDetailHeader(),
		{"0x0102", "9", "sender,quoted", "recipient", "7", "3", "inner", "100001", "100001", "100001", "100001", "10000"},
		{"0x0304", "9", "sender,quoted", "recipient", "7", "3", "broker_direct", "100001", "100001", "100001", "100001", "10000"},
		{"0x0102", "9", "sender,quoted", "recipient", "7", "3", "relay1", "100001", "100001", "100001", "100001", "10000"},
		{"0xcanonical", "9", "sender,quoted", "recipient", "7", "3", "relay2", "100001", "100001", "100001", "100001", "10000"},
		{"0x0304", "9", "sender,quoted", "recipient", "7", "3", "broker1", "100001", "100001", "50000", "100001", "10000"},
		{"0x0304", "9", "sender,quoted", "recipient", "7", "3", "broker2", "100001", "100001", "50001", "100001", "10000"},
	}
	if got := readMetricTestCSV(t, dir, metric.OutputMetricName()); !reflect.DeepEqual(got, want) {
		t.Fatalf("first block CSV = %v, want %v", got, want)
	}
	metric.UpdateMeasureRecord(&message.BlockInfoMsg{
		BlockBodyLength: 1, Epoch: 8, SenderShardID: 2,
		InnerShardTxs: []*core.Transaction{{TxHash: []byte{5}, Sender: "s", Recipient: "r"}},
	})
	want = append(want, []string{"0x05", "0", "s", "r", "8", "2", "inner", "0", "0", "0", "21000", ""})
	if got := readMetricTestCSV(t, dir, metric.OutputMetricName()); !reflect.DeepEqual(got, want) {
		t.Fatalf("second block CSV = %v, want %v", got, want)
	}
	metric.OutputRecord()
	metric.OutputRecord()
	if got := readMetricTestCSV(t, dir, metric.OutputMetricName()); !reflect.DeepEqual(got, want) {
		t.Fatalf("OutputRecord duplicated or changed streamed rows: %v", got)
	}
	if metric.output.file != nil || metric.output.writer != nil || !metric.output.closed {
		t.Fatal("OutputRecord retained an open stream")
	}
}

func TestTxDetailsStreamsCompletedRowsAndDeduplicatesReceipts(t *testing.T) {
	dir := metricTestOutputDir(t)
	metric := NewTestTxDetail()
	t.Cleanup(metric.output.close)
	tx := &core.Transaction{
		TxHash: []byte{1}, Time: time.UnixMilli(1000),
		Broker1EnterPoolTime: time.UnixMilli(1100), Broker1LeavePoolTime: time.UnixMilli(1500),
	}
	block := &message.BlockInfoMsg{
		BlockBodyLength: 1, ProposeTime: time.UnixMilli(2000), CommitTime: time.UnixMilli(3000),
		InnerShardTxs: []*core.Transaction{tx},
	}
	metric.UpdateMeasureRecord(block)
	want := [][]string{
		legacyTxDetailHeader(),
		{"1", "1000", "2000", "3000", "", "", "", "", "2000", "1100", "1500", "", ""},
	}
	if got := readMetricTestCSV(t, dir, metric.OutputMetricName()); !reflect.DeepEqual(got, want) {
		t.Fatalf("first block CSV = %v, want %v", got, want)
	}
	if len(metric.txHash2DetailTime) != 0 || len(metric.written) != 1 {
		t.Fatalf("completed detail retained: pending=%d written=%d", len(metric.txHash2DetailTime), len(metric.written))
	}
	metric.UpdateMeasureRecord(block)
	metric.UpdateMeasureRecord(&message.BlockInfoMsg{
		BlockBodyLength: 1, ProposeTime: time.UnixMilli(4000), CommitTime: time.UnixMilli(5000),
		InnerShardTxs: []*core.Transaction{{TxHash: []byte{2}, Time: time.UnixMilli(2000)}},
	})
	want = append(want, []string{"2", "2000", "4000", "5000", "", "", "", "", "3000", "", "", "", ""})
	if got := readMetricTestCSV(t, dir, metric.OutputMetricName()); !reflect.DeepEqual(got, want) {
		t.Fatalf("duplicate/second block CSV = %v, want %v", got, want)
	}
	if len(metric.txHash2DetailTime) != 0 || len(metric.written) != 2 {
		t.Fatalf("completed detail retained: pending=%d written=%d", len(metric.txHash2DetailTime), len(metric.written))
	}
	metric.OutputRecord()
	metric.OutputRecord()
	if got := readMetricTestCSV(t, dir, metric.OutputMetricName()); !reflect.DeepEqual(got, want) {
		t.Fatalf("OutputRecord duplicated or changed streamed rows: %v", got)
	}
	if metric.written != nil || metric.output.file != nil || metric.output.writer != nil || !metric.output.closed {
		t.Fatal("OutputRecord retained deduplication state or an open stream")
	}
}

func TestTxDetailsKeepsBothHalvesAcrossReceiptOrder(t *testing.T) {
	for _, broker := range []bool{false, true} {
		for _, secondFirst := range []bool{false, true} {
			name := "relay_first_then_second"
			if broker {
				name = "broker_first_then_second"
			}
			if secondFirst {
				name += "_reversed"
			}
			t.Run(name, func(t *testing.T) {
				dir := metricTestOutputDir(t)
				metric := NewTestTxDetail()
				t.Cleanup(metric.output.close)
				tx := &core.Transaction{
					TxHash: []byte{4}, RawTxHash: []byte{5}, Time: time.UnixMilli(1000),
					Broker1EnterPoolTime: time.UnixMilli(1100), Broker1LeavePoolTime: time.UnixMilli(1500),
					Broker2EnterPoolTime: time.UnixMilli(5000), Broker2LeavePoolTime: time.UnixMilli(6000),
				}
				first := &message.BlockInfoMsg{BlockBodyLength: 1, ProposeTime: time.UnixMilli(3000), CommitTime: time.UnixMilli(4000)}
				second := &message.BlockInfoMsg{BlockBodyLength: 1, ProposeTime: time.UnixMilli(6500), CommitTime: time.UnixMilli(7000)}
				wantRow := []string{"4", "1000", "3000", "7000", "4000", "7000", "", "", "6000", "", "", "", ""}
				if broker {
					first.Broker1Txs, second.Broker2Txs = []*core.Transaction{tx}, []*core.Transaction{tx}
					wantRow = []string{"5", "1000", "3000", "7000", "", "", "4000", "7000", "6000", "1100", "1500", "5000", "6000"}
				} else {
					first.Relay1Txs, second.Relay2Txs = []*core.Transaction{tx}, []*core.Transaction{tx}
				}
				if secondFirst {
					first, second = second, first
				}
				metric.UpdateMeasureRecord(first)
				if len(metric.txHash2DetailTime) != 1 || len(metric.written) != 0 {
					t.Fatalf("partial receipt was discarded: pending=%d written=%d", len(metric.txHash2DetailTime), len(metric.written))
				}
				if _, err := os.Stat(filepath.Join(dir, metric.OutputMetricName()+".csv")); err == nil {
					if rows := readMetricTestCSV(t, dir, metric.OutputMetricName()); len(rows) > 1 {
						t.Fatalf("partial receipt emitted a completed row: %v", rows)
					}
				} else if !os.IsNotExist(err) {
					t.Fatal(err)
				}
				metric.UpdateMeasureRecord(second)
				want := [][]string{legacyTxDetailHeader(), wantRow}
				if got := readMetricTestCSV(t, dir, metric.OutputMetricName()); !reflect.DeepEqual(got, want) {
					t.Fatalf("completed receipt CSV = %v, want %v", got, want)
				}
				if len(metric.txHash2DetailTime) != 0 || len(metric.written) != 1 {
					t.Fatalf("completed detail retained: pending=%d written=%d", len(metric.txHash2DetailTime), len(metric.written))
				}
				metric.UpdateMeasureRecord(first)
				metric.UpdateMeasureRecord(second)
				metric.OutputRecord()
				metric.OutputRecord()
				if got := readMetricTestCSV(t, dir, metric.OutputMetricName()); !reflect.DeepEqual(got, want) {
					t.Fatalf("duplicate receipts/OutputRecord changed CSV: %v", got)
				}
			})
		}
	}
}

func TestDetailOutputsCreateEmptyHeadersExactlyOnce(t *testing.T) {
	dir := metricTestOutputDir(t)
	details := NewTestTxDetail()
	execution := NewTestTxExecutionDetails()
	t.Cleanup(details.output.close)
	t.Cleanup(execution.output.close)
	details.UpdateMeasureRecord(&message.BlockInfoMsg{})
	execution.UpdateMeasureRecord(&message.BlockInfoMsg{})
	for i := 0; i < 2; i++ {
		details.OutputRecord()
		execution.OutputRecord()
	}
	for _, test := range []struct {
		name   string
		header []string
	}{
		{details.OutputMetricName(), legacyTxDetailHeader()},
		{execution.OutputMetricName(), legacyExecutionDetailHeader()},
	} {
		if got := readMetricTestCSV(t, dir, test.name); !reflect.DeepEqual(got, [][]string{test.header}) {
			t.Fatalf("empty %s CSV = %v, want one legacy header", test.name, got)
		}
	}
}

func TestTxDetailsOutputFlushesUnmatchedReceiptOnce(t *testing.T) {
	dir := metricTestOutputDir(t)
	metric := NewTestTxDetail()
	t.Cleanup(metric.output.close)
	metric.UpdateMeasureRecord(&message.BlockInfoMsg{
		BlockBodyLength: 1, CommitTime: time.UnixMilli(4000), ProposeTime: time.UnixMilli(3000),
		Relay1Txs: []*core.Transaction{{TxHash: []byte{6}, Time: time.UnixMilli(1000)}},
	})
	metric.OutputRecord()
	first := readMetricTestCSV(t, dir, metric.OutputMetricName())
	if len(first) != 2 || first[1][0] != "6" || first[1][3] != "" || first[1][4] != "4000" {
		t.Fatalf("unmatched receipt was lost or given a fabricated completion: %v", first)
	}
	if len(metric.txHash2DetailTime) != 0 || metric.written != nil {
		t.Fatal("OutputRecord retained unmatched details/deduplication state")
	}
	metric.OutputRecord()
	if got := readMetricTestCSV(t, dir, metric.OutputMetricName()); !reflect.DeepEqual(got, first) {
		t.Fatalf("repeated output duplicated unmatched receipt: %v", got)
	}
}

func TestTCLRelayDropsOnlyMatchedTimestampAndKeepsLegacyOrdering(t *testing.T) {
	metricTestOutputDir(t)
	metric := NewTestModule_TCL_Relay()
	matched := &core.Transaction{TxHash: []byte{1}, Time: time.UnixMilli(1000)}
	unmatched := &core.Transaction{TxHash: []byte{2}, Time: time.UnixMilli(2000)}
	reversed := &core.Transaction{TxHash: []byte{3}, Time: time.UnixMilli(1000)}
	metric.UpdateMeasureRecord(&message.BlockInfoMsg{
		BlockBodyLength: 2, CommitTime: time.UnixMilli(3000), Relay1Txs: []*core.Transaction{matched, unmatched},
	})
	metric.UpdateMeasureRecord(&message.BlockInfoMsg{
		BlockBodyLength: 1, CommitTime: time.UnixMilli(7000), Relay2Txs: []*core.Transaction{matched},
	})
	if _, exists := metric.relay1CommitTS[string(matched.TxHash)]; exists {
		t.Fatal("matched relay1 timestamp was retained")
	}
	if got := metric.relay1CommitTS[string(unmatched.TxHash)]; !got.Equal(time.UnixMilli(3000)) {
		t.Fatalf("unmatched relay1 timestamp changed: %v", got)
	}
	metric.UpdateMeasureRecord(&message.BlockInfoMsg{
		BlockBodyLength: 1, CommitTime: time.UnixMilli(8000), Relay2Txs: []*core.Transaction{reversed},
	})
	metric.UpdateMeasureRecord(&message.BlockInfoMsg{
		BlockBodyLength: 1, CommitTime: time.UnixMilli(4000), Relay1Txs: []*core.Transaction{reversed},
	})
	// Preserve the existing arrival-order formula: a Relay2 received before its
	// Relay1 contributes total latency, but cannot add a matched-stage latency.
	if metric.txNum[0] != 2.5 || metric.totTxLatencyEpoch[0] != 13 ||
		metric.relay1CommitLatency[0] != 6000 || metric.relay2CommitLatency[0] != 4000 ||
		metric.ctxCommitLatency[0] != 6000 || metric.relay1TxNum[0] != 3 || metric.relay2TxNum[0] != 2 ||
		len(metric.relay1CommitTS) != 2 {
		t.Fatalf("legacy TCL counters changed: %+v", metric)
	}
	perEpoch, total := metric.OutputRecord()
	if len(perEpoch) != 1 || perEpoch[0] != 5.2 || total != 5.2 || metric.relay1CommitTS != nil {
		t.Fatalf("TCL output/state = %v, %v, %v", perEpoch, total, metric.relay1CommitTS)
	}
}
