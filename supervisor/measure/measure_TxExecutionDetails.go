package measure

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"encoding/hex"
	"strconv"
	"time"
)

type txExecutionRecord struct {
	txHash        string
	txNonce       string
	sender        string
	recipient     string
	epoch         string
	executedShard string
	stage         string
	gasUsed       string
	gasUsedFull   string
	gasUsedSplit  string
	gasUsedPacked string
	commitTime    string
}

type TestTxExecutionDetails struct {
	output metricCSVStream
}

func NewTestTxExecutionDetails() *TestTxExecutionDetails {
	return &TestTxExecutionDetails{}
}

func (ted *TestTxExecutionDetails) OutputMetricName() string {
	return "Tx_Execution_Details"
}

func (ted *TestTxExecutionDetails) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	if b.BlockBodyLength == 0 {
		return
	}

	for _, tx := range b.InnerShardTxs {
		stage := "inner"
		if tx.HasBroker {
			stage = "broker_direct"
		}
		ted.appendTx(b, tx, stage)
	}
	for _, tx := range b.Relay1Txs {
		ted.appendTx(b, tx, "relay1")
	}
	for _, tx := range b.Relay2Txs {
		ted.appendTx(b, tx, "relay2")
	}
	for _, tx := range b.Broker1Txs {
		ted.appendTx(b, tx, "broker1")
	}
	for _, tx := range b.Broker2Txs {
		ted.appendTx(b, tx, "broker2")
	}
	ted.output.flush()
}

func (ted *TestTxExecutionDetails) HandleExtraMessage([]byte) {}

func (ted *TestTxExecutionDetails) OutputRecord() ([]float64, float64) {
	ted.writeToCSV()
	return []float64{}, 0
}

func (ted *TestTxExecutionDetails) appendTx(b *message.BlockInfoMsg, tx *core.Transaction, stage string) {
	gasUsed := tx.GasUsed
	gasUsedFull := gasUsed
	gasUsedSplit := splitGasUsed(gasUsed, stage)
	gasUsedPacked := packedGasUsed(tx, stage)

	record := txExecutionRecord{
		txHash:        executionTxHash(tx),
		txNonce:       strconv.FormatUint(tx.Nonce, 10),
		sender:        tx.Sender,
		recipient:     tx.Recipient,
		epoch:         strconv.Itoa(b.Epoch),
		executedShard: strconv.FormatUint(b.SenderShardID, 10),
		stage:         stage,
		gasUsed:       strconv.FormatUint(gasUsed, 10),
		gasUsedFull:   strconv.FormatUint(gasUsedFull, 10),
		gasUsedSplit:  strconv.FormatUint(gasUsedSplit, 10),
		gasUsedPacked: strconv.FormatUint(gasUsedPacked, 10),
		commitTime:    timestampToUnixMilliString(b.CommitTime),
	}
	ted.output.write(ted.OutputMetricName(), executionDetailColumns(), []string{
		record.txHash, record.txNonce, record.sender, record.recipient,
		record.epoch, record.executedShard, record.stage, record.gasUsed,
		record.gasUsedFull, record.gasUsedSplit, record.gasUsedPacked, record.commitTime,
	})
}

func executionTxHash(tx *core.Transaction) string {
	if tx.SourceTxHash != "" {
		return tx.SourceTxHash
	}
	hash := tx.TxHash
	if len(tx.RawTxHash) > 0 {
		hash = tx.RawTxHash
	}
	if len(hash) == 0 {
		return ""
	}
	return "0x" + hex.EncodeToString(hash)
}

// splitGasUsed keeps the legacy CSV column name. Regular relay stages now
// charge full gas in both shards; only broker stages retain Beta attribution.
func splitGasUsed(gasUsed uint64, stage string) uint64 {
	switch stage {
	case "relay1":
		return core.RelayStageGasUsed(gasUsed)
	case "relay2":
		return core.RelayStageGasUsed(gasUsed)
	case "broker1":
		return uint64(float64(gasUsed) * params.Beta)
	case "broker2":
		return gasUsed - uint64(float64(gasUsed)*params.Beta)
	default:
		return gasUsed
	}
}

// packedGasUsed mirrors the gas charged by the block packer. Every execution
// record, including relay1 and relay2, reserves the transaction's full gas in
// the block that contains it.
func packedGasUsed(tx *core.Transaction, _ string) uint64 {
	return core.EffectiveGasUsed(tx)
}

func timestampToUnixMilliString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return strconv.FormatInt(t.UnixMilli(), 10)
}

func (ted *TestTxExecutionDetails) writeToCSV() {
	ted.output.open(ted.OutputMetricName(), executionDetailColumns())
	ted.output.close()
}

func executionDetailColumns() []string {
	return []string{
		"tx_hash",
		"tx_nonce",
		"sender",
		"recipient",
		"epoch",
		"executed_shard",
		"stage",
		"gasUsed",
		"gasUsed_full",
		"gasUsed_split",
		"gasUsed_packed",
		"commit_time",
	}

}
