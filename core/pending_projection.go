package core

import "fmt"

// CaptureFullPendingWindow previews the last block without changing the live
// queue. It owns copies of the selected window and forecast cross-shard TOP
// records; forecastBlock contains only fields needed for the forecast digest.
// Next-round marks are recorded before releasing the lock, so even a TOP that
// commits earlier than predicted can pass its mark to the real Relay2.
func (txpool *TxPool) CaptureFullPendingWindow(nextEpoch, gasBudget, blockGasLimit uint64, maxRecords int, resolve func(string) uint64) (forecastCrossTOP []*Transaction, window LocalWindowSelection, forecastBlock []*Transaction, err error) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	if nextEpoch <= txpool.pendingMarkEpoch || nextEpoch-txpool.pendingMarkEpoch != 1 {
		return nil, window, nil, fmt.Errorf("invalid full-window capture epoch %d after %d", nextEpoch, txpool.pendingMarkEpoch)
	}
	if gasBudget == 0 || blockGasLimit == 0 || resolve == nil {
		return nil, window, nil, fmt.Errorf("full-window capture requires positive Gas limits and a shard resolver")
	}
	if maxRecords > 0 && len(txpool.TxQueue) > maxRecords {
		return nil, window, nil, fmt.Errorf("early projection pool has %d records, limit %d", len(txpool.TxQueue), maxRecords)
	}
	for i, tx := range txpool.TxQueue {
		if tx == nil || tx.SourceTxHash == "" || tx.Sender == "" || tx.Recipient == "" || tx.IsGLASCallTask || tx.GasUsed == 0 {
			return nil, window, nil, fmt.Errorf("invalid early projection record at position %d", i)
		}
		if tx.GasUsed > blockGasLimit && (tx.GLASBaseGas == 0 || tx.GLASBaseGas > blockGasLimit) {
			return nil, window, nil, fmt.Errorf("early projection record %d exceeds block Gas limit", i)
		}
	}
	block := txpool.peekWindowTransactionsLocked(TxSelectionOptions{MaxGas: blockGasLimit})
	packed := make(map[*Transaction]struct{}, len(block))
	for _, tx := range block {
		packed[tx] = struct{}{}
	}
	residual := make([]*Transaction, 0, len(txpool.TxQueue)-len(block))
	for _, tx := range txpool.TxQueue {
		if _, selected := packed[tx]; !selected {
			residual = append(residual, tx)
		}
	}
	var sources map[string]struct{}
	if txpool.planningWindow != nil {
		sources = txpool.planningWindow.sources
	}
	window, err = selectLocalGasWindowWithSources(residual, gasBudget, blockGasLimit, sources)
	if err != nil {
		return nil, LocalWindowSelection{}, nil, err
	}

	forecastBlock = make([]*Transaction, len(block))
	digestFields := make([]Transaction, len(block))
	for i, tx := range block {
		digestFields[i] = Transaction{Nonce: tx.Nonce, Relayed: tx.Relayed,
			GLASStageEpoch: tx.GLASStageEpoch, GLASStageEpochSet: tx.GLASStageEpochSet}
		forecastBlock[i] = &digestFields[i]
		if !tx.Relayed && resolve(tx.Sender) != resolve(tx.Recipient) {
			forecastCrossTOP = append(forecastCrossTOP, tx)
		}
	}
	for _, tx := range window.Records {
		txpool.markNextSourceLocked(tx.SourceTxHash)
	}
	for _, tx := range forecastCrossTOP {
		txpool.markNextSourceLocked(tx.SourceTxHash)
	}
	window.Records = (&TxPool{TxQueue: window.Records}).Snapshot().TxQueue
	forecastCrossTOP = (&TxPool{TxQueue: forecastCrossTOP}).Snapshot().TxQueue
	return forecastCrossTOP, window, forecastBlock, nil
}
