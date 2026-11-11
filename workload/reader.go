// Package workload defines common input admission independently of an account
// partitioning algorithm. The existing canonical two-table parser is reused;
// this package does not build or expose any partition graph.
package workload

import (
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"time"

	"blockEmulator/core"
	gdataset "blockEmulator/glasfreight/dataset"
	"blockEmulator/glasfreight/model"
)

type Reader struct {
	inner         *gdataset.Reader
	blockGasLimit uint64
	// Hashes remain recorded after completion, so a duplicated source row can
	// never be interpreted as a second logical transaction by another method.
	seen map[[32]byte]struct{}
}

func Open(transactionsPath, callsPath string, blockGasLimit uint64) (*Reader, error) {
	inner, err := gdataset.Open(transactionsPath, callsPath)
	if err != nil {
		return nil, err
	}
	if blockGasLimit == 0 {
		inner.Close()
		return nil, fmt.Errorf("unified workload requires a positive block Gas limit")
	}
	return &Reader{inner: inner, blockGasLimit: blockGasLimit, seen: make(map[[32]byte]struct{})}, nil
}

func NewReader(transactions, calls io.Reader, blockGasLimit uint64) (*Reader, error) {
	if blockGasLimit == 0 {
		return nil, fmt.Errorf("unified workload requires a positive block Gas limit")
	}
	inner, err := gdataset.NewReader(transactions, calls)
	if err != nil {
		return nil, err
	}
	return &Reader{inner: inner, blockGasLimit: blockGasLimit, seen: make(map[[32]byte]struct{})}, nil
}

// Next uses the same filtering and canonical order for every method: successful,
// non-create, non-self TOP transactions, including contract interactions. Invalid
// Gas is a data error, never a method-specific skip or intrinsic-Gas fallback.
func (reader *Reader) Next() (*model.Transaction, error) {
	record, err := reader.inner.Next()
	if err != nil {
		return nil, err
	}
	if !record.HasGasUsed || record.GasUsed == 0 {
		return nil, fmt.Errorf("transaction %s requires positive gasUsed", record.SourceHash)
	}
	if record.GasUsed > reader.blockGasLimit {
		return nil, fmt.Errorf("transaction %s baseGas=%d exceeds BlockGasLimit=%d", record.SourceHash, record.GasUsed, reader.blockGasLimit)
	}
	var hash [32]byte
	if _, err := hex.Decode(hash[:], []byte(record.SourceHash[2:])); err != nil {
		return nil, err
	}
	if _, duplicate := reader.seen[hash]; duplicate {
		return nil, fmt.Errorf("duplicate unified transaction %s", record.SourceHash)
	}
	reader.seen[hash] = struct{}{}
	return record, nil
}

func (reader *Reader) Close() error          { return reader.inner.Close() }
func (reader *Reader) Stats() gdataset.Stats { return reader.inner.Stats() }

// CoreTransaction preserves original Gas and call occurrences as immutable
// metadata. Execution subsequently prices the stage under the current mapping;
// baseline graph builders remain free to use only TOP endpoints/counts/base Gas.
func CoreTransaction(record *model.Transaction, nonce uint64, callGas uint64) (*core.Transaction, error) {
	if record == nil || !record.HasGasUsed || record.GasUsed == 0 {
		return nil, fmt.Errorf("transaction requires positive gasUsed")
	}
	value, ok := new(big.Int).SetString(record.Value, 10)
	if !ok || value.Sign() < 0 {
		return nil, fmt.Errorf("transaction %s has invalid value %q", record.SourceHash, record.Value)
	}
	tx := core.NewTransaction(record.From, record.To, value, nonce, time.Now())
	tx.GasUsed, tx.GLASBaseGas, tx.GLASCallGasPerCall = record.GasUsed, record.GasUsed, callGas
	for _, call := range record.Calls {
		if call.IsCCGEdge && call.CallerIsSCA && call.CalleeIsSCA && call.Caller != "" && call.Callee != "" {
			tx.GLASCallRelations = append(tx.GLASCallRelations, core.GLASCallRelation{Caller: call.Caller, Callee: call.Callee})
		}
	}
	tx.ExecDelayMs = core.ComputeTxExecDelayMs(tx.GasUsed)
	tx.SourceTxHash, tx.TxCategory = record.SourceHash, record.Category
	tx.InputSenderIsContract, tx.InputRecipientIsContract = record.FromIsContract, record.ToIsContract
	return tx, nil
}
