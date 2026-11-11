// Package dataset adapts the canonical two-table workload reader to the
// GLAS-FREIGHT model. CSV parsing is intentionally shared with the JiangZiya
// dataset layer; placement, graph construction and optimization are not.
package dataset

import (
	"io"

	"blockEmulator/glasfreight/model"
	jdataset "blockEmulator/internal/tracecsv/dataset"
	jmodel "blockEmulator/internal/tracecsv/model"
)

type Stats = jdataset.Stats

type Reader struct {
	inner *jdataset.Reader
}

func Open(transactionPath, callPath string) (*Reader, error) {
	inner, err := jdataset.Open(transactionPath, callPath)
	if err != nil {
		return nil, err
	}
	return &Reader{inner: inner}, nil
}

func NewReader(transactions, calls io.Reader) (*Reader, error) {
	inner, err := jdataset.NewReader(transactions, calls)
	if err != nil {
		return nil, err
	}
	return &Reader{inner: inner}, nil
}

func (reader *Reader) Next() (*model.Transaction, error) {
	var record *jmodel.Transaction
	for {
		var err error
		record, err = reader.inner.Next()
		if err != nil {
			return nil, err
		}
		if record.Usable() {
			break
		}
	}
	converted := &model.Transaction{
		BlockNumber: record.BlockNumber, TransactionIndex: record.TransactionIndex,
		SourceHash: record.SourceHash, From: record.From, To: record.To,
		Value: record.Value, GasLimit: record.GasLimit, HasGasLimit: record.HasGasLimit,
		GasUsed: record.GasUsed, HasGasUsed: record.HasGasUsed,
		FromIsContract: record.FromIsContract, ToIsContract: record.ToIsContract,
		Category: record.Category,
		Calls:    make([]model.Call, 0, len(record.Calls)),
	}
	for _, call := range record.Calls {
		converted.Calls = append(converted.Calls, model.Call{
			Caller: call.Caller, Callee: call.Callee,
			GasUsed: call.CallGasUsed, HasGasUsed: call.HasCallGas,
			IsCCGEdge: call.IsCCGEdge, CallerIsSCA: call.CallerIsContract,
			CalleeIsSCA: call.CalleeIsContract,
		})
	}
	return converted, nil
}

func (reader *Reader) Stats() Stats { return reader.inner.Stats() }

func (reader *Reader) Close() error { return reader.inner.Close() }
