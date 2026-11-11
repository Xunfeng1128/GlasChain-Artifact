package dataset

import jdataset "blockEmulator/internal/tracecsv/dataset"

type Checkpoint = jdataset.Checkpoint

func (reader *Reader) Checkpoint() Checkpoint { return reader.inner.Checkpoint() }

func OpenAtCheckpoint(transactionPath, callPath string, cp Checkpoint) (*Reader, error) {
	inner, err := jdataset.OpenAtCheckpoint(transactionPath, callPath, cp)
	if err != nil {
		return nil, err
	}
	return &Reader{inner: inner}, nil
}
