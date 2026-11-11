package committee

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/workload"
)

type committeeInput struct {
	legacy             *csv.Reader
	file               *os.File
	unified            *workload.Reader
	accepted, expected int
}

func openCommitteeInput(legacyPath string, expected int) (*committeeInput, error) {
	input := &committeeInput{expected: expected}
	if params.UnifiedExecution {
		reader, err := workload.Open(params.UnifiedTransactionsFile, params.UnifiedCallsFile, params.BlockGasLimit)
		if err != nil {
			return nil, err
		}
		input.unified = reader
	} else {
		file, err := os.Open(legacyPath)
		if err != nil {
			return nil, err
		}
		input.file, input.legacy = file, csv.NewReader(file)
	}
	return input, nil
}

func (input *committeeInput) Close() error {
	if input.unified != nil {
		return input.unified.Close()
	}
	return input.file.Close()
}

func (input *committeeInput) Next(nonce uint64) (*core.Transaction, error) {
	if input.unified != nil {
		if input.expected > 0 && input.accepted >= input.expected {
			return nil, io.EOF
		}
		record, err := input.unified.Next()
		if err == io.EOF && input.accepted < input.expected {
			return nil, fmt.Errorf("unified input ended after %d accepted transactions; requested %d", input.accepted, input.expected)
		}
		if err != nil {
			return nil, err
		}
		tx, err := workload.CoreTransaction(record, nonce, params.UnifiedCrossCallGas)
		if err != nil {
			return nil, err
		}
		input.accepted++
		return tx, nil
	}
	for {
		row, err := input.legacy.Read()
		if err != nil {
			return nil, err
		}
		if tx, accepted := data2tx(row, nonce); accepted {
			return tx, nil
		}
	}
}

func priceCommitteeInput(tx *core.Transaction, shardFor func(string) uint64) error {
	if !params.UnifiedExecution {
		return nil
	}
	return core.PriceGLASStage(tx, shardFor)
}
