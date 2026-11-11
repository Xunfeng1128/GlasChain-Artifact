// Package dataset performs a streaming merge join of the canonical
// transactions.csv and contract_calls.csv tables. Memory is bounded by one
// transaction's calls plus records already injected but not yet completed.
package dataset

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/big"
	"os"
	"strconv"
	"strings"

	"blockEmulator/internal/tracecsv/model"
)

type position struct {
	block uint64
	index uint64
}

func comparePosition(a, b position) int {
	if a.block < b.block {
		return -1
	}
	if a.block > b.block {
		return 1
	}
	if a.index < b.index {
		return -1
	}
	if a.index > b.index {
		return 1
	}
	return 0
}

// Stats makes filtering and join coverage explicit in experiment logs.
type Stats struct {
	TransactionRows uint64
	CallRows        uint64
	MatchedCalls    uint64
	UnmatchedCalls  uint64
	UsableTxs       uint64
	SkippedTxs      uint64
}

// Reader streams transactions and attaches all contiguous trace rows with the
// same (blockNumber, transactionIndex, transactionHash).
type Reader struct {
	txFile   *os.File
	callFile *os.File
	txCSV    *csv.Reader
	callCSV  *csv.Reader
	txCols   map[string]int
	callCols map[string]int

	nextCall *model.Call
	callEOF  bool
	closed   bool

	lastTxPos     position
	haveLastTxPos bool
	lastCallPos   position
	haveLastCall  bool
	stats         Stats
	// Absolute bases are nonzero only for an explicitly resumed replay reader.
	txOffsetBase   int64
	callOffsetBase int64
}

var transactionColumns = []string{
	"blockNumber", "transactionIndex", "transactionHash", "from", "to",
	"toCreate", "value", "gasUsed", "receiptStatus", "fromIsContract",
	"toIsContract", "txCategory",
}

var callColumns = []string{
	"blockNumber", "transactionIndex", "transactionHash", "traceAddress",
	"callDepth", "caller", "callee", "callType", "callGasUsed", "error",
	"txStatus", "callerIsContract", "calleeIsContract", "isCCGEdge",
}

// Open opens both canonical tables. The caller must Close the returned reader.
func Open(transactionPath, callPath string) (*Reader, error) {
	txFile, err := os.Open(transactionPath)
	if err != nil {
		return nil, fmt.Errorf("open JiangZiya transactions: %w", err)
	}
	callFile, err := os.Open(callPath)
	if err != nil {
		txFile.Close()
		return nil, fmt.Errorf("open JiangZiya contract calls: %w", err)
	}
	r, err := NewReader(txFile, callFile)
	if err != nil {
		callFile.Close()
		txFile.Close()
		return nil, err
	}
	r.txFile = txFile
	r.callFile = callFile
	return r, nil
}

// NewReader constructs a reader over caller-owned streams. It is primarily
// useful for deterministic unit tests.
func NewReader(transactions, calls io.Reader) (*Reader, error) {
	if transactions == nil || calls == nil {
		return nil, fmt.Errorf("both JiangZiya CSV streams are required")
	}
	txCSV, txCols, err := projectedReader(transactions, "transactions.csv", transactionColumns)
	if err != nil {
		return nil, err
	}
	callCSV, callCols, err := projectedReader(calls, "contract_calls.csv", callColumns)
	if err != nil {
		return nil, err
	}
	r := &Reader{txCSV: txCSV, callCSV: callCSV, txCols: txCols, callCols: callCols}
	if err := r.advanceCall(); err != nil {
		return nil, err
	}
	return r, nil
}

func projectedReader(input io.Reader, table string, required []string) (*csv.Reader, map[string]int, error) {
	r := csv.NewReader(input)
	header, err := r.Read()
	if err == io.EOF {
		return nil, nil, fmt.Errorf("%s is empty", table)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read %s header: %w", table, err)
	}
	columns := make(map[string]int, len(header))
	for i, raw := range header {
		name := strings.TrimSpace(raw)
		if _, duplicate := columns[name]; duplicate {
			return nil, nil, fmt.Errorf("%s duplicates header %q", table, name)
		}
		columns[name] = i
	}
	for _, name := range required {
		if _, ok := columns[name]; !ok {
			return nil, nil, fmt.Errorf("%s is missing required column %q", table, name)
		}
	}
	r.FieldsPerRecord = len(header)
	return r, columns, nil
}

// Next returns the next top-level transaction with its trace rows.
func (r *Reader) Next() (*model.Transaction, error) {
	if r.closed {
		return nil, fmt.Errorf("JiangZiya dataset reader is closed")
	}
	row, err := r.txCSV.Read()
	if err == io.EOF {
		for r.nextCall != nil {
			r.stats.UnmatchedCalls++
			if err := r.advanceCall(); err != nil {
				return nil, err
			}
		}
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("transactions.csv row %d: %w", r.stats.TransactionRows+2, err)
	}
	r.stats.TransactionRows++
	tx, err := parseTransaction(row, r.txCols)
	if err != nil {
		return nil, fmt.Errorf("transactions.csv row %d: %w", r.stats.TransactionRows+1, err)
	}
	pos := position{block: tx.BlockNumber, index: tx.TransactionIndex}
	if r.haveLastTxPos && comparePosition(pos, r.lastTxPos) <= 0 {
		return nil, fmt.Errorf("transactions.csv is not strictly ordered at block %d index %d", pos.block, pos.index)
	}
	r.lastTxPos, r.haveLastTxPos = pos, true

	for r.nextCall != nil && comparePosition(callPosition(r.nextCall), pos) < 0 {
		r.stats.UnmatchedCalls++
		if err := r.advanceCall(); err != nil {
			return nil, err
		}
	}
	for r.nextCall != nil && comparePosition(callPosition(r.nextCall), pos) == 0 {
		call := *r.nextCall
		if call.TransactionHash == tx.SourceHash {
			// The placement CCG only needs successful internal C2C edges. The
			// top-level EOA -> contract invocation remains available on the
			// transaction record and is deliberately not part of W_v.
			if call.IsCCGEdge {
				tx.Calls = append(tx.Calls, call)
			}
			r.stats.MatchedCalls++
		} else {
			r.stats.UnmatchedCalls++
		}
		if err := r.advanceCall(); err != nil {
			return nil, err
		}
	}
	if tx.Usable() {
		r.stats.UsableTxs++
	} else {
		r.stats.SkippedTxs++
	}
	return tx, nil
}

func (r *Reader) advanceCall() error {
	r.nextCall = nil
	if r.callEOF {
		return nil
	}
	row, err := r.callCSV.Read()
	if err == io.EOF {
		r.callEOF = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("contract_calls.csv row %d: %w", r.stats.CallRows+2, err)
	}
	r.stats.CallRows++
	call, err := parseCall(row, r.callCols)
	if err != nil {
		return fmt.Errorf("contract_calls.csv row %d: %w", r.stats.CallRows+1, err)
	}
	pos := callPosition(call)
	if r.haveLastCall && comparePosition(pos, r.lastCallPos) < 0 {
		return fmt.Errorf("contract_calls.csv is not ordered at block %d index %d", pos.block, pos.index)
	}
	r.lastCallPos, r.haveLastCall = pos, true
	r.nextCall = call
	return nil
}

func callPosition(call *model.Call) position {
	return position{block: call.BlockNumber, index: call.TransactionIndex}
}

func parseTransaction(row []string, cols map[string]int) (*model.Transaction, error) {
	block, err := uintCell(row, cols, "blockNumber")
	if err != nil {
		return nil, err
	}
	index, err := uintCell(row, cols, "transactionIndex")
	if err != nil {
		return nil, err
	}
	hash, err := normalizeHash(cell(row, cols, "transactionHash"))
	if err != nil {
		return nil, err
	}
	from, err := normalizeAddress(cell(row, cols, "from"), false)
	if err != nil {
		return nil, fmt.Errorf("from: %w", err)
	}
	to, err := normalizeAddress(cell(row, cols, "to"), true)
	if err != nil {
		return nil, fmt.Errorf("to: %w", err)
	}
	created, err := normalizeAddress(cell(row, cols, "toCreate"), true)
	if err != nil {
		return nil, fmt.Errorf("toCreate: %w", err)
	}
	gasLimit, hasGasLimit, err := nullableUint(optionalCell(row, cols, "gasLimit"))
	if err != nil {
		return nil, fmt.Errorf("gasLimit: %w", err)
	}
	gas, hasGas, err := nullableUint(cell(row, cols, "gasUsed"))
	if err != nil {
		return nil, fmt.Errorf("gasUsed: %w", err)
	}
	status, err := statusCell(row, cols, "receiptStatus")
	if err != nil {
		return nil, err
	}
	fromContract, err := boolCell(row, cols, "fromIsContract")
	if err != nil {
		return nil, err
	}
	toContract, err := boolCell(row, cols, "toIsContract")
	if err != nil {
		return nil, err
	}
	value := strings.TrimSpace(cell(row, cols, "value"))
	if value == "" {
		value = "0"
	}
	if _, ok := new(big.Int).SetString(value, 10); !ok {
		return nil, fmt.Errorf("invalid value %q", value)
	}
	return &model.Transaction{
		BlockNumber:      block,
		TransactionIndex: index,
		SourceHash:       hash,
		From:             from,
		To:               to,
		ToCreate:         created,
		Value:            value,
		GasLimit:         gasLimit,
		HasGasLimit:      hasGasLimit,
		GasUsed:          gas,
		HasGasUsed:       hasGas,
		ReceiptStatus:    status,
		FromIsContract:   fromContract,
		ToIsContract:     toContract,
		Category:         strings.ToLower(strings.TrimSpace(cell(row, cols, "txCategory"))),
	}, nil
}

func parseCall(row []string, cols map[string]int) (*model.Call, error) {
	block, err := uintCell(row, cols, "blockNumber")
	if err != nil {
		return nil, err
	}
	index, err := uintCell(row, cols, "transactionIndex")
	if err != nil {
		return nil, err
	}
	hash, err := normalizeHash(cell(row, cols, "transactionHash"))
	if err != nil {
		return nil, err
	}
	depth64, err := uintCell(row, cols, "callDepth")
	if err != nil {
		return nil, err
	}
	if depth64 > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("callDepth %d overflows int", depth64)
	}
	caller, err := normalizeAddress(cell(row, cols, "caller"), true)
	if err != nil {
		return nil, fmt.Errorf("caller: %w", err)
	}
	callee, err := normalizeAddress(cell(row, cols, "callee"), true)
	if err != nil {
		return nil, fmt.Errorf("callee: %w", err)
	}
	gas, hasGas, err := nullableUint(cell(row, cols, "callGasUsed"))
	if err != nil {
		return nil, fmt.Errorf("callGasUsed: %w", err)
	}
	txStatus, err := statusCell(row, cols, "txStatus")
	if err != nil {
		return nil, err
	}
	callerContract, err := boolCell(row, cols, "callerIsContract")
	if err != nil {
		return nil, err
	}
	calleeContract, err := boolCell(row, cols, "calleeIsContract")
	if err != nil {
		return nil, err
	}
	isCCG, err := boolCell(row, cols, "isCCGEdge")
	if err != nil {
		return nil, err
	}
	traceError := nullableText(cell(row, cols, "error"))
	if isCCG && (depth64 == 0 || txStatus != 1 || traceError != "" || !callerContract || !calleeContract || caller == "" || callee == "") {
		return nil, fmt.Errorf("isCCGEdge=true is not a successful internal contract-to-contract call")
	}
	return &model.Call{
		BlockNumber:      block,
		TransactionIndex: index,
		TransactionHash:  hash,
		TraceAddress:     strings.TrimSpace(cell(row, cols, "traceAddress")),
		Depth:            int(depth64),
		Caller:           caller,
		Callee:           callee,
		CallType:         strings.ToLower(strings.TrimSpace(cell(row, cols, "callType"))),
		CallGasUsed:      gas,
		HasCallGas:       hasGas,
		Error:            traceError,
		TxStatus:         txStatus,
		CallerIsContract: callerContract,
		CalleeIsContract: calleeContract,
		IsCCGEdge:        isCCG,
	}, nil
}

func cell(row []string, cols map[string]int, name string) string { return row[cols[name]] }

func optionalCell(row []string, cols map[string]int, name string) string {
	index, ok := cols[name]
	if !ok || index < 0 || index >= len(row) {
		return ""
	}
	return row[index]
}

func uintCell(row []string, cols map[string]int, name string) (uint64, error) {
	raw := strings.TrimSpace(cell(row, cols, name))
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q", name, raw)
	}
	return value, nil
}

func nullableUint(raw string) (uint64, bool, error) {
	raw = strings.TrimSpace(raw)
	if isNull(raw) {
		return 0, false, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

func boolCell(row []string, cols map[string]int, name string) (bool, error) {
	raw := strings.ToLower(strings.TrimSpace(cell(row, cols, name)))
	switch raw {
	case "1", "true":
		return true, nil
	case "0", "false":
		return false, nil
	default:
		return false, fmt.Errorf("invalid %s boolean %q", name, raw)
	}
}

func statusCell(row []string, cols map[string]int, name string) (int, error) {
	raw := strings.TrimSpace(cell(row, cols, name))
	if raw == "0" {
		return 0, nil
	}
	if raw == "1" {
		return 1, nil
	}
	return 0, fmt.Errorf("%s must be 0 or 1, got %q", name, raw)
}

func normalizeHash(raw string) (string, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if len(raw) != 66 || !strings.HasPrefix(raw, "0x") || !isHex(raw[2:]) {
		return "", fmt.Errorf("invalid transaction hash %q", raw)
	}
	return raw, nil
}

func normalizeAddress(raw string, optional bool) (string, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if isNull(raw) {
		if optional {
			return "", nil
		}
		return "", fmt.Errorf("address is empty")
	}
	raw = strings.TrimPrefix(raw, "0x")
	if len(raw) != 40 || !isHex(raw) {
		return "", fmt.Errorf("invalid address %q", raw)
	}
	return raw, nil
}

func isHex(raw string) bool {
	for _, ch := range raw {
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return false
		}
	}
	return true
}

func nullableText(raw string) string {
	if isNull(raw) {
		return ""
	}
	return strings.TrimSpace(raw)
}

func isNull(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "null", "none", "nan":
		return true
	default:
		return false
	}
}

func (r *Reader) Stats() Stats { return r.stats }

func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	var first error
	if r.callFile != nil {
		first = r.callFile.Close()
	}
	if r.txFile != nil {
		if err := r.txFile.Close(); first == nil {
			first = err
		}
	}
	return first
}
