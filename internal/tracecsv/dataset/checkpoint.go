package dataset

import (
	"encoding/csv"
	"fmt"
	"io"

	"blockEmulator/internal/tracecsv/model"
)

// Checkpoint captures a CSV record boundary and the merge join's one-row call
// lookahead. It is intentionally opaque and small; transaction/call histories
// are not retained. Checkpoints belong to immutable copies of the same files.
type Checkpoint struct {
	txOffset, callOffset        int64
	nextCall                    *model.Call
	callEOF                     bool
	lastTxPos, lastCallPos      position
	haveLastTxPos, haveLastCall bool
	stats                       Stats
}

func (r *Reader) Checkpoint() Checkpoint {
	cp := Checkpoint{
		txOffset:   r.txOffsetBase + r.txCSV.InputOffset(),
		callOffset: r.callOffsetBase + r.callCSV.InputOffset(),
		callEOF:    r.callEOF,
		lastTxPos:  r.lastTxPos, lastCallPos: r.lastCallPos,
		haveLastTxPos: r.haveLastTxPos, haveLastCall: r.haveLastCall,
		stats: r.stats,
	}
	if r.nextCall != nil {
		copy := *r.nextCall
		cp.nextCall = &copy
	}
	return cp
}

// OpenAtCheckpoint resumes the canonical parser without scanning earlier rows.
// InputOffset (not the file descriptor offset) handles CSV read-ahead, quoted
// newlines and the already-read lookahead correctly.
func OpenAtCheckpoint(transactionPath, callPath string, cp Checkpoint) (*Reader, error) {
	r, err := Open(transactionPath, callPath)
	if err != nil {
		return nil, err
	}
	bad := func(err error) (*Reader, error) { r.Close(); return nil, err }
	if cp.txOffset < 0 || cp.callOffset < 0 {
		return bad(fmt.Errorf("negative dataset checkpoint offset"))
	}
	if _, err = r.txFile.Seek(cp.txOffset, io.SeekStart); err != nil {
		return bad(err)
	}
	if _, err = r.callFile.Seek(cp.callOffset, io.SeekStart); err != nil {
		return bad(err)
	}
	txFields, callFields := r.txCSV.FieldsPerRecord, r.callCSV.FieldsPerRecord
	r.txCSV, r.callCSV = csv.NewReader(r.txFile), csv.NewReader(r.callFile)
	r.txCSV.FieldsPerRecord, r.callCSV.FieldsPerRecord = txFields, callFields
	r.txOffsetBase, r.callOffsetBase = cp.txOffset, cp.callOffset
	r.nextCall = nil
	if cp.nextCall != nil {
		copy := *cp.nextCall
		r.nextCall = &copy
	}
	r.callEOF, r.lastTxPos, r.lastCallPos = cp.callEOF, cp.lastTxPos, cp.lastCallPos
	r.haveLastTxPos, r.haveLastCall, r.stats = cp.haveLastTxPos, cp.haveLastCall, cp.stats
	return r, nil
}
