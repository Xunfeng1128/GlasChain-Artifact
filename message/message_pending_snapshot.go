package message

import "blockEmulator/core"

const (
	CPendingSnapshot  MessageType = "pendingPoolSnapshot"
	CExecutionForward MessageType = "executionForward"
)

// PendingSnapshot is a deep, read-only copy of one paused shard's current
// pending queue. It is not a promise that all in-flight network records have
// arrived. Epoch and TargetSeq bind the view to one partition preparation.
type PendingSnapshot struct {
	Epoch              uint64
	TargetSeq          uint64
	FromShard          uint64
	Txs                []*core.Transaction
	Error              string `json:",omitempty"`
	LocalWindow        bool   `json:",omitempty"`
	TotalRecords       int    `json:",omitempty"`
	TotalCallRelations int    `json:",omitempty"`
}

// ExecutionForward carries existing execution stages to their current owner.
// It never creates a new Relay stage or changes a source injection timestamp.
// A recipient with an older installed map buffers it until Epoch commits.
type ExecutionForward struct {
	Epoch        uint64
	FromShard    uint64
	Txs          []*core.Transaction
	PendingMarks *core.PendingMarks `json:",omitempty"`
}
