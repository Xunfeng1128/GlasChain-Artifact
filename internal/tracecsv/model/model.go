// Package model contains the simulator-independent records used by the
// online, causal JiangZiya placement reproduction.
package model

import "fmt"

// Transaction is one canonical Ethereum top-level transaction and the small
// subset of receipt metadata needed by the BlockEmulator adapter.
type Transaction struct {
	BlockNumber      uint64
	TransactionIndex uint64
	SourceHash       string
	From             string
	To               string
	ToCreate         string
	Value            string
	GasLimit         uint64
	HasGasLimit      bool
	GasUsed          uint64
	HasGasUsed       bool
	ReceiptStatus    int
	FromIsContract   bool
	ToIsContract     bool
	Category         string
	Calls            []Call
}

// Usable reports whether BlockEmulator can replay this transaction without
// inventing creation or failure semantics. Failed and creation transactions
// remain visible to dataset statistics but are not injected.
func (t Transaction) Usable() bool {
	return t.ReceiptStatus == 1 && t.From != "" && t.To != "" && t.ToCreate == "" && t.From != t.To
}

// Call is one canonical trace call. Only IsCCGEdge contract-to-contract calls
// become CCG edges. A successful top-level EOA -> contract invocation is
// counted separately on the entry contract vertex.
type Call struct {
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	TraceAddress     string
	Depth            int
	Caller           string
	Callee           string
	CallType         string
	CallGasUsed      uint64
	HasCallGas       bool
	Error            string
	TxStatus         int
	CallerIsContract bool
	CalleeIsContract bool
	IsCCGEdge        bool
}

// EdgeKey identifies a directed caller -> callee contract dependency.
type EdgeKey struct {
	Caller string
	Callee string
}

func (k EdgeKey) String() string { return fmt.Sprintf("%s->%s", k.Caller, k.Callee) }

// EdgeStats aggregates one directed dependency in a completed-workload
// window. Q is q_ij and MeanGas is c_ij.
type EdgeStats struct {
	Q               uint64
	GasSum          uint64
	GasObservations uint64
}

func (e EdgeStats) MeanGas() (float64, bool) {
	if e.GasObservations == 0 {
		return 0, false
	}
	return float64(e.GasSum) / float64(e.GasObservations), true
}

// NodeStats keeps the invocation counts used to audit the CCG. W_v is how many
// times execution entered this contract during the completed workload window:
// one count for every top-level invocation plus one for every eligible incoming
// contract-to-contract call. OutgoingInvocations is retained to validate the
// directed q_ij edge totals and to audit cross-call attribution.
type NodeStats struct {
	TopLevelInvocations uint64
	IncomingInvocations uint64
	OutgoingInvocations uint64
}

// VertexWeight returns W_v, the total number of times this contract was
// invoked. Its placement-dependent decomposition is:
//
//	W_v = local incoming invocations + cross-shard incoming invocations.
func (n NodeStats) VertexWeight() uint64 {
	return n.TopLevelInvocations + n.IncomingInvocations
}

// Placement maps contract addresses (lower-case hex without 0x) to shards.
type Placement map[string]uint64

// Move is one contract-only placement change.
type Move struct {
	Contract string
	From     uint64
	To       uint64
}

// TxObservation preserves just enough per-top-level-transaction information
// to recompute Contract-CSTx ratios for a candidate placement.
type TxObservation struct {
	SourceHash string
	From       string
	To         string
	Category   string
	CallEdges  []EdgeKey
}
