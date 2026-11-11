// Package model defines the simulator-independent records used by
// GLAS-FREIGHT. Unlike JiangZiya's contract-only model, every EOA and smart
// contract that appears in a transaction or call trace is a placement vertex.
package model

import "fmt"

type VertexKind uint8

const (
	EOA VertexKind = iota
	SmartContract
)

func (kind VertexKind) String() string {
	if kind == SmartContract {
		return "sca"
	}
	return "eoa"
}

type EdgeKind uint8

const (
	TopLevelTransaction EdgeKind = iota
	InternalContractCall
)

func (kind EdgeKind) String() string {
	if kind == InternalContractCall {
		return "sca_sca_call"
	}
	return "top_level_tx"
}

type Call struct {
	Caller      string
	Callee      string
	GasUsed     uint64
	HasGasUsed  bool
	IsCCGEdge   bool
	CallerIsSCA bool
	CalleeIsSCA bool
}

type Transaction struct {
	AttachedCallModel bool
	CallGasPerCall    uint64
	BlockNumber       uint64
	TransactionIndex  uint64
	SourceHash        string
	From              string
	To                string
	Value             string
	GasLimit          uint64
	HasGasLimit       bool
	GasUsed           uint64
	HasGasUsed        bool
	FromIsContract    bool
	ToIsContract      bool
	Category          string
	Calls             []Call
}

// PendingTxObservation is the pre-execution projection exposed to the
// placement graph before a transaction is formally executed. GasUsed is the
// pre-executed execution cost for the frozen candidate transaction; internal
// call traces remain hidden until they are explicitly modeled as another
// pre-execution output.
type PendingTxObservation struct {
	SourceHash     string
	From           string
	To             string
	GasUsed        float64
	FromIsContract bool
	ToIsContract   bool
	InjectionSeq   uint64
	QueueShard     uint64
}

type Placement map[string]uint64

type EdgeKey struct {
	From string
	To   string
	Kind EdgeKind
}

func (key EdgeKey) String() string {
	return fmt.Sprintf("%s:%s->%s", key.Kind.String(), key.From, key.To)
}

type VertexStats struct {
	AttachedCallBudget float64
	Kind               VertexKind
	IncomingTxCount    float64
	IncomingTxGas      float64
	OutgoingTxCount    float64
	OutgoingTxGas      float64
	IncomingCallCount  float64
	IncomingCallGas    float64
	// Attributed call gas apportions the parent transaction gas over its
	// internal calls. Unlike nested trace gas, it does not recursively double
	// count the same top-level execution budget.
	IncomingCallAttributedGas float64
	OutgoingCallCount         float64
	OutgoingCallGas           float64
	OutgoingCallAttributedGas float64
}

// HotGas is the service demand anchored to this account. Incoming top-level
// gas exposes popular recipients/entry contracts; attributed incoming-call
// gas exposes contracts repeatedly reached from other contracts without
// recursively double-counting nested trace gas. Exact candidate loads are
// still recomputed from weighted transaction observations.
func (stats VertexStats) HotGas() float64 {
	return stats.IncomingTxGas + stats.IncomingCallAttributedGas + stats.AttachedCallBudget
}

func (stats VertexStats) ServiceGas(rhoCall float64) float64 {
	return stats.IncomingTxGas + stats.AttachedCallBudget + rhoCall*stats.IncomingCallAttributedGas
}

type EdgeStats struct {
	Count          float64
	Gas            float64
	AttributedGas  float64
	CallGasSamples float64
	// LogicalAffinity apportions one logical transaction across its top-level
	// edge and distinct internal dependency edges. It is the additive proxy
	// used to seed a low union cross-transaction ratio; exact candidate ranking
	// still scores each transaction and applies the true union rule.
	LogicalAffinity float64
}

// AffinityGas is the amount of cross-shard work avoided when an edge is
// co-located. For internal calls, AttributedGas apportions the parent
// transaction gas over its calls and therefore shares the same unit as block
// gas; Gas retains trace call-gas for auditing.
func (stats EdgeStats) AffinityGas(kind EdgeKind, rhoCall float64) float64 {
	if kind == InternalContractCall {
		return rhoCall * stats.AttributedGas
	}
	return stats.Gas
}

func (stats EdgeStats) AffinityTransactions() float64 {
	return stats.LogicalAffinity
}

type TxObservation struct {
	AttachedCallModel bool
	CallGasPerCall    uint64
	SourceHash        string
	From              string
	To                string
	GasUsed           float64
	Weight            float64
	CallEdges         []EdgeKey
}

type StageKind uint8

const (
	Relay2Stage StageKind = iota
	CallTaskStage
)

// StageObservation represents execution work that has already started but is
// still queued when an epoch closes. It contributes load to the next map, but
// it is not another logical transaction and therefore never increments TPS or
// the cross-transaction denominator.
type StageObservation struct {
	AttachedCallModel bool
	CallGasPerCall    uint64
	CallEdges         []EdgeKey
	SourceHash        string
	Owner             string
	GasUsed           float64
	Weight            float64
	Kind              StageKind
}

type Move struct {
	Address string
	Kind    VertexKind
	From    uint64
	To      uint64
}
