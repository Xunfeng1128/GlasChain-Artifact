// Package planner selects account placements from a frozen GlasChain planning window.
package planner

import (
	"fmt"

	"blockEmulator/glasfreight/model"
	"blockEmulator/glasfreight/optimizer"
)

// StageKind identifies the physical execution stage represented by one pool
// record. A logical transaction can therefore have several distinct records.
type StageKind uint8

const (
	TopLevelStage StageKind = iota
	Relay2Stage
	CallTaskStage
)

func (kind StageKind) String() string {
	switch kind {
	case TopLevelStage:
		return "top"
	case Relay2Stage:
		return "relay2"
	case CallTaskStage:
		return "call_task"
	default:
		return fmt.Sprintf("unknown(%d)", kind)
	}
}

// FrozenRecord is the planner's immutable representation of a physical pool
// record. ID must identify the execution stage, not merely the logical source
// transaction. QueueIndex preserves the order in the original shard; ArrivalSeq
// gives callers a global ordering key when records from several shards migrate
// into one projected queue.
//
// Calls is pre-execution metadata for a logical source. It is used by the
// frozen-workload graph builder; actual future Relay2/CallTask records are produced by
// ExpandFunc so their timing exactly follows the runtime protocol.
type FrozenRecord struct {
	ID              string
	ExecutionDigest string
	SourceHash      string
	Stage           StageKind
	From            string
	To              string
	Caller          string
	Callee          string
	Owner           string
	GasUsed         uint64
	BaseGasUsed     uint64
	CallGasPerCall  uint64
	AttachedCallGas uint64
	StageEpoch      uint64
	StageEpochSet   bool
	FromIsContract  bool
	ToIsContract    bool
	Calls           []model.Call
	Category        string
	DependencyKey   string
	Priority        int
	ArrivalSeq      uint64
	OriginShard     uint64
	QueueIndex      uint64
	Affinity        float64
}

// PlacementOwner returns the account whose placement owns this physical
// execution stage.
func (record FrozenRecord) PlacementOwner() string {
	if record.Owner != "" {
		return record.Owner
	}
	switch record.Stage {
	case TopLevelStage:
		return record.From
	case Relay2Stage:
		return record.To
	case CallTaskStage:
		if record.Callee != "" {
			return record.Callee
		}
		return record.To
	default:
		return ""
	}
}

func (record FrozenRecord) clone() FrozenRecord {
	record.Calls = append([]model.Call(nil), record.Calls...)
	return record
}

func (record FrozenRecord) validate() error {
	if record.ID == "" {
		return fmt.Errorf("execution record ID is required")
	}
	if record.GasUsed == 0 {
		return fmt.Errorf("execution record %s has zero gasUsed", record.ID)
	}
	if record.Priority < 0 {
		return fmt.Errorf("execution record %s has negative priority", record.ID)
	}
	if record.PlacementOwner() == "" {
		return fmt.Errorf("execution record %s has no placement owner", record.ID)
	}
	return nil
}

// FrozenPools holds the immutable records selected for planning.
type FrozenPools struct {
	ShardCount uint64
	Records    map[uint64][]FrozenRecord
	Digest     string
}

// Clone makes a deep copy of the records and call traces.
func (pools FrozenPools) Clone() FrozenPools {
	clone := FrozenPools{
		ShardCount: pools.ShardCount,
		Digest:     pools.Digest,
		Records:    make(map[uint64][]FrozenRecord, len(pools.Records)),
	}
	for shard, records := range pools.Records {
		copied := make([]FrozenRecord, len(records))
		for index, record := range records {
			copied[index] = record.clone()
		}
		clone.Records[shard] = copied
	}
	return clone
}

// PlacementResolver supplies a deterministic initial shard for an address not
// explicitly present in a sparse placement map (normally the system hash map).
type PlacementResolver func(address string) (uint64, error)

// ExpandFunc returns the physical descendants of a record under a placement.
type ExpandFunc func(record FrozenRecord, placement model.Placement) ([]FrozenRecord, error)

// PrepareFunc reprices a still-uncommitted stage for the candidate map. It
// must preserve stage identity, owner and dependencies and must be idempotent.
type PrepareFunc func(record FrozenRecord, placement model.Placement) (FrozenRecord, error)

type SimulationConfig struct {
	ShardCount     uint64
	BlocksPerEpoch uint64
	BlockGasLimit  uint64
	Expand         ExpandFunc
	Prepare        PrepareFunc
	ResolveShard   PlacementResolver
}

// Config controls the demand-only cost-FENNEL search.
type Config struct {
	PlannerMode      string
	SelectionMode    string
	Epoch            uint64
	ShardCount       uint64
	BlocksPerEpoch   uint64
	BlockGasLimit    uint64
	Iterations       int
	EvaluationBudget int
	// Nil uses the trial default 0.5; a non-nil zero is H-only.
	ObjectiveWeight *float64
	// AttachedCallObjective declares that Prepare and Expand implement the
	// attached-call TOP/RELAY2 model.
	AttachedCallObjective bool
	Expand                ExpandFunc
	Prepare               PrepareFunc
	ResolveShard          PlacementResolver
	OptimizerConfig       optimizer.Config
}

// ExecutionEvaluation records a candidate and its score.
type ExecutionEvaluation struct {
	Index           int
	Iteration       int
	PlacementDigest string
	Score           optimizer.ExecutionScore
	Improved        bool
	DurationMS      int64
}
