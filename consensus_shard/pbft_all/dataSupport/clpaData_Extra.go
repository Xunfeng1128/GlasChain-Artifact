package dataSupport

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"sync"
)

type Data_supportCLPA struct {
	// PartitionStateLock protects the CLPA prepare/map/commit control fields.
	PartitionStateLock sync.Mutex
	// Immutable protocol capability: CLPA, CLPA_Gas and GlasChain share
	// epoch/Relay validation and installation receipts in both schedulers.
	ControlEventsEnabled bool
	// ControlPolling changes only scheduling; it never disables the protocol.
	ControlPolling         bool
	ControlEpoch           uint64
	ControlReadySent       bool
	ControlMigrationSent   bool
	LatestPartitionInstall *message.PartitionInstalled
	ControlRequest         *message.Request
	LastNormalCommittedSeq uint64
	// PendingSnapshotMode is a worker capability, enabled only for GLAS_Pending.
	PendingSnapshotMode      bool
	PendingSnapshotRequested bool
	PendingSnapshotPaused    bool
	// Protected by PartitionStateLock. Staging alone never sets PartitionOn.
	PendingEarlyPlan                *message.PendingEarlyPlan
	PendingEarlyPlanView            uint64
	PendingEarlyActivationRequested bool
	// Protected by PartitionStateLock; activated only by the existing partition commit.
	// Next sequence observed through committed blocks, protected by
	// PartitionStateLock so the view-change monitor never races sequenceID.
	PendingSnapshotObservedSeq uint64
	// Future forwards wait for their sender's committed routing epoch. They do
	// not participate in Relay sequence acknowledgements.
	FutureExecutionForwards []*message.ExecutionForward
	GARETLock               sync.Mutex
	GARETPending            *message.GARETPlacement
	ModifiedMap             []map[string]uint64                   // record the modified map from the decider(s)
	AccountTransferRound    uint64                                // denote how many times accountTransfer do
	ReceivedNewAccountState map[string]*core.AccountState         // the new accountState From other Shards
	ReceivedNewTx           []*core.Transaction                   // new transactions from other shards' pool
	AccountStateTx          map[uint64]*message.AccountStateAndTx // the map of accountState and transactions, pool
	PartitionOn             bool                                  // judge nextEpoch is partition or not

	// PendingEpoch 是 supervisor 通过 CPartitionPrepare 预告的"下一轮 partition epoch"。
	// 单调递增：只在 handlePartitionPrepare 中写入，HandleinPropose 中读取。
	PendingEpoch uint64

	// PendingTargetSeq 是 supervisor 钦定的"对齐目标块高"。propose 入口的判定：
	//   sequenceID < PendingTargetSeq                  → 继续出普通块（不 barrier）
	//   sequenceID >= PendingTargetSeq && !PartitionOn → barrier 等大 map
	//   sequenceID >= PendingTargetSeq && PartitionOn  → 走 partition 流程
	// 取代了"下一次 propose 即 barrier"的最小修复语义，让所有 shard 在同一 seq 出 partition。
	PendingTargetSeq uint64

	PartitionReady map[uint64]bool // judge whether all shards has done all txs
	P_ReadyLock    sync.Mutex      // lock for ready

	ReadyRelaySeq map[uint64]uint64 // last ordinary relay required by each event-mode Ready
	ReadySeq      map[uint64]uint64 // record the seqid when the shard is ready
	ReadySeqLock  sync.Mutex        // lock for seqMap

	CollectOver bool       // judge whether all txs is collected or not
	CollectLock sync.Mutex // lock for collect
}

func NewCLPADataSupport() *Data_supportCLPA {
	return &Data_supportCLPA{
		ModifiedMap:             make([]map[string]uint64, 0),
		AccountTransferRound:    0,
		ReceivedNewAccountState: make(map[string]*core.AccountState),
		ReceivedNewTx:           make([]*core.Transaction, 0),
		AccountStateTx:          make(map[uint64]*message.AccountStateAndTx),
		PartitionOn:             false,
		PartitionReady:          make(map[uint64]bool),
		CollectOver:             false,
		ReadySeq:                make(map[uint64]uint64),
		ReadyRelaySeq:           make(map[uint64]uint64),
	}
}
