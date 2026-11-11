package message

import (
	"blockEmulator/core"
	"bytes"
	"encoding/gob"
	"log"
)

var (
	AccountState_and_TX MessageType = "AccountState&txs"
	PartitionReq        RequestType = "PartitionReq"
	CPartitionMsg       MessageType = "PartitionModifiedMap"
	CPartitionReady     MessageType = "ready for partition"
	CPartitionPrepare   MessageType = "PartitionPrepare"
)

// GARETPlacement is committed with the account transfer, so routing and pool
// repricing observe one catalog and one complete group assignment.
type GARETPlacement struct {
	CatalogDigest string
	Epoch         uint64
	Placement     []uint64
}

type PartitionModifiedMap struct {
	Epoch             uint64 `json:",omitempty"`
	PartitionModified map[string]uint64
	GARET             *GARETPlacement         `json:",omitempty"`
	PlanningWindow    *PendingExecutionWindow `json:",omitempty"`
}

// PartitionPrepare 是 supervisor 在算 CLPA / 发大 map 之前，先广播给各 shard
// 的轻量预告消息。
//   - Epoch: 这是 supervisor 即将进入的 partition 轮次（== 上一轮 AccountTransferRound+1）
//   - TargetSeq: supervisor 指定的"全局对齐目标块高"。所有 shard 在本地 sequenceID
//     推进到 TargetSeq 才进入 partition；在那之前继续出普通块。
//     supervisor 取 max(每个 shard 上报的最高 BlockHeight) + Buffer。
type PartitionPrepare struct {
	Epoch     uint64
	TargetSeq uint64
	// Snapshot requests a read-only pending-pool view at the CLPA pause point.
	// Only the opt-in GLAS_Pending worker adapter honors this field.
	Snapshot bool `json:",omitempty"`
}

type AccountTransferMsg struct {
	TargetSeq      uint64 `json:",omitempty"`
	ModifiedMap    map[string]uint64
	Addrs          []string
	AccountState   []*core.AccountState
	ATid           uint64
	GARET          *GARETPlacement         `json:",omitempty"`
	PlanningWindow *PendingExecutionWindow `json:",omitempty"`
}

type PartitionReady struct {
	// Enabled CLPA control paths wait for the last ordinary-block Relay, since
	// partition commits advance sequenceID without emitting a Relay message.
	RelaySeq  uint64 `json:",omitempty"`
	Epoch     uint64 `json:",omitempty"`
	FromShard uint64
	NowSeqID  uint64
}

// this message used in inter-shard, it will be sent between leaders.
type AccountStateAndTx struct {
	Epoch        uint64 `json:",omitempty"`
	Addrs        []string
	AccountState []*core.AccountState
	Txs          []*core.Transaction
	FromShard    uint64
	GARET        *GARETPlacement    `json:",omitempty"`
	PendingMarks *core.PendingMarks `json:",omitempty"`
}

func (atm *AccountTransferMsg) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(atm)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeAccountTransferMsg(content []byte) *AccountTransferMsg {
	var atm AccountTransferMsg

	decoder := gob.NewDecoder(bytes.NewReader(content))
	err := decoder.Decode(&atm)
	if err != nil {
		log.Panic(err)
	}

	return &atm
}
