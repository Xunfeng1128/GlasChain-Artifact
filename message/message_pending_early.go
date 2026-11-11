package message

import (
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
)

const (
	CPendingEarlyReport   MessageType = "PendingEarlyReport"
	CPendingEarlyPlan     MessageType = "PendingEarlyPlan"
	CPendingEarlyPlanAck  MessageType = "PendingEarlyPlanAck"
	CPendingEarlyActivate MessageType = "PendingEarlyActivate"
)

type PendingEarlyReport struct {
	Epoch             uint64
	TargetSeq         uint64
	FromShard         uint64
	Leader            uint64
	View              uint64
	ObservedBlockHash string
	LastBlockDigest   string
	FullRecords       int
	FullCallRelations int
	FullWindow        []*core.Transaction `json:",omitempty"`
	ForecastCrossTOP  []*core.Transaction `json:",omitempty"`
}

// PendingEarlyPlan stages a complete plan without authorizing migration.
type PendingEarlyPlan struct {
	TargetSeq uint64
	Plan      PartitionModifiedMap
	Digest    string
}

type PendingEarlyPlanAck struct {
	Epoch     uint64
	TargetSeq uint64
	FromShard uint64
	Leader    uint64
	View      uint64
	Digest    string
}

type PendingEarlyActivate struct {
	Epoch     uint64
	TargetSeq uint64
	Digest    string
}

// PendingEarlyBlockDigest binds the consumed execution-stage multiset. Block
// information groups LOCAL, RELAY1 and RELAY2, so their original packing order
// is deliberately not required by the supervisor's last-block check.
func PendingEarlyBlockDigest(txs []*core.Transaction) string {
	stages := make([][17]byte, len(txs))
	for i, tx := range txs {
		record := &stages[i]
		if tx == nil {
			record[8] = 0x80
			continue
		}
		binary.BigEndian.PutUint64(record[:8], tx.Nonce)
		if tx.Relayed {
			record[8] |= 1
		}
		if tx.GLASStageEpochSet {
			record[8] |= 2
		}
		binary.BigEndian.PutUint64(record[9:], tx.GLASStageEpoch)
	}
	sort.Slice(stages, func(i, j int) bool {
		for k := range stages[i] {
			if stages[i][k] != stages[j][k] {
				return stages[i][k] < stages[j][k]
			}
		}
		return false
	})
	hash := sha256.New()
	for i := range stages {
		_, _ = hash.Write(stages[i][:])
	}
	return hex.EncodeToString(hash.Sum(nil))
}
