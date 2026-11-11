package message

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Installation receipts are observations of an already committed CLPA migration,
// not an extra consensus phase. Queries only replay the saved receipt.
const (
	CPartitionInstalled    MessageType = "PartitionInstalled"
	CPartitionInstallQuery MessageType = "PartitionInstallQuery"
)

type PartitionInstalled struct {
	Epoch         uint64
	FromShard     uint64
	TargetSeq     uint64
	CommittedSeq  uint64
	MappingDigest string
}

type PartitionInstallQuery struct{ Epoch uint64 }

// encoding/json sorts string map keys. Normalize nil and an empty delta map to
// the same value because both mean that no account placement changes this round.
func PartitionMappingDigest(mapping map[string]uint64) string {
	if mapping == nil {
		mapping = map[string]uint64{}
	}
	payload, err := json.Marshal(mapping)
	if err != nil {
		panic(err)
	} // string -> uint64 is always JSON encodable.
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

// PartitionPlanDigest binds installation to the complete routing decision.
// GARET may change an unobserved account's group without any address delta.
// Preserve the existing wire identity for every non-GARET mode.
func PartitionPlanDigest(mapping map[string]uint64, groups *GARETPlacement) string {
	if groups == nil {
		return PartitionMappingDigest(mapping)
	}
	if mapping == nil {
		mapping = map[string]uint64{}
	}
	payload, err := json.Marshal(struct {
		Map   map[string]uint64
		GARET *GARETPlacement
	}{mapping, groups})
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}
