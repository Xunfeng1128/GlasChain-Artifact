package message

import (
	"blockEmulator/core"
	"testing"
)

func TestPendingEarlyBlockDigestIsStageMultiset(t *testing.T) {
	a := &core.Transaction{Nonce: 123}
	b := &core.Transaction{Nonce: 27, Relayed: true, GLASStageEpoch: 3, GLASStageEpochSet: true}
	digest := PendingEarlyBlockDigest([]*core.Transaction{a, b})
	if digest != PendingEarlyBlockDigest([]*core.Transaction{b, a}) {
		t.Fatal("different grouping order changed digest")
	}
	modified := *b
	modified.GLASStageEpoch++
	if digest == PendingEarlyBlockDigest([]*core.Transaction{a, &modified}) ||
		digest == PendingEarlyBlockDigest([]*core.Transaction{a, b, b}) {
		t.Fatal("changed stage multiset had the same digest")
	}
}
