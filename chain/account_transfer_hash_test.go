package chain

import (
	"bytes"
	"testing"

	"blockEmulator/core"
)

func TestMigrationBlockHashIsDeterministic(t *testing.T) {
	headerA := &core.BlockHeader{
		ParentBlockHash: []byte("same-parent"),
		StateRoot:       []byte("same-state-root"),
		TxRoot:          []byte("empty-tx-root"),
		Number:          17,
	}
	headerB := &core.BlockHeader{
		ParentBlockHash: append([]byte(nil), headerA.ParentBlockHash...),
		StateRoot:       append([]byte(nil), headerA.StateRoot...),
		TxRoot:          append([]byte(nil), headerA.TxRoot...),
		Number:          headerA.Number,
	}
	context := []byte("epoch-and-map-digest")

	hashA := migrationBlockHash(headerA, 3, context)
	hashB := migrationBlockHash(headerB, 3, append([]byte(nil), context...))
	if !bytes.Equal(hashA, hashB) {
		t.Fatalf("identical migration blocks produced different hashes: %x != %x", hashA, hashB)
	}
}

func TestMigrationBlockHashBindsConsensusContext(t *testing.T) {
	header := &core.BlockHeader{
		ParentBlockHash: []byte("parent"),
		StateRoot:       []byte("state-root"),
		TxRoot:          []byte("tx-root"),
		Number:          8,
	}
	baseline := migrationBlockHash(header, 1, []byte("epoch-1-map-a"))

	tests := []struct {
		name    string
		header  *core.BlockHeader
		shardID uint64
		context []byte
	}{
		{name: "different shard", header: header, shardID: 2, context: []byte("epoch-1-map-a")},
		{name: "different map epoch", header: header, shardID: 1, context: []byte("epoch-2-map-a")},
		{name: "different map digest", header: header, shardID: 1, context: []byte("epoch-1-map-b")},
		{name: "different state root", header: &core.BlockHeader{ParentBlockHash: []byte("parent"), StateRoot: []byte("other-state-root"), TxRoot: []byte("tx-root"), Number: 8}, shardID: 1, context: []byte("epoch-1-map-a")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := migrationBlockHash(test.header, test.shardID, test.context)
			if bytes.Equal(got, baseline) {
				t.Fatalf("changed migration identity reused hash %x", got)
			}
		})
	}
}
