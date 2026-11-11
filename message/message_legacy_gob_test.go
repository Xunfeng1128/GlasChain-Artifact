package message

import (
	"blockEmulator/core"
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

func legacyTransferFixture(i int) *AccountTransferMsg {
	m := &AccountTransferMsg{
		TargetSeq: 7, ModifiedMap: map[string]uint64{"account-a": 1},
		Addrs: []string{"account-a"}, ATid: 1,
		AccountState: []*core.AccountState{{AcAddress: "account-a", Nonce: 5,
			Balance: big.NewInt(1234567), StorageRoot: []byte{1, 2, 3}, CodeHash: []byte{4, 5}}},
	}
	if i >= 1 {
		m.PlanningWindow = &PendingExecutionWindow{Epoch: 1}
	}
	if i == 2 {
		m.PlanningWindow.Sources = []string{}
	}
	if i >= 3 {
		m.PlanningWindow.Sources = []string{"tx-a", "tx-b"}
	}
	if i == 4 {
		m.GARET = &GARETPlacement{CatalogDigest: "catalog", Epoch: 1, Placement: []uint64{1, 0}}
	}
	return m
}

// Gob type numbers are process-wide. Run only this helper in a fresh process,
// matching the baseline generator, rather than depending on other test order.
func TestLegacyAccountTransferGobHelper(t *testing.T) {
	if os.Getenv("GLAS_TEST_LEGACY_GOB_HELPER") != "1" {
		return
	}
	for i := 0; i < 5; i++ {
		raw := legacyTransferFixture(i).Encode()
		fmt.Printf("%d %d %s\n", i, len(raw), hex.EncodeToString(raw))
	}
	os.Exit(0)
}

func TestLegacyAccountTransferGobBytesMatchBaseline(t *testing.T) {
	// Golden bytes were generated from message_CLPA.go and
	// message_pending_window.go extracted directly from commit e87a012f.
	// Single-entry maps avoid Gob's pre-existing random map iteration order.
	want, err := os.ReadFile("testdata/account_transfer_e87a012f.gob.txt")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLegacyAccountTransferGobHelper$")
	cmd.Env = append(os.Environ(), "GLAS_TEST_LEGACY_GOB_HELPER=1")
	got, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("legacy wire helper: %v\n%s", err, got)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("legacy Gob bytes differ from e87a012f\nwant:\n%s\ngot:\n%s", want, got)
	}
	for i, line := range strings.Split(strings.TrimSpace(string(want)), "\n") {
		fields := strings.Fields(line)
		raw, err := hex.DecodeString(fields[2])
		if err != nil {
			t.Fatal(err)
		}
		decoded, expected := DecodeAccountTransferMsg(raw), legacyTransferFixture(i)
		if expected.PlanningWindow != nil && len(expected.PlanningWindow.Sources) == 0 {
			expected.PlanningWindow.Sources = nil // Gob's existing empty-slice normalization.
		}
		if !reflect.DeepEqual(decoded, expected) {
			t.Fatalf("cannot decode baseline Gob fixture %d", i)
		}
	}
}
