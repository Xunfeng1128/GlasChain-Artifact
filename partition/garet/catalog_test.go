package garet

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func address(number int) string { return fmt.Sprintf("%040x", number) }

func TestCatalogEqualGroupsAndIntervals(t *testing.T) {
	catalog, err := NewCatalog([]string{address(90), address(30), address(10), "0X" + strings.ToUpper(address(10)), address(70), address(50)}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.AddressCount() != 5 || catalog.GroupCount() != 2 || catalog.data.Groups[0].Count != 3 || catalog.data.Groups[1].Count != 2 {
		t.Fatalf("unexpected catalog: %+v", catalog.data)
	}
	for _, test := range []struct{ number, group int }{{0, 0}, {10, 0}, {49, 0}, {50, 0}, {51, 1}, {90, 1}, {100, 1}} {
		group, err := catalog.GroupFor("0x" + address(test.number))
		if err != nil || group != test.group {
			t.Fatalf("address %d: group=%d err=%v", test.number, group, err)
		}
	}
	if !reflect.DeepEqual(catalog.InitialPlacement(2), []uint64{0, 1}) {
		t.Fatal("wrong initial mapping")
	}
	if catalog.InitialPlacement(0) != nil {
		t.Fatal("zero shards accepted")
	}
	if _, err := catalog.GroupFor("bad"); err == nil {
		t.Fatal("malformed address accepted")
	}
}

func TestCatalogRoundTripAndIdentityOnlyDigest(t *testing.T) {
	first, err := NewCatalog([]string{address(1), address(2), address(3), address(4)}, 2)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCatalog([]string{address(4), address(1), address(1), address(3), "0x" + address(2)}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() != second.Digest() {
		t.Fatal("ordering or frequency changed catalog")
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveCatalog(path, first); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadCatalog(path)
	if err != nil || loaded.Digest() != first.Digest() {
		t.Fatalf("round-trip: %v", err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), address(1)) || strings.Contains(string(encoded), address(3)) {
		t.Fatal("catalog retained non-boundary identities")
	}
}

func TestCatalogRejectsInvalidInputs(t *testing.T) {
	for _, input := range []string{"", "0x1", strings.Repeat("g", 40), strings.Repeat("a", 41)} {
		if _, err := NormalizeAddress(input); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
	if _, err := NewCatalog([]string{address(1)}, 2); err == nil {
		t.Fatal("accepted empty groups")
	}
	if _, err := NewCatalog([]string{address(1)}, 0); err == nil {
		t.Fatal("accepted zero groups")
	}
	for _, body := range []string{
		`{"schema_version":2,"address_count":1,"groups":[{"upper_bound":"` + address(1) + `","count":1}]}`,
		`{"schema_version":1,"address_count":1,"groups":[{"upper_bound":"` + address(1) + `","count":2}]}`,
		`{"schema_version":1,"address_count":2,"groups":[{"upper_bound":"` + address(2) + `","count":1},{"upper_bound":"` + address(1) + `","count":1}]}`,
		`{"schema_version":1,"address_count":1,"groups":[{"upper_bound":"` + address(1) + `","count":1}],"unexpected":true}`,
		`{"schema_version":1,"address_count":0,"groups":[{"upper_bound":"` + address(1) + `","count":18446744073709551615},{"upper_bound":"` + address(2) + `","count":1}]}`,
	} {
		path := filepath.Join(t.TempDir(), "invalid.json")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadCatalog(path); err == nil {
			t.Fatalf("accepted invalid catalog %s", body)
		}
	}
}
