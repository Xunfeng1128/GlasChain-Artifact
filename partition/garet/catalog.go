// Package garet implements fixed account groups and the gas prediction and
// full-shuffling heuristic in GARET. It does not construct a transaction graph.
package garet

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const DefaultGroups = 100

type catalogGroup struct {
	UpperBound string `json:"upper_bound"`
	Count      uint64 `json:"count"`
}

type catalogData struct {
	SchemaVersion int            `json:"schema_version"`
	AddressCount  uint64         `json:"address_count"`
	Groups        []catalogGroup `json:"groups"`
}

// Catalog stores only the fixed address interval boundaries and sample counts.
// Newly encountered addresses use the same intervals without changing groups.
type Catalog struct{ data catalogData }

// NormalizeAddress accepts 40 hexadecimal digits with an optional 0x prefix.
func NormalizeAddress(address string) (string, error) {
	address = strings.TrimSpace(address)
	if strings.HasPrefix(address, "0x") || strings.HasPrefix(address, "0X") {
		address = address[2:]
	}
	if len(address) != 40 {
		return "", fmt.Errorf("address must have 40 hexadecimal digits: %q", address)
	}
	if _, err := hex.DecodeString(address); err != nil {
		return "", fmt.Errorf("invalid address %q: %w", address, err)
	}
	return strings.ToLower(address), nil
}

// NewCatalog sorts and deduplicates identities, then makes contiguous groups
// whose populations differ by at most one. Extra identities go to earlier groups.
// Address frequency and Gas are deliberately absent from this API.
func NewCatalog(addresses []string, groups int) (*Catalog, error) {
	if groups <= 0 {
		return nil, fmt.Errorf("group count must be positive")
	}
	unique := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		normalized, err := NormalizeAddress(address)
		if err != nil {
			return nil, err
		}
		unique[normalized] = struct{}{}
	}
	if len(unique) < groups {
		return nil, fmt.Errorf("%d distinct addresses cannot form %d nonempty groups", len(unique), groups)
	}
	ordered := make([]string, 0, len(unique))
	for address := range unique {
		ordered = append(ordered, address)
	}
	sort.Strings(ordered)
	catalog := &Catalog{data: catalogData{SchemaVersion: 1, AddressCount: uint64(len(ordered)), Groups: make([]catalogGroup, groups)}}
	position, base, remainder := 0, len(ordered)/groups, len(ordered)%groups
	for group := range catalog.data.Groups {
		count := base
		if group < remainder {
			count++
		}
		position += count
		catalog.data.Groups[group] = catalogGroup{UpperBound: ordered[position-1], Count: uint64(count)}
	}
	return catalog, nil
}

func (catalog *Catalog) GroupCount() int      { return len(catalog.data.Groups) }
func (catalog *Catalog) AddressCount() uint64 { return catalog.data.AddressCount }

func (catalog *Catalog) GroupFor(address string) (int, error) {
	if catalog == nil || len(catalog.data.Groups) == 0 {
		return 0, fmt.Errorf("empty GARET catalog")
	}
	normalized, err := NormalizeAddress(address)
	if err != nil {
		return 0, err
	}
	group := sort.Search(len(catalog.data.Groups), func(i int) bool { return catalog.data.Groups[i].UpperBound >= normalized })
	if group == len(catalog.data.Groups) {
		group--
	}
	return group, nil
}

// InitialPlacement distributes groups round-robin; counts differ by at most one
// when the configured group count is not divisible by the shard count.
func (catalog *Catalog) InitialPlacement(shards int) []uint64 {
	if catalog == nil || shards <= 0 {
		return nil
	}
	placement := make([]uint64, catalog.GroupCount())
	for group := range placement {
		placement[group] = uint64(group % shards)
	}
	return placement
}

// Digest identifies canonical catalog content, independent of file formatting.
func (catalog *Catalog) Digest() string {
	encoded, _ := json.Marshal(catalog.data)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func (catalog *Catalog) validate() error {
	if catalog == nil || catalog.data.SchemaVersion != 1 || len(catalog.data.Groups) == 0 {
		return fmt.Errorf("invalid GARET catalog schema or empty groups")
	}
	var total uint64
	minimum, maximum := uint64(math.MaxUint64), uint64(0)
	previous := ""
	for group, entry := range catalog.data.Groups {
		normalized, err := NormalizeAddress(entry.UpperBound)
		if err != nil || normalized != entry.UpperBound || normalized <= previous {
			return fmt.Errorf("group %d has invalid or unordered canonical upper boundary", group)
		}
		if entry.Count == 0 || math.MaxUint64-total < entry.Count {
			return fmt.Errorf("group %d has zero or overflowing population", group)
		}
		total += entry.Count
		if entry.Count < minimum {
			minimum = entry.Count
		}
		if entry.Count > maximum {
			maximum = entry.Count
		}
		previous = normalized
	}
	if total != catalog.data.AddressCount || maximum-minimum > 1 {
		return fmt.Errorf("catalog populations must sum to address_count and differ by at most one")
	}
	return nil
}

func LoadCatalog(path string) (*Catalog, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	catalog := &Catalog{}
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&catalog.data); err != nil {
		return nil, fmt.Errorf("decode GARET catalog: %w", err)
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("GARET catalog must contain one JSON object")
	}
	if err := catalog.validate(); err != nil {
		return nil, err
	}
	return catalog, nil
}

// SaveCatalog atomically replaces path after the complete catalog is written.
func SaveCatalog(path string, catalog *Catalog) error {
	if err := catalog.validate(); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".garet-catalog-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(catalog.data); err != nil {
		return err
	}
	if err := file.Chmod(0644); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
