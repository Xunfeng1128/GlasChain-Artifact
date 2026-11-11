package planner

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"blockEmulator/glasfreight/model"
)

func clonePlacement(source model.Placement) model.Placement {
	clone := make(model.Placement, len(source))
	for address, shard := range source {
		clone[address] = shard
	}
	return clone
}

func resolveAddress(placement model.Placement, address string, shardCount uint64, resolver PlacementResolver) (uint64, error) {
	if shard, exists := placement[address]; exists {
		if shard >= shardCount {
			return 0, fmt.Errorf("address %s has invalid shard %d", address, shard)
		}
		return shard, nil
	}
	if resolver == nil {
		return 0, fmt.Errorf("placement is missing address %s", address)
	}
	shard, err := resolver(address)
	if err != nil {
		return 0, fmt.Errorf("resolve shard for %s: %w", address, err)
	}
	if shard >= shardCount {
		return 0, fmt.Errorf("resolver returned invalid shard %d for %s", shard, address)
	}
	return shard, nil
}

func validateFrozenPools(pools FrozenPools, shardCount uint64) error {
	if shardCount == 0 || pools.ShardCount == 0 || pools.ShardCount != shardCount {
		return fmt.Errorf("snapshot shard count %d does not match planner shard count %d", pools.ShardCount, shardCount)
	}
	seen := make(map[string]bool)
	for shard, records := range pools.Records {
		if shard >= shardCount {
			return fmt.Errorf("snapshot contains invalid shard %d", shard)
		}
		for _, record := range records {
			if err := record.validate(); err != nil {
				return err
			}
			if seen[record.ID] {
				return fmt.Errorf("duplicate execution record ID %s", record.ID)
			}
			seen[record.ID] = true
		}
	}
	return nil
}

func prepareRecord(record FrozenRecord, placement model.Placement, config SimulationConfig) (FrozenRecord, error) {
	if config.Prepare == nil {
		return record, nil
	}
	prepared, err := config.Prepare(record.clone(), placement)
	if err != nil {
		return FrozenRecord{}, err
	}
	if prepared.ID != record.ID || prepared.SourceHash != record.SourceHash || prepared.Stage != record.Stage ||
		prepared.PlacementOwner() != record.PlacementOwner() || prepared.DependencyKey != record.DependencyKey {
		return FrozenRecord{}, fmt.Errorf("pricing changed stage identity/owner/dependency for %s", record.ID)
	}
	if err := prepared.validate(); err != nil {
		return FrozenRecord{}, err
	}
	if prepared.AttachedCallGas > prepared.GasUsed || (config.BlockGasLimit > 0 && prepared.BaseGasUsed > config.BlockGasLimit) {
		return FrozenRecord{}, fmt.Errorf("priced stage %s gas=%d exceeds capacity=%d or has invalid surcharge", record.ID, prepared.GasUsed, config.BlockGasLimit)
	}
	return prepared, nil
}

func placementDigest(placement model.Placement) string {
	addresses := make([]string, 0, len(placement))
	for address := range placement {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	parts := make([]string, 0, len(addresses))
	for _, address := range addresses {
		parts = append(parts, address+"="+strconv.FormatUint(placement[address], 10))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])
}
