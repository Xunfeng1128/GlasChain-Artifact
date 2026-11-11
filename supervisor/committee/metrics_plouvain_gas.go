package committee

import (
	"blockEmulator/partition/plouvain"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type plouvainGasMetricsWriter struct {
	roundFile     *os.File
	roundCSV      *csv.Writer
	shardFile     *os.File
	shardCSV      *csv.Writer
	communityFile *os.File
	communityCSV  *csv.Writer
	closeOnce     sync.Once
}

func newPLouvainGasMetricsWriter(resultRoot string) (*plouvainGasMetricsWriter, error) {
	dir := filepath.Join(resultRoot, "supervisor_measureOutput")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	roundFile, err := os.Create(filepath.Join(dir, "PLouvainGas_Round.csv"))
	if err != nil {
		return nil, err
	}
	shardFile, err := os.Create(filepath.Join(dir, "PLouvainGas_Shard.csv"))
	if err != nil {
		roundFile.Close()
		return nil, err
	}
	communityFile, err := os.Create(filepath.Join(dir, "PLouvainGas_Community.csv"))
	if err != nil {
		roundFile.Close()
		shardFile.Close()
		return nil, err
	}
	w := &plouvainGasMetricsWriter{
		roundFile: roundFile, roundCSV: csv.NewWriter(roundFile),
		shardFile: shardFile, shardCSV: csv.NewWriter(shardFile),
		communityFile: communityFile, communityCSV: csv.NewWriter(communityFile),
	}
	w.roundCSV.Write([]string{
		"Round", "GraphLogicalTxs", "GraphRelationships", "GraphGas", "CommunityCount", "LouvainLevels",
		"MoveOperations", "MovedAccounts", "RefinementPasses", "Beta", "BlockGasLimit",
		"BeforeCrossTxRate", "AfterCrossTxRate", "BeforeCrossGasRatio", "AfterCrossGasRatio",
		"BeforeCrossOverheadGas", "AfterCrossOverheadGas", "BeforeTotalPhysicalGas", "AfterTotalPhysicalGas",
		"BeforeMaxPhysicalGas", "AfterMaxPhysicalGas", "BeforeMaxModeledLoad", "AfterMaxModeledLoad",
		"BeforeMaxRequiredBlocks", "AfterMaxRequiredBlocks", "BeforePhysicalGasCV", "AfterPhysicalGasCV",
	})
	w.shardCSV.Write([]string{
		"Round", "Phase", "ShardID", "AccountCount", "InnerGas", "CrossIncidentGas", "ModeledLoad", "PhysicalGas", "RequiredBlocks",
	})
	w.communityCSV.Write([]string{
		"Round", "CommunityID", "AccountCount", "InternalGas", "ExternalGas", "AssignedShard",
	})
	if err := w.flush(); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

func (w *plouvainGasMetricsWriter) Write(round uint64, beta float64, blockGasLimit uint64, result plouvain.Result) error {
	if w == nil {
		return nil
	}
	w.roundCSV.Write([]string{
		strconv.FormatUint(round, 10), strconv.FormatUint(result.GraphTransactions, 10), strconv.Itoa(result.GraphRelationships), gasFloat(result.GraphGas),
		strconv.Itoa(len(result.Communities)), strconv.Itoa(result.LouvainLevels), strconv.Itoa(result.MoveOperations), strconv.Itoa(result.MovedAccounts),
		strconv.Itoa(result.RefinementPasses), strconv.FormatFloat(beta, 'g', -1, 64), strconv.FormatUint(blockGasLimit, 10),
		metricFloat(result.Before.CrossTransactionRate), metricFloat(result.After.CrossTransactionRate), metricFloat(result.Before.CrossGasRatio), metricFloat(result.After.CrossGasRatio),
		gasFloat(result.Before.CrossOverheadGas), gasFloat(result.After.CrossOverheadGas), gasFloat(result.Before.TotalPhysicalGas), gasFloat(result.After.TotalPhysicalGas),
		gasFloat(result.Before.MaxPhysicalGas), gasFloat(result.After.MaxPhysicalGas), gasFloat(result.Before.MaxModeledLoad), gasFloat(result.After.MaxModeledLoad),
		strconv.FormatUint(result.Before.MaxRequiredBlocks, 10), strconv.FormatUint(result.After.MaxRequiredBlocks, 10),
		metricFloat(result.Before.PhysicalGasCV), metricFloat(result.After.PhysicalGasCV),
	})
	w.writeShardMetrics(round, "before", result.Before)
	w.writeShardMetrics(round, "after", result.After)
	for _, community := range result.Communities {
		w.communityCSV.Write([]string{
			strconv.FormatUint(round, 10), strconv.FormatUint(community.CommunityID, 10), strconv.Itoa(community.AccountCount),
			gasFloat(community.InternalGas), gasFloat(community.ExternalGas), strconv.FormatUint(community.AssignedShard, 10),
		})
	}
	return w.flush()
}

func (w *plouvainGasMetricsWriter) writeShardMetrics(round uint64, phase string, metrics plouvain.Metrics) {
	for _, load := range metrics.ShardLoads {
		w.shardCSV.Write([]string{
			strconv.FormatUint(round, 10), phase, strconv.FormatUint(load.ShardID, 10), strconv.Itoa(load.AccountCount),
			gasFloat(load.InnerGas), gasFloat(load.CrossIncidentGas), gasFloat(load.ModeledLoad), gasFloat(load.PhysicalGas), strconv.FormatUint(load.RequiredBlocks, 10),
		})
	}
}

func (w *plouvainGasMetricsWriter) flush() error {
	w.roundCSV.Flush()
	w.shardCSV.Flush()
	w.communityCSV.Flush()
	if err := w.roundCSV.Error(); err != nil {
		return err
	}
	if err := w.shardCSV.Error(); err != nil {
		return err
	}
	return w.communityCSV.Error()
}

func (w *plouvainGasMetricsWriter) Close() error {
	if w == nil {
		return nil
	}
	var closeErr error
	w.closeOnce.Do(func() {
		if err := w.flush(); err != nil {
			closeErr = err
		}
		for _, file := range []*os.File{w.roundFile, w.shardFile, w.communityFile} {
			if err := file.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}

func gasFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 0, 64)
}

func metricFloat(value float64) string {
	return fmt.Sprintf("%.9f", value)
}
