package committee

import (
	"blockEmulator/partition/txallo"
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// These are planning-graph counts, not execution TPS or Gas measurements.
type txalloContribMetricsWriter struct {
	roundFile *os.File
	roundCSV  *csv.Writer
	shardFile *os.File
	shardCSV  *csv.Writer
	closeOnce sync.Once
}

func newTxAlloContribMetricsWriter(resultRoot string) (*txalloContribMetricsWriter, error) {
	dir := filepath.Join(resultRoot, "supervisor_measureOutput")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	roundFile, err := os.Create(filepath.Join(dir, "TxAlloContrib_Round.csv"))
	if err != nil {
		return nil, err
	}
	shardFile, err := os.Create(filepath.Join(dir, "TxAlloContrib_Shard.csv"))
	if err != nil {
		roundFile.Close()
		return nil, err
	}
	w := &txalloContribMetricsWriter{roundFile: roundFile, roundCSV: csv.NewWriter(roundFile), shardFile: shardFile, shardCSV: csv.NewWriter(shardFile)}
	w.roundCSV.Write([]string{
		"Round", "GraphLogicalTxs", "GraphRelationships", "CommunityCount", "MovedAccounts", "RefinementPasses", "Beta",
		"BeforeCrossTxRate", "AfterCrossTxRate", "BeforeMaxModeledCountLoad", "AfterMaxModeledCountLoad", "BeforeCountLoadCV", "AfterCountLoadCV",
	})
	w.shardCSV.Write([]string{
		"Round", "Phase", "ShardID", "AccountCount", "InnerTransactions", "CrossIncidentTransactions", "ModeledCountLoad",
	})
	if err := w.flush(); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

func (w *txalloContribMetricsWriter) Write(round uint64, beta float64, result txallo.Result) error {
	if w == nil {
		return nil
	}
	w.roundCSV.Write([]string{
		strconv.FormatUint(round, 10), strconv.FormatUint(result.GraphTransactions, 10), strconv.Itoa(result.GraphRelationships),
		strconv.Itoa(result.CommunityCount), strconv.Itoa(result.MovedAccounts), strconv.Itoa(result.RefinementPasses), strconv.FormatFloat(beta, 'g', -1, 64),
		metricFloat(result.Before.CrossTransactionRate), metricFloat(result.After.CrossTransactionRate),
		metricFloat(result.Before.MaxModeledLoad), metricFloat(result.After.MaxModeledLoad),
		metricFloat(result.Before.ModeledLoadCV), metricFloat(result.After.ModeledLoadCV),
	})
	w.writeShards(round, "before", result.Before)
	w.writeShards(round, "after", result.After)
	return w.flush()
}

func (w *txalloContribMetricsWriter) writeShards(round uint64, phase string, metrics txallo.Metrics) {
	for _, load := range metrics.ShardLoads {
		w.shardCSV.Write([]string{
			strconv.FormatUint(round, 10), phase, strconv.FormatUint(load.ShardID, 10), strconv.Itoa(load.AccountCount),
			strconv.FormatUint(load.InnerTransactions, 10), strconv.FormatUint(load.CrossIncidentTransactions, 10), metricFloat(load.ModeledLoad),
		})
	}
}

func (w *txalloContribMetricsWriter) flush() error {
	w.roundCSV.Flush()
	w.shardCSV.Flush()
	if err := w.roundCSV.Error(); err != nil {
		return err
	}
	return w.shardCSV.Error()
}

func (w *txalloContribMetricsWriter) Close() error {
	if w == nil {
		return nil
	}
	var closeErr error
	w.closeOnce.Do(func() {
		closeErr = w.flush()
		for _, file := range []*os.File{w.roundFile, w.shardFile} {
			if err := file.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}
