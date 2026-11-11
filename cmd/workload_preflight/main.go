// workload_preflight checks a canonical workload without starting simulator
// nodes. Its stdout is one JSON report; nonzero exit means input is not cleared.
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"blockEmulator/workload"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("workload_preflight", flag.ContinueOnError)
	flags.SetOutput(stderr)
	transactions := flags.String("transactions", "", "canonical transactions CSV path")
	calls := flags.String("calls", "", "canonical contract calls CSV path")
	count := flags.Uint64("count", 0, "number of usable canonical transactions to check")
	limit := flags.Uint64("block-gas-limit", 0, "hard Gas limit of one block")
	callGas := flags.Uint64("cross-call-gas", 0, "Gas charged per cross-shard CCG call occurrence (zero is allowed)")
	requireCallGas := flags.Bool("require-call-gas", false, "require measured callGasUsed on every eligible internal call for JiangZiya placement")
	prefixCount := flags.Uint64("e2-prefix-count", 0, "write the first N accepted source hashes; requires --e2-prefix-file")
	prefixFile := flags.String("e2-prefix-file", "", "destination CSV for the E2 hash prefix; requires --e2-prefix-count")
	parseErr := flags.Parse(args)
	report := workload.PreflightReport{
		SchemaVersion: 1, RequestedTransactions: *count, BlockGasLimit: *limit,
		CrossCallGas: *callGas, Examples: make([]workload.PreflightExample, 0),
		RequireCallGas: *requireCallGas,
	}
	if parseErr == nil && flags.NArg() != 0 {
		parseErr = fmt.Errorf("unexpected positional arguments")
	}
	if parseErr == nil {
		provided := make(map[string]bool)
		flags.Visit(func(f *flag.Flag) { provided[f.Name] = true })
		for _, name := range []string{"transactions", "calls", "count", "block-gas-limit", "cross-call-gas"} {
			if !provided[name] {
				parseErr = fmt.Errorf("required flag --%s is missing", name)
				break
			}
		}
		if provided["e2-prefix-count"] || provided["e2-prefix-file"] {
			if *prefixCount == 0 || *prefixCount > *count || strings.TrimSpace(*prefixFile) == "" {
				parseErr = fmt.Errorf("E2 prefix requires a nonempty output file and a count in [1, --count]")
			}
		}
	}
	if parseErr == nil && (strings.TrimSpace(*transactions) == "" || strings.TrimSpace(*calls) == "") {
		parseErr = fmt.Errorf("transactions and calls paths must not be empty")
	}
	exitCode := 0
	if parseErr != nil {
		report.Error = parseErr.Error()
		exitCode = 2
	} else {
		var err error
		options := workload.PreflightOptions{
			Count: *count, BlockGasLimit: *limit, CrossCallGas: *callGas,
			RequireCallGas: *requireCallGas,
		}
		if *prefixCount == 0 {
			report, err = workload.PreflightFiles(*transactions, *calls, options)
		} else {
			report, err = preflightWithPrefix(*transactions, *calls, options, *prefixFile, *prefixCount, report)
		}
		if err != nil || !report.Valid {
			exitCode = 1
			if err != nil {
				report.Valid = false
				report.Error = err.Error()
			}
		}
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintf(stderr, "write preflight JSON: %v\n", err)
		return 2
	}
	return exitCode
}

// Publish a new prefix only after the entire workload, including all records
// after the prefix boundary, passes the regular feasibility checks.
func preflightWithPrefix(txs, calls string, options workload.PreflightOptions, output string, count uint64, report workload.PreflightReport) (workload.PreflightReport, error) {
	if existing, err := os.Stat(output); err == nil {
		for _, input := range []string{txs, calls} {
			if source, err := os.Stat(input); err == nil && os.SameFile(existing, source) {
				return report, fmt.Errorf("E2 prefix output must differ from workload input %s", input)
			}
		}
	}
	temp, err := os.CreateTemp(filepath.Dir(output), ".e2-prefix-*")
	if err != nil {
		return report, err
	}
	defer os.Remove(temp.Name())
	writer := csv.NewWriter(temp)
	if err := writer.Write([]string{"source_tx_hash"}); err != nil {
		temp.Close()
		return report, err
	}
	options.ObserveSource = func(number uint64, hash string) error {
		if number <= count {
			return writer.Write([]string{hash})
		}
		return nil
	}
	report, err = workload.PreflightFiles(txs, calls, options)
	writer.Flush()
	closeErr := temp.Close()
	if err != nil || !report.Valid {
		return report, err
	}
	if err := writer.Error(); err != nil {
		return report, err
	}
	if closeErr != nil {
		return report, closeErr
	}
	return report, os.Rename(temp.Name(), output)
}
